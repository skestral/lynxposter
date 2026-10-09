from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from html import unescape
from io import BytesIO
from types import SimpleNamespace

import pytest
from atproto_client.models.blob_ref import BlobRef
from PIL import Image

from app.adapters import get_service_definition
from app.adapters.common import service_attachments
from app.adapters.mastodon import MastodonDestinationAdapter
from app.domain import PublishResult
from app.models import AccountPostRef, CanonicalPost
from app.services.alerts import AlertDispatcher
from app.services.delivery import enqueue_due_scheduled_posts, process_delivery_queue
from app.services.posts import get_post
from test_scheduled_post_api import api_stack, _create_destination_account, _create_persona


def _create_targets(SessionLocal, *, with_mastodon=False):
    with SessionLocal() as session:
        persona = _create_persona(session, slug="bluesky-gallery-flows")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        mastodon = _create_destination_account(session, persona) if with_mastodon else None
        session.commit()
        return persona.id, bluesky.id, mastodon.id if mastodon else None


def _create_uploads(client, persona_id, target_ids, count, *, status="draft"):
    files = []
    for index in range(count):
        output = BytesIO()
        Image.new("RGB", (40 + index, 20), (index * 17 % 256, 80, 120)).save(output, format="PNG")
        files.append(("uploads", (f"image-{index}.png", output.getvalue(), "image/png")))
    return client.post("/scheduled-posts", data={
        "persona_id": persona_id,
        "body": "API gallery",
        "status": status,
        "target_account_ids": json.dumps(target_ids),
        "publish_overrides_json": "{}",
        "metadata_json": "{}",
        "alt_texts": json.dumps([f"Alt {index}" for index in range(count)]),
    }, files=files)


def _fake_bluesky_client(monkeypatch):
    captured = SimpleNamespace(uploads=[], posts=[])

    def upload_blob(data):
        captured.uploads.append(data)
        return SimpleNamespace(blob=BlobRef(
            mime_type="image/png", size=len(data),
            ref={"$link": "bafkreigh2akiscaildc5eymqv7y6ygv4c2uowuuvjxyfu7gi27oa2ouwke"},
        ))

    def send_post(**kwargs):
        captured.posts.append(kwargs)
        return SimpleNamespace(uri="at://did:plc:test/app.bsky.feed.post/gallery-api")

    monkeypatch.setattr("app.adapters.bluesky._get_client", lambda *args, **kwargs: SimpleNamespace(
        upload_blob=upload_blob, send_post=send_post,
    ))
    return captured


@pytest.mark.parametrize("delivery_path", ["due_scheduler", "api_send_now"])
def test_ten_image_api_create_edit_preserves_order_through_delivery(api_stack, monkeypatch, delivery_path):
    client, SessionLocal = api_stack
    persona_id, bluesky_id, _ = _create_targets(SessionLocal)
    captured = _fake_bluesky_client(monkeypatch)
    created = _create_uploads(client, persona_id, [bluesky_id], 10)
    assert created.status_code == 200
    payload = created.json()
    assert [item["alt_text"] for item in payload["attachments"]] == [f"Alt {index}" for index in range(10)]
    ordered_ids = [item["id"] for item in reversed(payload["attachments"])]
    post_id = payload["id"]

    edited = client.put(f"/scheduled-posts/{post_id}", json={
        "body": "Reordered gallery",
        "attachment_order": ordered_ids,
        "status": "scheduled" if delivery_path == "due_scheduler" else "draft",
        "scheduled_for": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
    })
    assert edited.status_code == 200
    assert [item["id"] for item in edited.json()["attachments"]] == ordered_ids
    assert captured.posts == []

    if delivery_path == "due_scheduler":
        with SessionLocal() as session:
            enqueue_due_scheduled_posts(session, run_id="ten-image-due")
            process_delivery_queue(session, AlertDispatcher(), run_id="ten-image-due", post_id=post_id)
            session.commit()
    else:
        sent = client.post(f"/scheduled-posts/{post_id}/send-now")
        assert sent.status_code == 200

    assert len(captured.posts) == 1
    assert captured.posts[0]["text"] == "Reordered gallery"
    gallery = captured.posts[0]["embed"]
    assert gallery.py_type == "app.bsky.embed.gallery"
    assert [item.alt for item in gallery.items] == [f"Alt {index}" for index in reversed(range(10))]
    for index, data in zip(reversed(range(10)), captured.uploads, strict=True):
        with Image.open(BytesIO(data)) as image:
            assert image.size == (40 + index, 20)
    with SessionLocal() as session:
        saved = get_post(session, post_id)
        assert saved.status == "posted"
        assert saved.delivery_jobs[0].status == "posted"
        assert len(saved.delivery_jobs[0].attempts) == 1
        assert session.query(AccountPostRef).filter_by(post_id=post_id, account_id=bluesky_id).one().external_id == "gallery-api"


def test_api_rejects_queued_eleven_image_gallery_before_delivery(api_stack, monkeypatch):
    client, SessionLocal = api_stack
    persona_id, bluesky_id, _ = _create_targets(SessionLocal)
    captured = _fake_bluesky_client(monkeypatch)
    response = _create_uploads(client, persona_id, [bluesky_id], 11, status="queued")
    assert response.status_code == 400
    assert "10 images" in response.json()["detail"]
    assert captured.uploads == captured.posts == []
    with SessionLocal() as session:
        assert session.query(CanonicalPost).count() == 0


def test_api_destination_subsets_allow_ten_image_bluesky_and_four_image_mastodon(api_stack, monkeypatch):
    client, SessionLocal = api_stack
    persona_id, bluesky_id, mastodon_id = _create_targets(SessionLocal, with_mastodon=True)
    captured = _fake_bluesky_client(monkeypatch)
    created = _create_uploads(client, persona_id, [bluesky_id, mastodon_id], 11)
    assert created.status_code == 200
    post_id = created.json()["id"]
    attachment_ids = [item["id"] for item in created.json()["attachments"]]
    # Drafts retain unsupported source sets; both promotion paths must validate
    # the media selected for each destination before any client is called.
    sent = client.post(f"/scheduled-posts/{post_id}/send-now")
    assert sent.status_code == 400
    assert "10 images" in sent.json()["detail"]
    edited_invalid = client.put(f"/scheduled-posts/{post_id}", json={"status": "queued"})
    assert edited_invalid.status_code == 400
    assert captured.posts == []
    with SessionLocal() as session:
        assert get_post(session, post_id).status == "draft"

    bluesky_order = list(reversed(attachment_ids[1:]))
    mastodon_order = [attachment_ids[index] for index in [0, 10, 4, 2]]
    selected = client.put(f"/scheduled-posts/{post_id}", json={
        "status": "queued",
        "publish_overrides_json": {
            bluesky_id: {"attachment_ids": bluesky_order},
            mastodon_id: {"attachment_ids": mastodon_order},
        },
    })
    assert selected.status_code == 200
    assert len(selected.json()["attachments"]) == 11
    assert selected.json()["last_error"] is None

    mastodon_uploads = []

    class FakeMastodon(MastodonDestinationAdapter):
        def publish(self, session, post, persona, account, *, context=None):
            mastodon_uploads.extend(item.id for item in service_attachments(post, account))
            return PublishResult(service="mastodon", external_id="masto-gallery", external_url="https://example.social/@me/masto-gallery")

    from app.services import delivery
    original_factory = delivery.get_destination_adapter_for_account
    monkeypatch.setattr(delivery, "get_destination_adapter_for_account", lambda account: FakeMastodon() if account.service == "mastodon" else original_factory(account))
    with SessionLocal() as session:
        process_delivery_queue(session, AlertDispatcher(), post_id=post_id)
        session.commit()
        assert {job.status for job in get_post(session, post_id).delivery_jobs} == {"posted"}

    assert mastodon_uploads == mastodon_order
    assert len(captured.posts) == 1
    assert [item.alt for item in captured.posts[0]["embed"].items] == [f"Alt {index}" for index in range(10, 0, -1)]
    assert len(captured.uploads) == 10


def test_persona_page_displays_bluesky_limits_for_existing_and_new_connections(api_stack):
    client, SessionLocal = api_stack
    persona_id, bluesky_id, _ = _create_targets(SessionLocal)
    response = client.get(f"/personas/{persona_id}/page")
    assert response.status_code == 200
    html = unescape(response.text)
    create_form = html.split('<form id="account-create-form"', 1)[1].split("</form>", 1)[0]
    existing_form = html.split(f'data-account-id="{bluesky_id}"', 1)[1].split("</form>", 1)[0]
    assert 'data-service="bluesky"' in create_form
    assert "Publishing limits" in create_form
    assert "Publishing" in existing_form
    for note in get_service_definition("bluesky").composer_constraints.notes:
        assert note in create_form
        assert note in existing_form
