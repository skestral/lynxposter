from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from io import BytesIO
from types import SimpleNamespace

import pytest

from pathlib import Path

from starlette.datastructures import FormData, UploadFile

from app.main import _read_scheduled_post_payload
from app.services.auth import Principal
from app.domain import CanonicalPostPayload, ExternalPostRefPayload, MediaItem
from app.models import CanonicalPost, DeliveryJob, MediaAttachment
from app.schemas import ScheduledPostCreate, ScheduledPostUpdate
from app.services.personas import create_account, create_persona, get_persona, replace_routes
from app.services.storage import settings as storage_settings
from app.services.posts import (
    create_scheduled_post,
    delete_scheduled_post,
    reconcile_pending_relationships,
    schedule_post_now,
    update_scheduled_post,
    upsert_polled_post,
)


def _create_persona(session, *, slug: str = "posts-persona", max_retries: int = 3):
    return create_persona(
        session,
        {
            "name": "Posts Persona",
            "slug": slug,
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": max_retries},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def _create_account(session, persona, *, service: str, label: str, source_enabled: bool, destination_enabled: bool):
    default_credentials = {
        "mastodon": {"instance": "https://example.social", "token": "secret", "handle": "@me@example.social"},
        "discord": {"webhook_url": "https://discord.test"},
        "bluesky": {"handle": "me.bsky.social", "password": "pw"},
    }
    return create_account(
        session,
        persona,
        {
            "service": service,
            "label": label,
            "handle_or_identifier": label,
            "is_enabled": True,
            "source_enabled": source_enabled,
            "destination_enabled": destination_enabled,
            "credentials_json": default_credentials[service],
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


def test_update_scheduled_post_retargets_jobs_and_cancels_removed_destinations(session):
    persona = _create_persona(session)
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)
    discord = _create_account(session, persona, service="discord", label="Discord", source_enabled=False, destination_enabled=True)

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello world",
                "status": "draft",
                "target_account_ids": [mastodon.id, discord.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    updated = update_scheduled_post(
        session,
        post,
        ScheduledPostUpdate.model_validate(
            {
                "target_account_ids": [discord.id],
                "status": "queued",
            }
        ),
    )

    jobs = {job.target_account_id: job for job in updated.delivery_jobs}
    assert jobs[discord.id].status == "queued"
    assert jobs[mastodon.id].status == "cancelled"
    assert updated.status == "queued"


def test_schedule_post_now_sets_queue_state_and_timestamp(session):
    persona = _create_persona(session, slug="schedule-now")
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Queue me",
                "status": "draft",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    queued = schedule_post_now(session, post)

    assert queued.status == "queued"
    assert queued.scheduled_for is not None
    assert queued.delivery_jobs[0].status == "queued"


def test_create_scheduled_post_persists_media_attachments(session, tmp_path):
    persona = _create_persona(session, slug="attachments")
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)

    file_path = tmp_path / "cat.jpg"
    file_path.write_bytes(b"fake-jpeg")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "With image",
                "status": "draft",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(
                storage_path=Path(file_path),
                mime_type="image/jpeg",
                alt_text="Cat photo",
                size_bytes=file_path.stat().st_size,
                checksum="abc123",
                sort_order=0,
            )
        ],
    )

    assert len(post.attachments) == 1
    assert post.attachments[0].mime_type == "image/jpeg"
    assert post.attachments[0].alt_text == "Cat photo"
    assert post.attachments[0].storage_path.endswith("cat.jpg")


def test_update_scheduled_post_appends_media_attachments(session, tmp_path):
    persona = _create_persona(session, slug="attachments-update")
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)

    first_path = tmp_path / "cat.jpg"
    second_path = tmp_path / "dog.jpg"
    first_path.write_bytes(b"first-jpeg")
    second_path.write_bytes(b"second-jpeg")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "With images",
                "status": "draft",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(
                storage_path=Path(first_path),
                mime_type="image/jpeg",
                alt_text="Cat photo",
                size_bytes=first_path.stat().st_size,
                checksum="abc123",
                sort_order=0,
            )
        ],
    )

    updated = update_scheduled_post(
        session,
        post,
        ScheduledPostUpdate.model_validate({"body": "With more images"}),
        [
            MediaItem(
                storage_path=Path(second_path),
                mime_type="image/jpeg",
                alt_text="Dog photo",
                size_bytes=second_path.stat().st_size,
                checksum="def456",
                sort_order=0,
            )
        ],
    )

    assert len(updated.attachments) == 2
    assert [attachment.sort_order for attachment in updated.attachments] == [0, 1]
    assert updated.attachments[1].alt_text == "Dog photo"
    assert updated.attachments[1].storage_path.endswith("dog.jpg")


def test_delete_scheduled_post_removes_draft_and_children(session, tmp_path, monkeypatch):
    persona = _create_persona(session, slug="attachments-delete")
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)

    uploads_dir = tmp_path / "uploads"
    uploads_dir.mkdir()
    file_path = uploads_dir / "cat.jpg"
    file_path.write_bytes(b"fake-jpeg")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Delete me",
                "status": "draft",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(
                storage_path=Path(file_path),
                mime_type="image/jpeg",
                alt_text="Cat photo",
                size_bytes=file_path.stat().st_size,
                checksum="abc123",
                sort_order=0,
            )
        ],
    )

    monkeypatch.setattr(
        "app.services.storage.settings",
        replace(storage_settings, uploads_dir=uploads_dir, imported_media_dir=tmp_path / "imported"),
    )

    delete_scheduled_post(session, post)

    assert session.get(CanonicalPost, post.id) is None
    assert session.query(MediaAttachment).count() == 0
    assert session.query(DeliveryJob).count() == 0
    assert not file_path.exists()


def test_delete_scheduled_post_rejects_non_draft(session):
    persona = _create_persona(session, slug="attachments-delete-queued")
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Queued post",
                "status": "scheduled",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": datetime.now(timezone.utc),
            }
        ),
        [],
    )

    with pytest.raises(ValueError, match="Only draft scheduled posts can be deleted."):
        delete_scheduled_post(session, post)

    assert session.get(CanonicalPost, post.id) is not None


def test_read_scheduled_post_payload_collects_starlette_uploads(monkeypatch):
    upload = UploadFile(filename="cat.jpg", file=BytesIO(b"jpeg-bytes"), headers={"content-type": "image/jpeg"})
    form = FormData(
        [
            ("body", "Hello"),
            ("status", "draft"),
            ("target_account_ids", "[]"),
            ("publish_overrides_json", "{}"),
            ("metadata_json", "{}"),
            ("scheduled_for", ""),
            ("alt_texts", "[]"),
            ("uploads", upload),
        ]
    )

    class _DummyRequest:
        headers = {"content-type": "multipart/form-data; boundary=test"}
        state = SimpleNamespace()

        async def form(self):
            return form

    monkeypatch.setattr(
        "app.main.get_request_principal",
        lambda request: Principal(
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
            is_authenticated=True,
        ),
    )

    payload, uploads, alt_texts = asyncio.run(_read_scheduled_post_payload(_DummyRequest()))

    assert payload["body"] == "Hello"
    assert len(uploads) == 1
    assert uploads[0].filename == "cat.jpg"
    assert alt_texts == []


def test_schedule_post_now_requires_at_least_one_destination(session):
    persona = _create_persona(session, slug="schedule-now-empty")
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Queue me",
                "status": "draft",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    post.delivery_jobs[0].status = "cancelled"
    mastodon.destination_enabled = False
    session.flush()

    with pytest.raises(ValueError, match="Select at least one destination account"):
        schedule_post_now(session, post)


def test_reconcile_pending_relationships_links_reply_and_queues_delivery(session):
    persona = _create_persona(session, slug="reply-chain")
    source = _create_account(session, persona, service="bluesky", label="Bluesky", source_enabled=True, destination_enabled=True)
    destination = _create_account(session, persona, service="discord", label="Discord", source_enabled=False, destination_enabled=True)

    replace_routes(
        session,
        get_persona(session, persona.id),
        [{"source_account_id": source.id, "destination_account_id": destination.id, "is_enabled": True}],
    )

    imported_parent = upsert_polled_post(
        session,
        get_persona(session, persona.id),
        source,
        CanonicalPostPayload(
            body="Imported parent",
            external_refs=[ExternalPostRefPayload(external_id="source-parent", external_url="https://bsky.app/post/source-parent")],
            published_at=datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc),
        ),
    )

    child = upsert_polled_post(
        session,
        get_persona(session, persona.id),
        source,
        CanonicalPostPayload(
            body="Imported reply",
            external_refs=[ExternalPostRefPayload(external_id="source-child", external_url="https://bsky.app/post/source-child")],
            reply_to_external=ExternalPostRefPayload(external_id="source-parent"),
            published_at=datetime(2026, 4, 15, 12, 5, tzinfo=timezone.utc),
        ),
    )

    assert imported_parent.id != child.id
    assert child.reply_to_post_id == imported_parent.id
    assert any(job.target_account_id == destination.id and job.status == "queued" for job in child.delivery_jobs)


def test_reconcile_pending_relationships_resolves_late_arriving_reply_target(session):
    persona = _create_persona(session, slug="late-reply")
    source = _create_account(session, persona, service="bluesky", label="Bluesky", source_enabled=True, destination_enabled=True)
    destination = _create_account(session, persona, service="discord", label="Discord", source_enabled=False, destination_enabled=True)

    replace_routes(
        session,
        get_persona(session, persona.id),
        [{"source_account_id": source.id, "destination_account_id": destination.id, "is_enabled": True}],
    )

    child = upsert_polled_post(
        session,
        get_persona(session, persona.id),
        source,
        CanonicalPostPayload(
            body="Imported reply first",
            external_refs=[ExternalPostRefPayload(external_id="source-child", external_url="https://bsky.app/post/source-child")],
            reply_to_external=ExternalPostRefPayload(external_id="source-parent"),
            published_at=datetime(2026, 4, 15, 12, 5, tzinfo=timezone.utc),
        ),
    )

    assert child.reply_to_post_id is None
    assert child.delivery_jobs == []

    parent = upsert_polled_post(
        session,
        get_persona(session, persona.id),
        source,
        CanonicalPostPayload(
            body="Imported parent later",
            external_refs=[ExternalPostRefPayload(external_id="source-parent", external_url="https://bsky.app/post/source-parent")],
            published_at=datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc),
        ),
    )

    changed = reconcile_pending_relationships(session, child)

    assert changed is True
    assert child.reply_to_post_id == parent.id
    assert any(job.target_account_id == destination.id and job.status == "queued" for job in child.delivery_jobs)
