from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest
from PIL import Image

from app.models import AccountPostRef
from app.services.alerts import AlertDispatcher
from app.services.delivery import process_delivery_queue
from app.services.delivery_lock import publishing_guard
from app.services.posts import get_post
from app.services import storage
from test_scheduled_post_api import api_stack, _create_destination_account, _create_persona


def _png(color, size=(32, 24)):
    output = BytesIO()
    Image.new("RGB", size, color).save(output, format="PNG")
    return output.getvalue()


@pytest.fixture()
def partial_failure(api_stack, monkeypatch, tmp_path):
    client, SessionLocal = api_stack
    uploads_dir = tmp_path / "uploads"
    monkeypatch.setattr(storage, "settings", replace(storage.settings, uploads_dir=uploads_dir, imported_media_dir=tmp_path / "imported"))
    with SessionLocal() as session:
        persona = _create_persona(session, slug="failed-post-recovery")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        mastodon = _create_destination_account(session, persona, service="mastodon", label="Mastodon")
        session.commit()
        persona_id, bluesky_id, mastodon_id = persona.id, bluesky.id, mastodon.id

    state = SimpleNamespace(
        client=client, SessionLocal=SessionLocal, persona_id=persona_id,
        bluesky_id=bluesky_id, mastodon_id=mastodon_id, uploads_dir=uploads_dir,
        phase="invalid_image", transient_left=0, bluesky_calls=[], mastodon_uploads=[], mastodon_posts=[],
    )

    def send_images(body, images, alts, **kwargs):
        state.bluesky_calls.append((body, list(images), list(alts)))
        return SimpleNamespace(uri="at://did:plc:test/app.bsky.feed.post/already-published")

    class FakeMastodonClient:
        def instance(self):
            return {}

        def media_post(self, media_path, **kwargs):
            with Image.open(media_path) as image:
                state.mastodon_uploads.append((image.size, image.getpixel((0, 0)), kwargs.get("description")))
            if state.phase == "invalid_image":
                raise ValueError("Destination rejected the image as invalid.")
            if state.transient_left:
                state.transient_left -= 1
                raise RuntimeError("Temporary provider failure after the correction.")
            return {"id": "replacement-media", "url": "https://example.social/media/replacement"}

        def status_post(self, body, **kwargs):
            state.mastodon_posts.append((body, kwargs))
            return {"id": "recovered-post", "url": "https://example.social/@me/recovered-post"}

    monkeypatch.setattr("app.adapters.bluesky._get_client", lambda *args, **kwargs: SimpleNamespace(send_images=send_images))
    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda *args, **kwargs: FakeMastodonClient())
    return state


def _exhaust_original_delivery(state, *, custom_success_override=False, custom_failure_override=False):
    response = state.client.post("/scheduled-posts", data={
        "persona_id": state.persona_id, "body": "Original caption", "status": "draft",
        "target_account_ids": json.dumps([state.bluesky_id, state.mastodon_id]),
        "publish_overrides_json": "{}", "metadata_json": "{}", "alt_texts": '["Original image"]',
    }, files=[("uploads", ("original.png", _png("red"), "image/png"))])
    assert response.status_code == 200
    state.post_id = response.json()["id"]
    state.original_id = response.json()["attachments"][0]["id"]
    state.original_overrides = {
        state.bluesky_id: {"body": "Custom published caption", "attachment_ids": [state.original_id]},
    } if custom_success_override else {}
    if custom_failure_override:
        state.original_overrides[state.mastodon_id] = {
            "body": "Original destination caption", "attachment_ids": [state.original_id],
        }
    queued = state.client.put(f"/scheduled-posts/{state.post_id}", json={
        "status": "queued", "publish_overrides_json": state.original_overrides,
    })
    assert queued.status_code == 200
    for cycle in range(3):
        with state.SessionLocal() as session:
            process_delivery_queue(session, AlertDispatcher(), post_id=state.post_id, run_id=f"initial-{cycle}")
            session.commit()
    with state.SessionLocal() as session:
        post = get_post(session, state.post_id)
        jobs = {job.target_account_id: job for job in post.delivery_jobs}
        assert post.status == "failed"
        assert jobs[state.mastodon_id].status == "failed"
        assert jobs[state.mastodon_id].attempt_count == jobs[state.mastodon_id].max_retries == 3
        state.failed_attempt_ids = {attempt.id for attempt in jobs[state.mastodon_id].attempts}
        successful = jobs[state.bluesky_id]
        state.success_snapshot = (successful.id, successful.external_id, successful.external_url, successful.delivered_at, successful.attempt_count)
        state.success_attempt_ids = {attempt.id for attempt in successful.attempts}
        ref = session.query(AccountPostRef).filter_by(post_id=post.id, account_id=state.bluesky_id).one()
        state.success_ref_snapshot = (ref.id, ref.external_id, ref.external_url)
        state.original_path = Path(post.attachments[0].storage_path)
        state.original_bytes = state.original_path.read_bytes()
        # Successful destinations stay locked even after connection settings change.
        successful.target_account.is_enabled = False
        successful.target_account.credentials_json = {}
        session.commit()
    assert len(state.bluesky_calls) == 1


def _assert_success_unchanged(state, session):
    post = get_post(session, state.post_id)
    successful = next(job for job in post.delivery_jobs if job.target_account_id == state.bluesky_id)
    assert (successful.id, successful.external_id, successful.external_url, successful.delivered_at, successful.attempt_count) == state.success_snapshot
    assert {attempt.id for attempt in successful.attempts} == state.success_attempt_ids
    ref = session.query(AccountPostRef).filter_by(post_id=post.id, account_id=state.bluesky_id).one()
    assert (ref.id, ref.external_id, ref.external_url) == state.success_ref_snapshot
    assert len(state.bluesky_calls) == 1
    return post


@pytest.mark.parametrize("custom_success_override,custom_failure_override", [(False, False), (True, False), (True, True)])
def test_atomic_correct_image_and_caption_retries_only_failed_destination(partial_failure, custom_success_override, custom_failure_override):
    state = partial_failure
    _exhaust_original_delivery(
        state, custom_success_override=custom_success_override, custom_failure_override=custom_failure_override,
    )
    state.phase = "corrected"
    state.transient_left = 1
    corrected_overrides = deepcopy(state.original_overrides)
    if custom_failure_override:
        corrected_overrides[state.mastodon_id]["body"] = "Corrected destination caption"
    response = state.client.put(f"/scheduled-posts/{state.post_id}?send_now=true", data={
        "body": "Corrected caption", "status": "failed",
        "target_account_ids": json.dumps([state.bluesky_id, state.mastodon_id]),
        "publish_overrides_json": json.dumps(corrected_overrides), "metadata_json": "{}",
        "deleted_attachment_ids": json.dumps([state.original_id]), "alt_texts": '["Replacement image"]',
    }, files=[("uploads", ("replacement.png", _png("blue", (48, 36)), "image/png"))])
    assert response.status_code == 200, response.text
    # API returns the committed plan before its background retry starts.
    assert response.json()["deliveries"][state.mastodon_id]["attempt_count"] == 0
    assert response.json()["deliveries"][state.mastodon_id]["last_error"] is None
    assert [item["alt_text"] for item in response.json()["attachments"]] == ["Replacement image"]
    with state.SessionLocal() as session:
        post = _assert_success_unchanged(state, session)
        assert post.body == "Corrected caption"
        assert post.publish_overrides_json[state.bluesky_id]["body"] == ("Custom published caption" if custom_success_override else "Original caption")
        failed = next(job for job in post.delivery_jobs if job.target_account_id == state.mastodon_id)
        assert failed.status == "queued"
        assert failed.attempt_count == 1
        assert state.failed_attempt_ids < {attempt.id for attempt in failed.attempts}
        assert len(failed.attempts) == 4
        assert len(post.attachments) == 1
        assert post.attachments[0].id != state.original_id
        if custom_failure_override:
            # A replacement inherits the failed target's explicit selection of the deleted image.
            assert post.publish_overrides_json[state.mastodon_id] == {
                "body": "Corrected destination caption", "attachment_ids": [post.attachments[0].id],
            }
    assert not state.original_path.exists()

    with state.SessionLocal() as session:
        process_delivery_queue(session, AlertDispatcher(), post_id=state.post_id, run_id="retry-after-transient")
        session.commit()
        post = _assert_success_unchanged(state, session)
        recovered = next(job for job in post.delivery_jobs if job.target_account_id == state.mastodon_id)
        assert post.status == recovered.status == "posted"
        assert recovered.attempt_count == 2
        assert len(recovered.attempts) == 5
        assert recovered.last_error is None
        assert session.query(AccountPostRef).filter_by(post_id=post.id, account_id=state.mastodon_id).one().external_id == "recovered-post"
    assert state.mastodon_uploads[-2:] == [((48, 36), (0, 0, 255), "Replacement image")] * 2
    assert len(state.mastodon_posts) == 1
    assert state.mastodon_posts[0][0] == ("Corrected destination caption" if custom_failure_override else "Corrected caption")


def test_explicit_send_now_resets_exhausted_failure_budget_without_republishing_success(partial_failure):
    state = partial_failure
    _exhaust_original_delivery(state)
    response = state.client.post(f"/scheduled-posts/{state.post_id}/send-now")
    assert response.status_code == 200
    assert response.json()["deliveries"][state.mastodon_id]["attempt_count"] == 0
    assert response.json()["deliveries"][state.mastodon_id]["last_error"] is None
    with state.SessionLocal() as session:
        post = _assert_success_unchanged(state, session)
        failed = next(job for job in post.delivery_jobs if job.target_account_id == state.mastodon_id)
        assert failed.status == "queued"
        assert failed.attempt_count == 1
        assert len(failed.attempts) == 4


def test_rejected_atomic_edit_rolls_back_plan_preserves_original_and_removes_new_upload(partial_failure):
    state = partial_failure
    _exhaust_original_delivery(state, custom_success_override=True)
    existing_files = {path.name for path in state.uploads_dir.iterdir()}
    response = state.client.put(f"/scheduled-posts/{state.post_id}?send_now=true", data={
        "body": "x" * 501, "status": "failed",
        "target_account_ids": json.dumps([state.bluesky_id, state.mastodon_id]),
        "publish_overrides_json": json.dumps(state.original_overrides), "metadata_json": "{}",
        "deleted_attachment_ids": json.dumps([state.original_id]), "alt_texts": '["Rejected replacement"]',
    }, files=[("uploads", ("rejected.png", _png("green"), "image/png"))])
    assert response.status_code == 400
    assert "500 characters" in response.json()["detail"]
    assert {path.name for path in state.uploads_dir.iterdir()} == existing_files
    assert state.original_path.read_bytes() == state.original_bytes
    with state.SessionLocal() as session:
        post = _assert_success_unchanged(state, session)
        assert post.body == "Original caption"
        assert post.status == "failed"
        assert post.publish_overrides_json == state.original_overrides
        assert [attachment.id for attachment in post.attachments] == [state.original_id]
        failed = next(job for job in post.delivery_jobs if job.target_account_id == state.mastodon_id)
        assert failed.status == "failed"
        assert failed.attempt_count == 3
        assert {attempt.id for attempt in failed.attempts} == state.failed_attempt_ids
    assert len(state.mastodon_uploads) == 3
    assert state.mastodon_posts == []


@contextmanager
def _publishing_in_other_thread():
    started = Event()
    release = Event()

    def hold_guard():
        with publishing_guard():
            started.set()
            assert release.wait(timeout=15)

    with ThreadPoolExecutor(max_workers=1) as executor:
        task = executor.submit(hold_guard)
        try:
            assert started.wait(timeout=5)
            yield
        finally:
            release.set()
        task.result(timeout=5)


def _request_recovery_action(state, action):
    if action == "send_now":
        return state.client.post(f"/scheduled-posts/{state.post_id}/send-now")
    suffix = "?send_now=true" if action == "atomic_edit" else ""
    return state.client.put(f"/scheduled-posts/{state.post_id}{suffix}", data={
        "body": "Concurrent correction", "status": "draft",
        "target_account_ids": json.dumps([state.bluesky_id, state.mastodon_id]),
        "publish_overrides_json": json.dumps(state.original_overrides), "metadata_json": "{}",
        "deleted_attachment_ids": json.dumps([state.original_id]), "alt_texts": '["Concurrent replacement"]',
    }, files=[("uploads", ("concurrent.png", _png("green"), "image/png"))])


def _assert_rejected_recovery_unchanged(state, *, post_status="failed", job_status="failed"):
    assert state.original_path.read_bytes() == state.original_bytes
    with state.SessionLocal() as session:
        post = _assert_success_unchanged(state, session)
        assert post.body == "Original caption"
        assert post.status == post_status
        assert post.publish_overrides_json == state.original_overrides
        assert [attachment.id for attachment in post.attachments] == [state.original_id]
        failed = next(job for job in post.delivery_jobs if job.target_account_id == state.mastodon_id)
        assert failed.status == job_status
        assert failed.attempt_count == 3
        assert {attempt.id for attempt in failed.attempts} == state.failed_attempt_ids
    assert len(state.mastodon_uploads) == 3
    assert state.mastodon_posts == []


@pytest.mark.parametrize("action", ["edit", "atomic_edit", "send_now"])
def test_recovery_api_rejects_concurrent_publish_and_cleans_replacement(partial_failure, monkeypatch, action):
    from app.main import store_upload

    state = partial_failure
    _exhaust_original_delivery(state, custom_success_override=True)
    existing_files = {path.name for path in state.uploads_dir.iterdir()}
    created_paths = []

    async def track_upload(*args, **kwargs):
        item = await store_upload(*args, **kwargs)
        created_paths.append(item.storage_path)
        return item

    monkeypatch.setattr("app.main.store_upload", track_upload)
    with _publishing_in_other_thread():
        response = _request_recovery_action(state, action)
        assert response.status_code == 409, response.text
        assert "publishing task is in progress" in response.json()["detail"]
    assert len(created_paths) == (0 if action == "send_now" else 1)
    assert all(not path.exists() for path in created_paths)
    assert {path.name for path in state.uploads_dir.iterdir()} == existing_files
    _assert_rejected_recovery_unchanged(state)


@pytest.mark.parametrize("action", ["atomic_edit", "send_now"])
def test_recovery_api_rejects_posting_job_after_publish_guard_releases(partial_failure, action):
    state = partial_failure
    _exhaust_original_delivery(state, custom_success_override=True)
    with state.SessionLocal() as session:
        post = get_post(session, state.post_id)
        post.status = "posting"
        next(job for job in post.delivery_jobs if job.target_account_id == state.mastodon_id).status = "posting"
        session.commit()
    existing_files = {path.name for path in state.uploads_dir.iterdir()}
    with _publishing_in_other_thread():
        pass

    response = _request_recovery_action(state, action)
    assert response.status_code == 400, response.text
    assert "currently being published" in response.json()["detail"]
    assert {path.name for path in state.uploads_dir.iterdir()} == existing_files
    _assert_rejected_recovery_unchanged(state, post_status="posting", job_status="posting")
