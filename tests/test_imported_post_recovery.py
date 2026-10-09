from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from io import BytesIO
from types import SimpleNamespace

import pytest
from PIL import Image

from app.adapters.common import looks_like_historical_backfill
from app.domain import CanonicalPostPayload, ExternalPostRefPayload, MediaItem
from app.models import AccountPostRef, CanonicalPost, DeliveryAttempt, DeliveryJob
from app.services import storage
from app.services.alerts import AlertDispatcher
from app.services.auth import Principal
from app.services.delivery import enqueue_due_scheduled_posts, process_delivery_queue
from app.services.personas import create_account, get_persona, replace_routes
from app.services.posts import get_post, refresh_post_status, upsert_polled_post
from test_scheduled_post_api import api_stack, _create_destination_account, _create_persona


def _png(color):
    output = BytesIO()
    Image.new("RGB", (40, 30), color).save(output, format="PNG")
    return output.getvalue()


@pytest.fixture()
def imported_failure(api_stack, monkeypatch, tmp_path):
    client, SessionLocal = api_stack
    imported_dir = tmp_path / "imported"
    imported_dir.mkdir()
    original_path = imported_dir / "source.png"
    original_path.write_bytes(_png("red"))
    monkeypatch.setattr(storage, "settings", replace(
        storage.settings, uploads_dir=tmp_path / "uploads", imported_media_dir=imported_dir,
    ))
    observed_at = datetime.now(timezone.utc)
    with SessionLocal() as session:
        persona = _create_persona(session, slug="imported-recovery")
        source = create_account(session, persona, {
            "service": "instagram", "label": "Instagram source", "handle_or_identifier": "source",
            "is_enabled": True, "source_enabled": True, "destination_enabled": False,
            "credentials_json": {"access_token": "test-token", "instagram_user_id": "test-source"},
            "source_settings_json": {}, "publish_settings_json": {},
        })
        succeeded = _create_destination_account(session, persona, service="bluesky", label="Published Bluesky")
        failed = _create_destination_account(session, persona, service="mastodon", label="Failed Mastodon")
        replace_routes(session, get_persona(session, persona.id), [
            {"source_account_id": source.id, "destination_account_id": account.id, "is_enabled": True}
            for account in (succeeded, failed)
        ])
        payload = CanonicalPostPayload(
            body="Original imported caption", published_at=observed_at,
            media=[MediaItem(original_path, "image/png", "Original image", original_path.stat().st_size, "source-checksum")],
            metadata={"link": "https://www.instagram.com/p/source-recovery/"},
            external_refs=[ExternalPostRefPayload("source-recovery", "https://www.instagram.com/p/source-recovery/", observed_at)],
        )
        post = upsert_polled_post(session, persona, source, payload)
        jobs = {job.target_account_id: job for job in post.delivery_jobs}
        success = jobs[succeeded.id]
        success.status = "posted"
        success.external_id = "at://did:plc:recovery/app.bsky.feed.post/already-posted"
        success.external_url = "https://bsky.app/profile/recovery.test/post/already-posted"
        success.delivered_at = observed_at
        success.attempt_count = 1
        session.add(DeliveryAttempt(delivery_job_id=success.id, status="posted", finished_at=observed_at))
        session.add(AccountPostRef(
            post_id=post.id, account_id=succeeded.id, external_id=success.external_id,
            external_url=success.external_url, observed_at=observed_at,
        ))
        failure = jobs[failed.id]
        failure.status = "failed"
        failure.attempt_count = failure.max_retries = 3
        failure.last_error = post.last_error = "The destination rejected the source image."
        failure.last_error_class = "ValueError"
        for _ in range(3):
            session.add(DeliveryAttempt(
                delivery_job_id=failure.id, status="failed", finished_at=observed_at,
                error_class="ValueError", error_message=failure.last_error,
            ))
        refresh_post_status(post)
        # An already-published destination must not need a working connection to recover its sibling.
        succeeded.is_enabled = False
        succeeded.credentials_json = {}
        session.commit()
        refs = {ref.account_id: (ref.id, ref.external_id, ref.external_url) for ref in session.query(AccountPostRef)}
        state = SimpleNamespace(
            client=client, SessionLocal=SessionLocal, post_id=post.id, persona_id=persona.id,
            source_id=source.id, succeeded_id=succeeded.id, failed_id=failed.id,
            original_id=post.attachments[0].id, original_path=original_path, payload=payload,
            success_snapshot=(success.id, success.external_id, success.external_url, success.delivered_at, success.attempt_count),
            source_ref=refs[source.id], success_ref=refs[succeeded.id], uploads=[], posts=[], transient_left=0,
        )

    class FakeMastodonClient:
        def instance(self):
            return {}

        def media_post(self, path, **kwargs):
            with Image.open(path) as image:
                state.uploads.append((image.size, image.getpixel((0, 0)), kwargs.get("description")))
            if state.transient_left:
                state.transient_left -= 1
                raise RuntimeError("A temporary media upload failure.")
            return {"id": "recovered-media", "url": "https://example.social/media/recovered"}

        def status_post(self, body, **kwargs):
            state.posts.append((body, kwargs))
            return {"id": "recovered-import", "url": "https://example.social/@me/recovered-import"}

    def reject_success_republish(*args, **kwargs):
        raise AssertionError("An already successful Bluesky destination must not publish again.")

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda *args, **kwargs: FakeMastodonClient())
    monkeypatch.setattr("app.adapters.bluesky._get_client", reject_success_republish)
    return state


def _assert_identity_preserved(state, session):
    post = get_post(session, state.post_id)
    assert post.origin_kind == "account_import"
    assert post.origin_account_id == state.source_id
    assert session.query(CanonicalPost).count() == 1
    assert session.query(DeliveryJob).count() == 2
    success = next(job for job in post.delivery_jobs if job.target_account_id == state.succeeded_id)
    delivered_at = success.delivered_at.replace(tzinfo=timezone.utc)
    assert (success.id, success.external_id, success.external_url, delivered_at, success.attempt_count) == state.success_snapshot
    assert len(success.attempts) == 1
    refs = {ref.account_id: (ref.id, ref.external_id, ref.external_url) for ref in session.query(AccountPostRef)}
    assert refs[state.source_id] == state.source_ref
    assert refs[state.succeeded_id] == state.success_ref
    return post


def test_failed_import_is_visible_in_planner_and_recovery_detail(imported_failure):
    state = imported_failure
    listing = state.client.get("/scheduled-posts")
    assert listing.status_code == 200
    assert [post["id"] for post in listing.json()] == [state.post_id]
    planner = state.client.get("/scheduled-posts/page")
    assert planner.status_code == 200
    assert f"/scheduled-posts/{state.post_id}/page" in planner.text
    detail = state.client.get(f"/scheduled-posts/{state.post_id}")
    assert detail.status_code == 200
    assert detail.json()["origin_kind"] == "account_import"
    assert detail.json()["display_status"] == "partial_failure"
    assert state.client.get(f"/scheduled-posts/{state.post_id}/page").status_code == 200
    # Recovery does not expose deletion of the imported source record.
    assert state.client.delete(f"/scheduled-posts/{state.post_id}").status_code == 404


def test_imported_recovery_draft_stays_visible_and_preserves_source_identity(imported_failure):
    state = imported_failure
    response = state.client.put(f"/scheduled-posts/{state.post_id}", json={
        "body": "Corrected draft caption", "status": "draft", "metadata_json": {"editor_note": "fixed"},
    })
    assert response.status_code == 200
    assert response.json()["status"] == "draft"
    assert response.json()["metadata_json"]["manual_recovery"] is True
    assert state.client.get(f"/scheduled-posts/{state.post_id}/page").status_code == 200
    assert state.post_id in {post["id"] for post in state.client.get("/scheduled-posts").json()}
    with state.SessionLocal() as session:
        post = _assert_identity_preserved(state, session)
        assert post.body == "Corrected draft caption"
        assert post.metadata_json["editor_note"] == "fixed"
    assert state.posts == state.uploads == []


@pytest.mark.parametrize("queue_delivery", [True, False])
def test_source_rescan_preserves_repaired_draft_targets_and_retry_budget(imported_failure, queue_delivery):
    state = imported_failure
    with state.SessionLocal() as session:
        persona = get_persona(session, state.persona_id)
        extra = _create_destination_account(session, persona, service="discord", label="New route excluded from repair")
        replace_routes(session, get_persona(session, persona.id), [
            {"source_account_id": state.source_id, "destination_account_id": account_id, "is_enabled": True}
            for account_id in (state.succeeded_id, state.failed_id, extra.id)
        ])
        session.commit()
    response = state.client.put(f"/scheduled-posts/{state.post_id}", json={
        "body": "Repaired draft awaiting review", "status": "draft",
        "target_account_ids": [state.succeeded_id, state.failed_id],
    })
    assert response.status_code == 200
    with state.SessionLocal() as session:
        post = _assert_identity_preserved(state, session)
        assert post.status == "draft"
        before = {
            job.target_account_id: (job.id, job.status, job.attempt_count, job.max_retries, job.last_error, job.queued_at)
            for job in post.delivery_jobs
        }
        rescanned = upsert_polled_post(
            session, post.persona, post.origin_account, state.payload, queue_delivery=queue_delivery,
            delivery_suppressed_reason="autorun_import_delivery_disabled",
        )
        session.commit()
        assert rescanned.id == state.post_id
        post = _assert_identity_preserved(state, session)
        assert post.status == "draft"
        assert post.body == "Repaired draft awaiting review"
        assert post.metadata_json["manual_recovery"] is True
        assert "delivery_suppressed_reason" not in post.metadata_json
        assert {
            job.target_account_id: (job.id, job.status, job.attempt_count, job.max_retries, job.last_error, job.queued_at)
            for job in post.delivery_jobs
        } == before
    assert state.posts == state.uploads == []


def test_scheduled_manual_import_is_enqueued_and_published_by_autorun(imported_failure, monkeypatch):
    state = imported_failure
    monkeypatch.setattr("app.services.delivery._autorun_import_delivery_enabled", lambda: False)
    with state.SessionLocal() as session:
        post = get_post(session, state.post_id)
        post.published_at = datetime.now(timezone.utc) - timedelta(days=60)
        assert looks_like_historical_backfill(post, post.persona, post.origin_account)
        session.commit()
    response = state.client.put(f"/scheduled-posts/{state.post_id}", json={
        "body": "Intentionally scheduled recovery", "status": "scheduled",
        "scheduled_for": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
    })
    assert response.status_code == 200
    assert response.json()["metadata_json"]["manual_recovery"] is True
    with state.SessionLocal() as session:
        enqueue_due_scheduled_posts(session, run_id="due-import-recovery")
        session.commit()
        post = _assert_identity_preserved(state, session)
        assert post.status == "queued"
        pending = next(job for job in post.delivery_jobs if job.target_account_id == state.failed_id)
        assert pending.status == "queued"
        assert pending.attempt_count == 0
        process_delivery_queue(session, AlertDispatcher(), post_id=state.post_id, run_id="autorun-import-recovery", trigger="autorun")
        session.commit()
        assert _assert_identity_preserved(state, session).status == "posted"
    assert len(state.posts) == 1
    assert state.posts[0][0] == "Intentionally scheduled recovery"


def test_autorun_continues_manual_recovery_after_transient_failure(imported_failure, monkeypatch):
    state = imported_failure
    state.transient_left = 1
    monkeypatch.setattr("app.services.delivery._autorun_import_delivery_enabled", lambda: False)
    with state.SessionLocal() as session:
        post = get_post(session, state.post_id)
        post.published_at = datetime.now(timezone.utc) - timedelta(days=60)
        assert looks_like_historical_backfill(post, post.persona, post.origin_account)
        session.commit()
    response = state.client.post(f"/scheduled-posts/{state.post_id}/send-now")
    assert response.status_code == 200
    with state.SessionLocal() as session:
        post = _assert_identity_preserved(state, session)
        pending = next(job for job in post.delivery_jobs if job.target_account_id == state.failed_id)
        assert pending.status == "queued"
        assert pending.attempt_count == 1
        assert len(pending.attempts) == 4
        assert state.posts == []
        process_delivery_queue(session, AlertDispatcher(), post_id=state.post_id, run_id="continue-manual-import", trigger="autorun")
        session.commit()
        post = _assert_identity_preserved(state, session)
        recovered = next(job for job in post.delivery_jobs if job.target_account_id == state.failed_id)
        assert post.status == recovered.status == "posted"
        assert recovered.attempt_count == 2
        assert len(recovered.attempts) == 5
    assert len(state.uploads) == 2
    assert len(state.posts) == 1


def test_due_enqueue_does_not_reopen_exhausted_failure_while_sibling_is_pending(imported_failure):
    state = imported_failure
    with state.SessionLocal() as session:
        post = get_post(session, state.post_id)
        post.metadata_json = {**post.metadata_json, "manual_recovery": True}
        post.scheduled_for = datetime.now(timezone.utc) - timedelta(minutes=1)
        post.status = "queued"
        pending_account = _create_destination_account(session, post.persona, service="discord", label="Pending sibling")
        session.add(DeliveryJob(post=post, target_account=pending_account, status="queued", max_retries=3))
        session.commit()
        failed = next(job for job in post.delivery_jobs if job.target_account_id == state.failed_id)
        previous_failure = (failed.attempt_count, failed.last_error, failed.last_error_class, failed.queued_at)
        for cycle in range(2):
            enqueue_due_scheduled_posts(session, run_id=f"exhausted-sibling-{cycle}")
            session.commit()
            post = get_post(session, state.post_id)
            jobs = {job.target_account_id: job for job in post.delivery_jobs}
            assert jobs[state.failed_id].status == "failed"
            assert (
                jobs[state.failed_id].attempt_count, jobs[state.failed_id].last_error,
                jobs[state.failed_id].last_error_class, jobs[state.failed_id].queued_at,
            ) == previous_failure
            assert jobs[pending_account.id].status == "queued"
            assert jobs[state.succeeded_id].status == "posted"
            assert post.status == "queued"
    assert state.posts == state.uploads == []


def test_imported_atomic_media_correction_retries_same_post_only_once(imported_failure):
    state = imported_failure
    response = state.client.put(f"/scheduled-posts/{state.post_id}?send_now=true", data={
        "body": "Corrected imported caption", "status": "failed",
        "target_account_ids": json.dumps([state.succeeded_id, state.failed_id]),
        "metadata_json": "{}", "deleted_attachment_ids": json.dumps([state.original_id]),
        "alt_texts": '["Replacement image"]',
    }, files=[("uploads", ("replacement.png", _png("blue"), "image/png"))])
    assert response.status_code == 200
    assert response.json()["id"] == state.post_id
    assert response.json()["metadata_json"]["manual_recovery"] is True
    assert response.json()["deliveries"][state.failed_id]["attempt_count"] == 0
    with state.SessionLocal() as session:
        post = _assert_identity_preserved(state, session)
        assert post.status == "posted"
        failed = next(job for job in post.delivery_jobs if job.target_account_id == state.failed_id)
        assert failed.status == "posted"
        assert failed.external_id == "recovered-import"
        assert failed.attempt_count == 1
        assert len(failed.attempts) == 4
    assert state.uploads == [((40, 30), (0, 0, 255), "Replacement image")]
    assert len(state.posts) == 1
    assert state.posts[0][0] == "Corrected imported caption"
    assert not state.original_path.exists()
    # A completed manual recovery remains available for its delivery history.
    assert state.client.get(f"/scheduled-posts/{state.post_id}").status_code == 200
    assert state.post_id in {post["id"] for post in state.client.get("/scheduled-posts").json()}


def test_failed_import_supports_explicit_manual_retry_on_existing_route(imported_failure):
    state = imported_failure
    response = state.client.post(f"/scheduled-posts/{state.post_id}/send-now")
    assert response.status_code == 200
    assert response.json()["metadata_json"]["manual_recovery"] is True
    assert response.json()["deliveries"][state.failed_id]["attempt_count"] == 0
    with state.SessionLocal() as session:
        post = _assert_identity_preserved(state, session)
        assert post.status == "posted"
        assert next(job for job in post.delivery_jobs if job.target_account_id == state.failed_id).attempt_count == 1
    assert state.uploads == [((40, 30), (255, 0, 0), "Original image")]
    assert len(state.posts) == 1


def test_imported_recovery_rejects_unrelated_persona_access(imported_failure, monkeypatch):
    state = imported_failure
    monkeypatch.setattr("app.main.build_principal_from_request", lambda request: Principal(
        user_id="unrelated-user", display_name="Unrelated", role="user", timezone="UTC", is_authenticated=True,
    ))
    assert state.client.get("/scheduled-posts").json() == []
    assert state.client.get(f"/scheduled-posts/{state.post_id}").status_code == 404
    assert state.client.get(f"/scheduled-posts/{state.post_id}/page").status_code == 404
    assert state.client.put(f"/scheduled-posts/{state.post_id}", json={"body": "Forbidden"}).status_code == 404
    assert state.client.post(f"/scheduled-posts/{state.post_id}/send-now").status_code == 404
    with state.SessionLocal() as session:
        assert _assert_identity_preserved(state, session).body == "Original imported caption"
    assert state.posts == state.uploads == []


@pytest.mark.parametrize("status", ["posted", "skipped"])
def test_healthy_and_skipped_imports_are_not_editable_without_recovery_flag(imported_failure, status):
    state = imported_failure
    with state.SessionLocal() as session:
        post = get_post(session, state.post_id)
        next(job for job in post.delivery_jobs if job.target_account_id == state.failed_id).status = status
        refresh_post_status(post)
        session.commit()
    assert state.client.get("/scheduled-posts").json() == []
    assert state.client.get(f"/scheduled-posts/{state.post_id}").status_code == 404
    assert state.client.get(f"/scheduled-posts/{state.post_id}/page").status_code == 404
    assert state.client.put(f"/scheduled-posts/{state.post_id}", json={"body": "Unexpected edit"}).status_code == 404
    assert state.client.post(f"/scheduled-posts/{state.post_id}/send-now").status_code == 404
    assert state.posts == state.uploads == []
