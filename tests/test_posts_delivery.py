from __future__ import annotations

from datetime import datetime, timezone

from app.adapters.bluesky import BlueskyDestinationAdapter
from app.adapters.mastodon import MastodonDestinationAdapter
from app.adapters.tumblr import TumblrDestinationAdapter
from app.domain import CanonicalPostPayload, ExternalPostRefPayload, MediaItem, PublishResult
from app.models import AccountPostRef, AccountRoute, AlertEvent, CanonicalPost, DeliveryJob
from app.schemas import ScheduledPostCreate
from app.services.alerts import AlertDispatcher
from app.services.delivery import process_delivery_queue
from app.services.personas import create_account, create_persona, get_persona, replace_routes
from app.services.posts import (
    create_scheduled_post,
    scheduled_post_delivery_breakdown,
    scheduled_post_display_status,
    upsert_polled_post,
)


def _create_persona(session, *, max_retries: int = 3):
    return create_persona(
        session,
        {
            "name": "Persona",
            "slug": f"persona-{max_retries}",
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": max_retries},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def _create_account(session, persona, *, service: str, label: str, source_enabled: bool, destination_enabled: bool):
    default_credentials = {
        "bluesky": {"handle": "me.bsky.social", "password": "pw"},
        "mastodon": {"instance": "https://example.social", "token": "secret", "handle": "@me@example.social"},
        "discord": {"webhook_url": "https://discord.test"},
        "telegram": {"bot_token": "telegram-secret", "channel_id": "@lynxposter_test"},
        "tumblr": {
            "consumer_key": "tumblr-key",
            "consumer_secret": "tumblr-secret",
            "oauth_token": "oauth-token",
            "oauth_secret": "oauth-secret",
            "blog_name": "lynxposter",
        },
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


def test_manual_composer_defaults_to_enabled_destination_accounts(session):
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
                "target_account_ids": [],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    assert post.origin_kind == "composer"
    assert {job.target_account_id for job in post.delivery_jobs} == {mastodon.id, discord.id}
    assert post.status == "draft"


def test_published_post_is_deduped_when_polled_back_from_same_account(session, monkeypatch):
    persona = _create_persona(session)
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=True, destination_enabled=True)

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello from composer",
                "status": "queued",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    class FakeDestinationAdapter:
        def validate(self, post, persona, account):
            return []

        def publish(self, session, post, persona, account, *, context=None):
            return PublishResult(service=account.service, external_id="remote-1", external_url="https://example.social/@me/remote-1")

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", lambda account: FakeDestinationAdapter())

    process_delivery_queue(session, AlertDispatcher(), run_id="run-1")

    imported = upsert_polled_post(
        session,
        get_persona(session, persona.id),
        mastodon,
        CanonicalPostPayload(
            body="Hello from composer",
            external_refs=[ExternalPostRefPayload(external_id="remote-1", external_url="https://example.social/@me/remote-1")],
        ),
    )

    assert imported.id == post.id
    assert session.query(CanonicalPost).count() == 1


def test_removed_route_cancels_queued_import_delivery(session):
    persona = _create_persona(session)
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky", source_enabled=True, destination_enabled=True)
    discord = _create_account(session, persona, service="discord", label="Discord", source_enabled=False, destination_enabled=True)

    replace_routes(
        session,
        get_persona(session, persona.id),
        [{"source_account_id": bluesky.id, "destination_account_id": discord.id, "is_enabled": True}],
    )

    post = upsert_polled_post(
        session,
        get_persona(session, persona.id),
        bluesky,
        CanonicalPostPayload(
            body="Imported post",
            external_refs=[ExternalPostRefPayload(external_id="source-1", external_url="https://bsky.app/post/1")],
        ),
    )
    assert post.delivery_jobs[0].status == "queued"

    replace_routes(session, get_persona(session, persona.id), [])
    process_delivery_queue(session, AlertDispatcher(), run_id="run-2")

    job = session.query(DeliveryJob).one()
    assert job.status == "cancelled"


def test_failed_delivery_records_alert_after_max_retries(session, monkeypatch):
    persona = _create_persona(session, max_retries=1)
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)

    create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello failure",
                "status": "queued",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    class FakeDestinationAdapter:
        def validate(self, post, persona, account):
            return []

        def publish(self, session, post, persona, account, *, context=None):
            raise RuntimeError("boom")

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", lambda account: FakeDestinationAdapter())

    process_delivery_queue(session, AlertDispatcher(), run_id="run-3")

    job = session.query(DeliveryJob).one()
    assert job.status == "failed"
    assert job.attempt_count == 1
    assert session.query(AlertEvent).count() == 1


def test_scheduled_post_display_status_tracks_partial_and_full_outcomes(session):
    persona = _create_persona(session, max_retries=1)
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)
    discord = _create_account(session, persona, service="discord", label="Discord", source_enabled=False, destination_enabled=True)

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello outcomes",
                "status": "queued",
                "target_account_ids": [mastodon.id, discord.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    jobs = {job.target_account_id: job for job in post.delivery_jobs}
    jobs[mastodon.id].status = "posted"
    jobs[discord.id].status = "failed"
    jobs[discord.id].last_error = "Webhook rejected the payload."

    breakdown = scheduled_post_delivery_breakdown(post)

    assert scheduled_post_display_status(post) == "partial_failure"
    assert [item["label"] for item in breakdown["succeeded"]] == ["Mastodon"]
    assert breakdown["failed"][0]["label"] == "Discord"
    assert breakdown["failed"][0]["last_error"] == "Webhook rejected the payload."

    jobs[mastodon.id].status = "failed"
    jobs[mastodon.id].last_error = "Token expired."

    breakdown = scheduled_post_delivery_breakdown(post)

    assert scheduled_post_display_status(post) == "failure"
    assert [item["label"] for item in breakdown["failed"]] == ["Discord", "Mastodon"]

    jobs[mastodon.id].status = "posted"
    jobs[mastodon.id].last_error = None
    jobs[discord.id].status = "posted"
    jobs[discord.id].last_error = None

    assert scheduled_post_display_status(post) == "success"


def test_mastodon_publish_uses_persona_default_when_account_visibility_is_blank(session, monkeypatch):
    persona = _create_persona(session)
    persona.settings_json = {"visibility": "public"}
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)
    mastodon.publish_settings_json = {"visibility": ""}

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello Mastodon",
                "status": "queued",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    captured: dict[str, str | None] = {"visibility": None, "language": None}

    class FakeClient:
        def status_post(self, body, in_reply_to_id=None, media_ids=None, visibility=None, language=None):
            captured["visibility"] = visibility
            captured["language"] = language
            return {"id": "mastodon-1", "url": "https://example.social/@me/mastodon-1"}

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())

    process_delivery_queue(session, AlertDispatcher(), run_id="run-4")

    session.refresh(post)
    assert captured["visibility"] == "public"
    assert captured["language"] is None
    assert post.delivery_jobs[0].status == "posted"


def test_mastodon_publish_uses_account_language_with_legacy_persona_fallback(session, monkeypatch):
    persona = _create_persona(session)
    persona.settings_json = {"mastodon_lang": "en"}
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)
    mastodon.publish_settings_json = {"language": ""}

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello Mastodon",
                "status": "queued",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    captured: dict[str, str | None] = {"language": None}

    class FakeClient:
        def status_post(self, body, in_reply_to_id=None, media_ids=None, visibility=None, language=None):
            captured["language"] = language
            return {"id": "mastodon-2", "url": "https://example.social/@me/mastodon-2"}

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())

    process_delivery_queue(session, AlertDispatcher(), run_id="run-legacy-lang")

    session.refresh(post)
    assert captured["language"] == "en"
    assert post.delivery_jobs[0].status == "posted"


def test_mastodon_validate_rejects_invalid_visibility_with_clear_message(session):
    persona = _create_persona(session)
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)
    post = CanonicalPost(persona_id=persona.id, origin_kind="composer", body="Hello", metadata_json={"visibility": "friends-only"})

    issues = MastodonDestinationAdapter().validate(post, persona, mastodon)

    assert len(issues) == 1
    assert issues[0].field == "visibility"
    assert "friends-only" in issues[0].message


def test_bluesky_validate_rejects_mixed_media_and_multiple_videos(session, tmp_path):
    persona = _create_persona(session)
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky", source_enabled=False, destination_enabled=True)
    image_path = tmp_path / "photo.jpg"
    video_path = tmp_path / "clip.mp4"
    second_video_path = tmp_path / "clip-2.mp4"
    image_path.write_bytes(b"jpeg")
    video_path.write_bytes(b"video")
    second_video_path.write_bytes(b"video")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello",
                "status": "draft",
                "target_account_ids": [bluesky.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(storage_path=image_path, mime_type="image/jpeg", alt_text="", size_bytes=4, checksum="img-1", sort_order=0),
            MediaItem(storage_path=video_path, mime_type="video/mp4", alt_text="", size_bytes=5, checksum="vid-1", sort_order=1),
            MediaItem(storage_path=second_video_path, mime_type="video/mp4", alt_text="", size_bytes=5, checksum="vid-2", sort_order=2),
        ],
    )

    issues = BlueskyDestinationAdapter().validate(post, persona, bluesky)
    messages = [issue.message for issue in issues]

    assert any("only one video attachment" in message for message in messages)
    assert any("up to 4 images or one MP4 video" in message for message in messages)


def test_tumblr_validate_rejects_mixed_image_and_video_attachments(session, tmp_path):
    persona = _create_persona(session)
    tumblr = _create_account(session, persona, service="tumblr", label="Tumblr", source_enabled=False, destination_enabled=True)
    image_path = tmp_path / "photo.jpg"
    video_path = tmp_path / "clip.mp4"
    image_path.write_bytes(b"jpeg")
    video_path.write_bytes(b"video")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello",
                "status": "draft",
                "target_account_ids": [tumblr.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(storage_path=image_path, mime_type="image/jpeg", alt_text="", size_bytes=4, checksum="img-1", sort_order=0),
            MediaItem(storage_path=video_path, mime_type="video/mp4", alt_text="", size_bytes=5, checksum="vid-1", sort_order=1),
        ],
    )

    issues = TumblrDestinationAdapter().validate(post, persona, tumblr)

    assert len(issues) == 1
    assert "photo set or one video" in issues[0].message


def test_telegram_media_group_publish_records_all_message_refs_and_dedupes_polled_posts(session, monkeypatch, tmp_path):
    persona = _create_persona(session)
    telegram = _create_account(session, persona, service="telegram", label="Telegram", source_enabled=True, destination_enabled=True)

    image_one = tmp_path / "one.jpg"
    image_two = tmp_path / "two.jpg"
    image_one.write_bytes(b"one")
    image_two.write_bytes(b"two")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Telegram album",
                "status": "queued",
                "target_account_ids": [telegram.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(storage_path=image_one, mime_type="image/jpeg", alt_text="", size_bytes=3, checksum="one", sort_order=0),
            MediaItem(storage_path=image_two, mime_type="image/jpeg", alt_text="", size_bytes=3, checksum="two", sort_order=1),
        ],
    )

    def fake_request(bot_token, method, *, json_body=None, data=None, files=None, timeout=30):
        assert bot_token == "telegram-secret"
        assert method == "sendMediaGroup"
        assert data["chat_id"] == "@lynxposter_test"
        return [
            {
                "message_id": 501,
                "date": int(datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc).timestamp()),
                "chat": {"id": -100555666, "username": "lynxposter_test"},
            },
            {
                "message_id": 502,
                "date": int(datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc).timestamp()),
                "chat": {"id": -100555666, "username": "lynxposter_test"},
            },
        ]

    monkeypatch.setattr("app.adapters.telegram._telegram_request", fake_request)

    process_delivery_queue(session, AlertDispatcher(), run_id="telegram-group")

    refs = session.query(AccountPostRef).filter(AccountPostRef.account_id == telegram.id).all()
    assert {ref.external_id for ref in refs} == {"501", "502"}
    assert post.delivery_jobs[0].status == "posted"

    imported = upsert_polled_post(
        session,
        get_persona(session, persona.id),
        telegram,
        CanonicalPostPayload(
            body="Telegram album",
            external_refs=[ExternalPostRefPayload(external_id="502", external_url="https://t.me/lynxposter_test/502")],
        ),
    )

    assert imported.id == post.id
    assert session.query(CanonicalPost).count() == 1
