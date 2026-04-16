from __future__ import annotations

import sys
from datetime import datetime, timezone
from types import SimpleNamespace

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


def test_crossposted_copy_polled_from_other_source_does_not_requeue_new_destinations(session, monkeypatch):
    persona = _create_persona(session)
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky", source_enabled=True, destination_enabled=True)
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=True, destination_enabled=True)
    discord = _create_account(session, persona, service="discord", label="Discord", source_enabled=False, destination_enabled=True)

    replace_routes(
        session,
        get_persona(session, persona.id),
        [
            {"source_account_id": bluesky.id, "destination_account_id": mastodon.id, "is_enabled": True},
            {"source_account_id": mastodon.id, "destination_account_id": discord.id, "is_enabled": True},
        ],
    )

    post = upsert_polled_post(
        session,
        get_persona(session, persona.id),
        bluesky,
        CanonicalPostPayload(
            body="Imported from Bluesky",
            external_refs=[ExternalPostRefPayload(external_id="bsky-1", external_url="https://bsky.app/post/1")],
        ),
    )

    class FakeDestinationAdapter:
        def validate(self, post, persona, account):
            return []

        def publish(self, session, post, persona, account, *, context=None):
            return PublishResult(
                service=account.service,
                external_id="mastodon-1",
                external_url="https://example.social/@me/mastodon-1",
            )

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", lambda account: FakeDestinationAdapter())

    process_delivery_queue(session, AlertDispatcher(), run_id="run-crosspost-source")

    session.refresh(post)
    assert {job.target_account_id for job in post.delivery_jobs} == {mastodon.id}
    assert post.delivery_jobs[0].status == "posted"

    imported = upsert_polled_post(
        session,
        get_persona(session, persona.id),
        mastodon,
        CanonicalPostPayload(
            body="Imported from Bluesky",
            external_refs=[ExternalPostRefPayload(external_id="mastodon-1", external_url="https://example.social/@me/mastodon-1")],
        ),
    )

    session.refresh(post)
    assert imported.id == post.id
    assert {job.target_account_id for job in post.delivery_jobs} == {mastodon.id}
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


def test_delivery_attempt_response_payload_serializes_datetime_values(session, monkeypatch):
    persona = _create_persona(session)
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello serialization",
                "status": "queued",
                "target_account_ids": [mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    published_at = datetime(2026, 4, 16, 0, 23, 29, 866300, tzinfo=timezone.utc)

    class FakeDestinationAdapter:
        def validate(self, post, persona, account):
            return []

        def publish(self, session, post, persona, account, *, context=None):
            return PublishResult(
                service=account.service,
                external_id="mastodon-serialized",
                external_url="https://example.social/@me/mastodon-serialized",
                raw={
                    "id": "mastodon-serialized",
                    "created_at": published_at,
                    "history": [{"published_at": published_at}],
                },
            )

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", lambda account: FakeDestinationAdapter())

    process_delivery_queue(session, AlertDispatcher(), run_id="run-serialized")

    session.refresh(post)
    attempt = post.delivery_jobs[0].attempts[0]
    assert post.delivery_jobs[0].status == "posted"
    assert attempt.response_payload["created_at"] == published_at.isoformat()
    assert attempt.response_payload["history"][0]["published_at"] == published_at.isoformat()


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


def test_bluesky_treats_video_mime_without_mp4_suffix_as_video(session, monkeypatch, tmp_path):
    persona = _create_persona(session)
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky", source_enabled=False, destination_enabled=True)
    video_path = tmp_path / "clip.upload"
    video_path.write_bytes(b"video")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Video time",
                "status": "queued",
                "target_account_ids": [bluesky.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(storage_path=video_path, mime_type="video/mp4", alt_text="Clip", size_bytes=5, checksum="vid-1", sort_order=0),
        ],
    )

    captured: dict[str, object] = {}

    class FakeClient:
        def send_video(self, body, data, alt_text, facets=None):
            captured["method"] = "send_video"
            captured["body"] = body
            captured["bytes"] = data
            captured["alt_text"] = alt_text
            return SimpleNamespace(uri="at://did:plc:test/app.bsky.feed.post/bsky-video-1")

        def send_images(self, *args, **kwargs):
            raise AssertionError("send_images should not be used for a video attachment.")

        def send_post(self, *args, **kwargs):
            raise AssertionError("send_post should not be used for a video attachment.")

    monkeypatch.setattr("app.adapters.bluesky._get_client", lambda config, update_session=None: FakeClient())

    preview = BlueskyDestinationAdapter().preview(post, persona, bluesky)
    result = BlueskyDestinationAdapter().publish(session, post, persona, bluesky)

    assert preview.action == "send_video"
    assert captured["method"] == "send_video"
    assert captured["body"] == "Video time"
    assert captured["alt_text"] == "Clip"
    assert result.external_id == "bsky-video-1"


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


def test_tumblr_treats_video_mime_without_mp4_suffix_as_video(session, monkeypatch, tmp_path):
    persona = _create_persona(session)
    tumblr = _create_account(session, persona, service="tumblr", label="Tumblr", source_enabled=False, destination_enabled=True)
    video_path = tmp_path / "clip.upload"
    video_path.write_bytes(b"video")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Tumblr video",
                "status": "queued",
                "target_account_ids": [tumblr.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(storage_path=video_path, mime_type="video/mp4", alt_text="", size_bytes=5, checksum="vid-1", sort_order=0),
        ],
    )

    captured: dict[str, object] = {}

    class FakeTumblrClient:
        def create_video(self, blog_name, *, state, caption, data):
            captured["method"] = "create_video"
            captured["blog_name"] = blog_name
            captured["caption"] = caption
            captured["data"] = data
            return {"id": "tumblr-video-1"}

        def create_photo(self, *args, **kwargs):
            raise AssertionError("create_photo should not be used for a video attachment.")

        def create_text(self, *args, **kwargs):
            raise AssertionError("create_text should not be used for a video attachment.")

    monkeypatch.setitem(sys.modules, "pytumblr", SimpleNamespace(TumblrRestClient=lambda *args: FakeTumblrClient()))

    preview = TumblrDestinationAdapter().preview(post, persona, tumblr)
    result = TumblrDestinationAdapter().publish(session, post, persona, tumblr)

    assert preview.action == "create_video"
    assert captured["method"] == "create_video"
    assert captured["caption"] == "Tumblr video"
    assert captured["data"] == str(video_path)
    assert result.external_id == "tumblr-video-1"


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


def test_discord_publish_requests_wait_and_records_message_id(session, monkeypatch):
    persona = _create_persona(session)
    discord = _create_account(session, persona, service="discord", label="Discord", source_enabled=False, destination_enabled=True)

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello Discord",
                "status": "queued",
                "target_account_ids": [discord.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [],
    )

    captured: dict[str, object] = {}

    class FakeResponse:
        content = b'{"id":"discord-123"}'

        def raise_for_status(self):
            return None

        def json(self):
            return {"id": "discord-123"}

    def fake_post(url, *, params=None, json=None, data=None, files=None, timeout=30):
        captured["url"] = url
        captured["params"] = params
        captured["json"] = json
        captured["data"] = data
        captured["files"] = files
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("app.adapters.discord.requests.post", fake_post)

    process_delivery_queue(session, AlertDispatcher(), run_id="run-discord-wait")

    session.refresh(post)
    assert captured["params"] == {"wait": "true"}
    assert captured["json"] == {"content": "Hello Discord"}
    assert captured["data"] is None
    assert captured["files"] is None
    assert post.delivery_jobs[0].status == "posted"
    assert post.delivery_jobs[0].external_id == "discord-123"
    ref = session.query(AccountPostRef).filter(AccountPostRef.account_id == discord.id).one()
    assert ref.external_id == "discord-123"


def test_discord_prefers_configured_destination_link_and_runs_after_that_destination(session, monkeypatch):
    persona = _create_persona(session)
    discord = _create_account(session, persona, service="discord", label="Discord", source_enabled=False, destination_enabled=True)
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)
    discord.publish_settings_json = {"link_preference_order": "mastodon,source"}
    session.flush()

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Route this link",
                "status": "queued",
                "target_account_ids": [discord.id, mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {"link": "https://bsky.app/profile/me/post/source-1"},
                "scheduled_for": None,
            }
        ),
        [],
    )

    original_factory = __import__("app.services.delivery", fromlist=["get_destination_adapter_for_account"]).get_destination_adapter_for_account
    captured: dict[str, object] = {}

    class FakeMastodonAdapter:
        def validate(self, post, persona, account):
            return []

        def publish(self, session, post, persona, account, *, context=None):
            return PublishResult(
                service=account.service,
                external_id="mastodon-42",
                external_url="https://example.social/@me/42",
            )

    class FakeResponse:
        content = b'{"id":"discord-link-1"}'

        def raise_for_status(self):
            return None

        def json(self):
            return {"id": "discord-link-1"}

    def fake_adapter_factory(account):
        if account.service == "mastodon":
            return FakeMastodonAdapter()
        return original_factory(account)

    def fake_post(url, *, params=None, json=None, data=None, files=None, timeout=30):
        captured["params"] = params
        captured["json"] = json
        return FakeResponse()

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", fake_adapter_factory)
    monkeypatch.setattr("app.adapters.discord.requests.post", fake_post)

    process_delivery_queue(session, AlertDispatcher(), run_id="run-discord-preferred-link")

    session.refresh(post)
    jobs = {job.target_account_id: job for job in post.delivery_jobs}
    assert jobs[mastodon.id].status == "posted"
    assert jobs[discord.id].status == "posted"
    assert captured["params"] == {"wait": "true"}
    assert captured["json"] == {"content": "Route this link\nSource: https://example.social/@me/42"}


def test_discord_waits_when_preferred_destination_link_is_not_ready(session, monkeypatch):
    persona = _create_persona(session)
    discord = _create_account(session, persona, service="discord", label="Discord", source_enabled=False, destination_enabled=True)
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon", source_enabled=False, destination_enabled=True)
    discord.publish_settings_json = {"link_preference_order": "mastodon,source"}
    session.flush()

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Wait for Mastodon",
                "status": "queued",
                "target_account_ids": [discord.id, mastodon.id],
                "publish_overrides_json": {},
                "metadata_json": {"link": "https://bsky.app/profile/me/post/source-2"},
                "scheduled_for": None,
            }
        ),
        [],
    )

    jobs = {job.target_account_id: job for job in post.delivery_jobs}
    jobs[mastodon.id].status = "scheduled"

    def fake_post(*args, **kwargs):
        raise AssertionError("Discord should stay queued until the preferred destination link is ready.")

    monkeypatch.setattr("app.adapters.discord.requests.post", fake_post)

    process_delivery_queue(session, AlertDispatcher(), run_id="run-discord-hold")

    session.refresh(post)
    refreshed_jobs = {job.target_account_id: job for job in post.delivery_jobs}
    assert refreshed_jobs[discord.id].status == "queued"
    assert refreshed_jobs[discord.id].external_id is None


def test_discord_publish_uses_payload_json_for_multipart_requests(session, monkeypatch, tmp_path):
    persona = _create_persona(session)
    discord = _create_account(session, persona, service="discord", label="Discord", source_enabled=False, destination_enabled=True)

    image_path = tmp_path / "photo.jpg"
    image_path.write_bytes(b"jpeg")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello with media",
                "status": "queued",
                "target_account_ids": [discord.id],
                "publish_overrides_json": {},
                "metadata_json": {"link": "https://example.com/source"},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(storage_path=image_path, mime_type="image/jpeg", alt_text="", size_bytes=4, checksum="img-1", sort_order=0),
        ],
    )

    captured: dict[str, object] = {}

    class FakeResponse:
        content = b'{"id":"discord-media-123"}'

        def raise_for_status(self):
            return None

        def json(self):
            return {"id": "discord-media-123"}

    def fake_post(url, *, params=None, json=None, data=None, files=None, timeout=30):
        captured["url"] = url
        captured["params"] = params
        captured["json"] = json
        captured["data"] = data
        captured["files"] = files
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("app.adapters.discord.requests.post", fake_post)

    process_delivery_queue(session, AlertDispatcher(), run_id="run-discord-multipart")

    session.refresh(post)
    assert captured["params"] == {"wait": "true"}
    assert captured["json"] is None
    assert captured["data"] == {"payload_json": "{\"content\": \"Hello with media\\nSource: https://example.com/source\"}"}
    assert isinstance(captured["files"], dict)
    assert "files[0]" in captured["files"]
    assert post.delivery_jobs[0].status == "posted"
    assert post.delivery_jobs[0].external_id == "discord-media-123"
