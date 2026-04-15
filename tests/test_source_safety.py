from __future__ import annotations

from datetime import datetime, timezone

from app.adapters.instagram import InstagramSourceAdapter
from app.adapters.mastodon import MastodonSourceAdapter
from app.adapters.telegram import TelegramSourceAdapter
from app.domain import CanonicalPostPayload, ExternalPostRefPayload
from app.models import AccountSyncState, CanonicalPost, DeliveryJob, RunEvent
from app.services.alerts import AlertDispatcher
from app.services.delivery import poll_sources, process_delivery_queue
from app.services.personas import create_account, create_persona, get_persona, replace_routes
from app.services.posts import upsert_polled_post


def _create_persona(session, *, slug: str, import_existing_posts: bool = False):
    return create_persona(
        session,
        {
            "name": "Persona",
            "slug": slug,
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {"post_time_limit": 12, "import_existing_posts": import_existing_posts},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def _create_account(session, persona, *, service: str, label: str, source_enabled: bool, destination_enabled: bool, credentials_json: dict):
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
            "credentials_json": credentials_json,
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


def test_mastodon_first_scan_sets_baseline_without_importing_history(session, monkeypatch):
    persona = _create_persona(session, slug="mastodon-baseline")
    account = _create_account(
        session,
        persona,
        service="mastodon",
        label="Mastodon",
        source_enabled=True,
        destination_enabled=False,
        credentials_json={"instance": "https://example.social", "token": "secret"},
    )

    class FakeClient:
        def account_verify_credentials(self):
            return {"id": "acct-1"}

        def account_statuses(self, account_id, since_id=None, exclude_reblogs=True, limit=40):
            return [
                {
                    "id": "101",
                    "created_at": datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
                    "content": "<p>Old post</p>",
                    "media_attachments": [],
                    "url": "https://example.social/@me/101",
                    "visibility": "public",
                },
                {
                    "id": "102",
                    "created_at": datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc),
                    "content": "<p>Newest old post</p>",
                    "media_attachments": [],
                    "url": "https://example.social/@me/102",
                    "visibility": "public",
                },
            ]

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())

    result = MastodonSourceAdapter().poll(session, persona, account, AccountSyncState(source_account_id=account.id, state_json={}))

    assert result.posts == []
    assert result.cursor == "102"
    assert result.next_state["last_seen_id"] == "102"
    assert "without importing historical posts" in (result.note or "")


def test_instagram_first_scan_sets_baseline_without_importing_history(session, monkeypatch):
    persona = _create_persona(session, slug="instagram-baseline")
    account = _create_account(
        session,
        persona,
        service="instagram",
        label="Instagram",
        source_enabled=True,
        destination_enabled=False,
        credentials_json={"api_key": "secret"},
    )

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "data": [
                    {
                        "id": "media-1",
                        "caption": "Hello",
                        "media_url": "https://example.com/1.jpg",
                        "permalink": "https://instagram.example/p/1",
                        "timestamp": "2026-04-12T12:00:00+00:00",
                        "media_type": "IMAGE",
                    }
                ]
            }

    monkeypatch.setattr("app.adapters.instagram.requests.get", lambda *args, **kwargs: FakeResponse())
    monkeypatch.setattr("app.adapters.instagram.now_utc", lambda: datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc))
    monkeypatch.setattr("app.adapters.common.now_utc", lambda: datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc))

    result = InstagramSourceAdapter().poll(session, persona, account, AccountSyncState(source_account_id=account.id, state_json={}))

    assert result.posts == []
    assert "without importing historical posts" in (result.note or "")
    assert result.next_state["last_seen_at"].startswith("2026-04-14T00:00:00")


def test_telegram_first_scan_sets_baseline_without_importing_history(session, monkeypatch):
    persona = _create_persona(session, slug="telegram-baseline")
    account = _create_account(
        session,
        persona,
        service="telegram",
        label="Telegram",
        source_enabled=True,
        destination_enabled=True,
        credentials_json={"bot_token": "telegram-secret", "channel_id": "-100123456"},
    )

    def fake_request(bot_token, method, *, json_body=None, data=None, files=None, timeout=30):
        assert bot_token == "telegram-secret"
        if method == "getWebhookInfo":
            return {"url": ""}
        if method == "getUpdates":
            return [
                {
                    "update_id": 200,
                    "channel_post": {
                        "message_id": 10,
                        "chat": {"id": -100123456, "type": "channel", "title": "Test Channel"},
                        "date": int(datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc).timestamp()),
                        "text": "Historical Telegram post",
                    },
                }
            ]
        raise AssertionError(f"Unexpected Telegram method {method}")

    monkeypatch.setattr("app.adapters.telegram._telegram_request", fake_request)
    monkeypatch.setattr("app.adapters.telegram.now_utc", lambda: datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc))
    monkeypatch.setattr("app.adapters.common.now_utc", lambda: datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc))

    result = TelegramSourceAdapter().poll(session, persona, account, AccountSyncState(source_account_id=account.id, state_json={}))

    assert result.posts == []
    assert result.cursor == "201"
    assert "without importing historical posts" in (result.note or "")
    assert result.next_state["last_seen_at"].startswith("2026-04-14T00:00:00")


def test_historical_import_backfill_is_cancelled_before_publish(session, monkeypatch):
    persona = _create_persona(session, slug="historical-backfill")
    source_account = _create_account(
        session,
        persona,
        service="mastodon",
        label="Mastodon",
        source_enabled=True,
        destination_enabled=False,
        credentials_json={"instance": "https://example.social", "token": "secret"},
    )
    destination_account = _create_account(
        session,
        persona,
        service="bluesky",
        label="Bluesky",
        source_enabled=False,
        destination_enabled=True,
        credentials_json={"handle": "me.bsky.social", "password": "pw"},
    )

    replace_routes(
        session,
        get_persona(session, persona.id),
        [{"source_account_id": source_account.id, "destination_account_id": destination_account.id, "is_enabled": True}],
    )

    post = upsert_polled_post(
        session,
        get_persona(session, persona.id),
        source_account,
        CanonicalPostPayload(
            body="Very old imported post",
            published_at=datetime(2026, 1, 10, 12, 0, tzinfo=timezone.utc),
            external_refs=[ExternalPostRefPayload(external_id="historic-1", external_url="https://example.social/@me/historic-1")],
        ),
    )

    publish_called = {"value": False}

    class FakeDestinationAdapter:
        def validate(self, post, persona, account):
            return []

        def publish(self, session, post, persona, account, *, context=None):
            publish_called["value"] = True
            raise AssertionError("Historical backfill should not reach live publish.")

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", lambda account: FakeDestinationAdapter())

    process_delivery_queue(session, AlertDispatcher(), run_id="run-backfill")

    job = session.query(DeliveryJob).one()
    assert publish_called["value"] is False
    assert job.status == "cancelled"
    assert "historical backfill" in (job.last_error or "").lower()
    assert post.status == "cancelled"


def test_autorun_blocks_first_sync_historical_import_accounts(session, monkeypatch):
    persona = _create_persona(session, slug="autorun-guard", import_existing_posts=True)
    account = _create_account(
        session,
        persona,
        service="mastodon",
        label="Mastodon",
        source_enabled=True,
        destination_enabled=False,
        credentials_json={"instance": "https://example.social", "token": "secret"},
    )

    poll_called = {"value": False}

    class FakeSourceAdapter:
        def poll(self, session, persona, account, sync_state):
            poll_called["value"] = True
            raise AssertionError("Autorun guard should prevent first-sync historical imports from polling.")

    monkeypatch.setattr("app.services.delivery.get_source_adapter_for_account", lambda account: FakeSourceAdapter())

    poll_sources(session, AlertDispatcher(), run_id="autorun-guard", trigger="autorun")

    assert poll_called["value"] is False
    events = session.query(RunEvent).filter(RunEvent.operation == "poll").all()
    assert any("manual-only" in event.message for event in events)


def test_manual_run_can_still_poll_first_sync_import_account(session, monkeypatch):
    persona = _create_persona(session, slug="manual-guard", import_existing_posts=True)
    account = _create_account(
        session,
        persona,
        service="mastodon",
        label="Mastodon",
        source_enabled=True,
        destination_enabled=False,
        credentials_json={"instance": "https://example.social", "token": "secret"},
    )

    class FakeSourceAdapter:
        def poll(self, session, persona, account, sync_state):
            return MastodonSourceAdapter().poll(session, persona, account, sync_state)

    class FakeClient:
        def account_verify_credentials(self):
            return {"id": "acct-1"}

        def account_statuses(self, account_id, since_id=None, exclude_reblogs=True, limit=40):
            return [
                {
                    "id": "101",
                    "created_at": datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc),
                    "content": "<p>Imported on manual run</p>",
                    "media_attachments": [],
                    "url": "https://example.social/@me/101",
                    "visibility": "public",
                }
            ]

    monkeypatch.setattr("app.services.delivery.get_source_adapter_for_account", lambda account: FakeSourceAdapter())
    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())
    monkeypatch.setattr("app.adapters.common.now_utc", lambda: datetime(2026, 4, 12, 18, 0, tzinfo=timezone.utc))

    poll_sources(session, AlertDispatcher(), run_id="manual-guard", trigger="manual")

    assert session.query(CanonicalPost).filter(CanonicalPost.origin_kind == "account_import").count() == 1
    assert session.query(RunEvent).filter(RunEvent.operation == "poll", RunEvent.severity == "warning").count() == 0
