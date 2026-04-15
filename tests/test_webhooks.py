from __future__ import annotations

import os

from app.config import reload_settings
from app.services.webhooks import send_discord_webhook_payload, send_test_webhook_payload, send_webhook_payload


APP_SETTING_KEYS = [
    "APP_ENV_FILE",
    "APP_INSTANCE_NAME",
    "WEBHOOK_LOGGING_ENABLED",
    "WEBHOOK_LOGGING_ENDPOINT",
    "WEBHOOK_LOGGING_BEARER_TOKEN",
    "WEBHOOK_LOGGING_TIMEOUT_SECONDS",
    "WEBHOOK_LOGGING_RETRY_COUNT",
    "DISCORD_NOTIFICATION_WEBHOOK_ENABLED",
    "DISCORD_NOTIFICATION_WEBHOOK_URL",
    "DISCORD_NOTIFICATION_WEBHOOK_USERNAME",
]


class _FakeResponse:
    def raise_for_status(self) -> None:
        return None


def _restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    reload_settings()


def test_send_webhook_payload_posts_json_with_bearer_token(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    previous = {key: os.environ.get(key) for key in APP_SETTING_KEYS}
    captured: list[dict[str, object]] = []
    try:
        for key in APP_SETTING_KEYS:
            os.environ.pop(key, None)
        os.environ["APP_ENV_FILE"] = str(env_path)
        os.environ["WEBHOOK_LOGGING_ENABLED"] = "true"
        os.environ["WEBHOOK_LOGGING_ENDPOINT"] = "https://ha.example/api/webhook/test"
        os.environ["WEBHOOK_LOGGING_BEARER_TOKEN"] = "token-123"
        os.environ["WEBHOOK_LOGGING_TIMEOUT_SECONDS"] = "7"
        os.environ["WEBHOOK_LOGGING_RETRY_COUNT"] = "1"
        reload_settings()

        def fake_post(url, json, headers, timeout):
            captured.append(
                {
                    "url": url,
                    "json": json,
                    "headers": headers,
                    "timeout": timeout,
                }
            )
            return _FakeResponse()

        monkeypatch.setattr("app.services.webhooks.requests.post", fake_post)

        send_webhook_payload({"event_type": "test", "severity": "info", "message": "hello"})

        assert captured == [
            {
                "url": "https://ha.example/api/webhook/test",
                "json": {"event_type": "test", "severity": "info", "message": "hello"},
                "headers": {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer token-123",
                },
                "timeout": 7,
            }
        ]
    finally:
        _restore_env(previous)


def test_send_discord_webhook_payload_posts_formatted_embed(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    previous = {key: os.environ.get(key) for key in APP_SETTING_KEYS}
    captured: list[dict[str, object]] = []
    try:
        for key in APP_SETTING_KEYS:
            os.environ.pop(key, None)
        os.environ["APP_ENV_FILE"] = str(env_path)
        os.environ["DISCORD_NOTIFICATION_WEBHOOK_ENABLED"] = "true"
        os.environ["DISCORD_NOTIFICATION_WEBHOOK_URL"] = "https://discord.example/api/webhooks/test"
        os.environ["DISCORD_NOTIFICATION_WEBHOOK_USERNAME"] = "Ops Bot"
        os.environ["WEBHOOK_LOGGING_TIMEOUT_SECONDS"] = "9"
        reload_settings()

        def fake_post(url, json, headers, timeout):
            captured.append(
                {
                    "url": url,
                    "json": json,
                    "headers": headers,
                    "timeout": timeout,
                }
            )
            return _FakeResponse()

        monkeypatch.setattr("app.services.webhooks.requests.post", fake_post)

        send_discord_webhook_payload(
            {
                "event_type": "publish_failed",
                "severity": "error",
                "message": "Publish failed",
                "instance": "LynxPoster",
                "persona_name": "Savannah",
                "account_label": "Telegram",
                "service": "telegram",
                "operation": "publish",
                "payload": {"retry": 1},
            }
        )

        assert len(captured) == 1
        request = captured[0]
        assert request["url"] == "https://discord.example/api/webhooks/test"
        assert request["headers"] == {"Content-Type": "application/json"}
        assert request["timeout"] == 9
        assert request["json"]["username"] == "Ops Bot"
        assert request["json"]["embeds"][0]["title"] == "ERROR | publish_failed"
        assert request["json"]["embeds"][0]["fields"][1]["value"] == "Savannah"
    finally:
        _restore_env(previous)


def test_send_test_webhook_payload_routes_to_requested_destination(monkeypatch):
    captured: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr("app.services.webhooks.send_webhook_payload", lambda payload, force=False: captured.append(("generic", payload)))
    monkeypatch.setattr("app.services.webhooks.send_discord_webhook_payload", lambda payload, force=False: captured.append(("discord", payload)))

    payload = {"event_type": "settings_test", "severity": "info", "message": "hello"}

    send_test_webhook_payload(payload)
    send_test_webhook_payload(payload, destination="discord")

    assert captured == [("generic", payload), ("discord", payload)]
