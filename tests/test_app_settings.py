from __future__ import annotations

import os
from pathlib import Path

from app.config import get_settings, reload_settings
from app.schemas import AppSettingsUpdate
from app.services.alerts import AlertDispatcher
from app.services.app_settings import send_settings_test_webhook, update_app_settings
from app.services.scheduler import CrossposterScheduler


APP_SETTING_KEYS = [
    "APP_ENV_FILE",
    "APP_CONFIG_DIR",
    "APP_BASE_URL",
    "APP_INSTANCE_NAME",
    "APP_PORT",
    "SCHEDULER_AUTORUN_INTERVAL_SECONDS",
    "WEBHOOK_LOGGING_ENABLED",
    "WEBHOOK_LOGGING_ENDPOINT",
    "WEBHOOK_LOGGING_BEARER_TOKEN",
    "WEBHOOK_LOGGING_TIMEOUT_SECONDS",
    "WEBHOOK_LOGGING_RETRY_COUNT",
    "WEBHOOK_LOGGING_MIN_SEVERITY",
    "DISCORD_NOTIFICATION_WEBHOOK_ENABLED",
    "DISCORD_NOTIFICATION_WEBHOOK_URL",
    "DISCORD_NOTIFICATION_WEBHOOK_USERNAME",
    "DISCORD_NOTIFICATION_MIN_SEVERITY",
    "AUTH_OIDC_ENABLED",
    "AUTH_OIDC_ISSUER_URL",
    "AUTH_OIDC_CLIENT_ID",
    "AUTH_OIDC_CLIENT_SECRET",
    "AUTH_OIDC_SCOPE",
    "AUTH_OIDC_GROUPS_CLAIM",
    "AUTH_OIDC_USERNAME_CLAIM",
    "AUTH_OIDC_ADMIN_GROUPS",
    "AUTH_OIDC_USER_GROUPS",
    "AUTH_SESSION_SECRET",
    "INSTAGRAM_WEBHOOKS_ENABLED",
    "INSTAGRAM_WEBHOOK_VERIFY_TOKEN",
    "INSTAGRAM_APP_SECRET",
]


class _FakeJob:
    def __init__(self) -> None:
        self.next_run_time = None


class _FakeScheduler:
    def __init__(self) -> None:
        self.job = _FakeJob()
        self.rescheduled_seconds: int | None = None
        self.paused = False

    def get_job(self, job_id: str):
        return self.job

    def pause_job(self, job_id: str) -> None:
        self.paused = True

    def resume_job(self, job_id: str) -> None:
        self.paused = False

    def reschedule_job(self, job_id: str, trigger: str, seconds: int) -> None:
        self.rescheduled_seconds = seconds


def _restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    reload_settings()


def test_update_app_settings_writes_env_and_refreshes_runtime(tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text("EXTRA_SETTING=keep\nWEBHOOK_LOGGING_ENDPOINT=https://old.example/webhook\n", encoding="utf-8")
    previous = {key: os.environ.get(key) for key in APP_SETTING_KEYS}
    try:
        for key in APP_SETTING_KEYS:
            os.environ.pop(key, None)
        os.environ["APP_ENV_FILE"] = str(env_path)
        reload_settings()

        updated = update_app_settings(
            AppSettingsUpdate(
                instance_name="Test Node",
                app_base_url="https://lynxposter.example.com",
                app_port=8123,
                scheduler_automation_interval_seconds=600,
                webhook_logging_enabled=True,
                webhook_logging_endpoint="https://ha.example/api/webhook/lynxposter",
                webhook_logging_bearer_token="bearer-secret",
                webhook_logging_timeout_seconds=7,
                webhook_logging_retry_count=4,
                webhook_logging_min_severity="error",
                discord_notification_enabled=True,
                discord_notification_webhook_url="https://discord.com/api/webhooks/123/test",
                discord_notification_username="LynxPoster Bot",
                discord_notification_min_severity="critical",
                auth_oidc_enabled=True,
                auth_oidc_issuer_url="https://auth.example.com",
                auth_oidc_client_id="lynxposter",
                auth_oidc_client_secret="super-secret",
                auth_oidc_scope="openid profile email",
                auth_oidc_groups_claim="groups",
                auth_oidc_username_claim="preferred_username",
                auth_oidc_admin_groups="admins",
                auth_oidc_user_groups="users",
                auth_session_secret="session-secret",
                instagram_webhooks_enabled=True,
                instagram_webhook_verify_token="verify-me",
                instagram_app_secret="instagram-secret",
            )
        )

        text = env_path.read_text(encoding="utf-8")
        assert "EXTRA_SETTING=keep" in text
        assert "APP_INSTANCE_NAME=Test Node" in text
        assert "APP_BASE_URL=https://lynxposter.example.com" in text
        assert "WEBHOOK_LOGGING_ENDPOINT=https://ha.example/api/webhook/lynxposter" in text
        assert "DISCORD_NOTIFICATION_WEBHOOK_URL=https://discord.com/api/webhooks/123/test" in text
        assert "AUTH_OIDC_ENABLED=true" in text
        assert "AUTH_OIDC_ISSUER_URL=https://auth.example.com" in text
        assert "INSTAGRAM_WEBHOOKS_ENABLED=true" in text
        assert "INSTAGRAM_WEBHOOK_VERIFY_TOKEN=verify-me" in text
        assert updated.instance_name == "Test Node"
        assert updated.app_base_url == "https://lynxposter.example.com"
        assert updated.scheduler_automation_interval_seconds == 600

        current = get_settings()
        assert current.instance_name == "Test Node"
        assert current.app_base_url == "https://lynxposter.example.com"
        assert current.app_port == 8123
        assert current.webhook_logging_enabled is True
        assert current.webhook_logging_retry_count == 4
        assert current.discord_notification_enabled is True
        assert current.discord_notification_username == "LynxPoster Bot"
        assert current.auth_oidc_enabled is True
        assert current.auth_oidc_admin_groups == "admins"
        assert current.instagram_webhooks_enabled is True
        assert current.instagram_webhook_verify_token == "verify-me"
    finally:
        _restore_env(previous)


def test_update_app_settings_normalizes_oidc_scope_before_write(tmp_path):
    env_path = tmp_path / ".env"
    previous = {key: os.environ.get(key) for key in APP_SETTING_KEYS}
    try:
        for key in APP_SETTING_KEYS:
            os.environ.pop(key, None)
        os.environ["APP_ENV_FILE"] = str(env_path)
        reload_settings()

        updated = update_app_settings(
            AppSettingsUpdate(
                instance_name="OIDC Scope Test",
                app_port=8000,
                scheduler_automation_interval_seconds=300,
                webhook_logging_enabled=False,
                webhook_logging_endpoint="",
                webhook_logging_bearer_token="",
                webhook_logging_timeout_seconds=10,
                webhook_logging_retry_count=2,
                webhook_logging_min_severity="warning",
                discord_notification_enabled=False,
                discord_notification_webhook_url="",
                discord_notification_username="LynxPoster",
                discord_notification_min_severity="warning",
                auth_oidc_enabled=True,
                auth_oidc_scope=" openid, profile   email   groups groups ",
            )
        )

        text = env_path.read_text(encoding="utf-8")
        assert "AUTH_OIDC_SCOPE=openid profile email groups" in text
        assert updated.auth_oidc_scope == "openid profile email groups"
        assert get_settings().auth_oidc_scope == "openid profile email groups"
    finally:
        _restore_env(previous)


def test_send_settings_test_webhook_uses_current_saved_endpoints(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    previous = {key: os.environ.get(key) for key in APP_SETTING_KEYS}
    captured: list[dict[str, object]] = []
    try:
        for key in APP_SETTING_KEYS:
            os.environ.pop(key, None)
        os.environ["APP_ENV_FILE"] = str(env_path)
        reload_settings()

        update_app_settings(
            AppSettingsUpdate(
                instance_name="Webhook Test",
                app_port=8000,
                scheduler_automation_interval_seconds=300,
                webhook_logging_enabled=False,
                webhook_logging_endpoint="https://ha.example/api/webhook/test",
                webhook_logging_bearer_token="token-123",
                webhook_logging_timeout_seconds=5,
                webhook_logging_retry_count=1,
                webhook_logging_min_severity="warning",
                discord_notification_enabled=True,
                discord_notification_webhook_url="https://discord.com/api/webhooks/test",
                discord_notification_username="Ops Bot",
                discord_notification_min_severity="error",
            )
        )

        def fake_send(payload, *, destination="generic"):
            captured.append({"destination": destination, "payload": payload})

        monkeypatch.setattr("app.services.app_settings.send_test_webhook_payload", fake_send)

        send_settings_test_webhook()

        assert len(captured) == 2
        generic = next(item for item in captured if item["destination"] == "generic")
        discord = next(item for item in captured if item["destination"] == "discord")

        assert generic["payload"]["event_type"] == "settings_test"
        assert generic["payload"]["operation"] == "test_webhook"
        assert generic["payload"]["payload"]["source"] == "settings_page"
        assert discord["payload"]["instance"] == "Webhook Test"
    finally:
        _restore_env(previous)


def test_scheduler_refresh_configuration_uses_updated_interval(tmp_path):
    env_path = tmp_path / ".env"
    previous = {key: os.environ.get(key) for key in APP_SETTING_KEYS}
    try:
        for key in APP_SETTING_KEYS:
            os.environ.pop(key, None)
        os.environ["APP_ENV_FILE"] = str(env_path)
        reload_settings()

        scheduler = CrossposterScheduler(AlertDispatcher())
        scheduler._scheduler = _FakeScheduler()
        scheduler.refresh_configuration()
        assert scheduler.get_status()["autorun_interval_seconds"] == 300

        update_app_settings(
            AppSettingsUpdate(
                instance_name="Scheduler Test",
                app_port=8000,
                scheduler_automation_interval_seconds=900,
                webhook_logging_enabled=False,
                webhook_logging_endpoint="",
                webhook_logging_bearer_token="",
                webhook_logging_timeout_seconds=10,
                webhook_logging_retry_count=2,
                webhook_logging_min_severity="warning",
                discord_notification_enabled=False,
                discord_notification_webhook_url="",
                discord_notification_username="LynxPoster",
                discord_notification_min_severity="warning",
            )
        )

        scheduler.refresh_configuration()

        assert scheduler._scheduler.rescheduled_seconds == 900
        assert scheduler.get_status()["autorun_interval_seconds"] == 900
    finally:
        _restore_env(previous)


def test_settings_default_env_file_lives_under_app_data_config(tmp_path):
    previous = {key: os.environ.get(key) for key in APP_SETTING_KEYS + ["APP_DATA_DIR"]}
    try:
        for key in APP_SETTING_KEYS:
            os.environ.pop(key, None)
        os.environ["APP_DATA_DIR"] = str(tmp_path / "appdata")
        reload_settings()

        current = get_settings()

        assert current.config_dir == Path(os.environ["APP_DATA_DIR"]) / "config"
        assert current.env_file_path == Path(os.environ["APP_DATA_DIR"]) / "config" / ".env"
    finally:
        _restore_env(previous)
        if previous.get("APP_DATA_DIR") is None:
            os.environ.pop("APP_DATA_DIR", None)
