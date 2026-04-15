from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from app.config import get_settings, reload_settings
from app.schemas import AppSettingsRead, AppSettingsUpdate
from app.services.webhooks import send_test_webhook_payload


APP_SETTINGS_ENV_MAP = {
    "instance_name": "APP_INSTANCE_NAME",
    "app_base_url": "APP_BASE_URL",
    "app_port": "APP_PORT",
    "scheduler_automation_interval_seconds": "SCHEDULER_AUTORUN_INTERVAL_SECONDS",
    "webhook_logging_enabled": "WEBHOOK_LOGGING_ENABLED",
    "webhook_logging_endpoint": "WEBHOOK_LOGGING_ENDPOINT",
    "webhook_logging_bearer_token": "WEBHOOK_LOGGING_BEARER_TOKEN",
    "webhook_logging_timeout_seconds": "WEBHOOK_LOGGING_TIMEOUT_SECONDS",
    "webhook_logging_retry_count": "WEBHOOK_LOGGING_RETRY_COUNT",
    "webhook_logging_min_severity": "WEBHOOK_LOGGING_MIN_SEVERITY",
    "discord_notification_enabled": "DISCORD_NOTIFICATION_WEBHOOK_ENABLED",
    "discord_notification_webhook_url": "DISCORD_NOTIFICATION_WEBHOOK_URL",
    "discord_notification_username": "DISCORD_NOTIFICATION_WEBHOOK_USERNAME",
    "discord_notification_min_severity": "DISCORD_NOTIFICATION_MIN_SEVERITY",
    "auth_oidc_enabled": "AUTH_OIDC_ENABLED",
    "auth_oidc_issuer_url": "AUTH_OIDC_ISSUER_URL",
    "auth_oidc_client_id": "AUTH_OIDC_CLIENT_ID",
    "auth_oidc_client_secret": "AUTH_OIDC_CLIENT_SECRET",
    "auth_oidc_scope": "AUTH_OIDC_SCOPE",
    "auth_oidc_groups_claim": "AUTH_OIDC_GROUPS_CLAIM",
    "auth_oidc_username_claim": "AUTH_OIDC_USERNAME_CLAIM",
    "auth_oidc_admin_groups": "AUTH_OIDC_ADMIN_GROUPS",
    "auth_oidc_user_groups": "AUTH_OIDC_USER_GROUPS",
    "auth_session_secret": "AUTH_SESSION_SECRET",
    "instagram_webhooks_enabled": "INSTAGRAM_WEBHOOKS_ENABLED",
    "instagram_webhook_verify_token": "INSTAGRAM_WEBHOOK_VERIFY_TOKEN",
    "instagram_app_secret": "INSTAGRAM_APP_SECRET",
}


def read_app_settings() -> AppSettingsRead:
    settings = get_settings()
    return AppSettingsRead(
        instance_name=settings.instance_name,
        app_base_url=settings.app_base_url,
        app_port=settings.app_port,
        scheduler_automation_interval_seconds=settings.scheduler_automation_interval_seconds,
        webhook_logging_enabled=settings.webhook_logging_enabled,
        webhook_logging_endpoint=settings.webhook_logging_endpoint,
        webhook_logging_bearer_token=settings.webhook_logging_bearer_token,
        webhook_logging_timeout_seconds=settings.webhook_logging_timeout_seconds,
        webhook_logging_retry_count=settings.webhook_logging_retry_count,
        webhook_logging_min_severity=settings.webhook_logging_min_severity,
        discord_notification_enabled=settings.discord_notification_enabled,
        discord_notification_webhook_url=settings.discord_notification_webhook_url,
        discord_notification_username=settings.discord_notification_username,
        discord_notification_min_severity=settings.discord_notification_min_severity,
        auth_oidc_enabled=settings.auth_oidc_enabled,
        auth_oidc_issuer_url=settings.auth_oidc_issuer_url,
        auth_oidc_client_id=settings.auth_oidc_client_id,
        auth_oidc_client_secret=settings.auth_oidc_client_secret,
        auth_oidc_scope=settings.auth_oidc_scope,
        auth_oidc_groups_claim=settings.auth_oidc_groups_claim,
        auth_oidc_username_claim=settings.auth_oidc_username_claim,
        auth_oidc_admin_groups=settings.auth_oidc_admin_groups,
        auth_oidc_user_groups=settings.auth_oidc_user_groups,
        auth_session_secret=settings.auth_session_secret,
        instagram_webhooks_enabled=settings.instagram_webhooks_enabled,
        instagram_webhook_verify_token=settings.instagram_webhook_verify_token,
        instagram_app_secret=settings.instagram_app_secret,
        config_dir=str(settings.config_dir),
        env_file_path=str(settings.env_file_path),
        app_data_dir=str(settings.app_data_dir),
        database_path=str(settings.database_path),
        uploads_dir=str(settings.uploads_dir),
        imported_media_dir=str(settings.imported_media_dir),
        logs_dir=str(settings.logs_dir),
        backups_dir=str(settings.backups_dir),
        updated_at=datetime.now(timezone.utc),
    )


def update_app_settings(payload: AppSettingsUpdate) -> AppSettingsRead:
    settings = get_settings()
    env_updates = {
        "APP_INSTANCE_NAME": payload.instance_name,
        "APP_BASE_URL": payload.app_base_url,
        "APP_PORT": str(payload.app_port),
        "SCHEDULER_AUTORUN_INTERVAL_SECONDS": str(payload.scheduler_automation_interval_seconds),
        "WEBHOOK_LOGGING_ENABLED": "true" if payload.webhook_logging_enabled else "false",
        "WEBHOOK_LOGGING_ENDPOINT": payload.webhook_logging_endpoint,
        "WEBHOOK_LOGGING_BEARER_TOKEN": payload.webhook_logging_bearer_token,
        "WEBHOOK_LOGGING_TIMEOUT_SECONDS": str(payload.webhook_logging_timeout_seconds),
        "WEBHOOK_LOGGING_RETRY_COUNT": str(payload.webhook_logging_retry_count),
        "WEBHOOK_LOGGING_MIN_SEVERITY": payload.webhook_logging_min_severity,
        "DISCORD_NOTIFICATION_WEBHOOK_ENABLED": "true" if payload.discord_notification_enabled else "false",
        "DISCORD_NOTIFICATION_WEBHOOK_URL": payload.discord_notification_webhook_url,
        "DISCORD_NOTIFICATION_WEBHOOK_USERNAME": payload.discord_notification_username,
        "DISCORD_NOTIFICATION_MIN_SEVERITY": payload.discord_notification_min_severity,
        "AUTH_OIDC_ENABLED": "true" if payload.auth_oidc_enabled else "false",
        "AUTH_OIDC_ISSUER_URL": payload.auth_oidc_issuer_url,
        "AUTH_OIDC_CLIENT_ID": payload.auth_oidc_client_id,
        "AUTH_OIDC_CLIENT_SECRET": payload.auth_oidc_client_secret,
        "AUTH_OIDC_SCOPE": payload.auth_oidc_scope,
        "AUTH_OIDC_GROUPS_CLAIM": payload.auth_oidc_groups_claim,
        "AUTH_OIDC_USERNAME_CLAIM": payload.auth_oidc_username_claim,
        "AUTH_OIDC_ADMIN_GROUPS": payload.auth_oidc_admin_groups,
        "AUTH_OIDC_USER_GROUPS": payload.auth_oidc_user_groups,
        "AUTH_SESSION_SECRET": payload.auth_session_secret,
        "INSTAGRAM_WEBHOOKS_ENABLED": "true" if payload.instagram_webhooks_enabled else "false",
        "INSTAGRAM_WEBHOOK_VERIFY_TOKEN": payload.instagram_webhook_verify_token,
        "INSTAGRAM_APP_SECRET": payload.instagram_app_secret,
    }
    _write_env_updates(settings.env_file_path, env_updates)
    for key, value in env_updates.items():
        os.environ[key] = value
    reload_settings()
    return read_app_settings()


def send_settings_test_webhook() -> None:
    settings = get_settings()
    payload = {
        "event_type": "settings_test",
        "severity": "info",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "instance": settings.instance_name,
        "persona_id": None,
        "persona_name": None,
        "account_id": None,
        "service": "settings",
        "operation": "test_webhook",
        "post_id": None,
        "delivery_job_id": None,
        "message": "LynxPoster settings test webhook",
        "payload": {
            "source": "settings_page",
            "webhook_logging_enabled": settings.webhook_logging_enabled,
            "min_severity": settings.webhook_logging_min_severity,
        },
    }
    attempts = 0
    errors: list[str] = []

    if settings.webhook_logging_endpoint:
        attempts += 1
        try:
            send_test_webhook_payload(payload)
        except Exception as exc:
            errors.append(f"Generic webhook failed: {exc}")

    if settings.discord_notification_webhook_url:
        attempts += 1
        try:
            send_test_webhook_payload(payload, destination="discord")
        except Exception as exc:
            errors.append(f"Discord webhook failed: {exc}")

    if attempts == 0:
        raise ValueError("Configure a Home Assistant webhook endpoint or Discord notification webhook URL before sending a test notification.")
    if errors:
        raise RuntimeError("; ".join(errors))


def _write_env_updates(path: Path, updates: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    updated_keys: set[str] = set()
    new_lines: list[str] = []

    for line in existing_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            new_lines.append(line)
            continue

        key, _value = line.split("=", 1)
        key = key.strip()
        if key in updates:
            new_lines.append(f"{key}={updates[key]}")
            updated_keys.add(key)
        else:
            new_lines.append(line)

    if existing_lines and new_lines and new_lines[-1] != "":
        new_lines.append("")

    for field_name in APP_SETTINGS_ENV_MAP.values():
        if field_name not in updated_keys:
            new_lines.append(f"{field_name}={updates[field_name]}")

    path.write_text("\n".join(new_lines).rstrip("\n") + "\n", encoding="utf-8")
