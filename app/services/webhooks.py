from __future__ import annotations

import json
from typing import Any

import requests

from app.config import get_settings

SEVERITY_ORDER = {
    "debug": 10,
    "info": 20,
    "warning": 30,
    "error": 40,
    "critical": 50,
}


def severity_meets_threshold(severity: str, threshold: str) -> bool:
    return SEVERITY_ORDER.get(severity.lower(), 0) >= SEVERITY_ORDER.get(threshold.lower(), 30)


DISCORD_SEVERITY_COLORS = {
    "debug": 0x95A5A6,
    "info": 0x3498DB,
    "warning": 0xF1C40F,
    "error": 0xE74C3C,
    "critical": 0x8E0000,
}


def send_webhook_payload(payload: dict[str, Any], *, force: bool = False) -> None:
    settings = get_settings()
    if not force and (not settings.webhook_logging_enabled or not settings.webhook_logging_endpoint):
        return
    if not settings.webhook_logging_endpoint:
        return

    headers = {"Content-Type": "application/json"}
    if settings.webhook_logging_bearer_token:
        headers["Authorization"] = f"Bearer {settings.webhook_logging_bearer_token}"

    last_error: Exception | None = None
    for _ in range(max(settings.webhook_logging_retry_count, 0) + 1):
        try:
            response = requests.post(
                settings.webhook_logging_endpoint,
                json=payload,
                headers=headers,
                timeout=settings.webhook_logging_timeout_seconds,
            )
            response.raise_for_status()
            return
        except Exception as exc:  # pragma: no cover - callers validate fallback behavior.
            last_error = exc
    if last_error:
        raise last_error


def send_discord_webhook_payload(payload: dict[str, Any], *, force: bool = False) -> None:
    settings = get_settings()
    if not force and (not settings.discord_notification_enabled or not settings.discord_notification_webhook_url):
        return
    if not settings.discord_notification_webhook_url:
        return

    last_error: Exception | None = None
    discord_payload = _build_discord_webhook_payload(payload, settings.discord_notification_username or "LynxPoster")
    headers = {"Content-Type": "application/json"}
    for _ in range(max(settings.webhook_logging_retry_count, 0) + 1):
        try:
            response = requests.post(
                settings.discord_notification_webhook_url,
                json=discord_payload,
                headers=headers,
                timeout=settings.webhook_logging_timeout_seconds,
            )
            response.raise_for_status()
            return
        except Exception as exc:  # pragma: no cover - callers validate fallback behavior.
            last_error = exc
    if last_error:
        raise last_error


def send_test_webhook_payload(payload: dict[str, Any], *, destination: str = "generic") -> None:
    if destination == "discord":
        send_discord_webhook_payload(payload, force=True)
        return
    send_webhook_payload(payload, force=True)


def _build_discord_webhook_payload(payload: dict[str, Any], username: str) -> dict[str, Any]:
    severity = str(payload.get("severity", "info")).lower()
    persona_label = payload.get("persona_name") or payload.get("persona_id") or "No persona"
    account_label = payload.get("account_label") or payload.get("account_id") or "No account"
    service_label = payload.get("service") or "n/a"
    operation_label = payload.get("operation") or "n/a"
    details = payload.get("payload") or {}
    serialized_details = json.dumps(details, ensure_ascii=True, default=str)
    if len(serialized_details) > 900:
        serialized_details = f"{serialized_details[:897]}..."

    embed = {
        "title": f"{severity.upper()} | {payload.get('event_type', 'notification')}",
        "description": str(payload.get("message", ""))[:4096],
        "color": DISCORD_SEVERITY_COLORS.get(severity, DISCORD_SEVERITY_COLORS["info"]),
        "timestamp": payload.get("timestamp"),
        "fields": [
            {"name": "Instance", "value": str(payload.get("instance") or "unknown"), "inline": True},
            {"name": "Persona", "value": str(persona_label)[:1024], "inline": True},
            {"name": "Account", "value": str(account_label)[:1024], "inline": True},
            {"name": "Service", "value": str(service_label)[:1024], "inline": True},
            {"name": "Operation", "value": str(operation_label)[:1024], "inline": True},
        ],
        "footer": {"text": "LynxPoster"},
    }
    if payload.get("post_id"):
        embed["fields"].append({"name": "Post", "value": str(payload["post_id"])[:1024], "inline": False})
    if payload.get("delivery_job_id"):
        embed["fields"].append({"name": "Delivery Job", "value": str(payload["delivery_job_id"])[:1024], "inline": False})
    if details:
        embed["fields"].append({"name": "Payload", "value": f"```json\n{serialized_details}\n```", "inline": False})

    return {
        "username": username[:80] if username else "LynxPoster",
        "embeds": [embed],
    }
