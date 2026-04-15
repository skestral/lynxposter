from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import AlertEvent, RunEvent
from app.services.live_updates import (
    LIVE_UPDATE_TOPIC_ALERT_EVENTS,
    LIVE_UPDATE_TOPIC_DASHBOARD,
    LIVE_UPDATE_TOPIC_LOGS,
    LIVE_UPDATE_TOPIC_RUN_EVENTS,
    publish_live_update,
)
from app.services.webhooks import send_discord_webhook_payload, send_webhook_payload, severity_meets_threshold

def _build_run_event_payload(event: RunEvent) -> dict[str, Any]:
    settings = get_settings()
    payload = dict(event.metadata_json or {})
    persona_name = payload.pop("_persona_name", None)
    return {
        "event_type": "run_event",
        "severity": event.severity,
        "timestamp": event.created_at.isoformat(),
        "instance": settings.instance_name,
        "persona_id": event.persona_id,
        "persona_name": persona_name,
        "account_id": event.account_id,
        "account_label": event.account.label if event.account else None,
        "service": event.service,
        "operation": event.operation,
        "post_id": event.post_id,
        "delivery_job_id": event.delivery_job_id,
        "message": event.message,
        "payload": payload,
    }


def _build_alert_payload(event: AlertEvent) -> dict[str, Any]:
    settings = get_settings()
    payload = dict(event.payload_json or {})
    payload.setdefault("event_type", event.event_type)
    payload.setdefault("severity", event.severity)
    payload.setdefault("timestamp", event.created_at.isoformat())
    payload.setdefault("instance", settings.instance_name)
    payload.setdefault("persona_id", event.persona_id)
    payload.setdefault("persona_name", event.persona.name if event.persona else None)
    payload.setdefault("account_id", event.account_id)
    payload.setdefault("account_label", event.account.label if event.account else None)
    payload.setdefault("service", event.service)
    payload.setdefault("operation", event.operation)
    payload.setdefault("post_id", event.post_id)
    payload.setdefault("delivery_job_id", event.delivery_job_id)
    payload.setdefault("message", event.message)
    payload.setdefault("payload", payload.get("payload", {}))
    return payload


def log_run_event(
    session: Session,
    *,
    run_id: str,
    operation: str,
    message: str,
    severity: str = "info",
    persona_id: str | None = None,
    persona_name: str | None = None,
    account_id: str | None = None,
    service: str | None = None,
    post_id: str | None = None,
    delivery_job_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> RunEvent:
    settings = get_settings()
    metadata_json = dict(metadata or {})
    if persona_name:
        metadata_json["_persona_name"] = persona_name
    event = RunEvent(
        run_id=run_id,
        persona_id=persona_id,
        account_id=account_id,
        service=service,
        operation=operation,
        severity=severity,
        message=message,
        post_id=post_id,
        delivery_job_id=delivery_job_id,
        metadata_json=metadata_json,
    )
    session.add(event)
    session.flush()
    if severity_meets_threshold(severity, settings.webhook_logging_min_severity):
        try:
            send_webhook_payload(_build_run_event_payload(event))
        except Exception:
            pass
    if severity_meets_threshold(severity, settings.discord_notification_min_severity):
        try:
            send_discord_webhook_payload(_build_run_event_payload(event))
        except Exception:
            pass
    publish_live_update(
        LIVE_UPDATE_TOPIC_RUN_EVENTS,
        LIVE_UPDATE_TOPIC_DASHBOARD,
        LIVE_UPDATE_TOPIC_LOGS,
    )
    return event


def log_alert_event(session: Session, alert: AlertEvent) -> AlertEvent:
    session.add(alert)
    session.flush()
    try:
        send_webhook_payload(_build_alert_payload(alert))
    except Exception:
        pass
    try:
        send_discord_webhook_payload(_build_alert_payload(alert))
    except Exception:
        pass
    publish_live_update(
        LIVE_UPDATE_TOPIC_ALERT_EVENTS,
        LIVE_UPDATE_TOPIC_DASHBOARD,
        LIVE_UPDATE_TOPIC_LOGS,
    )
    return alert
