from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Any

from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Account, AlertEvent, CanonicalPost, DeliveryJob, Persona
from app.services.events import log_alert_event

class AlertDispatcher:
    def __init__(self) -> None:
        self._seen_by_run: dict[str, set[str]] = defaultdict(set)

    def _fingerprint(self, payload: dict[str, Any]) -> str:
        raw = "|".join(
            [
                str(payload.get("event_type", "")),
                str(payload.get("persona_id", "")),
                str(payload.get("account_id", "")),
                str(payload.get("service", "")),
                str(payload.get("operation", "")),
                str(payload.get("post_id", "")),
                str(payload.get("delivery_job_id", "")),
                str(payload.get("error_class", "")),
                str(payload.get("message", "")),
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def emit_hard_failure(
        self,
        session: Session,
        *,
        run_id: str,
        operation: str,
        message: str,
        persona: Persona | None = None,
        account: Account | None = None,
        service: str | None = None,
        post: CanonicalPost | None = None,
        delivery_job: DeliveryJob | None = None,
        error_class: str | None = None,
        retry_count: int = 0,
        event_type: str = "hard_failure",
        severity: str = "error",
        payload: dict[str, Any] | None = None,
    ) -> AlertEvent | None:
        settings = get_settings()
        full_payload = {
            "event_type": event_type,
            "severity": severity,
            "timestamp": None,
            "instance": settings.instance_name,
            "persona_id": persona.id if persona else None,
            "persona_name": persona.name if persona else None,
            "account_id": account.id if account else None,
            "service": service or (account.service if account else None),
            "operation": operation,
            "post_id": post.id if post else None,
            "delivery_job_id": delivery_job.id if delivery_job else None,
            "message": message,
            "error_class": error_class,
            "retry_count": retry_count,
            "payload": payload or {},
        }
        fingerprint = self._fingerprint(full_payload)
        if fingerprint in self._seen_by_run[run_id]:
            return None
        self._seen_by_run[run_id].add(fingerprint)

        alert = AlertEvent(
            run_id=run_id,
            fingerprint=fingerprint,
            event_type=event_type,
            severity=severity,
            persona_id=full_payload["persona_id"],
            account_id=full_payload["account_id"],
            service=full_payload["service"],
            operation=operation,
            post_id=full_payload["post_id"],
            delivery_job_id=full_payload["delivery_job_id"],
            message=message,
            error_class=error_class,
            retry_count=retry_count,
            payload_json=full_payload,
        )
        log_alert_event(session, alert)
        return alert

    def clear_run(self, run_id: str) -> None:
        self._seen_by_run.pop(run_id, None)
