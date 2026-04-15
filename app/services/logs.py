from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.orm import Session, selectinload

from app.models import Account, AlertEvent, Persona, RunEvent
from app.schemas import AlertEventRead, RunEventRead


def _apply_common_filters(stmt: Select, model, filters: dict[str, str | None], *, owner_user_id: str | None = None) -> Select:
    if owner_user_id is not None:
        stmt = stmt.join(Persona, Persona.id == model.persona_id).where(Persona.owner_user_id == owner_user_id)
    if filters.get("persona_id"):
        stmt = stmt.where(model.persona_id == filters["persona_id"])
    if filters.get("account_id"):
        stmt = stmt.where(model.account_id == filters["account_id"])
    if filters.get("service"):
        stmt = stmt.where(model.service == filters["service"])
    if filters.get("severity"):
        stmt = stmt.where(model.severity == filters["severity"])
    if filters.get("operation"):
        stmt = stmt.where(model.operation == filters["operation"])
    if filters.get("since"):
        since = datetime.fromisoformat(filters["since"])
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        stmt = stmt.where(model.created_at >= since)
    return stmt


def list_run_events(
    session: Session,
    filters: dict[str, str | None] | None = None,
    limit: int = 100,
    *,
    owner_user_id: str | None = None,
) -> list[RunEvent]:
    stmt = (
        select(RunEvent)
        .options(selectinload(RunEvent.persona), selectinload(RunEvent.account).selectinload(Account.persona))
        .order_by(RunEvent.created_at.desc())
        .limit(limit)
    )
    stmt = _apply_common_filters(stmt, RunEvent, filters or {}, owner_user_id=owner_user_id)
    return list(session.scalars(stmt))


def list_alert_events(
    session: Session,
    filters: dict[str, str | None] | None = None,
    limit: int = 100,
    *,
    owner_user_id: str | None = None,
) -> list[AlertEvent]:
    stmt = (
        select(AlertEvent)
        .options(selectinload(AlertEvent.persona), selectinload(AlertEvent.account).selectinload(Account.persona))
        .order_by(AlertEvent.created_at.desc())
        .limit(limit)
    )
    stmt = _apply_common_filters(stmt, AlertEvent, filters or {}, owner_user_id=owner_user_id)
    return list(session.scalars(stmt))


def clear_alert_events(session: Session, filters: dict[str, str | None] | None = None, *, owner_user_id: str | None = None) -> int:
    stmt = select(AlertEvent)
    stmt = _apply_common_filters(stmt, AlertEvent, filters or {}, owner_user_id=owner_user_id)
    alerts = list(session.scalars(stmt))
    for alert in alerts:
        session.delete(alert)
    session.flush()
    return len(alerts)


def recent_logs_filter_window(hours: int = 24) -> dict[str, str]:
    return {"since": (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()}


def _resolve_persona_name(
    *,
    explicit_name: str | None,
    persona,
    account: Account | None,
) -> str | None:
    if explicit_name:
        return explicit_name
    if persona is not None:
        return persona.name
    if account is not None and account.persona is not None:
        return account.persona.name
    return None


def serialize_run_event(event: RunEvent) -> RunEventRead:
    metadata = dict(event.metadata_json or {})
    persona_name = _resolve_persona_name(
        explicit_name=metadata.get("_persona_name"),
        persona=event.persona,
        account=event.account,
    )
    account_label = event.account.label if event.account else None
    return RunEventRead(
        id=event.id,
        run_id=event.run_id,
        persona_id=event.persona_id,
        persona_name=persona_name,
        account_id=event.account_id,
        account_label=account_label,
        service=event.service,
        operation=event.operation,
        severity=event.severity,
        message=event.message,
        post_id=event.post_id,
        delivery_job_id=event.delivery_job_id,
        metadata_json=metadata,
        created_at=event.created_at,
    )


def serialize_alert_event(event: AlertEvent) -> AlertEventRead:
    payload = dict(event.payload_json or {})
    persona_name = _resolve_persona_name(
        explicit_name=payload.get("persona_name"),
        persona=event.persona,
        account=event.account,
    )
    account_label = event.account.label if event.account else None
    return AlertEventRead(
        id=event.id,
        run_id=event.run_id,
        fingerprint=event.fingerprint,
        event_type=event.event_type,
        severity=event.severity,
        persona_id=event.persona_id,
        persona_name=persona_name,
        account_id=event.account_id,
        account_label=account_label,
        service=event.service,
        operation=event.operation,
        post_id=event.post_id,
        delivery_job_id=event.delivery_job_id,
        message=event.message,
        error_class=event.error_class,
        retry_count=event.retry_count,
        payload_json=payload,
        created_at=event.created_at,
    )


_SEVERITY_RANK = {
    "debug": 0,
    "info": 1,
    "warning": 2,
    "error": 3,
    "critical": 4,
}


def _highest_severity(events: list[RunEventRead]) -> str:
    if not events:
        return "info"
    return max(events, key=lambda event: _SEVERITY_RANK.get(event.severity, 1)).severity


def _event_posts_found(event: RunEventRead) -> int:
    raw_value = (event.metadata_json or {}).get("imported_count")
    try:
        return max(0, int(raw_value or 0))
    except (TypeError, ValueError):
        return 0


def _event_reposted_count(event: RunEventRead) -> int:
    if event.operation != "publish" or event.severity == "error":
        return 0
    return 1 if event.message.startswith("Published post ") else 0


def _event_queued_count(event: RunEventRead) -> int:
    if event.operation != "schedule":
        return 0
    return 1 if " is now queued" in event.message else 0


def _event_trigger(event: RunEventRead) -> str | None:
    return (event.metadata_json or {}).get("trigger")


def _count_summary(events: list[RunEventRead]) -> dict[str, int]:
    persona_ids = {event.persona_id or event.persona_name for event in events if event.persona_id or event.persona_name}
    account_ids = {event.account_id for event in events if event.account_id}
    return {
        "personas": len(persona_ids),
        "accounts": len(account_ids),
        "posts_found": sum(_event_posts_found(event) for event in events),
        "reposted": sum(_event_reposted_count(event) for event in events),
        "queued": sum(_event_queued_count(event) for event in events),
        "errors": sum(1 for event in events if event.severity == "error"),
        "warnings": sum(1 for event in events if event.severity == "warning"),
        "events": len(events),
    }


def summarize_run_events(events: list[RunEventRead], *, limit_runs: int | None = None) -> list[dict[str, Any]]:
    runs_by_id: OrderedDict[str, list[RunEventRead]] = OrderedDict()
    for event in sorted(events, key=lambda item: item.created_at, reverse=True):
        if event.run_id not in runs_by_id:
            if limit_runs is not None and len(runs_by_id) >= limit_runs:
                continue
            runs_by_id[event.run_id] = []
        runs_by_id[event.run_id].append(event)

    run_summaries: list[dict[str, Any]] = []
    for run_id, run_events in runs_by_id.items():
        persona_groups: OrderedDict[str, dict[str, Any]] = OrderedDict()
        system_events: list[RunEventRead] = []
        for event in run_events:
            persona_key = event.persona_id or (f"name:{event.persona_name}" if event.persona_name else "")
            if persona_key:
                group = persona_groups.get(persona_key)
                if group is None:
                    group = {
                        "persona_id": event.persona_id,
                        "persona_name": event.persona_name or "Unknown persona",
                        "events": [],
                    }
                    persona_groups[persona_key] = group
                group["events"].append(event)
            else:
                system_events.append(event)

        persona_summaries: list[dict[str, Any]] = []
        for group in persona_groups.values():
            persona_events = group["events"]
            persona_summaries.append(
                {
                    **group,
                    "counts": _count_summary(persona_events),
                    "severity": _highest_severity(persona_events),
                    "latest_at": max(event.created_at for event in persona_events),
                }
            )

        cycle_events = [event for event in run_events if event.operation == "automation_cycle"]
        summary_event = cycle_events[0] if cycle_events else run_events[0]
        trigger = _event_trigger(summary_event) or next((_event_trigger(event) for event in run_events if _event_trigger(event)), None)

        run_summaries.append(
            {
                "run_id": run_id,
                "trigger": trigger,
                "summary_message": summary_event.message,
                "severity": _highest_severity(run_events),
                "latest_at": max(event.created_at for event in run_events),
                "started_at": min(event.created_at for event in run_events),
                "finished_at": max(event.created_at for event in run_events),
                "counts": _count_summary(run_events),
                "persona_summaries": persona_summaries,
                "system_events": system_events,
                "events": run_events,
            }
        )
    return run_summaries
