from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import ceil, floor
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.models import Account
from app.services.alerts import AlertDispatcher

INSTAGRAM_TOKEN_LIFETIME = timedelta(days=60)
INSTAGRAM_TOKEN_WARNING_WINDOW = timedelta(days=5)

INSTAGRAM_TOKEN_RECORDED_AT_KEY = "_lynxposter_instagram_token_recorded_at"
INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY = "_lynxposter_instagram_token_estimated_expires_at"
INSTAGRAM_TOKEN_ALERT_FLAGS_KEY = "_lynxposter_instagram_token_alert_flags"

INSTAGRAM_TOKEN_ALERT_WARNING = "warning_5_days"
INSTAGRAM_TOKEN_ALERT_EXPIRED = "expired"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalized_alert_flags(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    flags: list[str] = []
    for item in value:
        flag = str(item or "").strip()
        if flag and flag not in flags:
            flags.append(flag)
    return flags


def _days_remaining_label(delta: timedelta) -> int:
    total_days = delta.total_seconds() / 86400
    if total_days >= 0:
        return max(0, ceil(total_days))
    return floor(total_days)


def instagram_token_present(account: Account) -> bool:
    return bool(str((account.credentials_json or {}).get("api_key") or "").strip())


def build_instagram_token_status(account: Account, *, now: datetime | None = None) -> dict[str, Any] | None:
    if account.service != "instagram":
        return None

    credentials = dict(account.credentials_json or {})
    token = str(credentials.get("api_key") or "").strip()
    if not token:
        return {
            "token_present": False,
            "tracking_enabled": False,
            "state": "missing",
            "badge_class": "text-bg-secondary",
            "badge_label": "No token",
            "recorded_at": None,
            "estimated_expires_at": None,
            "days_remaining": None,
            "summary": "Add an Instagram access token to enable lifecycle tracking.",
        }

    current_time = now or _now_utc()
    recorded_at = _coerce_datetime(credentials.get(INSTAGRAM_TOKEN_RECORDED_AT_KEY))
    estimated_expires_at = _coerce_datetime(credentials.get(INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY))
    if not recorded_at or not estimated_expires_at:
        return {
            "token_present": True,
            "tracking_enabled": False,
            "state": "untracked",
            "badge_class": "text-bg-warning",
            "badge_label": "Tracking Not Recorded",
            "recorded_at": recorded_at,
            "estimated_expires_at": estimated_expires_at,
            "days_remaining": None,
            "summary": "Tracking starts when you save a new token or use Record Token Refreshed after renewing it in Meta.",
        }

    remaining = estimated_expires_at - current_time
    days_remaining = _days_remaining_label(remaining)
    if remaining <= timedelta(0):
        return {
            "token_present": True,
            "tracking_enabled": True,
            "state": "expired",
            "badge_class": "text-bg-danger",
            "badge_label": "Expired",
            "recorded_at": recorded_at,
            "estimated_expires_at": estimated_expires_at,
            "days_remaining": days_remaining,
            "summary": "This estimated expiry window has passed. Refresh the token in Meta and record the refresh here.",
        }
    if remaining <= INSTAGRAM_TOKEN_WARNING_WINDOW:
        return {
            "token_present": True,
            "tracking_enabled": True,
            "state": "warning",
            "badge_class": "text-bg-warning",
            "badge_label": f"{days_remaining} day{'s' if days_remaining != 1 else ''} left",
            "recorded_at": recorded_at,
            "estimated_expires_at": estimated_expires_at,
            "days_remaining": days_remaining,
            "summary": "This token is approaching the end of Meta's typical long-lived access-token window.",
        }
    return {
        "token_present": True,
        "tracking_enabled": True,
        "state": "ok",
        "badge_class": "text-bg-success",
        "badge_label": f"{days_remaining} day{'s' if days_remaining != 1 else ''} left",
        "recorded_at": recorded_at,
        "estimated_expires_at": estimated_expires_at,
        "days_remaining": days_remaining,
        "summary": "Estimated from the last recorded token refresh. Meta notes that actual token lifetimes can change or expire early.",
    }


def apply_instagram_token_tracking(
    credentials: dict[str, Any] | None,
    *,
    previous_credentials: dict[str, Any] | None = None,
    refreshed_at: datetime | None = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    current = dict(credentials or {})
    previous = dict(previous_credentials or {})

    token = str(current.get("api_key") or "").strip()
    previous_token = str(previous.get("api_key") or "").strip()
    if not token:
        current.pop(INSTAGRAM_TOKEN_RECORDED_AT_KEY, None)
        current.pop(INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY, None)
        current.pop(INSTAGRAM_TOKEN_ALERT_FLAGS_KEY, None)
        return current

    existing_recorded_at = _coerce_datetime(previous.get(INSTAGRAM_TOKEN_RECORDED_AT_KEY))
    existing_expires_at = _coerce_datetime(previous.get(INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY))
    alerts = _normalized_alert_flags(previous.get(INSTAGRAM_TOKEN_ALERT_FLAGS_KEY))

    if force_refresh or token != previous_token or not existing_recorded_at or not existing_expires_at:
        refreshed_on = refreshed_at or _now_utc()
        current[INSTAGRAM_TOKEN_RECORDED_AT_KEY] = refreshed_on.isoformat()
        current[INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY] = (refreshed_on + INSTAGRAM_TOKEN_LIFETIME).isoformat()
        current[INSTAGRAM_TOKEN_ALERT_FLAGS_KEY] = []
        return current

    current[INSTAGRAM_TOKEN_RECORDED_AT_KEY] = existing_recorded_at.isoformat()
    current[INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY] = existing_expires_at.isoformat()
    current[INSTAGRAM_TOKEN_ALERT_FLAGS_KEY] = alerts
    return current


def record_instagram_token_refresh(account: Account, *, refreshed_at: datetime | None = None) -> None:
    account.credentials_json = apply_instagram_token_tracking(
        account.credentials_json,
        previous_credentials=account.credentials_json,
        refreshed_at=refreshed_at,
        force_refresh=True,
    )


def check_instagram_token_expiry(session: Session, alerts: AlertDispatcher, *, run_id: str, now: datetime | None = None) -> int:
    current_time = now or _now_utc()
    accounts = list(
        session.scalars(
            select(Account)
            .options(selectinload(Account.persona))
            .where(Account.service == "instagram", Account.is_enabled.is_(True))
            .order_by(Account.label, Account.id)
        )
    )
    emitted = 0
    for account in accounts:
        status = build_instagram_token_status(account, now=current_time)
        if not status or not status.get("tracking_enabled"):
            continue

        credentials = dict(account.credentials_json or {})
        flags = _normalized_alert_flags(credentials.get(INSTAGRAM_TOKEN_ALERT_FLAGS_KEY))
        expires_at = status.get("estimated_expires_at")
        if status["state"] == "expired":
            alert_flag = INSTAGRAM_TOKEN_ALERT_EXPIRED
            severity = "error"
            message = (
                f"Instagram token for {account.label} is estimated to have expired"
                f"{f' on {expires_at:%Y-%m-%d %H:%M UTC}' if isinstance(expires_at, datetime) else ''}. "
                "Refresh it in Meta and record the refresh in LynxPoster."
            )
        elif status["state"] == "warning":
            alert_flag = INSTAGRAM_TOKEN_ALERT_WARNING
            severity = "warning"
            days_remaining = status.get("days_remaining")
            message = (
                f"Instagram token for {account.label} is estimated to expire in {days_remaining} day"
                f"{'' if days_remaining == 1 else 's'}"
                f"{f' on {expires_at:%Y-%m-%d %H:%M UTC}' if isinstance(expires_at, datetime) else ''}. "
                "Refresh it in Meta before the token window closes."
            )
        else:
            continue

        if alert_flag in flags:
            continue

        alert = alerts.emit_hard_failure(
            session,
            run_id=run_id,
            operation="instagram_token_lifecycle",
            message=message,
            persona=account.persona,
            account=account,
            service="instagram",
            event_type="instagram_token_expiry",
            severity=severity,
            payload={
                "recorded_at": status.get("recorded_at").isoformat() if isinstance(status.get("recorded_at"), datetime) else None,
                "estimated_expires_at": expires_at.isoformat() if isinstance(expires_at, datetime) else None,
                "days_remaining": status.get("days_remaining"),
                "tracking_state": status.get("state"),
                "alert_flag": alert_flag,
            },
        )
        if alert is None:
            continue

        flags.append(alert_flag)
        credentials[INSTAGRAM_TOKEN_ALERT_FLAGS_KEY] = flags
        account.credentials_json = credentials
        emitted += 1

    if emitted:
        session.flush()
    return emitted
