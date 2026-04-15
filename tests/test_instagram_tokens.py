from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.models import AlertEvent
from app.services.alerts import AlertDispatcher
from app.services.instagram_tokens import (
    INSTAGRAM_TOKEN_ALERT_EXPIRED,
    INSTAGRAM_TOKEN_ALERT_FLAGS_KEY,
    INSTAGRAM_TOKEN_ALERT_WARNING,
    INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY,
    INSTAGRAM_TOKEN_LIFETIME,
    INSTAGRAM_TOKEN_RECORDED_AT_KEY,
    build_instagram_token_status,
    check_instagram_token_expiry,
)
from app.services.personas import create_account, create_persona, record_account_token_refresh, update_account


def _create_persona(session):
    return create_persona(
        session,
        {
            "name": "Instagram Persona",
            "slug": "instagram-persona",
            "is_enabled": True,
            "timezone": "UTC",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def _create_instagram_account(session, persona, *, token: str = "instagram-token", user_id: str = "17841400000000000"):
    return create_account(
        session,
        persona,
        {
            "service": "instagram",
            "label": "Instagram",
            "handle_or_identifier": "@studio",
            "is_enabled": True,
            "source_enabled": True,
            "destination_enabled": True,
            "credentials_json": {"api_key": token, "instagram_user_id": user_id},
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


def test_create_instagram_account_records_token_window(session, monkeypatch):
    persona = _create_persona(session)
    frozen_now = datetime(2026, 4, 15, 9, 30, tzinfo=timezone.utc)
    monkeypatch.setattr("app.services.instagram_tokens._now_utc", lambda: frozen_now)

    account = _create_instagram_account(session, persona)

    assert account.credentials_json[INSTAGRAM_TOKEN_RECORDED_AT_KEY] == frozen_now.isoformat()
    assert account.credentials_json[INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY] == (frozen_now + INSTAGRAM_TOKEN_LIFETIME).isoformat()
    assert account.credentials_json[INSTAGRAM_TOKEN_ALERT_FLAGS_KEY] == []


def test_update_instagram_account_preserves_existing_token_window_when_token_is_unchanged(session, monkeypatch):
    persona = _create_persona(session)
    first_seen = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("app.services.instagram_tokens._now_utc", lambda: first_seen)
    account = _create_instagram_account(session, persona)

    later = datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("app.services.instagram_tokens._now_utc", lambda: later)
    updated = update_account(
        session,
        persona,
        account,
        {
            "label": "Instagram Updated",
            "credentials_json": {"api_key": "instagram-token", "instagram_user_id": "17841400000000000"},
        },
    )

    assert updated.label == "Instagram Updated"
    assert updated.credentials_json[INSTAGRAM_TOKEN_RECORDED_AT_KEY] == first_seen.isoformat()
    assert updated.credentials_json[INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY] == (first_seen + INSTAGRAM_TOKEN_LIFETIME).isoformat()


def test_record_account_token_refresh_resets_expiry_window_and_clears_alert_flags(session, monkeypatch):
    persona = _create_persona(session)
    initial_now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("app.services.instagram_tokens._now_utc", lambda: initial_now)
    account = _create_instagram_account(session, persona)

    account.credentials_json[INSTAGRAM_TOKEN_ALERT_FLAGS_KEY] = [INSTAGRAM_TOKEN_ALERT_WARNING, INSTAGRAM_TOKEN_ALERT_EXPIRED]
    refreshed_now = datetime(2026, 4, 15, 7, 45, tzinfo=timezone.utc)
    monkeypatch.setattr("app.services.instagram_tokens._now_utc", lambda: refreshed_now)

    updated = record_account_token_refresh(session, persona, account)

    assert updated.credentials_json[INSTAGRAM_TOKEN_RECORDED_AT_KEY] == refreshed_now.isoformat()
    assert updated.credentials_json[INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY] == (refreshed_now + INSTAGRAM_TOKEN_LIFETIME).isoformat()
    assert updated.credentials_json[INSTAGRAM_TOKEN_ALERT_FLAGS_KEY] == []


def test_check_instagram_token_expiry_emits_alerts_once_per_token_cycle(session):
    warning_persona = _create_persona(session)
    expired_persona = create_persona(
        session,
        {
            "name": "Expired Persona",
            "slug": "expired-persona",
            "is_enabled": True,
            "timezone": "UTC",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )
    warning_account = _create_instagram_account(session, warning_persona, token="warning-token", user_id="17841400000000001")
    expired_account = _create_instagram_account(session, expired_persona, token="expired-token", user_id="17841400000000002")

    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    warning_account.credentials_json[INSTAGRAM_TOKEN_RECORDED_AT_KEY] = (now - timedelta(days=55)).isoformat()
    warning_account.credentials_json[INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY] = (now + timedelta(days=5)).isoformat()
    warning_account.credentials_json[INSTAGRAM_TOKEN_ALERT_FLAGS_KEY] = []

    expired_account.credentials_json[INSTAGRAM_TOKEN_RECORDED_AT_KEY] = (now - timedelta(days=62)).isoformat()
    expired_account.credentials_json[INSTAGRAM_TOKEN_ESTIMATED_EXPIRES_AT_KEY] = (now - timedelta(hours=12)).isoformat()
    expired_account.credentials_json[INSTAGRAM_TOKEN_ALERT_FLAGS_KEY] = []

    alerts = AlertDispatcher()

    emitted = check_instagram_token_expiry(session, alerts, run_id="run-1", now=now)

    assert emitted == 2
    saved_alerts = session.query(AlertEvent).order_by(AlertEvent.created_at).all()
    assert len(saved_alerts) == 2
    assert saved_alerts[0].event_type == "instagram_token_expiry"
    assert saved_alerts[0].operation == "instagram_token_lifecycle"
    assert {alert.severity for alert in saved_alerts} == {"warning", "error"}
    assert INSTAGRAM_TOKEN_ALERT_WARNING in warning_account.credentials_json[INSTAGRAM_TOKEN_ALERT_FLAGS_KEY]
    assert INSTAGRAM_TOKEN_ALERT_EXPIRED in expired_account.credentials_json[INSTAGRAM_TOKEN_ALERT_FLAGS_KEY]

    emitted_again = check_instagram_token_expiry(session, alerts, run_id="run-2", now=now)

    assert emitted_again == 0
    assert session.query(AlertEvent).count() == 2


def test_build_instagram_token_status_reports_untracked_token():
    account = SimpleNamespace(
        service="instagram",
        credentials_json={"api_key": "token"},
    )

    status = build_instagram_token_status(account, now=datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc))

    assert status is not None
    assert status["state"] == "untracked"
    assert status["tracking_enabled"] is False
