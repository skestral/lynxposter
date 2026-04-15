from __future__ import annotations

from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.main import app
from app.models import Account, AlertEvent, RunEvent
from app.services.auth import Principal
from app.services.personas import create_account, create_persona


def _create_persona(
    session,
    *,
    slug: str = "instagram-account-api",
    owner_user_id: str | None = "admin-user",
):
    return create_persona(
        session,
        {
            "name": "Instagram Persona",
            "slug": slug,
            "owner_user_id": owner_user_id,
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def _create_instagram_account(session, persona):
    return create_account(
        session,
        persona,
        {
            "service": "instagram",
            "label": "Instagram",
            "handle_or_identifier": "larkyn.lynx",
            "is_enabled": True,
            "source_enabled": True,
            "destination_enabled": True,
            "credentials_json": {
                "api_key": "graph-token",
                "instagrapi_username": "larkyn.lynx",
                "instagrapi_password": "insta-password",
            },
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


def _validation_payload():
    return {
        "label": "Instagram",
        "handle_or_identifier": "larkyn.lynx",
        "is_enabled": True,
        "source_enabled": True,
        "destination_enabled": True,
        "credentials_json": {
            "api_key": "graph-token",
            "instagrapi_username": "larkyn.lynx",
            "instagrapi_password": "insta-password",
        },
        "source_settings_json": {},
        "publish_settings_json": {},
    }


@pytest.fixture()
def api_stack(monkeypatch, tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'instagram-account-api.db'}", future=True, connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, class_=Session)
    Base.metadata.create_all(engine)

    @contextmanager
    def _db_session_override():
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr("app.main.db_session", _db_session_override)
    monkeypatch.setattr("app.main.bootstrap", lambda: None)
    monkeypatch.setattr("app.main.CrossposterScheduler.start", lambda self: None)
    monkeypatch.setattr("app.main.CrossposterScheduler.stop", lambda self: None)
    monkeypatch.setattr(
        "app.main.build_principal_from_request",
        lambda request: Principal(
            user_id="admin-user",
            display_name="Lynx",
            role="admin",
            timezone="UTC",
            is_authenticated=True,
        ),
    )

    with TestClient(app) as client:
        yield client, SessionLocal

    Base.metadata.drop_all(engine)
    engine.dispose()


def test_validate_instagram_account_login_api_saves_session_and_logs_success(api_stack, monkeypatch):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="instagram-account-api-success")
        account = _create_instagram_account(session, persona)
        session.commit()
        persona_id = persona.id
        account_id = account.id

    monkeypatch.setattr(
        "app.main.validate_instagram_account_login",
        lambda credentials, previous_credentials=None: (
            {
                **dict(credentials or {}),
                "instagrapi_username": "validated-user",
                "instagrapi_sessionid": "persisted-session",
            },
            "persisted-session",
            "validated-user",
        ),
    )

    response = api_client.post(
        f"/personas/{persona_id}/accounts/{account_id}/instagram-login/validate",
        json=_validation_payload(),
    )

    assert response.status_code == 200
    assert response.json()["session_id_saved"] is True

    with SessionLocal() as session:
        saved = session.get(Account, account_id)
        assert saved is not None
        assert saved.credentials_json["instagrapi_sessionid"] == "persisted-session"
        assert saved.credentials_json["instagrapi_username"] == "validated-user"
        assert saved.last_health_status == "ok"
        assert saved.last_error is None
        run_event = session.query(RunEvent).one()
        assert run_event.operation == "instagram_auth_validate"


def test_validate_instagram_account_login_api_records_alert_on_failure(api_stack, monkeypatch):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="instagram-account-api-failure")
        account = _create_instagram_account(session, persona)
        session.commit()
        persona_id = persona.id
        account_id = account.id

    def _raise_validation_error(credentials, previous_credentials=None):
        raise RuntimeError("Instagram login failed. Challenge required.")

    monkeypatch.setattr("app.main.validate_instagram_account_login", _raise_validation_error)

    response = api_client.post(
        f"/personas/{persona_id}/accounts/{account_id}/instagram-login/validate",
        json=_validation_payload(),
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Instagram login failed. Challenge required."

    with SessionLocal() as session:
        saved = session.get(Account, account_id)
        assert saved is not None
        assert saved.last_health_status == "error"
        assert saved.last_error == "Instagram login failed. Challenge required."
        alert = session.query(AlertEvent).one()
        assert alert.operation == "instagram_auth_validate"
        assert alert.event_type == "account_validation_failure"
