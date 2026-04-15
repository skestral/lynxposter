from __future__ import annotations

from contextlib import contextmanager

from fastapi.testclient import TestClient
from starlette.requests import Request
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.main import app
from app.models import AlertEvent, InstagramGiveawayWebhookEvent, Persona, RunEvent
from app.services.auth import Principal
from app.main import _dismiss_dashboard_alerts, _visible_dashboard_alerts


def _request_with_session() -> Request:
    return Request({"type": "http", "headers": [], "session": {}})


def _create_persona(session, *, name: str = "Dashboard", slug: str = "dashboard") -> Persona:
    persona = Persona(
        name=name,
        slug=slug,
        is_enabled=True,
        timezone="UTC",
        settings_json={},
        retry_settings_json={"max_retries": 3},
        throttle_settings_json={"max_per_hour": 0, "overflow_posts": "retry"},
    )
    session.add(persona)
    session.flush()
    return persona


def _create_alert(session, persona: Persona, *, run_id: str, message: str) -> AlertEvent:
    alert = AlertEvent(
        run_id=run_id,
        fingerprint=f"{run_id}-fingerprint",
        event_type="publish_failed",
        severity="error",
        persona_id=persona.id,
        operation="publish",
        message=message,
        retry_count=0,
        payload_json={},
    )
    session.add(alert)
    session.flush()
    return alert


def _create_run_event(session, persona: Persona, *, run_id: str, message: str) -> RunEvent:
    event = RunEvent(
        run_id=run_id,
        persona_id=persona.id,
        operation="poll",
        severity="info",
        message=message,
        metadata_json={},
    )
    session.add(event)
    session.flush()
    return event


def test_dashboard_alert_clear_only_hides_alerts_from_dashboard(session):
    persona = _create_persona(session)
    first = _create_alert(session, persona, run_id="run-1", message="First alert")
    second = _create_alert(session, persona, run_id="run-2", message="Second alert")
    request = _request_with_session()

    visible_before = _visible_dashboard_alerts(request, [second, first])
    dismissed_count = _dismiss_dashboard_alerts(request, visible_before)
    visible_after = _visible_dashboard_alerts(request, [second, first])

    assert [alert.id for alert in visible_before] == [second.id, first.id]
    assert dismissed_count == 2
    assert visible_after == []
    assert session.get(AlertEvent, first.id) is not None
    assert session.get(AlertEvent, second.id) is not None


def test_dashboard_alert_clear_does_not_hide_newer_alerts(session):
    persona = _create_persona(session)
    older = _create_alert(session, persona, run_id="run-older", message="Older alert")
    request = _request_with_session()

    _dismiss_dashboard_alerts(request, [older])

    newer = _create_alert(session, persona, run_id="run-newer", message="Newer alert")
    visible = _visible_dashboard_alerts(request, [newer, older])

    assert [alert.id for alert in visible] == [newer.id]


def test_dashboard_route_renders_persona_names_for_alerts_and_run_events(monkeypatch, tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'dashboard.db'}",
        future=True,
        connect_args={"check_same_thread": False},
    )
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

    try:
        with SessionLocal() as session:
            persona = _create_persona(session, name="Savannah", slug="savannah-dashboard")
            _create_alert(session, persona, run_id="run-alert", message="Dashboard alert")
            _create_run_event(session, persona, run_id="run-event", message="Dashboard event")
            session.add(
                InstagramGiveawayWebhookEvent(
                    provider_event_field="comments",
                    event_type="comment",
                    payload_json={
                        "entry": {"id": "instagram-account"},
                        "change": {
                            "field": "comments",
                            "value": {
                                "id": "comment-1",
                                "text": "Count me in @friend",
                                "from": {"id": "user-1", "username": "entrant.one"},
                            },
                        },
                    },
                    signature_valid=True,
                    processed=True,
                )
            )
            session.add(
                InstagramGiveawayWebhookEvent(
                    provider_event_field="messages",
                    event_type="message",
                    payload_json={
                        "entry": {"id": "instagram-account"},
                        "change": {
                            "field": "messages",
                            "value": {
                                "message": {"mid": "test-mid", "text": "Inbox ping"},
                                "from": {"id": "user-2", "username": "dm.user"},
                            },
                        },
                    },
                    signature_valid=True,
                    processed=True,
                )
            )
            session.commit()

        with TestClient(app) as client:
            response = client.get("/")

        assert response.status_code == 200
        assert "Savannah" in response.text
        assert "Dashboard alert" in response.text
        assert "Dashboard event" in response.text
        assert "Instagram Webhooks" in response.text
        assert "Comments" in response.text
        assert "Count me in @friend" in response.text
        assert "Messages" in response.text
        assert "No persona" not in response.text
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()
