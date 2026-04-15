from __future__ import annotations

from contextlib import contextmanager

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.main import app
from app.services.auth import Principal
from app.services.live_updates import (
    LIVE_UPDATE_TOPIC_RUN_EVENTS,
    live_update_snapshot,
    publish_live_update,
    reset_live_updates,
)


def test_live_update_snapshot_changes_only_requested_topics():
    reset_live_updates()

    before = live_update_snapshot(["run_events", "alert_events"])
    publish_live_update("run_events", "dashboard")
    after = live_update_snapshot(["run_events", "alert_events"])

    assert before["token"] != after["token"]
    assert before["versions"]["alert_events"] == 0
    assert after["versions"]["alert_events"] == 0
    assert after["versions"]["run_events"] == 1


def test_live_update_status_api_reports_topic_versions(monkeypatch, tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'live-updates.db'}",
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
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
            is_authenticated=True,
        ),
    )

    try:
        reset_live_updates()
        with TestClient(app) as client:
            before = client.get("/live-updates/status", params={"topics": "run_events,alert_events"})
            publish_live_update(LIVE_UPDATE_TOPIC_RUN_EVENTS)
            after = client.get("/live-updates/status", params={"topics": "run_events,alert_events"})

        assert before.status_code == 200
        assert after.status_code == 200
        before_json = before.json()
        after_json = after.json()
        assert before_json["versions"]["run_events"] == 0
        assert after_json["versions"]["run_events"] == 1
        assert after_json["versions"]["alert_events"] == 0
        assert before_json["token"] != after_json["token"]
    finally:
        reset_live_updates()
        Base.metadata.drop_all(engine)
        engine.dispose()
