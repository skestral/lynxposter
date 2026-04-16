from __future__ import annotations

from contextlib import contextmanager

from app.models import RunEvent
from app.services.alerts import AlertDispatcher
from app.services.scheduler import CrossposterScheduler


@contextmanager
def _session_scope(session):
    yield session


class FakeJob:
    def __init__(self) -> None:
        self.paused = False
        self.next_run_time = None


class FakeScheduler:
    def __init__(self) -> None:
        self.job = FakeJob()

    def get_job(self, job_id: str):
        return self.job

    def pause_job(self, job_id: str) -> None:
        self.job.paused = True

    def resume_job(self, job_id: str) -> None:
        self.job.paused = False


def test_manual_cycle_logs_and_runs_all_phases(session, monkeypatch):
    scheduler = CrossposterScheduler(AlertDispatcher())
    scheduler._scheduler = FakeScheduler()

    calls: list[str] = []
    monkeypatch.setattr("app.services.scheduler.db_session", lambda: _session_scope(session))
    monkeypatch.setattr("app.services.scheduler.poll_sources", lambda db, alerts, *, run_id=None, trigger="manual": calls.append(f"poll:{trigger}"))
    monkeypatch.setattr("app.services.scheduler.enqueue_due_scheduled_posts", lambda db, *, run_id=None: calls.append("due"))
    monkeypatch.setattr("app.services.scheduler.reconcile_pending_posts", lambda db, *, run_id=None: calls.append("reconcile"))
    monkeypatch.setattr("app.services.scheduler.process_delivery_queue", lambda db, alerts, *, run_id=None: calls.append("delivery"))
    monkeypatch.setattr("app.services.scheduler.process_instagram_giveaway_lifecycle", lambda db, alerts, *, run_id=None: calls.append("giveaways"))
    monkeypatch.setattr("app.services.scheduler.cleanup_stale_media_files", lambda db, *, run_id=None: calls.append("cleanup"))

    result = scheduler.run_manual_cycle()

    assert result["status"] == "ok"
    assert calls == ["poll:manual", "due", "reconcile", "delivery", "giveaways", "cleanup"]
    events = session.query(RunEvent).filter(RunEvent.operation == "automation_cycle").order_by(RunEvent.created_at).all()
    assert len(events) == 2
    assert "started" in events[0].message.lower()
    assert "completed" in events[1].message.lower()


def test_pause_and_resume_toggle_automation_and_log(session, monkeypatch):
    scheduler = CrossposterScheduler(AlertDispatcher())
    scheduler._scheduler = FakeScheduler()
    monkeypatch.setattr("app.services.scheduler.db_session", lambda: _session_scope(session))

    paused = scheduler.pause_automation()
    resumed = scheduler.resume_automation()

    assert paused["automation_enabled"] is False
    assert resumed["automation_enabled"] is True
    control_events = session.query(RunEvent).filter(RunEvent.operation == "automation_cycle").all()
    assert any("paused" in event.message.lower() for event in control_events)
    assert any("started" in event.message.lower() for event in control_events)
