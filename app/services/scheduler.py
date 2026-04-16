from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Iterator

from app.config import get_settings
from app.database import db_session
from app.services.alerts import AlertDispatcher
from app.services.delivery import enqueue_due_scheduled_posts, new_run_id, poll_sources, process_delivery_queue, reconcile_pending_posts
from app.services.events import log_run_event
from app.services.giveaways import process_instagram_giveaway_lifecycle
from app.services.instagram_tokens import check_instagram_token_expiry
from app.services.media_cleanup import cleanup_stale_media_files

class CrossposterScheduler:
    def __init__(self, alerts: AlertDispatcher) -> None:
        self.alerts = alerts
        self._scheduler = None
        self._cycle_lock = Lock()
        self._automation_enabled = True
        self._last_run_started_at: datetime | None = None
        self._last_run_finished_at: datetime | None = None
        self._last_run_trigger: str | None = None
        self._last_run_error: str | None = None
        self._last_run_id: str | None = None

    def _current_interval_seconds(self) -> int:
        return get_settings().scheduler_automation_interval_seconds

    def start(self) -> None:
        from apscheduler.schedulers.background import BackgroundScheduler

        if self._scheduler:
            return

        self._scheduler = BackgroundScheduler(timezone="UTC")
        self._scheduler.add_job(
            self._run_automation_cycle,
            "interval",
            seconds=self._current_interval_seconds(),
            id="automation-cycle",
            max_instances=1,
            coalesce=True,
        )
        self._scheduler.start()
        if not self._automation_enabled:
            self._scheduler.pause_job("automation-cycle")

    def stop(self) -> None:
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

    def pause_automation(self) -> dict[str, Any]:
        self._automation_enabled = False
        if self._scheduler and self._scheduler.get_job("automation-cycle"):
            self._scheduler.pause_job("automation-cycle")
        self._log_control_event("Automation paused")
        return self.get_status()

    def resume_automation(self) -> dict[str, Any]:
        self._automation_enabled = True
        if self._scheduler and self._scheduler.get_job("automation-cycle"):
            self._scheduler.resume_job("automation-cycle")
        self._log_control_event("Automation started")
        return self.get_status()

    def run_manual_cycle(self) -> dict[str, Any]:
        return self._run_cycle(trigger="manual")

    def refresh_configuration(self) -> dict[str, Any]:
        interval_seconds = self._current_interval_seconds()
        if self._scheduler and self._scheduler.get_job("automation-cycle"):
            self._scheduler.reschedule_job("automation-cycle", trigger="interval", seconds=interval_seconds)
            if not self._automation_enabled:
                self._scheduler.pause_job("automation-cycle")
        return self.get_status()

    def get_status(self) -> dict[str, Any]:
        job = self._scheduler.get_job("automation-cycle") if self._scheduler else None
        next_run_at = job.next_run_time if job and job.next_run_time else None
        return {
            "automation_enabled": self._automation_enabled,
            "scheduler_started": self._scheduler is not None,
            "cycle_in_progress": self._cycle_lock.locked(),
            "autorun_interval_seconds": self._current_interval_seconds(),
            "next_run_at": next_run_at.isoformat() if next_run_at else None,
            "last_run_started_at": self._last_run_started_at.isoformat() if self._last_run_started_at else None,
            "last_run_finished_at": self._last_run_finished_at.isoformat() if self._last_run_finished_at else None,
            "last_run_trigger": self._last_run_trigger,
            "last_run_error": self._last_run_error,
            "last_run_id": self._last_run_id,
        }

    def _run_automation_cycle(self) -> None:
        self._run_cycle(trigger="autorun")

    def _run_cycle(self, *, trigger: str) -> dict[str, Any]:
        run_id = new_run_id()
        if not self._cycle_lock.acquire(blocking=False):
            self._log_scheduler_event(
                run_id=run_id,
                severity="warning",
                message=f"{trigger.title()} automation cycle skipped because another cycle is already running.",
                trigger=trigger,
            )
            return {
                "status": "busy",
                "run_id": run_id,
                "message": "Another automation cycle is already running.",
                "trigger": trigger,
            }

        self._last_run_started_at = datetime.now(timezone.utc)
        self._last_run_trigger = trigger
        self._last_run_id = run_id
        self._last_run_error = None

        try:
            self._log_scheduler_event(
                run_id=run_id,
                message=f"{trigger.title()} automation cycle started.",
                trigger=trigger,
            )
            with self._session_scope() as session:
                poll_sources(session, self.alerts, run_id=run_id, trigger=trigger)
                enqueue_due_scheduled_posts(session, run_id=run_id)
                reconcile_pending_posts(session, run_id=run_id)
                process_delivery_queue(session, self.alerts, run_id=run_id)
                process_instagram_giveaway_lifecycle(session, self.alerts, run_id=run_id)
                check_instagram_token_expiry(session, self.alerts, run_id=run_id)
                cleanup_stale_media_files(session, run_id=run_id)
            self._log_scheduler_event(
                run_id=run_id,
                message=f"{trigger.title()} automation cycle completed.",
                trigger=trigger,
            )
            return {
                "status": "ok",
                "run_id": run_id,
                "message": "Automation cycle completed.",
                "trigger": trigger,
            }
        except Exception as exc:
            self._last_run_error = str(exc)
            self._log_scheduler_event(
                run_id=run_id,
                severity="error",
                message=f"{trigger.title()} automation cycle failed: {exc}",
                trigger=trigger,
            )
            with self._session_scope() as session:
                self.alerts.emit_hard_failure(
                    session,
                    run_id=run_id,
                    operation="automation_cycle",
                    message=str(exc),
                    error_class=exc.__class__.__name__,
                    payload={"trigger": trigger},
                )
            self.alerts.clear_run(run_id)
            return {
                "status": "error",
                "run_id": run_id,
                "message": str(exc),
                "trigger": trigger,
            }
        finally:
            self._last_run_finished_at = datetime.now(timezone.utc)
            self._cycle_lock.release()

    def _log_control_event(self, message: str) -> None:
        run_id = new_run_id()
        self._log_scheduler_event(run_id=run_id, message=message, trigger="control")

    def _log_scheduler_event(
        self,
        *,
        run_id: str,
        message: str,
        trigger: str,
        severity: str = "info",
    ) -> None:
        with self._session_scope() as session:
            log_run_event(
                session,
                run_id=run_id,
                operation="automation_cycle",
                severity=severity,
                message=message,
                metadata={
                    "trigger": trigger,
                    "autorun_interval_seconds": self._current_interval_seconds(),
                },
            )

    @contextmanager
    def _session_scope(self) -> Iterator:
        with db_session() as session:
            yield session
