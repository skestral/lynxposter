from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event
from types import SimpleNamespace

import pytest

from app.services.delivery_lock import DeliveryBusyError, edit_guard, publishing_guard
from app.services.scheduler import CrossposterScheduler


def test_publishing_tasks_serialize_and_edits_fail_while_busy():
    first_started = Event()
    release_first = Event()
    second_waiting = Event()
    second_started = Event()
    published: list[str] = []

    def first_publish():
        with publishing_guard():
            first_started.set()
            assert release_first.wait(timeout=5)
            published.append("first")

    def second_publish():
        second_waiting.set()
        with publishing_guard():
            second_started.set()
            published.append("second")

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(first_publish)
        try:
            assert first_started.wait(timeout=5)
            second = executor.submit(second_publish)
            assert second_waiting.wait(timeout=5)
            with pytest.raises(DeliveryBusyError, match="publishing task is in progress"):
                with edit_guard():
                    pytest.fail("An edit must not enter while publishing is active.")
            assert not second_started.is_set()
        finally:
            release_first.set()
        first.result(timeout=5)
        second.result(timeout=5)

    assert published == ["first", "second"]
    with edit_guard():
        pass


def test_guard_releases_after_failure_and_allows_nested_publishing():
    with pytest.raises(ValueError, match="failed publish"):
        with publishing_guard():
            with publishing_guard():
                raise ValueError("failed publish")

    with ThreadPoolExecutor(max_workers=1) as executor:
        def edit_after_failure():
            with edit_guard():
                return "edited"

        assert executor.submit(edit_after_failure).result(timeout=5) == "edited"


@pytest.mark.parametrize("fail_cycle", [False, True])
def test_scheduler_holds_shared_guard_for_logs_transactions_and_failure_alerts(monkeypatch, fail_cycle):
    checked_phases: list[str] = []

    def assert_edit_blocked(phase: str):
        def try_edit():
            with edit_guard():
                return "unexpected edit"

        with ThreadPoolExecutor(max_workers=1) as executor:
            with pytest.raises(DeliveryBusyError):
                executor.submit(try_edit).result(timeout=5)
        checked_phases.append(phase)

    @contextmanager
    def session_scope():
        assert_edit_blocked("transaction_enter")
        yield object()
        assert_edit_blocked("transaction_commit")

    scheduler = CrossposterScheduler(alerts=SimpleNamespace(
        emit_hard_failure=lambda *args, **kwargs: assert_edit_blocked("failure_alert"),
        clear_run=lambda *args: None,
    ))

    def log_scheduler_event(**kwargs):
        if "started" in kwargs["message"]:
            assert_edit_blocked("start_log")
        elif "completed" in kwargs["message"]:
            assert_edit_blocked("complete_log")
        else:
            assert_edit_blocked("failure_log")

    def poll(*args, **kwargs):
        assert_edit_blocked("poll")
        if fail_cycle:
            raise RuntimeError("Polling failed.")

    monkeypatch.setattr(scheduler, "_session_scope", session_scope)
    monkeypatch.setattr(scheduler, "_log_scheduler_event", log_scheduler_event)
    monkeypatch.setattr("app.services.scheduler.poll_sources", poll)
    monkeypatch.setattr("app.services.scheduler.process_delivery_queue", lambda *args, **kwargs: assert_edit_blocked("publish"))
    for name in (
        "enqueue_due_scheduled_posts", "reconcile_pending_posts",
        "process_instagram_giveaway_lifecycle", "check_instagram_token_expiry",
        "cleanup_stale_media_files",
    ):
        monkeypatch.setattr(f"app.services.scheduler.{name}", lambda *args, **kwargs: None)

    result = scheduler.run_manual_cycle()

    if fail_cycle:
        assert result["status"] == "error"
        assert checked_phases == ["start_log", "transaction_enter", "poll", "failure_log", "transaction_enter", "failure_alert", "transaction_commit"]
    else:
        assert result["status"] == "ok"
        assert checked_phases == ["start_log", "transaction_enter", "poll", "publish", "transaction_commit", "complete_log"]
    assert not scheduler._cycle_lock.locked()
    with edit_guard():
        pass


def test_scheduler_waits_for_manual_publisher_before_opening_any_database_session(monkeypatch):
    scheduler = CrossposterScheduler(alerts=object())
    scheduler_waiting = Event()
    database_touched = Event()

    @contextmanager
    def observed_guard(*, blocking=True):
        scheduler_waiting.set()
        with publishing_guard(blocking=blocking):
            yield

    @contextmanager
    def session_scope():
        database_touched.set()
        yield object()

    monkeypatch.setattr("app.services.scheduler.publishing_guard", observed_guard)
    monkeypatch.setattr(scheduler, "_session_scope", session_scope)
    monkeypatch.setattr("app.services.scheduler.log_run_event", lambda *args, **kwargs: None)
    for name in (
        "poll_sources", "enqueue_due_scheduled_posts", "reconcile_pending_posts", "process_delivery_queue",
        "process_instagram_giveaway_lifecycle", "check_instagram_token_expiry", "cleanup_stale_media_files",
    ):
        monkeypatch.setattr(f"app.services.scheduler.{name}", lambda *args, **kwargs: None)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with publishing_guard():
            future = executor.submit(scheduler.run_manual_cycle)
            assert scheduler_waiting.wait(timeout=5)
            assert not database_touched.is_set()
        assert future.result(timeout=5)["status"] == "ok"
    assert database_touched.is_set()
