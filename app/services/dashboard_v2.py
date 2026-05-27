from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


STATUS_LABELS = {
    "draft": "Draft",
    "scheduled": "Scheduled",
    "queued": "Queued",
    "publishing": "Publishing",
    "success": "Success",
    "partial_failure": "Needs Attention",
    "failure": "Failed",
    "failed": "Failed",
    "cancelled": "Cancelled",
}

STATUS_TONES = {
    "draft": "muted",
    "scheduled": "blue",
    "queued": "blue",
    "publishing": "blue",
    "success": "green",
    "partial_failure": "amber",
    "failure": "red",
    "failed": "red",
    "cancelled": "muted",
}


def _read(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(key, default)
    return getattr(source, key, default)


def _tzinfo(timezone_name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name or "UTC")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _local_date(value: datetime | None, timezone_name: str | None) -> Any:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(_tzinfo(timezone_name)).date()


def _scheduled_for(post: Any) -> datetime | None:
    value = _read(post, "scheduled_for")
    return value if isinstance(value, datetime) else None


def _post_status(post: Any) -> str:
    return str(_read(post, "display_status", None) or _read(post, "status", "draft") or "draft")


def _post_preview(post: Any, *, limit: int = 86) -> str:
    body = str(_read(post, "body", "") or "").strip().replace("\n", " ")
    if not body:
        return "Untitled post"
    return f"{body[: limit - 3]}..." if len(body) > limit else body


def _count_delivery_breakdown(posts: list[Any]) -> dict[str, int]:
    counts = {"succeeded": 0, "failed": 0, "cancelled": 0, "pending": 0}
    for post in posts:
        breakdown = _read(post, "delivery_breakdown")
        if not breakdown:
            continue
        for key in counts:
            counts[key] += len(_read(breakdown, key, []) or [])
    return counts


def _percent(value: int, total: int) -> int:
    if total <= 0:
        return 0
    return max(0, min(100, round(value / total * 100)))


def _metric(label: str, value: str, detail: str, tone: str, href: str | None = None) -> dict[str, str | None]:
    return {"label": label, "value": value, "detail": detail, "tone": tone, "href": href}


def _post_lane(status: str, count: int, total: int) -> dict[str, Any]:
    return {
        "key": status,
        "label": STATUS_LABELS.get(status, status.replace("_", " ").title()),
        "count": count,
        "tone": STATUS_TONES.get(status, "muted"),
        "width_pct": _percent(count, total),
    }


def build_dashboard_v2_view_model(
    *,
    personas: list[Any],
    posts: list[Any],
    run_groups: list[dict[str, Any]],
    alert_events: list[Any],
    scheduler_status: Any,
    giveaway_activity_monitor: dict[str, Any] | None,
    giveaway_metric_tally: dict[str, Any] | None,
    instagram_webhook_observability: dict[str, Any] | None,
    timezone_name: str | None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    today = now.astimezone(_tzinfo(timezone_name)).date()
    scheduled_today = [
        post
        for post in posts
        if _scheduled_for(post) is not None and _local_date(_scheduled_for(post), timezone_name) == today
    ]
    upcoming_posts = sorted(
        [post for post in posts if _scheduled_for(post) is not None],
        key=lambda post: _scheduled_for(post) or datetime.max.replace(tzinfo=timezone.utc),
    )[:5]
    recent_posts = posts[:5]

    status_counts: dict[str, int] = {}
    for post in posts:
        status = _post_status(post)
        status_counts[status] = status_counts.get(status, 0) + 1
    status_order = ["draft", "scheduled", "queued", "publishing", "success", "partial_failure", "failure", "cancelled"]
    status_lanes = [
        _post_lane(status, status_counts[status], len(posts))
        for status in status_order
        if status_counts.get(status, 0)
    ]

    delivery_counts = _count_delivery_breakdown(posts)
    delivery_total = sum(delivery_counts.values())
    delivery_success_pct = _percent(delivery_counts["succeeded"], delivery_total)
    delivery_chart = [
        {"label": "Succeeded", "count": delivery_counts["succeeded"], "tone": "green", "width_pct": _percent(delivery_counts["succeeded"], delivery_total)},
        {"label": "Pending", "count": delivery_counts["pending"], "tone": "blue", "width_pct": _percent(delivery_counts["pending"], delivery_total)},
        {"label": "Failed", "count": delivery_counts["failed"], "tone": "red", "width_pct": _percent(delivery_counts["failed"], delivery_total)},
        {"label": "Cancelled", "count": delivery_counts["cancelled"], "tone": "muted", "width_pct": _percent(delivery_counts["cancelled"], delivery_total)},
    ]

    giveaway_monitor = giveaway_activity_monitor or {}
    giveaway_metrics = giveaway_monitor.get("metrics") or {}
    giveaway_tally = giveaway_metric_tally or {
        "active": {},
        "all_time": {},
        "signal_rows": [],
        "outcome_rows": [],
        "active_signal_total": 0,
        "all_time_signal_total": 0,
        "active_meter_pct": 0,
        "all_time_meter_pct": 0,
    }
    giveaway_recent_events = giveaway_monitor.get("recent_events") or []
    open_giveaways = giveaway_monitor.get("open_giveaways") or []

    latest_run = run_groups[0] if run_groups else None
    latest_run_counts = latest_run.get("counts", {}) if latest_run else {}
    latest_run_errors = int(_read(latest_run_counts, "errors", 0) or 0)

    webhook_total = int((instagram_webhook_observability or {}).get("total_events") or 0)
    webhook_matched = int((instagram_webhook_observability or {}).get("matched_events") or 0)
    webhook_match_pct = _percent(webhook_matched, webhook_total)

    automation_enabled = bool(_read(scheduler_status, "automation_enabled", False))
    cycle_running = bool(_read(scheduler_status, "cycle_in_progress", False))
    automation_label = "Running" if cycle_running else ("Active" if automation_enabled else "Paused")
    automation_tone = "amber" if cycle_running else ("green" if automation_enabled else "muted")

    overview_metrics = [
        _metric("Autorun", automation_label, "Next pass queued" if automation_enabled else "Automation is paused", automation_tone, "/settings/page"),
        _metric("Scheduled", str(len(posts)), f"{len(scheduled_today)} due today", "blue", "/scheduled-posts/page"),
        _metric("Giveaways", str(giveaway_metrics.get("campaigns", 0)), f"{giveaway_metrics.get('entrants', 0)} tracked entrants", "pink", "/scheduled-posts/page"),
        _metric("Alerts", str(len(alert_events)), "Needs review" if alert_events else "All clear", "red" if alert_events else "green", "/logs/page"),
        _metric("Delivery", f"{delivery_success_pct}%" if delivery_total else "No data", f"{delivery_total} recent delivery states", "green" if delivery_success_pct >= 90 or not delivery_total else "amber", "/logs/page"),
    ]
    if instagram_webhook_observability is not None:
        overview_metrics.append(
            _metric("Instagram", f"{webhook_match_pct}%" if webhook_total else "No data", f"{webhook_total} webhook events / 7d", "pink", "/logs/page#instagram-webhooks")
        )

    return {
        "overview_metrics": overview_metrics,
        "delivery_counts": delivery_counts,
        "delivery_chart": delivery_chart,
        "delivery_total": delivery_total,
        "status_lanes": status_lanes,
        "upcoming_posts": upcoming_posts,
        "recent_posts": recent_posts,
        "upcoming_post_cards": [{"post": post, "preview": _post_preview(post)} for post in upcoming_posts],
        "recent_post_cards": [{"post": post, "preview": _post_preview(post)} for post in recent_posts],
        "scheduled_today_count": len(scheduled_today),
        "account_count": sum(len(_read(persona, "accounts", []) or []) for persona in personas),
        "enabled_persona_count": sum(1 for persona in personas if bool(_read(persona, "is_enabled", True))),
        "latest_run": latest_run,
        "latest_run_errors": latest_run_errors,
        "run_count": len(run_groups),
        "alert_count": len(alert_events),
        "giveaway_metrics": giveaway_metrics,
        "giveaway_metric_tally": giveaway_tally,
        "giveaway_rollups": giveaway_monitor.get("rollups") or [],
        "giveaway_recent_events": giveaway_recent_events,
        "open_giveaways": open_giveaways,
        "webhook_total": webhook_total,
        "webhook_matched": webhook_matched,
        "webhook_match_pct": webhook_match_pct,
        "webhook_observability": instagram_webhook_observability,
        "scheduler": {
            "label": automation_label,
            "tone": automation_tone,
            "automation_enabled": automation_enabled,
            "cycle_in_progress": cycle_running,
            "next_run_at": _read(scheduler_status, "next_run_at"),
            "last_run_trigger": _read(scheduler_status, "last_run_trigger"),
            "last_run_finished_at": _read(scheduler_status, "last_run_finished_at"),
            "interval_seconds": _read(scheduler_status, "autorun_interval_seconds", 0),
        },
    }
