from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Iterable

LIVE_UPDATE_TOPIC_GLOBAL = "global"
LIVE_UPDATE_TOPIC_DASHBOARD = "dashboard"
LIVE_UPDATE_TOPIC_LOGS = "logs"
LIVE_UPDATE_TOPIC_RUN_EVENTS = "run_events"
LIVE_UPDATE_TOPIC_ALERT_EVENTS = "alert_events"
LIVE_UPDATE_TOPIC_INSTAGRAM_WEBHOOKS = "instagram_webhooks"
LIVE_UPDATE_TOPIC_SCHEDULED_POSTS = "scheduled_posts"
LIVE_UPDATE_POLL_INTERVAL_MS = 5000

_versions: dict[str, int] = {LIVE_UPDATE_TOPIC_GLOBAL: 0}
_last_changed_at: dict[str, str] = {}
_lock = Lock()


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_live_update_topics(raw_topics: str | Iterable[str] | None) -> tuple[str, ...]:
    values: list[str] = []
    if raw_topics is None:
        return ()
    if isinstance(raw_topics, str):
        candidates = raw_topics.split(",")
    else:
        candidates = list(raw_topics)
    for raw in candidates:
        topic = str(raw or "").strip()
        if not topic or topic == LIVE_UPDATE_TOPIC_GLOBAL or topic in values:
            continue
        values.append(topic)
    return tuple(values)


def live_update_snapshot(raw_topics: str | Iterable[str] | None = None) -> dict[str, object]:
    topics = normalize_live_update_topics(raw_topics)
    with _lock:
        requested_versions = {topic: _versions.get(topic, 0) for topic in topics}
        global_version = _versions.get(LIVE_UPDATE_TOPIC_GLOBAL, 0)
        token_parts = [f"{LIVE_UPDATE_TOPIC_GLOBAL}:{global_version}", *[f"{topic}:{requested_versions[topic]}" for topic in topics]]
        timestamps = [_last_changed_at.get(topic) for topic in (LIVE_UPDATE_TOPIC_GLOBAL, *topics)]
    last_changed_at = max((value for value in timestamps if value), default=None)
    return {
        "topics": list(topics),
        "token": "|".join(token_parts),
        "global_version": global_version,
        "versions": requested_versions,
        "last_changed_at": last_changed_at,
        "poll_interval_ms": LIVE_UPDATE_POLL_INTERVAL_MS,
    }


def publish_live_update(*topics: str) -> dict[str, object]:
    normalized_topics = normalize_live_update_topics(topics)
    changed_at = _utcnow_iso()
    with _lock:
        _versions[LIVE_UPDATE_TOPIC_GLOBAL] = _versions.get(LIVE_UPDATE_TOPIC_GLOBAL, 0) + 1
        _last_changed_at[LIVE_UPDATE_TOPIC_GLOBAL] = changed_at
        for topic in normalized_topics:
            _versions[topic] = _versions.get(topic, 0) + 1
            _last_changed_at[topic] = changed_at
    return live_update_snapshot(normalized_topics)


def reset_live_updates() -> None:
    with _lock:
        _versions.clear()
        _versions[LIVE_UPDATE_TOPIC_GLOBAL] = 0
        _last_changed_at.clear()
