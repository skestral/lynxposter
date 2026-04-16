from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.models import Account, AccountSyncState, CanonicalPost, DeliveryJob, Persona


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def service_body(post: CanonicalPost, account: Account) -> str:
    overrides = post.publish_overrides_json or {}
    for key in (account.id, account.service):
        override = overrides.get(key)
        if isinstance(override, str):
            return override
        if isinstance(override, dict) and override.get("body") is not None:
            return str(override["body"])
    return post.body


def attachment_kind(attachment: Any) -> str:
    mime_type = str(getattr(attachment, "mime_type", "") or "").lower()
    if mime_type.startswith("video/"):
        return "video"
    if mime_type.startswith("image/"):
        return "image"

    suffix = Path(str(getattr(attachment, "storage_path", "") or "")).suffix.lower()
    if suffix in {".mp4", ".mov", ".m4v", ".webm"}:
        return "video"
    if suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tif", ".tiff"}:
        return "image"
    return "other"


def is_video_attachment(attachment: Any) -> bool:
    return attachment_kind(attachment) == "video"


def is_image_attachment(attachment: Any) -> bool:
    return attachment_kind(attachment) == "image"


def logical_post_limit_reached(persona: Persona, posts_published_within_hour: int) -> bool:
    limit = int((persona.throttle_settings_json or {}).get("max_per_hour", 0) or 0)
    return limit > 0 and posts_published_within_hour >= limit


def cutoff_for_initial_poll(persona: Persona, account: Account) -> datetime:
    raw_hours = (account.source_settings_json or {}).get("post_time_limit")
    if raw_hours in (None, ""):
        raw_hours = (persona.settings_json or {}).get("post_time_limit", 12)
    return now_utc() - timedelta(hours=int(raw_hours or 12))


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def import_existing_posts_on_first_scan(persona: Persona, account: Account) -> bool:
    raw = (account.source_settings_json or {}).get("import_existing_posts")
    if raw in (None, ""):
        raw = (persona.settings_json or {}).get("import_existing_posts", False)
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def autorun_initial_import_guard_reason(persona: Persona, account: Account, sync_state: AccountSyncState | None) -> str | None:
    if not is_initial_sync(sync_state):
        return None
    if not import_existing_posts_on_first_scan(persona, account):
        return None
    return (
        f"Skipped polling {account.label} during autorun because first-sync historical imports are manual-only. "
        "Disable 'import existing posts on first scan' or run a manual cycle when you intentionally want a backfill."
    )


def is_initial_sync(sync_state: AccountSyncState | None) -> bool:
    if sync_state is None:
        return True
    if sync_state.last_polled_at is not None:
        return False
    if sync_state.cursor:
        return False
    state_json = dict(sync_state.state_json or {})
    return not any(state_json.get(key) for key in ("last_seen_at", "last_seen_id"))


def looks_like_historical_backfill(post: CanonicalPost, persona: Persona, source_account: Account | None) -> bool:
    if source_account is None or import_existing_posts_on_first_scan(persona, source_account):
        return False
    published_at = _as_utc(post.published_at)
    created_at = _as_utc(post.created_at)
    if published_at is None or created_at is None:
        return False
    return created_at - published_at > timedelta(hours=int((source_account.source_settings_json or {}).get("post_time_limit") or (persona.settings_json or {}).get("post_time_limit", 12) or 12))


def delivery_summary(job: DeliveryJob) -> dict[str, Any]:
    label = job.target_account.label if job.target_account else job.target_account_id
    service = job.target_account.service if job.target_account else "unknown"
    return {
        "account_id": job.target_account_id,
        "label": label,
        "service": service,
        "status": job.status,
        "external_id": job.external_id,
        "external_url": job.external_url,
        "attempt_count": job.attempt_count,
        "last_error": job.last_error,
        "delivered_at": job.delivered_at,
    }
