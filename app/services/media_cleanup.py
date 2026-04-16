from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import MediaAttachment
from app.services.events import log_run_event
from app.services.storage import prune_unreferenced_managed_media_files, resolve_managed_media_path


def cleanup_stale_media_files(session: Session, *, run_id: str | None = None) -> dict[str, int]:
    settings = get_settings()
    referenced_paths = {
        str(resolved)
        for storage_path in session.scalars(select(MediaAttachment.storage_path))
        if (resolved := resolve_managed_media_path(storage_path)) is not None
    }
    result = prune_unreferenced_managed_media_files(
        referenced_paths,
        retention_days=settings.media_orphan_retention_days,
    )
    if run_id and (result["deleted"] or result["errors"]):
        log_run_event(
            session,
            run_id=run_id,
            operation="storage_cleanup",
            severity="warning" if result["errors"] else "info",
            message=(
                f"Removed {result['deleted']} stale media file(s) from managed storage."
                if result["deleted"]
                else "Managed media cleanup encountered file-system errors."
            ),
            metadata={
                "scanned_files": result["scanned"],
                "deleted_files": result["deleted"],
                "error_count": result["errors"],
                "retention_days": settings.media_orphan_retention_days,
            },
        )
    return result
