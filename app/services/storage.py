from __future__ import annotations

import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import requests
from fastapi import UploadFile

from app.config import get_settings
from app.domain import MediaItem
from app.utils import detect_mime_type, stable_checksum


settings = get_settings()
_MAX_FILENAME_STEM_LENGTH = 96
_INVALID_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


def ensure_storage_dirs() -> None:
    settings.app_data_dir.mkdir(parents=True, exist_ok=True)
    settings.uploads_dir.mkdir(parents=True, exist_ok=True)
    settings.imported_media_dir.mkdir(parents=True, exist_ok=True)
    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    settings.backups_dir.mkdir(parents=True, exist_ok=True)


def managed_media_roots() -> tuple[Path, Path]:
    return settings.uploads_dir.resolve(), settings.imported_media_dir.resolve()


def resolve_managed_media_path(path_like: str | Path) -> Path | None:
    try:
        candidate = Path(path_like).resolve(strict=False)
    except (OSError, RuntimeError, TypeError):
        return None
    for root in managed_media_roots():
        if candidate == root or candidate.is_relative_to(root):
            return candidate
    return None


def delete_managed_media_file(path_like: str | Path) -> bool:
    file_path = resolve_managed_media_path(path_like)
    if file_path is None or not file_path.is_file():
        return False
    file_path.unlink(missing_ok=True)
    return True


def prune_unreferenced_managed_media_files(
    referenced_paths: set[str],
    *,
    retention_days: int,
    now: datetime | None = None,
) -> dict[str, int]:
    ensure_storage_dirs()
    if retention_days <= 0:
        return {"scanned": 0, "deleted": 0, "errors": 0}

    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=retention_days)
    deleted = 0
    errors = 0
    scanned = 0

    for root in managed_media_roots():
        for file_path in root.rglob("*"):
            if not file_path.is_file():
                continue
            resolved = str(file_path.resolve())
            scanned += 1
            if resolved in referenced_paths:
                continue
            try:
                modified_at = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
            except OSError:
                errors += 1
                continue
            if modified_at > cutoff:
                continue
            try:
                file_path.unlink()
                deleted += 1
            except OSError:
                errors += 1

    return {"scanned": scanned, "deleted": deleted, "errors": errors}


def _normalized_filename(original_name: str, *, default_name: str = "media.bin") -> str:
    candidate = str(original_name or "").strip()
    if candidate:
        parsed = urlsplit(candidate)
        if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
            candidate = parsed.path
    candidate = unquote(candidate)
    candidate = Path(candidate).name

    if not candidate:
        candidate = default_name

    suffix = Path(candidate).suffix
    stem = Path(candidate).stem or Path(default_name).stem or "media"
    stem = _INVALID_FILENAME_CHARS.sub("-", stem).strip("._-") or "media"
    if len(stem) > _MAX_FILENAME_STEM_LENGTH:
        stem = stem[:_MAX_FILENAME_STEM_LENGTH].rstrip("._-") or "media"

    safe_suffix = re.sub(r"[^A-Za-z0-9.]+", "", suffix)[:16]
    if not safe_suffix:
        safe_suffix = Path(default_name).suffix or ".bin"

    return f"{stem}{safe_suffix}"


def _unique_path(directory: Path, original_name: str) -> Path:
    safe_name = _normalized_filename(original_name)
    suffix = Path(safe_name).suffix
    name = Path(safe_name).stem or "media"
    return directory / f"{name}-{uuid4().hex}{suffix}"


async def store_upload(upload: UploadFile, alt_text: str = "", sort_order: int = 0) -> MediaItem:
    ensure_storage_dirs()
    target = _unique_path(settings.uploads_dir, upload.filename or "upload.bin")
    with target.open("wb") as handle:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
    size_bytes = target.stat().st_size
    mime_type = upload.content_type or detect_mime_type(target)
    return MediaItem(
        storage_path=target,
        mime_type=mime_type,
        alt_text=alt_text,
        size_bytes=size_bytes,
        checksum=stable_checksum(target),
        sort_order=sort_order,
    )


def download_media(url: str, filename_hint: str, alt_text: str = "", sort_order: int = 0) -> MediaItem:
    ensure_storage_dirs()
    target = _unique_path(settings.imported_media_dir, filename_hint)
    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()
    with target.open("wb") as handle:
        shutil.copyfileobj(response.raw, handle)
    size_bytes = target.stat().st_size
    mime_type = response.headers.get("content-type") or detect_mime_type(target)
    return MediaItem(
        storage_path=target,
        mime_type=mime_type,
        alt_text=alt_text,
        size_bytes=size_bytes,
        checksum=stable_checksum(target),
        sort_order=sort_order,
    )

