from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from io import BytesIO

from app.services.storage import (
    _normalized_filename,
    delete_managed_media_file,
    download_media,
    prune_unreferenced_managed_media_files,
    settings,
)


class _FakeResponse:
    def __init__(self):
        self.raw = BytesIO(b"jpeg-bytes")
        self.headers = {"content-type": "image/jpeg"}

    def raise_for_status(self):
        return None


def test_normalized_filename_strips_query_params_and_truncates_length():
    filename = _normalized_filename(
        "670885535_17956262571115984_8589972950050577637_n.jpg?stp=dst-jpg_e35_tt6&_nc_cat=109&oh=abc",
    )

    assert "?" not in filename
    assert "&" not in filename
    assert filename.endswith(".jpg")
    assert len(filename) <= 120


def test_download_media_uses_sanitized_filename(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.storage.settings", replace(settings, imported_media_dir=tmp_path))
    monkeypatch.setattr("app.services.storage.requests.get", lambda *args, **kwargs: _FakeResponse())

    media = download_media(
        "https://instagram.example/media/670885535_17956262571115984_8589972950050577637_n.jpg?stp=dst-jpg_e35_tt6&_nc_cat=109",
        "670885535_17956262571115984_8589972950050577637_n.jpg?stp=dst-jpg_e35_tt6&_nc_cat=109",
    )

    assert media.storage_path.exists()
    assert media.storage_path.parent == tmp_path
    assert "?" not in media.storage_path.name
    assert media.storage_path.name.endswith(".jpg")


def test_delete_managed_media_file_ignores_paths_outside_managed_roots(monkeypatch, tmp_path):
    uploads_dir = tmp_path / "uploads"
    imported_dir = tmp_path / "imported"
    uploads_dir.mkdir()
    imported_dir.mkdir()
    monkeypatch.setattr(
        "app.services.storage.settings",
        replace(settings, uploads_dir=uploads_dir, imported_media_dir=imported_dir),
    )

    outside_file = tmp_path / "outside.jpg"
    outside_file.write_bytes(b"jpeg")

    assert delete_managed_media_file(outside_file) is False
    assert outside_file.exists()


def test_prune_unreferenced_managed_media_files_deletes_only_old_orphans(monkeypatch, tmp_path):
    uploads_dir = tmp_path / "uploads"
    imported_dir = tmp_path / "imported"
    uploads_dir.mkdir()
    imported_dir.mkdir()
    monkeypatch.setattr(
        "app.services.storage.settings",
        replace(settings, uploads_dir=uploads_dir, imported_media_dir=imported_dir),
    )

    referenced_file = uploads_dir / "keep.jpg"
    old_orphan = uploads_dir / "old-orphan.jpg"
    recent_orphan = imported_dir / "recent-orphan.jpg"
    for file_path in (referenced_file, old_orphan, recent_orphan):
        file_path.write_bytes(b"jpeg")

    old_timestamp = (datetime.now(timezone.utc) - timedelta(days=45)).timestamp()
    recent_timestamp = (datetime.now(timezone.utc) - timedelta(days=2)).timestamp()
    referenced_timestamp = (datetime.now(timezone.utc) - timedelta(days=45)).timestamp()
    referenced_file.touch()
    old_orphan.touch()
    recent_orphan.touch()
    import os

    os.utime(referenced_file, (referenced_timestamp, referenced_timestamp))
    os.utime(old_orphan, (old_timestamp, old_timestamp))
    os.utime(recent_orphan, (recent_timestamp, recent_timestamp))

    result = prune_unreferenced_managed_media_files(
        referenced_paths={str(referenced_file.resolve())},
        retention_days=30,
    )

    assert result["deleted"] == 1
    assert result["errors"] == 0
    assert referenced_file.exists()
    assert not old_orphan.exists()
    assert recent_orphan.exists()
