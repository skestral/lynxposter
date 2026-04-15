from __future__ import annotations

from dataclasses import replace
from io import BytesIO

from app.services.storage import _normalized_filename, download_media, settings


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
