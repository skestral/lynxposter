from __future__ import annotations

import shutil
import subprocess
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from app.adapters.instagram import InstagramDestinationAdapter, _prepared_graph_attachments
from app.config import get_settings
from app.models import Account, CanonicalPost, MediaAttachment, Persona
from app.services.storage import prepared_instagram_media_path, prune_unreferenced_managed_media_files
from app.services.video_preparation import _probe


@pytest.fixture
def preparation_settings(monkeypatch, tmp_path):
    settings = replace(get_settings(), app_base_url="https://lynxposter.example.com", uploads_dir=tmp_path / "uploads", imported_media_dir=tmp_path / "imported_media")
    monkeypatch.setattr("app.adapters.instagram.get_settings", lambda: settings)
    return settings


def _account():
    return Account(id="instagram", service="instagram", credentials_json={"api_key": "fake-token", "instagram_user_id": "123"}, publish_settings_json={})


def test_instagram_stages_jpeg_without_mutating_canonical_attachment(preparation_settings, tmp_path):
    path = tmp_path / "original.png"
    Image.new("RGBA", (400, 400), (10, 20, 30, 0)).save(path)
    original = path.read_bytes()
    attachment = MediaAttachment(id="photo", storage_path=str(path), mime_type="image/png", alt_text="Alt text", checksum="original", size_bytes=len(original))
    with _prepared_graph_attachments([attachment], _account()) as items:
        prepared = items[0]
        staged = Path(prepared.storage_path)
        assert staged.exists()
        assert prepared.mime_type == "image/jpeg"
        token = staged.parent.parent.name
        assert prepared_instagram_media_path("photo", token, "image.jpg", uploads_dir=preparation_settings.uploads_dir) == staged
        assert prepared.prepared_public_url.endswith(f"/photo/prepared/{token}/image.jpg")
        with Image.open(staged) as actual:
            assert actual.getpixel((0, 0)) == (255, 255, 255)
        assert attachment.storage_path == str(path)
        assert attachment.mime_type == "image/png"
        assert path.read_bytes() == original
    assert not staged.exists()
    assert path.read_bytes() == original


def test_instagram_waits_for_processing_with_public_copy_available(preparation_settings, tmp_path, monkeypatch):
    path = tmp_path / "original.png"
    Image.new("RGB", (400, 400), "blue").save(path)
    attachment = MediaAttachment(id="photo", storage_path=str(path), mime_type="image/png", alt_text="", size_bytes=path.stat().st_size)
    post = CanonicalPost(body="Caption", publish_overrides_json={}, attachments=[attachment])
    captured = {}
    polls = []
    monkeypatch.setattr("app.adapters.instagram.time.sleep", lambda _: None)

    def post_graph(endpoint, data, *, base_url):
        if endpoint == "123/media":
            url = data["image_url"]
            token = url.split("/")[-2]
            captured["staged"] = prepared_instagram_media_path("photo", token, "image.jpg", uploads_dir=preparation_settings.uploads_dir)
            assert captured["staged"].is_file()
            return {"id": "container"}
        assert endpoint == "123/media_publish"
        assert len(polls) == 12
        assert captured["staged"].is_file()
        return {"id": "published"}

    def get_graph(endpoint, data, *, base_url):
        assert captured["staged"].is_file()
        if endpoint == "container":
            polls.append(endpoint)
            return {"status_code": "FINISHED" if len(polls) == 12 else "IN_PROGRESS"}
        return {"permalink": "https://instagram.com/p/example"}

    monkeypatch.setattr("app.adapters.instagram._post_graph", post_graph)
    monkeypatch.setattr("app.adapters.instagram._get_graph", get_graph)
    result = InstagramDestinationAdapter().publish(None, post, Persona(), _account())
    assert result.external_id == "published"
    assert not captured["staged"].exists()
    assert path.exists()


def test_instagram_unreadable_video_fails_before_container_creation(preparation_settings, tmp_path, monkeypatch):
    path = tmp_path / "broken.mp4"
    path.write_bytes(b"invalid video")
    attachment = MediaAttachment(id="video", storage_path=str(path), mime_type="video/mp4", alt_text="", size_bytes=13)
    post = CanonicalPost(body="Caption", publish_overrides_json={}, attachments=[attachment])
    monkeypatch.setattr("app.adapters.instagram._post_graph", lambda *a, **k: pytest.fail("No container should be created for invalid media."))
    with pytest.raises(ValueError, match="video"):
        InstagramDestinationAdapter().publish(None, post, Persona(), _account())
    assert list((preparation_settings.uploads_dir / ".instagram-prepared").iterdir()) == []


def test_instagram_transcodes_actual_video_before_public_staging(preparation_settings, tmp_path):
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        pytest.skip("FFmpeg not installed")
    path = tmp_path / "source.mov"
    subprocess.run([shutil.which("ffmpeg"), "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=red:s=96x48:r=12:d=3.2", "-c:v", "mpeg4", str(path)], check=True, capture_output=True, timeout=30)
    original = path.read_bytes()
    attachment = MediaAttachment(id="video", storage_path=str(path), mime_type="video/quicktime", alt_text="", size_bytes=len(original))
    with _prepared_graph_attachments([attachment], _account()) as items:
        staged = Path(items[0].storage_path)
        info = _probe(staged)
        assert info.codec == "h264"
        assert 23 <= info.frame_rate <= 60
        assert info.duration >= 3
        assert staged.stat().st_size <= 300_000_000
    assert not staged.exists()
    assert path.read_bytes() == original


def test_instagram_missing_processing_status_does_not_publish(monkeypatch):
    from app.adapters.instagram import _wait_for_container_ready
    monkeypatch.setattr("app.adapters.instagram._get_graph", lambda *a, **k: {})
    with pytest.raises(RuntimeError, match="unknown processing status"):
        _wait_for_container_ready("container", "fake-token", base_url="https://example.test")


def test_prepared_public_path_rejects_unrelated_filenames_and_traversal(tmp_path):
    for attachment_id, token, filename in [("../secret", "instagram-1234abcd", "image.jpg"), ("photo", "../secret", "image.jpg"), ("photo", "instagram-1234abcd", "../secret"), ("photo", "instagram-1234abcd", "passwords.txt")]:
        assert prepared_instagram_media_path(attachment_id, token, filename, uploads_dir=tmp_path) is None


def test_instagram_validation_accepts_convertible_static_images(preparation_settings, tmp_path):
    path = tmp_path / "image.bmp"
    Image.new("RGB", (400, 400), "green").save(path)
    attachment = MediaAttachment(id="bmp", storage_path=str(path), mime_type="image/bmp", alt_text="", size_bytes=path.stat().st_size)
    post = CanonicalPost(body="Caption", publish_overrides_json={}, attachments=[attachment])
    assert InstagramDestinationAdapter().validate(post, Persona(), _account()) == []
    with _prepared_graph_attachments([attachment], _account()) as items:
        assert items[0].mime_type == "image/jpeg"
    assert path.exists()
