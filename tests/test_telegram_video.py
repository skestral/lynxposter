from __future__ import annotations

import json
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from app.adapters.telegram import TELEGRAM_FILE_MAX_BYTES, TelegramDestinationAdapter


def _post_and_account(path: Path, mime_type: str):
    attachment = SimpleNamespace(
        id="media-1", storage_path=str(path), mime_type=mime_type, alt_text="", sort_order=0,
        size_bytes=path.stat().st_size, checksum="original-checksum",
    )
    post = SimpleNamespace(body="Caption", attachments=[attachment], publish_overrides_json={})
    account = SimpleNamespace(
        id="telegram-1", service="telegram",
        credentials_json={"bot_token": "test-token", "channel_id": "@test_channel"},
    )
    return post, account


@pytest.mark.parametrize("album", [False, True])
@pytest.mark.parametrize("upload_fails", [False, True])
def test_telegram_uploads_prepared_mp4_and_preserves_sources(monkeypatch, tmp_path, album, upload_fails):
    path = tmp_path / "original.mov"
    path.write_bytes(b"original MOV")
    post, account = _post_and_account(path, "video/quicktime")
    if album:
        photo_path = tmp_path / "photo.png"
        Image.new("RGB", (100, 100), "red").save(photo_path)
        post.attachments.append(SimpleNamespace(
            id="media-2", storage_path=str(photo_path), mime_type="image/png", alt_text="", sort_order=1,
            size_bytes=photo_path.stat().st_size, checksum="photo-checksum",
        ))
    prepared_path = tmp_path / "prepared.mp4"
    captured = {}
    upload_handles = []

    @contextmanager
    def fake_prepare_video(source, *, limits):
        assert source == path
        assert limits.max_bytes == TELEGRAM_FILE_MAX_BYTES
        prepared_path.write_bytes(b"compatible MP4 bytes")
        try:
            yield SimpleNamespace(path=prepared_path, mime_type="video/mp4", width=640, height=480, duration=1.5)
        finally:
            prepared_path.unlink()

    def fake_request(token, method, **kwargs):
        captured.update(method=method, **kwargs)
        upload_handles.extend(value[1] for value in kwargs["files"].values())
        video_tuple = kwargs["files"]["file0" if album else "video"]
        assert video_tuple[0] == "prepared.mp4"
        assert video_tuple[1].read() == b"compatible MP4 bytes"
        assert video_tuple[2] == "video/mp4"
        if upload_fails:
            raise RuntimeError("Upload failed")
        message = {"message_id": 10, "date": 1_700_000_000, "chat": {"username": "test_channel"}}
        return [message, {**message, "message_id": 11}] if album else message

    monkeypatch.setattr("app.adapters.telegram.prepare_video", fake_prepare_video)
    monkeypatch.setattr("app.adapters.telegram._telegram_request", fake_request)
    if upload_fails:
        with pytest.raises(RuntimeError, match="Upload failed"):
            TelegramDestinationAdapter().publish(None, post, None, account)
    else:
        result = TelegramDestinationAdapter().publish(None, post, None, account)
        assert result.external_id == "10"

    assert captured["method"] == ("sendMediaGroup" if album else "sendVideo")
    video_metadata = json.loads(captured["data"]["media"])[0] if album else captured["data"]
    assert video_metadata["supports_streaming"] is True
    assert video_metadata["width"] == 640
    assert video_metadata["height"] == 480
    assert video_metadata["duration"] == 2
    assert all(handle.closed for handle in upload_handles)
    assert not prepared_path.exists()
    assert path.read_bytes() == b"original MOV"
    assert post.attachments[0].storage_path == str(path)
    assert post.attachments[0].mime_type == "video/quicktime"
    assert post.attachments[0].checksum == "original-checksum"


def test_telegram_prepares_photo_dimensions_before_upload(monkeypatch, tmp_path):
    path = tmp_path / "wide.png"
    Image.new("RGB", (6000, 400), "red").save(path)
    original_bytes = path.read_bytes()
    post, account = _post_and_account(path, "image/png")
    handles = []
    prepared_paths = []

    def fake_request(token, method, **kwargs):
        assert method == "sendPhoto"
        filename, handle, mime_type = kwargs["files"]["photo"]
        handles.append(handle)
        prepared_paths.append(Path(handle.name))
        with Image.open(handle) as image:
            assert max(image.size) <= 5000
            assert sum(image.size) <= 10000
            assert max(image.size) / min(image.size) <= 20
        assert prepared_paths[0].stat().st_size <= 10_000_000
        assert mime_type in {"image/jpeg", "image/png"}
        return {"message_id": 10}

    monkeypatch.setattr("app.adapters.telegram._telegram_request", fake_request)
    TelegramDestinationAdapter().publish(None, post, None, account)

    assert handles[0].closed
    assert not prepared_paths[0].exists()
    assert path.read_bytes() == original_bytes
    assert post.attachments[0].storage_path == str(path)


def test_telegram_rejects_extreme_photo_aspect_ratio_without_upload(monkeypatch, tmp_path):
    path = tmp_path / "panorama.png"
    Image.new("RGB", (2100, 100), "red").save(path)
    original_bytes = path.read_bytes()
    post, account = _post_and_account(path, "image/png")
    monkeypatch.setattr(
        "app.adapters.telegram._telegram_request", lambda *args, **kwargs: pytest.fail("Invalid photos must not upload")
    )
    with pytest.raises(ValueError, match="aspect ratio"):
        TelegramDestinationAdapter().publish(None, post, None, account)
    assert path.read_bytes() == original_bytes


def test_telegram_video_preparation_failure_does_not_upload(monkeypatch, tmp_path):
    path = tmp_path / "invalid.mov"
    path.write_bytes(b"invalid video")
    post, account = _post_and_account(path, "video/quicktime")

    @contextmanager
    def failing_preparation(source, *, limits):
        raise ValueError("Invalid video stream")
        yield

    monkeypatch.setattr("app.adapters.telegram.prepare_video", failing_preparation)
    monkeypatch.setattr(
        "app.adapters.telegram._telegram_request", lambda *args, **kwargs: pytest.fail("Unprepared video must not upload")
    )
    with pytest.raises(ValueError, match="Invalid video stream"):
        TelegramDestinationAdapter().publish(None, post, None, account)
    assert path.read_bytes() == b"invalid video"


def test_telegram_preview_describes_video_conversion_without_preparing(monkeypatch, tmp_path):
    path = tmp_path / "original.webm"
    path.write_bytes(b"original video")
    post, account = _post_and_account(path, "video/webm")
    monkeypatch.setattr("app.adapters.telegram.prepare_video", lambda *args, **kwargs: pytest.fail("Preview must not transcode"))

    preview = TelegramDestinationAdapter().preview(post, None, account)

    assert preview.endpoint_label == "/sendVideo"
    assert preview.request_shape["supports_streaming"] is True
    assert preview.request_shape["file"]["mime_type"] == "video/mp4"
    assert any("H.264" in note and "originals are preserved" in note for note in preview.notes)


def test_telegram_transcodes_real_mov_before_native_video_upload(monkeypatch, tmp_path):
    ffmpeg, ffprobe = shutil.which("ffmpeg"), shutil.which("ffprobe")
    if not ffmpeg or not ffprobe:
        pytest.skip("FFmpeg and ffprobe are required for codec integration")
    path = tmp_path / "source.mov"
    subprocess.run([
        ffmpeg, "-y", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=160x120:rate=15",
        "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100", "-t", "0.5",
        "-c:v", "mpeg4", "-pix_fmt", "yuv420p", "-c:a", "pcm_s16le", str(path),
    ], check=True, capture_output=True, timeout=30)
    original_bytes = path.read_bytes()
    post, account = _post_and_account(path, "video/quicktime")
    uploads = []

    def fake_request(token, method, **kwargs):
        assert method == "sendVideo"
        filename, handle, mime_type = kwargs["files"]["video"]
        uploads.append(Path(handle.name))
        assert filename.endswith(".mp4")
        assert mime_type == "video/mp4"
        result = subprocess.run([
            ffprobe, "-v", "error", "-show_streams", "-of", "json", str(uploads[0]),
        ], check=True, capture_output=True, text=True, timeout=30)
        streams = json.loads(result.stdout)["streams"]
        video = next(stream for stream in streams if stream["codec_type"] == "video")
        audio = next(stream for stream in streams if stream["codec_type"] == "audio")
        assert video["codec_name"] == "h264"
        assert video["pix_fmt"] == "yuv420p"
        assert audio["codec_name"] == "aac"
        assert kwargs["data"]["supports_streaming"] is True
        return {"message_id": 10}

    monkeypatch.setattr("app.adapters.telegram._telegram_request", fake_request)
    TelegramDestinationAdapter().publish(None, post, None, account)

    assert path.read_bytes() == original_bytes
    assert not uploads[0].exists()
