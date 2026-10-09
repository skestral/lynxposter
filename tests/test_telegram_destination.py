from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.adapters.telegram import (
    TELEGRAM_FILE_MAX_BYTES,
    TELEGRAM_PHOTO_MAX_BYTES,
    TELEGRAM_UPLOAD_TIMEOUT,
    TelegramDestinationAdapter,
)


def _account():
    return SimpleNamespace(
        id="telegram-account",
        service="telegram",
        credentials_json={"bot_token": "test-token", "channel_id": "@test_channel"},
    )


def _attachment(tmp_path, name, mime_type, *, sort_order=0, size_bytes=None):
    path = tmp_path / name
    path.write_bytes(b"test media")
    return SimpleNamespace(
        id=name,
        storage_path=str(path),
        mime_type=mime_type,
        alt_text="",
        sort_order=sort_order,
        size_bytes=path.stat().st_size if size_bytes is None else size_bytes,
    )


def _post(attachments, body="Video caption"):
    return SimpleNamespace(body=body, attachments=attachments, publish_overrides_json={})


def _message(message_id=10):
    return {"message_id": message_id, "date": 1_700_000_000, "chat": {"username": "test_channel"}}


@pytest.fixture(autouse=True)
def stub_media_preparation(monkeypatch):
    # These tests check Telegram routing and HTTP handles. Real codec and image
    # preparation are covered in dedicated helper and adapter integration tests.
    @contextmanager
    def fake_image(path, *, limits):
        yield SimpleNamespace(path=Path(path), mime_type="image/jpeg", width=640, height=480)

    @contextmanager
    def fake_video(path, *, limits):
        yield SimpleNamespace(path=Path(path), mime_type="video/mp4", width=640, height=480, duration=1)

    monkeypatch.setattr("app.adapters.telegram.prepare_image", fake_image)
    monkeypatch.setattr("app.adapters.telegram.prepare_video", fake_video)


@pytest.mark.parametrize(
    ("name", "mime_type", "expected_method", "file_field"),
    [
        ("clip.mp4", "video/mp4", "sendVideo", "video"),
        ("clip.mp4", "application/octet-stream", "sendVideo", "video"),
        ("clip.mov", "video/quicktime", "sendVideo", "video"),
        ("clip.webm", "video/webm", "sendVideo", "video"),
        ("photo.jpg", "image/jpeg", "sendPhoto", "photo"),
        ("notes.pdf", "application/pdf", "sendDocument", "document"),
    ],
)
def test_single_media_routes_to_matching_upload_and_preview(tmp_path, monkeypatch, name, mime_type, expected_method, file_field):
    adapter = TelegramDestinationAdapter()
    post = _post([_attachment(tmp_path, name, mime_type)])
    calls = []

    def fake_request(token, method, **kwargs):
        file_tuple = kwargs["files"][file_field]
        assert not file_tuple[1].closed
        assert file_tuple[1].read() == b"test media"
        calls.append((method, kwargs, file_tuple[1]))
        return _message()

    monkeypatch.setattr("app.adapters.telegram._telegram_request", fake_request)
    assert adapter.validate(post, None, _account()) == []
    preview = adapter.preview(post, None, _account())
    result = adapter.publish(None, post, None, _account())

    assert preview.endpoint_label == f"/{expected_method}"
    assert result.external_url == "https://t.me/test_channel/10"
    method, kwargs, handle = calls[0]
    assert method == expected_method
    expected_data = {"chat_id": "@test_channel", "caption": "Video caption"}
    if file_field == "video":
        expected_data.update(supports_streaming=True, width=640, height=480, duration=1)
        assert kwargs["files"][file_field][2] == "video/mp4"
    assert kwargs["data"] == expected_data
    assert kwargs["timeout"] == TELEGRAM_UPLOAD_TIMEOUT
    assert handle.closed


@pytest.mark.parametrize(
    ("video_name", "mime_type", "expected_types"),
    [
        ("clip.mp4", "video/mp4", ["photo", "video"]),
        ("clip.mov", "video/quicktime", ["photo", "video"]),
    ],
)
def test_mixed_video_albums_use_api_compatible_media_types(tmp_path, monkeypatch, video_name, mime_type, expected_types):
    adapter = TelegramDestinationAdapter()
    post = _post([
        _attachment(tmp_path, video_name, mime_type, sort_order=1),
        _attachment(tmp_path, "photo.jpg", "image/jpeg", sort_order=0),
    ])
    captured = {}
    handles = []

    def fake_request(token, method, **kwargs):
        captured.update(method=method, **kwargs)
        handles.extend(item[1] for item in kwargs["files"].values())
        assert all(not handle.closed for handle in handles)
        return [_message(10), _message(11)]

    monkeypatch.setattr("app.adapters.telegram._telegram_request", fake_request)
    assert adapter.validate(post, None, _account()) == []
    preview = adapter.preview(post, None, _account())
    result = adapter.publish(None, post, None, _account())
    media = json.loads(captured["data"]["media"])

    assert captured["method"] == "sendMediaGroup"
    assert [item["type"] for item in media] == expected_types
    assert [item["type"] for item in preview.request_shape["media"]] == expected_types
    assert [item["media"] for item in media] == ["attach://file0", "attach://file1"]
    assert media[0]["caption"] == "Video caption"
    assert "caption" not in media[1]
    assert captured["timeout"] == TELEGRAM_UPLOAD_TIMEOUT
    assert len(result.external_refs) == 2
    assert all(handle.closed for handle in handles)


@pytest.mark.parametrize("album", [False, True])
def test_upload_failure_closes_media_handles(tmp_path, monkeypatch, album):
    attachments = [_attachment(tmp_path, "clip.mp4", "video/mp4")]
    if album:
        attachments.append(_attachment(tmp_path, "photo.jpg", "image/jpeg", sort_order=1))
    handles = []

    def fake_request(token, method, **kwargs):
        handles.extend(item[1] for item in kwargs["files"].values())
        raise RuntimeError("Upload failed")

    monkeypatch.setattr("app.adapters.telegram._telegram_request", fake_request)
    with pytest.raises(RuntimeError, match="Upload failed"):
        TelegramDestinationAdapter().publish(None, _post(attachments), None, _account())
    assert all(handle.closed for handle in handles)


@pytest.mark.parametrize(
    ("mime_type", "limit"),
    [("video/mp4", TELEGRAM_FILE_MAX_BYTES), ("image/jpeg", TELEGRAM_PHOTO_MAX_BYTES)],
)
def test_validation_allows_oversized_images_and_videos_for_preparation(tmp_path, mime_type, limit):
    attachment = SimpleNamespace(
        id="oversized",
        storage_path=str(tmp_path / "not-stored.mp4"),
        mime_type=mime_type,
        sort_order=0,
        size_bytes=limit + 1,
    )
    adapter = TelegramDestinationAdapter()
    issues = adapter.validate(_post([attachment]), None, _account())
    assert issues == []
    attachment.size_bytes = limit
    assert adapter.validate(_post([attachment]), None, _account()) == []


def test_validation_checks_stored_file_size_instead_of_stale_metadata(tmp_path):
    attachment = _attachment(tmp_path, "notes.pdf", "application/pdf", size_bytes=1)
    with open(attachment.storage_path, "r+b") as handle:
        handle.truncate(TELEGRAM_FILE_MAX_BYTES + 1)
    issues = TelegramDestinationAdapter().validate(_post([attachment]), None, _account())
    assert len(issues) == 1
    assert "50 MB" in issues[0].message


def test_media_selection_override_is_used_for_upload_and_preview(tmp_path, monkeypatch):
    photo = _attachment(tmp_path, "photo.jpg", "image/jpeg")
    video = _attachment(tmp_path, "clip.mp4", "video/mp4", sort_order=1)
    post = _post([photo, video])
    post.publish_overrides_json = {"telegram": {"attachment_ids": [video.id], "body": "Selected clip"}}
    captured = {}

    def fake_request(token, method, **kwargs):
        captured.update(method=method, **kwargs)
        return _message()

    monkeypatch.setattr("app.adapters.telegram._telegram_request", fake_request)
    adapter = TelegramDestinationAdapter()
    preview = adapter.preview(post, None, _account())
    adapter.publish(None, post, None, _account())
    assert preview.endpoint_label == "/sendVideo"
    assert captured["method"] == "sendVideo"
    assert captured["data"]["caption"] == "Selected clip"
    assert list(captured["files"]) == ["video"]
