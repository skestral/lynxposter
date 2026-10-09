from __future__ import annotations

import shutil
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from random import Random
from types import SimpleNamespace

import pytest
from PIL import Image, UnidentifiedImageError

from app.adapters.base import ConfigurationError
from app.adapters.discord import DiscordDestinationAdapter
from app.adapters.tumblr import TumblrDestinationAdapter
from app.adapters.twitter import TwitterDestinationAdapter
from app.models import Account, CanonicalPost, MediaAttachment, Persona


def _stack(service, source_path, mime_type, *, publish_settings=None):
    credentials = {
        "twitter": {"app_key": "test", "app_secret": "test", "access_token": "test", "access_token_secret": "test"},
        "tumblr": {"consumer_key": "test", "consumer_secret": "test", "oauth_token": "test", "oauth_secret": "test", "blog_name": "test-blog"},
        "discord": {"webhook_url": "https://discord.test/webhook"},
    }
    attachment = MediaAttachment(storage_path=str(source_path), mime_type=mime_type, alt_text="Accessible art", sort_order=0)
    post = CanonicalPost(body="Art", publish_overrides_json={}, metadata_json={}, attachments=[attachment])
    account = Account(id="destination", service=service, credentials_json=credentials[service], publish_settings_json=publish_settings or {})
    persona = Persona(settings_json={})
    adapter = {"twitter": TwitterDestinationAdapter, "tumblr": TumblrDestinationAdapter, "discord": DiscordDestinationAdapter}[service]()
    return adapter, post, persona, account


def _install_upload_capture(monkeypatch, service, captured, *, media_type):
    def capture_path(path):
        path = Path(path)
        captured["path"] = path
        assert path.is_file()
        captured["bytes"] = path.read_bytes()
        if media_type == "image":
            with Image.open(path) as image:
                captured["format"] = image.format
                captured["mode"] = image.mode
        else:
            assert path.suffix == ".mp4"

    if service == "twitter":
        class FakeAPI:
            def media_upload(self, *, filename, media_category):
                captured["category"] = media_category
                capture_path(filename)
                return SimpleNamespace(media_id="media-id")

            def chunked_upload(self, *, filename, file_type, media_category, wait_for_async_finalize):
                captured["category"] = media_category
                captured["mime_type"] = file_type
                assert wait_for_async_finalize is True
                capture_path(filename)
                return SimpleNamespace(media_id="media-id")

            def create_media_metadata(self, media_id, alt_text):
                captured["alt_text"] = alt_text

        class FakeClient:
            def create_tweet(self, **kwargs):
                assert kwargs["media_ids"] == ["media-id"]
                return SimpleNamespace(data={"id": "tweet-id"})

        monkeypatch.setitem(sys.modules, "tweepy", SimpleNamespace(Client=lambda **kwargs: FakeClient(), OAuth1UserHandler=lambda *args: None, API=lambda *args: FakeAPI()))
    elif service == "tumblr":
        class FakeClient:
            def create_photo(self, blog_name, *, state, caption, data):
                assert len(data) == 1
                capture_path(data[0])
                return {"id": "tumblr-id"}

            def create_video(self, blog_name, *, state, caption, data):
                capture_path(data)
                return {"id": "tumblr-id"}

        monkeypatch.setitem(sys.modules, "pytumblr", SimpleNamespace(TumblrRestClient=lambda *args: FakeClient()))
    else:
        def fake_post(url, *, params, timeout, data, files):
            filename, handle, mime_type = files["files[0]"]
            captured["handle"] = handle
            captured["mime_type"] = mime_type
            captured["filename"] = filename
            capture_path(handle.name)
            assert not handle.closed
            return SimpleNamespace(content=b"{}", json=lambda: {"id": "discord-id"}, raise_for_status=lambda: None)

        monkeypatch.setattr("app.adapters.discord.requests.post", fake_post)


@pytest.mark.parametrize("service", ["twitter", "tumblr", "discord"])
def test_direct_upload_converts_unsupported_static_image_without_changing_source(service, monkeypatch, tmp_path):
    source_path = tmp_path / "art.bmp"
    Image.new("RGB", (120, 80), "orange").save(source_path)
    original = source_path.read_bytes()
    captured = {}
    _install_upload_capture(monkeypatch, service, captured, media_type="image")
    adapter, post, persona, account = _stack(service, source_path, "image/bmp")

    adapter.publish(None, post, persona, account)

    assert captured["format"] == "JPEG"
    assert not captured["path"].exists()
    assert source_path.read_bytes() == original
    if service == "twitter":
        assert captured["category"] == "tweet_image"
        assert captured["alt_text"] == "Accessible art"
    if service == "discord":
        assert captured["mime_type"] == "image/jpeg"
        assert captured["filename"] == "art.jpg"
        assert captured["handle"].closed


@pytest.mark.parametrize("service", ["twitter", "tumblr", "discord"])
def test_direct_upload_normalizes_video_and_cleans_temporary_output(service, monkeypatch, tmp_path):
    executable = shutil.which("ffmpeg")
    if not executable or not shutil.which("ffprobe"):
        pytest.skip("FFmpeg and ffprobe are required for this integration test")
    source_path = tmp_path / "clip.mov"
    subprocess.run([executable, "-v", "error", "-f", "lavfi", "-i", "color=c=orange:s=64x48:r=10", "-t", "1", "-an", "-c:v", "mpeg4", str(source_path)], check=True, capture_output=True)
    original = source_path.read_bytes()
    captured = {}
    _install_upload_capture(monkeypatch, service, captured, media_type="video")
    adapter, post, persona, account = _stack(service, source_path, "video/quicktime")

    adapter.publish(None, post, persona, account)

    assert not captured["path"].exists()
    assert source_path.read_bytes() == original
    if service == "twitter":
        assert captured["category"] == "tweet_video"
        assert captured["mime_type"] == "video/mp4"
        assert "alt_text" not in captured
    if service == "discord":
        assert captured["mime_type"] == "video/mp4"
        assert captured["filename"] == "clip.mp4"
        assert captured["handle"].closed


def test_discord_compresses_image_to_configured_file_budget(monkeypatch, tmp_path):
    source_path = tmp_path / "art.png"
    Image.frombytes("RGB", (150, 100), Random(8).randbytes(150 * 100 * 3)).save(source_path)
    original = source_path.read_bytes()
    captured = {}
    _install_upload_capture(monkeypatch, "discord", captured, media_type="image")
    adapter, post, persona, account = _stack("discord", source_path, "image/png", publish_settings={"max_upload_bytes": 6_000})

    adapter.publish(None, post, persona, account)

    assert len(captured["bytes"]) <= 6_000
    assert source_path.read_bytes() == original
    assert not captured["path"].exists()


def test_discord_rejects_oversized_audio_before_request(tmp_path):
    source_path = tmp_path / "track.wav"
    source_path.write_bytes(b"audio" * 10)
    adapter, post, persona, account = _stack("discord", source_path, "audio/wav", publish_settings={"max_upload_bytes": 10})

    with pytest.raises(ConfigurationError, match="audio exceeds"):
        adapter.publish(None, post, persona, account)


def test_discord_closes_file_and_removes_prepared_image_when_request_fails(monkeypatch, tmp_path):
    source_path = tmp_path / "art.bmp"
    Image.new("RGB", (120, 80), "orange").save(source_path)
    original = source_path.read_bytes()
    captured = {}

    def fail_request(url, **kwargs):
        captured["handle"] = kwargs["files"]["files[0]"][1]
        captured["path"] = Path(captured["handle"].name)
        raise RuntimeError("Simulated upload failure")

    monkeypatch.setattr("app.adapters.discord.requests.post", fail_request)
    adapter, post, persona, account = _stack("discord", source_path, "image/bmp")

    with pytest.raises(RuntimeError, match="Simulated upload failure"):
        adapter.publish(None, post, persona, account)

    assert captured["handle"].closed
    assert not captured["path"].exists()
    assert source_path.read_bytes() == original


def test_twitter_preserves_supported_animated_gif_and_uses_async_category(monkeypatch, tmp_path):
    source_path = tmp_path / "animation.gif"
    Image.new("RGB", (64, 48), "red").save(source_path, save_all=True, append_images=[Image.new("RGB", (64, 48), "blue")], duration=100, loop=0)
    original = source_path.read_bytes()
    captured = {}
    _install_upload_capture(monkeypatch, "twitter", captured, media_type="image")
    adapter, post, persona, account = _stack("twitter", source_path, "image/gif")

    adapter.publish(None, post, persona, account)

    assert captured["category"] == "tweet_gif"
    assert captured["mime_type"] == "image/gif"
    assert captured["bytes"] == original == source_path.read_bytes()
    with Image.open(source_path) as image:
        assert image.n_frames == 2


def test_twitter_rejects_mixed_video_and_photo(tmp_path):
    adapter, post, persona, account = _stack("twitter", tmp_path / "clip.mov", "video/quicktime")
    post.attachments.append(MediaAttachment(storage_path=str(tmp_path / "art.jpg"), mime_type="image/jpeg", sort_order=1))

    issues = adapter.validate(post, persona, account)

    assert any("one video per post" in issue.message for issue in issues)


def test_twitter_prepares_all_files_before_upload_and_cleans_up_if_second_is_invalid(monkeypatch, tmp_path):
    from app.adapters import twitter

    source_path = tmp_path / "art.bmp"
    Image.new("RGB", (120, 80), "orange").save(source_path)
    original = source_path.read_bytes()
    corrupt_path = tmp_path / "corrupt.jpg"
    corrupt_path.write_bytes(b"not an image")
    adapter, post, persona, account = _stack("twitter", source_path, "image/bmp")
    post.attachments.append(MediaAttachment(storage_path=str(corrupt_path), mime_type="image/jpeg", sort_order=1))
    prepared_paths = []
    uploads = []
    real_prepare_image = twitter.prepare_image

    @contextmanager
    def tracked_prepare_image(path, *, limits):
        with real_prepare_image(path, limits=limits) as prepared:
            prepared_paths.append(prepared.path)
            yield prepared

    class FakeAPI:
        def media_upload(self, **kwargs):
            uploads.append(kwargs)
            return SimpleNamespace(media_id="uploaded")

        def create_media_metadata(self, *args):
            pass

    monkeypatch.setattr(twitter, "prepare_image", tracked_prepare_image)
    monkeypatch.setitem(sys.modules, "tweepy", SimpleNamespace(Client=lambda **kwargs: None, OAuth1UserHandler=lambda *args: None, API=lambda *args: FakeAPI()))

    with pytest.raises(UnidentifiedImageError):
        adapter.publish(None, post, persona, account)

    assert uploads == []
    assert len(prepared_paths) == 1
    assert not prepared_paths[0].exists()
    assert source_path.read_bytes() == original


def test_tumblr_converts_cmyk_photo_to_rgb_without_changing_source(monkeypatch, tmp_path):
    source_path = tmp_path / "print.jpg"
    Image.new("CMYK", (120, 80), (0, 100, 50, 0)).save(source_path)
    original = source_path.read_bytes()
    captured = {}
    _install_upload_capture(monkeypatch, "tumblr", captured, media_type="image")
    adapter, post, persona, account = _stack("tumblr", source_path, "image/jpeg")

    adapter.publish(None, post, persona, account)

    assert captured["mode"] == "RGB"
    assert not captured["path"].exists()
    assert source_path.read_bytes() == original
