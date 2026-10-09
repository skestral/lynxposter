from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pytest
from PIL import Image, UnidentifiedImageError

from app.adapters.mastodon import (
    MASTODON_FALLBACK_IMAGE_LIMITS,
    MASTODON_FALLBACK_VIDEO_LIMITS,
    MastodonDestinationAdapter,
    _mastodon_image_limits,
    _mastodon_video_limits,
)
from app.models import Account, CanonicalPost, MediaAttachment, Persona


def _post_with_media(path: Path | None, *, mime_type: str = "image/png"):
    persona = Persona(id="persona-1", settings_json={})
    account = Account(
        id="mastodon-1",
        service="mastodon",
        credentials_json={"instance": "https://example.social", "token": "test-token"},
        publish_settings_json={},
    )
    post = CanonicalPost(body="A test post", metadata_json={}, publish_overrides_json={})
    if path is not None:
        post.attachments = [
            MediaAttachment(
                id="media-1",
                storage_path=str(path),
                mime_type=mime_type,
                alt_text="Image description",
                size_bytes=path.stat().st_size,
                checksum="original-checksum",
                sort_order=0,
            )
        ]
    return post, persona, account


@pytest.mark.parametrize("response_url", ["https://example.social/@me/101", None])
def test_mastodon_preserves_canonical_status_url_with_full_handle(monkeypatch, response_url):
    post, persona, account = _post_with_media(None)
    account.credentials_json["handle"] = "@me@example.social"

    class FakeClient:
        def status_post(self, body, **kwargs):
            return {"id": 101, "url": response_url}

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())
    result = MastodonDestinationAdapter().publish(None, post, persona, account)

    assert result.external_id == "101"
    assert result.external_url == "https://example.social/@me/101"


def test_mastodon_prepares_images_to_instance_limits_and_preserves_original(monkeypatch, tmp_path):
    path = tmp_path / "large.png"
    Image.effect_noise((800, 600), 100).convert("RGB").save(path)
    original_bytes = path.read_bytes()
    post, persona, account = _post_with_media(path)
    uploaded = []
    lookups = []

    class FakeClient:
        def instance(self):
            lookups.append(True)
            return {"configuration": {"media_attachments": {
                "image_size_limit": 20_000,
                "image_matrix_limit": 60_000,
                "supported_mime_types": ["image/jpeg"],
            }}}

        def media_post(self, media_path, mime_type=None, description=None):
            media_path = Path(media_path)
            with Image.open(media_path) as image:
                assert image.width * image.height <= 60_000
                assert image.format == "JPEG"
            assert media_path.stat().st_size <= 20_000
            assert mime_type == "image/jpeg"
            assert description == "Image description"
            uploaded.append(media_path)
            return SimpleNamespace(id="uploaded-1")

        def status_post(self, body, **kwargs):
            assert kwargs["media_ids"] == ["uploaded-1"]
            assert all(media_path.exists() for media_path in uploaded)
            return {"id": "post-1", "url": "https://example.social/@me/post-1"}

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())
    result = MastodonDestinationAdapter().publish(None, post, persona, account)

    assert result.external_id == "post-1"
    assert len(lookups) == 1
    assert path.read_bytes() == original_bytes
    assert post.attachments[0].storage_path == str(path)
    assert post.attachments[0].mime_type == "image/png"
    assert post.attachments[0].size_bytes == len(original_bytes)
    assert post.attachments[0].checksum == "original-checksum"
    assert uploaded[0] != path
    assert not uploaded[0].exists()


def test_mastodon_image_upload_failure_cleans_prepared_copy(monkeypatch, tmp_path):
    path = tmp_path / "original.png"
    Image.new("RGB", (100, 100), "red").save(path)
    original_bytes = path.read_bytes()
    post, persona, account = _post_with_media(path)
    uploaded = []

    class FakeClient:
        def instance(self):
            return {"configuration": {"media_attachments": {"supported_mime_types": ["image/jpeg"]}}}

        def media_post(self, media_path, **kwargs):
            uploaded.append(Path(media_path))
            assert uploaded[0].exists()
            raise RuntimeError("Upload failed")

        def status_post(self, *args, **kwargs):
            pytest.fail("A failed upload must not publish a status")

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())
    with pytest.raises(RuntimeError, match="Upload failed"):
        MastodonDestinationAdapter().publish(None, post, persona, account)

    assert path.read_bytes() == original_bytes
    assert not uploaded[0].exists()


def test_mastodon_preserves_animation_when_accepted_by_instance(monkeypatch, tmp_path):
    path = tmp_path / "animated.gif"
    frames = [Image.new("RGB", (20, 20), color) for color in ("red", "blue")]
    frames[0].save(path, save_all=True, append_images=frames[1:], duration=100, loop=0)
    original_bytes = path.read_bytes()
    post, persona, account = _post_with_media(path, mime_type="image/gif")

    class FakeClient:
        def instance(self):
            return {"configuration": {"media_attachments": {"supported_mime_types": ["image/gif", "image/jpeg"]}}}

        def media_post(self, media_path, mime_type=None, description=None):
            assert Path(media_path) == path
            assert path.read_bytes() == original_bytes
            assert mime_type == "image/gif"
            with Image.open(media_path) as image:
                assert image.is_animated
            return SimpleNamespace(id="animation-1")

        def status_post(self, body, **kwargs):
            return {"id": "post-1"}

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())
    MastodonDestinationAdapter().publish(None, post, persona, account)

    assert path.read_bytes() == original_bytes


@pytest.mark.parametrize("instance", [None, {}, {"configuration": None}, {"configuration": {"media_attachments": None}}])
def test_mastodon_uses_compatibility_limits_without_advertised_configuration(instance):
    client = SimpleNamespace(instance=lambda: instance)
    assert _mastodon_image_limits(client) == MASTODON_FALLBACK_IMAGE_LIMITS


def test_mastodon_uses_compatibility_limits_when_instance_lookup_fails():
    def failing_lookup():
        raise RuntimeError("Instance API unavailable")

    assert _mastodon_image_limits(SimpleNamespace(instance=failing_lookup)) == MASTODON_FALLBACK_IMAGE_LIMITS


def test_mastodon_ignores_invalid_limits_and_preserves_advertised_formats():
    client = SimpleNamespace(instance=lambda: {"configuration": {"media_attachments": {
        "image_size_limit": 0,
        "image_matrix_limit": True,
        "supported_mime_types": ["image/png", "video/mp4"],
    }}})
    limits = _mastodon_image_limits(client)
    assert limits.max_bytes == MASTODON_FALLBACK_IMAGE_LIMITS.max_bytes
    assert limits.max_pixels == MASTODON_FALLBACK_IMAGE_LIMITS.max_pixels
    assert limits.supported_mime_types == ("image/png",)


def test_mastodon_skips_media_limit_lookup_without_media(monkeypatch):
    post, persona, account = _post_with_media(None)

    class FakeClient:
        def instance(self):
            pytest.fail("Media limits are unnecessary for text posts")

        def status_post(self, body, **kwargs):
            return {"id": "post-1"}

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())
    assert MastodonDestinationAdapter().publish(None, post, persona, account).external_id == "post-1"


def test_mastodon_preview_explains_image_preparation_without_network(monkeypatch, tmp_path):
    path = tmp_path / "original.png"
    Image.new("RGB", (100, 100), "red").save(path)
    original_bytes = path.read_bytes()
    post, persona, account = _post_with_media(path)
    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: pytest.fail("Preview must not connect"))

    preview = MastodonDestinationAdapter().preview(post, persona, account)

    assert any("advertised media limits" in note and "originals are preserved" in note for note in preview.notes)
    assert path.read_bytes() == original_bytes


def test_mastodon_prepares_video_to_instance_limits_and_waits_until_ready(monkeypatch, tmp_path):
    path = tmp_path / "original.mov"
    path.write_bytes(b"original MOV")
    post, persona, account = _post_with_media(path, mime_type="video/quicktime")
    prepared_path = tmp_path / "prepared.mp4"
    events = []
    uploaded = []

    @contextmanager
    def fake_prepare_video(source, *, limits):
        assert source == path
        assert limits.max_bytes == 20_000
        assert limits.max_pixels == 640 * 480
        assert limits.max_frame_rate == 24
        prepared_path.write_bytes(b"prepared MP4")
        try:
            yield SimpleNamespace(path=prepared_path, mime_type="video/mp4")
        finally:
            prepared_path.unlink()

    class FakeClient:
        def instance(self):
            events.append("instance")
            return {"configuration": {"media_attachments": {
                "video_size_limit": 20_000,
                "video_matrix_limit": 640 * 480,
                "video_frame_rate_limit": 24,
                "supported_mime_types": ["video/mp4"],
            }}}

        def media_post(self, media_path, mime_type=None, description=None):
            events.append("upload")
            assert Path(media_path).read_bytes() == b"prepared MP4"
            assert mime_type == "video/mp4"
            assert description == "Image description"
            uploaded.append(media_path)
            return {"id": "video-1", "url": None}

        def media(self, media_id):
            assert media_id == "video-1"
            events.append("ready")
            return {"id": media_id, "url": "https://example.social/video.mp4"}

        def status_post(self, body, **kwargs):
            events.append("status")
            assert kwargs["media_ids"] == ["video-1"]
            assert prepared_path.exists()
            return {"id": "post-1"}

    monkeypatch.setattr("app.adapters.mastodon.prepare_video", fake_prepare_video)
    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())
    monkeypatch.setattr("app.adapters.mastodon.time.sleep", lambda seconds: events.append("wait"))
    result = MastodonDestinationAdapter().publish(None, post, persona, account)

    assert result.external_id == "post-1"
    assert events == ["instance", "upload", "wait", "ready", "status"]
    assert not prepared_path.exists()
    assert path.read_bytes() == b"original MOV"
    assert post.attachments[0].storage_path == str(path)
    assert post.attachments[0].mime_type == "video/quicktime"


@pytest.mark.parametrize("processing_error", [False, True])
def test_mastodon_processing_failure_does_not_publish_status(monkeypatch, tmp_path, processing_error):
    path = tmp_path / "photo.png"
    Image.new("RGB", (100, 100), "red").save(path)
    post, persona, account = _post_with_media(path)

    class FakeClient:
        def instance(self):
            return {}

        def media_post(self, media_path, **kwargs):
            return {"id": "media-1", "url": None}

        def media(self, media_id):
            return {"id": media_id, "url": None, "error": "Video processing failed"}

        def status_post(self, *args, **kwargs):
            pytest.fail("Unprocessed media must not publish")

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())
    monkeypatch.setattr("app.adapters.mastodon.time.sleep", lambda seconds: None)
    if not processing_error:
        ticks = iter([0, 121])
        monkeypatch.setattr("app.adapters.mastodon.time.monotonic", lambda: next(ticks))
    message = "processing failed" if processing_error else "upload timeout"
    with pytest.raises(RuntimeError, match=message):
        MastodonDestinationAdapter().publish(None, post, persona, account)
    assert path.exists()


def test_mastodon_video_limits_fall_back_when_not_advertised():
    assert _mastodon_video_limits({}) == MASTODON_FALLBACK_VIDEO_LIMITS


def test_mastodon_rejects_destination_without_mp4_support_before_upload(monkeypatch, tmp_path):
    path = tmp_path / "original.mov"
    path.write_bytes(b"original MOV")
    post, persona, account = _post_with_media(path, mime_type="video/quicktime")
    client = SimpleNamespace(instance=lambda: {"configuration": {"media_attachments": {"supported_mime_types": ["video/webm"]}}})
    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: client)
    monkeypatch.setattr("app.adapters.mastodon.prepare_video", lambda *args, **kwargs: pytest.fail("Unsupported media must not transcode"))
    with pytest.raises(ValueError, match="does not accept MP4"):
        MastodonDestinationAdapter().publish(None, post, persona, account)
    assert path.read_bytes() == b"original MOV"


def test_mastodon_transcodes_real_video_to_instance_pixel_and_frame_caps(monkeypatch, tmp_path):
    ffmpeg, ffprobe = shutil.which("ffmpeg"), shutil.which("ffprobe")
    if not ffmpeg or not ffprobe:
        pytest.skip("FFmpeg and ffprobe are required for codec integration")
    path = tmp_path / "source.mov"
    subprocess.run([
        ffmpeg, "-y", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=640x360:rate=30",
        "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100", "-t", "0.5",
        "-c:v", "mpeg4", "-pix_fmt", "yuv420p", "-c:a", "pcm_s16le", str(path),
    ], check=True, capture_output=True, timeout=30)
    original_bytes = path.read_bytes()
    post, persona, account = _post_with_media(path, mime_type="video/quicktime")
    uploads = []

    class FakeClient:
        def instance(self):
            return {"configuration": {"media_attachments": {
                "video_size_limit": 40_000,
                "video_matrix_limit": 160 * 90,
                "video_frame_rate_limit": 12,
                "supported_mime_types": ["video/mp4"],
            }}}

        def media_post(self, media_path, mime_type=None, description=None):
            uploads.append(Path(media_path))
            assert mime_type == "video/mp4"
            assert uploads[0].stat().st_size <= 40_000
            result = subprocess.run([
                ffprobe, "-v", "error", "-show_streams", "-of", "json", str(media_path),
            ], check=True, capture_output=True, text=True, timeout=30)
            streams = json.loads(result.stdout)["streams"]
            video = next(stream for stream in streams if stream["codec_type"] == "video")
            audio = next(stream for stream in streams if stream["codec_type"] == "audio")
            numerator, denominator = video["avg_frame_rate"].split("/")
            assert video["width"] * video["height"] <= 160 * 90
            assert int(numerator) / int(denominator) <= 12
            assert video["codec_name"] == "h264"
            assert video["pix_fmt"] == "yuv420p"
            assert audio["codec_name"] == "aac"
            return SimpleNamespace(id="video-1")

        def status_post(self, body, **kwargs):
            return {"id": "post-1"}

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: FakeClient())
    MastodonDestinationAdapter().publish(None, post, persona, account)

    assert path.read_bytes() == original_bytes
    assert not uploads[0].exists()


def test_mastodon_prepares_entire_album_before_any_remote_upload(monkeypatch, tmp_path):
    first_path = tmp_path / "valid.png"
    second_path = tmp_path / "invalid.png"
    Image.new("RGB", (100, 100), "red").save(first_path)
    second_path.write_bytes(b"invalid image")
    post, persona, account = _post_with_media(first_path)
    post.attachments.append(MediaAttachment(
        id="media-2", storage_path=str(second_path), mime_type="image/png", alt_text="", sort_order=1,
    ))
    client = SimpleNamespace(
        instance=lambda: {"configuration": {"media_attachments": {"supported_mime_types": ["image/jpeg"]}}},
        media_post=lambda *args, **kwargs: pytest.fail("Invalid local album must not upload any media"),
        status_post=lambda *args, **kwargs: pytest.fail("Invalid local album must not publish"),
    )
    prepared_paths = []
    from app.services.media_preparation import prepare_image as actual_prepare_image

    @contextmanager
    def tracked_prepare_image(path, *, limits):
        with actual_prepare_image(path, limits=limits) as prepared:
            prepared_paths.append(prepared.path)
            yield prepared

    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda config: client)
    monkeypatch.setattr("app.adapters.mastodon.prepare_image", tracked_prepare_image)
    with pytest.raises(UnidentifiedImageError, match="cannot identify image file"):
        MastodonDestinationAdapter().publish(None, post, persona, account)

    assert len(prepared_paths) == 1
    assert not prepared_paths[0].exists()
    assert first_path.exists()
    assert second_path.read_bytes() == b"invalid image"
