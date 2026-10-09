from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import BytesIO
from random import Random
from types import SimpleNamespace
import shutil
import subprocess

import pytest
from PIL import Image
from atproto import models
from atproto_client.models.blob_ref import BlobRef

from app.adapters.bluesky import BLUESKY_IMAGE_LIMITS, BlueskyDestinationAdapter, BlueskySourceAdapter
from app.models import Account, AccountSyncState, CanonicalPost, MediaAttachment, Persona
from app.services.video_preparation import _probe


def _post(paths):
    return CanonicalPost(body="Photos 🐾 #art", publish_overrides_json={}, attachments=[
        MediaAttachment(id=f"image-{index}", storage_path=str(path), mime_type="image/png", alt_text=f"Alt {index}", size_bytes=path.stat().st_size, sort_order=index)
        for index, path in enumerate(paths)
    ])


def _account():
    return Account(id="bsky", service="bluesky", credentials_json={"handle": "test.bsky.social"})


def test_bluesky_publish_normalizes_large_images_and_supplies_aspect_ratios(tmp_path, monkeypatch):
    path = tmp_path / "desktop-instagram.png"
    Image.frombytes("RGB", (1200, 1000), Random(2).randbytes(1200 * 1000 * 3)).save(path)
    original = path.read_bytes()
    assert len(original) > BLUESKY_IMAGE_LIMITS.max_bytes
    captured = {}

    def send_images(body, images, alts, **kwargs):
        captured.update(body=body, images=images, alts=alts, **kwargs)
        return SimpleNamespace(uri="at://did:plc:test/app.bsky.feed.post/image-post")

    monkeypatch.setattr("app.adapters.bluesky._get_client", lambda *a, **k: SimpleNamespace(send_images=send_images))
    result = BlueskyDestinationAdapter().publish(None, _post([path]), Persona(), _account())
    assert result.external_id == "image-post"
    assert len(captured["images"][0]) <= BLUESKY_IMAGE_LIMITS.max_bytes
    with Image.open(BytesIO(captured["images"][0])) as actual:
        ratio = captured["image_aspect_ratios"][0]
        assert (ratio.width, ratio.height) == actual.size
    assert captured["alts"] == ["Alt 0"]
    assert captured["body"] == "Photos 🐾 #art"
    assert path.read_bytes() == original


@pytest.mark.parametrize("count", [5, 10])
def test_bluesky_large_image_sets_use_current_gallery_embed(count, tmp_path, monkeypatch):
    paths = []
    for index in range(count):
        path = tmp_path / f"image-{index}.png"
        Image.new("RGB", (40, 20), "green").save(path)
        paths.append(path)
    captured = {}
    uploads = []

    def upload_blob(data):
        uploads.append(data)
        return SimpleNamespace(blob=BlobRef(mime_type="image/png", size=len(data), ref={"$link": "bafkreigh2akiscaildc5eymqv7y6ygv4c2uowuuvjxyfu7gi27oa2ouwke"}))

    def send_post(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(uri="at://did:plc:test/app.bsky.feed.post/gallery-post")

    monkeypatch.setattr("app.adapters.bluesky._get_client", lambda *a, **k: SimpleNamespace(upload_blob=upload_blob, send_post=send_post))
    post = _post(paths)
    adapter = BlueskyDestinationAdapter()
    assert adapter.validate(post, Persona(), _account()) == []
    assert adapter.preview(post, Persona(), _account()).action == "send_gallery"
    adapter.publish(None, post, Persona(), _account())
    embed = captured["embed"]
    assert embed.py_type == "app.bsky.embed.gallery"
    assert len(uploads) == count == len(embed.items)
    assert [item.alt for item in embed.items] == [f"Alt {index}" for index in range(count)]
    assert all((item.aspect_ratio.width, item.aspect_ratio.height) == (40, 20) for item in embed.items)
    # Exercise the installed SDK record model and wire serialization, not only a fake client.
    record = models.AppBskyFeedPost.Record(text=captured["text"], created_at="2026-10-08T00:00:00Z", embed=embed, facets=captured["facets"])
    assert record.model_dump(mode="json", by_alias=True)["embed"]["$type"] == "app.bsky.embed.gallery"


def test_bluesky_rejects_eleven_images_and_allows_video_preparation(tmp_path):
    path = tmp_path / "image.png"
    Image.new("RGB", (20, 20)).save(path)
    adapter = BlueskyDestinationAdapter()
    assert any("10 images" in issue.message for issue in adapter.validate(_post([path] * 11), Persona(), _account()))
    video = MediaAttachment(id="video", storage_path=str(tmp_path / "missing.webm"), mime_type="video/webm", size_bytes=300_000_001)
    post = CanonicalPost(body="Video", publish_overrides_json={}, attachments=[video])
    assert adapter.validate(post, Persona(), _account()) == []
    preview = adapter.preview(post, Persona(), _account())
    assert preview.request_shape["video"]["prepared_mime_type"] == "video/mp4"
    assert preview.request_shape["video"]["max_bytes"] == 300_000_000


def test_bluesky_source_imports_gallery_images_in_order(monkeypatch):
    now = datetime.now(timezone.utc)
    images = [SimpleNamespace(fullsize=f"https://cdn.bsky.app/image-{index}.png", alt=f"Alt {index}") for index in range(5)]
    post_view = SimpleNamespace(author=SimpleNamespace(handle="test.bsky.social"), uri="at://did:plc:test/app.bsky.feed.post/gallery", record=SimpleNamespace(text="Gallery", created_at=now.isoformat(), facets=[]), embed=SimpleNamespace(items=images))
    feed = SimpleNamespace(feed=[SimpleNamespace(post=post_view)])
    monkeypatch.setattr("app.adapters.bluesky._get_client", lambda *a, **k: SimpleNamespace(app=SimpleNamespace(bsky=SimpleNamespace(feed=SimpleNamespace(get_author_feed=lambda _: feed)))))
    downloads = []

    def download(url, filename, alt, order):
        downloads.append((url, alt, order))
        return SimpleNamespace(storage_path=filename)

    monkeypatch.setattr("app.adapters.bluesky.download_media", download)
    state = AccountSyncState(last_polled_at=now - timedelta(hours=1), state_json={"last_seen_at": (now - timedelta(hours=1)).isoformat()})
    result = BlueskySourceAdapter().poll(None, Persona(settings_json={}), _account(), state)
    assert len(result.posts[0].media) == 5
    assert downloads == [(image.fullsize, image.alt, index) for index, image in enumerate(images)]


def test_bluesky_uploads_actual_prepared_video_with_aspect_ratio(tmp_path, monkeypatch):
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        pytest.skip("FFmpeg not installed")
    source = tmp_path / "source.mov"
    subprocess.run([shutil.which("ffmpeg"), "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=blue:s=96x48:r=12:d=0.5", "-c:v", "mpeg4", str(source)], capture_output=True, check=True, timeout=30)
    original = source.read_bytes()
    captured = {}

    def send_video(body, data, alt_text, **kwargs):
        captured.update(data=data, alt_text=alt_text, **kwargs)
        return SimpleNamespace(uri="at://did:plc:test/app.bsky.feed.post/video")

    monkeypatch.setattr("app.adapters.bluesky._get_client", lambda *a, **k: SimpleNamespace(send_video=send_video))
    attachment = MediaAttachment(id="video", storage_path=str(source), mime_type="video/quicktime", alt_text="Video alt", size_bytes=len(original))
    post = CanonicalPost(body="Video", publish_overrides_json={}, attachments=[attachment])
    BlueskyDestinationAdapter().publish(None, post, Persona(), _account())
    uploaded = tmp_path / "uploaded.mp4"
    uploaded.write_bytes(captured["data"])
    info = _probe(uploaded)
    assert info.codec == "h264"
    assert info.pixel_format == "yuv420p"
    assert (captured["video_aspect_ratio"].width, captured["video_aspect_ratio"].height) == (info.width, info.height)
    assert captured["alt_text"] == "Video alt"
    assert source.read_bytes() == original
