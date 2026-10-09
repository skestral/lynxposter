from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from atproto import models
from atproto_client.models.blob_ref import BlobRef

from app.adapters.bluesky import BlueskyDestinationAdapter, BlueskySourceAdapter
from app.models import Account, AccountSyncState, CanonicalPost, MediaAttachment, Persona
from app.utils import stable_checksum


CID = "bafkreigh2akiscaildc5eymqv7y6ygv4c2uowuuvjxyfu7gi27oa2ouwke"
VIDEO_BYTES = b"\x00\x00\x00\x18ftypmp42" + b"original-video-blob"


def _account():
    return Account(id="source", service="bluesky", credentials_json={"handle": "artist.bsky.social"})


def _poll(monkeypatch, *, embed=None, record_embed=None, blob_download=None, initial=False):
    now = datetime.now(timezone.utc)
    record = SimpleNamespace(text="Source post", created_at=now.isoformat(), facets=[], embed=record_embed)
    post = SimpleNamespace(
        uri="at://did:plc:artist/app.bsky.feed.post/source-post",
        author=SimpleNamespace(handle="artist.bsky.social", did="did:plc:artist"), record=record, embed=embed,
    )
    client = SimpleNamespace(
        app=SimpleNamespace(bsky=SimpleNamespace(feed=SimpleNamespace(
            get_author_feed=lambda _: SimpleNamespace(feed=[SimpleNamespace(post=post)]),
        ))),
        com=SimpleNamespace(atproto=SimpleNamespace(sync=SimpleNamespace(get_blob=blob_download))),
    )
    monkeypatch.setattr("app.adapters.bluesky._get_client", lambda *a, **k: client)
    state = None if initial else AccountSyncState(
        last_polled_at=now - timedelta(hours=1), state_json={"last_seen_at": (now - timedelta(hours=1)).isoformat()},
    )
    return BlueskySourceAdapter().poll(None, Persona(settings_json={}), _account(), state)


@pytest.mark.parametrize("count", [4, 5, 10])
@pytest.mark.parametrize("quoted", [False, True])
def test_source_imports_sdk_image_and_gallery_views_without_truncation(monkeypatch, count, quoted):
    image_class = models.AppBskyEmbedImages.ViewImage if count <= 4 else models.AppBskyEmbedGallery.ViewImage
    images = [image_class(**{
        "fullsize": f"https://cdn.bsky.app/image-{index}.png", "alt": f"Alt {index}",
        "thumb" if count <= 4 else "thumbnail": f"https://cdn.bsky.app/thumb-{index}.png",
        "aspect_ratio": models.AppBskyEmbedDefs.AspectRatio(width=20, height=10),
    }) for index in range(count)]
    embed = models.AppBskyEmbedImages.View(images=images) if count <= 4 else models.AppBskyEmbedGallery.View(items=images)
    if quoted:
        embed = models.AppBskyEmbedRecordWithMedia.View(media=embed, record=models.AppBskyEmbedRecord.View(
            record=models.AppBskyEmbedRecord.ViewNotFound(uri="at://did:plc:artist/app.bsky.feed.post/quoted", not_found=True),
        ))
    downloads = []

    def download(url, filename, alt, order):
        downloads.append((url, alt, order))
        return SimpleNamespace(storage_path=Path(filename), alt_text=alt, sort_order=order)

    monkeypatch.setattr("app.adapters.bluesky.download_media", download)
    result = _poll(monkeypatch, embed=embed)

    assert len(result.posts[0].media) == count
    assert downloads == [(image.fullsize, image.alt, index) for index, image in enumerate(images)]
    if quoted:
        assert result.posts[0].quote_of_external.external_id == "quoted"


@pytest.mark.parametrize("quoted", [False, True])
def test_source_imports_original_video_blob_with_alt_text(monkeypatch, tmp_path, quoted):
    record_embed = models.AppBskyEmbedVideo.Main(
        video=BlobRef(mime_type="video/mp4", size=len(VIDEO_BYTES), ref={"$link": CID}), alt="A fox running",
    )
    embed = models.AppBskyEmbedVideo.View(cid=CID, playlist="https://video.bsky.app/playlist.m3u8", alt="A fox running")
    if quoted:
        record_embed = models.AppBskyEmbedRecordWithMedia.Main(media=record_embed, record=models.AppBskyEmbedRecord.Main(
            record=models.ComAtprotoRepoStrongRef.Main(uri="at://did:plc:artist/app.bsky.feed.post/quoted", cid=CID),
        ))
        embed = models.AppBskyEmbedRecordWithMedia.View(media=embed, record=models.AppBskyEmbedRecord.View(
            record=models.AppBskyEmbedRecord.ViewNotFound(uri="at://did:plc:artist/app.bsky.feed.post/quoted", not_found=True),
        ))
    blob_requests = []

    def download_blob(params):
        blob_requests.append(params)
        return VIDEO_BYTES

    monkeypatch.setattr("app.adapters.bluesky.get_settings", lambda: SimpleNamespace(imported_media_dir=tmp_path))
    monkeypatch.setattr("app.adapters.bluesky.download_media", lambda *args: pytest.fail("Video should use the original blob, not playlist or thumbnail."))
    result = _poll(monkeypatch, embed=embed, record_embed=record_embed, blob_download=download_blob)

    media = result.posts[0].media[0]
    assert blob_requests == [{"did": "did:plc:artist", "cid": CID}]
    assert media.storage_path.parent == tmp_path
    assert media.storage_path.read_bytes() == VIDEO_BYTES
    assert media.mime_type == "video/mp4"
    assert media.size_bytes == len(VIDEO_BYTES)
    assert media.checksum == stable_checksum(media.storage_path)
    assert media.alt_text == "A fox running"
    assert media.sort_order == 0
    if quoted:
        assert result.posts[0].quote_of_external.external_id == "quoted"


def test_source_video_first_scan_only_sets_baseline(monkeypatch, tmp_path):
    record_embed = models.AppBskyEmbedVideo.Main(video=BlobRef(mime_type="video/mp4", size=len(VIDEO_BYTES), ref={"$link": CID}))
    result = _poll(
        monkeypatch, record_embed=record_embed, initial=True,
        blob_download=lambda _: pytest.fail("Initial baseline must not download video."),
    )
    assert result.posts == []


def test_source_video_download_failure_removes_partial_local_file(monkeypatch, tmp_path):
    record_embed = models.AppBskyEmbedVideo.Main(video=BlobRef(mime_type="video/mp4", size=len(VIDEO_BYTES), ref={"$link": CID}))
    monkeypatch.setattr("app.adapters.bluesky.get_settings", lambda: SimpleNamespace(imported_media_dir=tmp_path))

    def fail_normalization(path, mime):
        raise OSError("file normalization failed")

    monkeypatch.setattr("app.adapters.bluesky.normalize_media_file", fail_normalization)
    with pytest.raises(OSError, match="normalization failed"):
        _poll(monkeypatch, record_embed=record_embed, blob_download=lambda _: VIDEO_BYTES)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("count", [1, 4, 5, 10])
def test_offline_preview_exposes_current_image_embed_and_numeric_limits(monkeypatch, count):
    def forbidden(*args, **kwargs):
        pytest.fail("Sandbox preview must not authenticate, prepare files, or upload media.")

    for name in ("_get_client", "prepare_image", "prepare_video"):
        monkeypatch.setattr(f"app.adapters.bluesky.{name}", forbidden)
    post = CanonicalPost(body="Gallery", publish_overrides_json={}, attachments=[
        MediaAttachment(storage_path=f"missing-image-{index}.png", mime_type="image/png", alt_text=f"Alt {index}", sort_order=index)
        for index in range(count)
    ])
    preview = BlueskyDestinationAdapter().preview(post, Persona(), _account())
    embed = preview.request_shape["embed"]
    images = embed["images"] if count <= 4 else embed["items"]

    assert embed["$type"] == ("app.bsky.embed.images" if count <= 4 else "app.bsky.embed.gallery")
    assert len(images) == count
    assert [image["alt"] for image in images] == [f"Alt {index}" for index in range(count)]
    assert all(image["image"]["$type"] == "blob" and "aspectRatio" in image for image in images)
    if count > 4:
        assert all(image["$type"] == "app.bsky.embed.gallery#image" for image in images)
    assert preview.request_shape["image_limits"]["max_bytes"] == 2_000_000
    assert any("2 MB" in note and "2,000,000 bytes" in note and "10 images" in note for note in preview.notes)


def test_offline_video_preview_exposes_mp4_embed_and_current_numeric_limits(monkeypatch):
    monkeypatch.setattr("app.adapters.bluesky.prepare_video", lambda *a, **k: pytest.fail("Preview cannot transcode."))
    post = CanonicalPost(body="Video", publish_overrides_json={}, attachments=[
        MediaAttachment(storage_path="missing.mov", mime_type="video/quicktime", alt_text="Video alt", sort_order=0),
    ])
    preview = BlueskyDestinationAdapter().preview(post, Persona(), _account())

    assert preview.request_shape["embed"]["$type"] == "app.bsky.embed.video"
    assert preview.request_shape["embed"]["video"]["mimeType"] == "video/mp4"
    assert preview.request_shape["embed"]["alt"] == "Video alt"
    assert preview.request_shape["video"]["max_bytes"] == 300_000_000
    assert preview.request_shape["video"]["max_duration_seconds"] == 600
    assert any("300 MB" in note and "300,000,000 bytes" in note and "600 seconds" in note for note in preview.notes)
