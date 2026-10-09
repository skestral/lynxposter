from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from atproto import Client, models
from atproto_client.models.blob_ref import BlobRef
from PIL import Image

from app.adapters.bluesky import BLUESKY_IMAGE_LIMITS
from app.adapters.mastodon import MASTODON_FALLBACK_IMAGE_LIMITS
from app.domain import MediaItem
from app.schemas import ScheduledPostCreate
from app.services.alerts import AlertDispatcher
from app.services.delivery import enqueue_due_scheduled_posts, process_delivery_queue
from app.services.posts import create_scheduled_post
from test_posts_delivery import _create_account, _create_persona


REPORTED_IMAGE_BYTES = 6_654_108
REPORTED_IMAGE_DIMENSIONS = (6144, 8160)
INSTANCE_IMAGE_PIXELS = 4096 * 4096


@pytest.fixture()
def reported_oversized_image(tmp_path):
    path = tmp_path / "instagram-desktop-original.jpg"
    with Image.new("RGB", REPORTED_IMAGE_DIMENSIONS, (40, 100, 160)) as image:
        image.save(path, format="JPEG", quality=95)
    # Trailing JPEG padding reproduces the reported byte size without a large
    # noise fixture. Other media tests cover compression of high-entropy images.
    padding_size = REPORTED_IMAGE_BYTES - path.stat().st_size
    assert padding_size > 0
    with path.open("ab") as output:
        output.write(b"\0" * padding_size)
    assert path.stat().st_size == REPORTED_IMAGE_BYTES
    with Image.open(path) as image:
        assert image.size == REPORTED_IMAGE_DIMENSIONS
        image.verify()
    return path


@pytest.mark.parametrize("instance_available", [True, False], ids=["advertised-limits", "instance-lookup-fails"])
def test_due_scheduled_post_prepares_exact_reported_image_for_both_destinations(
    session, monkeypatch, reported_oversized_image, instance_available,
):
    source = reported_oversized_image
    original_bytes = source.read_bytes()
    original_hash = sha256(original_bytes).hexdigest()
    bluesky_uploads = []
    bluesky_posts = []
    mastodon_uploads = []
    mastodon_posts = []
    instance_lookups = []

    def assert_original_unchanged():
        assert source.stat().st_size == REPORTED_IMAGE_BYTES
        assert sha256(source.read_bytes()).hexdigest() == original_hash
        with Image.open(source) as image:
            assert image.size == REPORTED_IMAGE_DIMENSIONS

    class FakeBlueskyClient:
        # Exercise the installed SDK's real images->blob->record assembly.
        send_images = Client.send_images

        def upload_blob(self, data):
            assert isinstance(data, bytes)
            assert len(data) <= BLUESKY_IMAGE_LIMITS.max_bytes == 2_000_000
            with Image.open(BytesIO(data)) as image:
                assert max(image.size) <= BLUESKY_IMAGE_LIMITS.max_dimension == 4000
                dimensions = image.size
            bluesky_uploads.append((data, dimensions))
            assert_original_unchanged()
            return SimpleNamespace(blob=BlobRef(
                mime_type="image/jpeg", size=len(data),
                ref={"$link": "bafkreigh2akiscaildc5eymqv7y6ygv4c2uowuuvjxyfu7gi27oa2ouwke"},
            ))

        def send_post(self, text, **kwargs):
            record = models.AppBskyFeedPost.Record(
                text=text, created_at="2026-10-09T00:00:00Z", embed=kwargs["embed"], facets=kwargs.get("facets"),
            ).model_dump(mode="json", by_alias=True)
            image = record["embed"]["images"][0]
            assert image["image"]["size"] == len(bluesky_uploads[0][0])
            assert (image["aspectRatio"]["width"], image["aspectRatio"]["height"]) == bluesky_uploads[0][1]
            assert image["alt"] == "Original image description"
            bluesky_posts.append(record)
            return SimpleNamespace(uri="at://did:plc:scheduled/app.bsky.feed.post/prepared-image")

    class FakeMastodonClient:
        def instance(self):
            instance_lookups.append(True)
            if not instance_available:
                raise RuntimeError("Instance metadata is temporarily unavailable.")
            return {"configuration": {"media_attachments": {
                "image_size_limit": 8 * 1024 * 1024,
                "image_matrix_limit": INSTANCE_IMAGE_PIXELS,
                "supported_mime_types": ["image/jpeg", "image/png", "image/webp"],
            }}}

        def media_post(self, media_path, *, mime_type=None, description=None):
            path = Path(media_path)
            assert path != source
            assert path.is_file()
            assert path.stat().st_size <= MASTODON_FALLBACK_IMAGE_LIMITS.max_bytes
            with Image.open(path) as image:
                assert image.width * image.height <= INSTANCE_IMAGE_PIXELS == MASTODON_FALLBACK_IMAGE_LIMITS.max_pixels
                assert image.size != REPORTED_IMAGE_DIMENSIONS
                dimensions = image.size
            assert mime_type == "image/jpeg"
            assert description == "Original image description"
            mastodon_uploads.append((path, dimensions))
            assert_original_unchanged()
            return {"id": "prepared-media", "url": "https://example.social/media/prepared"}

        def status_post(self, text, **kwargs):
            assert kwargs["media_ids"] == ["prepared-media"]
            assert mastodon_uploads[0][0].exists()
            mastodon_posts.append((text, kwargs))
            return {"id": "prepared-image", "url": "https://example.social/@me/prepared-image"}

    monkeypatch.setattr("app.adapters.bluesky._get_client", lambda *args, **kwargs: FakeBlueskyClient())
    monkeypatch.setattr("app.adapters.mastodon._get_client", lambda *args, **kwargs: FakeMastodonClient())
    # Retain the canonical file solely to verify preparation never changes it;
    # successful-delivery cleanup has separate coverage.
    monkeypatch.setattr("app.services.delivery.can_remove_post_media_after_delivery", lambda post: False)

    persona = _create_persona(session)
    accounts = [
        _create_account(session, persona, service=service, label=service, source_enabled=False, destination_enabled=True)
        for service in ("bluesky", "mastodon")
    ]
    post = create_scheduled_post(session, ScheduledPostCreate(
        persona_id=persona.id, body="Scheduled full-resolution photo", status="scheduled",
        target_account_ids=[account.id for account in accounts],
        scheduled_for=datetime.now(timezone.utc) - timedelta(minutes=1),
    ), [MediaItem(
        storage_path=source, mime_type="image/jpeg", alt_text="Original image description",
        size_bytes=REPORTED_IMAGE_BYTES, checksum=original_hash,
    )])
    session.commit()
    assert {job.status for job in post.delivery_jobs} == {"scheduled"}

    enqueue_due_scheduled_posts(session, run_id="reported-image-due")
    session.commit()
    assert {job.status for job in post.delivery_jobs} == {"queued"}
    process_delivery_queue(session, AlertDispatcher(), post_id=post.id, run_id="reported-image-publish")
    session.commit()

    assert post.status == "posted"
    assert len(post.delivery_jobs) == 2
    assert all(job.status == "posted" and job.attempt_count == 1 and job.last_error is None for job in post.delivery_jobs)
    assert len(bluesky_uploads) == len(bluesky_posts) == len(mastodon_uploads) == len(mastodon_posts) == 1
    assert instance_lookups == [True]
    assert not mastodon_uploads[0][0].exists()
    assert_original_unchanged()
    assert post.attachments[0].storage_path == str(source)
    assert post.attachments[0].size_bytes == REPORTED_IMAGE_BYTES
    assert post.attachments[0].checksum == original_hash
