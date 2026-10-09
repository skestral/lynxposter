from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path

from PIL import Image
from sqlalchemy.orm import Session

from app.adapters.base import ConfigurationError, DestinationAdapter, get_account_credentials
from app.adapters.common import is_image_attachment, is_video_attachment, service_attachments, service_body
from app.domain import PublishPreview, PublishResult, ValidationIssue
from app.models import Account, CanonicalPost, Persona
from app.services.media_preparation import ImageLimits, prepare_image
from app.services.video_preparation import VideoLimits, prepare_video


TWITTER_IMAGE_LIMITS = ImageLimits(max_bytes=5_000_000, supported_mime_types=("image/jpeg", "image/png", "image/webp", "image/gif"))
TWITTER_GIF_LIMITS = ImageLimits(max_bytes=15_000_000, supported_mime_types=("image/gif",))
# Keep a bounded budget for the existing v1 uploader; Post duration is 20 minutes.
TWITTER_VIDEO_LIMITS = VideoLimits(max_bytes=512_000_000, max_duration=1200, max_dimension=1280, max_frame_rate=60)


def _is_animated_gif(attachment) -> bool:
    path = Path(attachment.storage_path)
    if str(attachment.mime_type or "").lower() != "image/gif" and path.suffix.lower() != ".gif":
        return False
    try:
        with Image.open(path) as image:
            return image.format == "GIF" and bool(getattr(image, "is_animated", False))
    except OSError:
        return False


class TwitterDestinationAdapter(DestinationAdapter):
    service = "twitter"

    def validate(self, post: CanonicalPost, persona: Persona, account: Account) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        body = service_body(post, account)
        attachments = service_attachments(post, account)
        if len(body) > 280:
            issues.append(ValidationIssue(service="twitter", field="body", message="Twitter posts are limited to 280 characters in v1."))
        if len(attachments) > 4:
            issues.append(ValidationIssue(service="twitter", field="media", message="Twitter supports up to 4 attachments per post."))
        if any(not is_image_attachment(attachment) and not is_video_attachment(attachment) for attachment in attachments):
            issues.append(ValidationIssue(service="twitter", field="media", message="Twitter supports image or video attachments."))
        if len(attachments) > 1 and any(is_video_attachment(attachment) or _is_animated_gif(attachment) for attachment in attachments):
            issues.append(ValidationIssue(service="twitter", field="media", message="Twitter supports up to 4 photos, one animated GIF, or one video per post."))
        return issues

    def preview(
        self,
        post: CanonicalPost,
        persona: Persona,
        account: Account,
        *,
        context: dict[str, str | None] | None = None,
    ) -> PublishPreview:
        attachments = service_attachments(post, account)
        request_shape = {
            "text": service_body(post, account),
            "media_ids": [f"<uploaded-media-{index + 1}>" for index, _ in enumerate(attachments)],
            "in_reply_to_tweet_id": (context or {}).get("reply_external_id"),
            "quote_tweet_id": (context or {}).get("quote_external_id"),
        }
        notes = []
        if attachments:
            notes.append("Media uploads are skipped in sandbox mode, so media_ids are placeholders.")
        return PublishPreview(
            service="twitter",
            action="create_tweet",
            rendered_body=request_shape["text"],
            endpoint_label="tweepy.Client.create_tweet",
            request_shape=request_shape,
            notes=notes,
        )

    def publish(
        self,
        session: Session,
        post: CanonicalPost,
        persona: Persona,
        account: Account,
        *,
        context: dict[str, str | None] | None = None,
    ) -> PublishResult:
        import tweepy

        config = get_account_credentials(account)
        required = ("app_key", "app_secret", "access_token", "access_token_secret")
        if not all(config.get(key) for key in required):
            raise ConfigurationError("Twitter destination credentials are incomplete.")
        client = tweepy.Client(
            consumer_key=config["app_key"],
            consumer_secret=config["app_secret"],
            access_token=config["access_token"],
            access_token_secret=config["access_token_secret"],
        )
        oauth = tweepy.OAuth1UserHandler(
            config["app_key"],
            config["app_secret"],
            config["access_token"],
            config["access_token_secret"],
        )
        api = tweepy.API(oauth)

        media_ids = None
        attachments = service_attachments(post, account)
        if attachments:
            media_ids = []
            with ExitStack() as stack:
                prepared_media = []
                for attachment in attachments:
                    source_path = Path(attachment.storage_path)
                    is_video = is_video_attachment(attachment)
                    animated_gif = False
                    if is_video:
                        prepared = stack.enter_context(prepare_video(source_path, limits=TWITTER_VIDEO_LIMITS))
                        if prepared.duration < 0.5 or min(prepared.width, prepared.height) < 32 or not 1 / 3 <= prepared.width / prepared.height <= 3:
                            raise ConfigurationError("Twitter video must be at least 0.5 seconds, 32 pixels per dimension, and have an aspect ratio between 1:3 and 3:1.")
                    else:
                        animated_gif = _is_animated_gif(attachment)
                        limits = TWITTER_GIF_LIMITS if animated_gif else TWITTER_IMAGE_LIMITS
                        prepared = stack.enter_context(prepare_image(source_path, limits=limits))
                        if animated_gif:
                            with Image.open(prepared.path) as image:
                                if image.width > 1280 or image.height > 1080 or image.n_frames > 350 or image.width * image.height * image.n_frames > 300_000_000:
                                    raise ConfigurationError("Twitter animated GIF exceeds the supported dimensions, frame count, or total pixels; upload it as a video instead.")
                    prepared_media.append((attachment, prepared, is_video, animated_gif))

                for attachment, prepared, is_video, animated_gif in prepared_media:
                    if is_video:
                        media = api.chunked_upload(filename=str(prepared.path), file_type=prepared.mime_type, media_category="tweet_video", wait_for_async_finalize=True)
                    elif animated_gif:
                        media = api.chunked_upload(filename=str(prepared.path), file_type=prepared.mime_type, media_category="tweet_gif", wait_for_async_finalize=True)
                    else:
                        media = api.media_upload(filename=str(prepared.path), media_category="tweet_image")
                    processing = getattr(media, "processing_info", {}) or {}
                    if processing.get("state") == "failed" or processing.get("error"):
                        raise ConfigurationError(f"Twitter could not process uploaded media: {processing.get('error') or 'processing failed'}")
                    if attachment.alt_text and not is_video:
                        api.create_media_metadata(media.media_id, attachment.alt_text[:1000])
                    media_ids.append(media.media_id)

        response = client.create_tweet(
            text=service_body(post, account),
            media_ids=media_ids,
            in_reply_to_tweet_id=(context or {}).get("reply_external_id"),
            quote_tweet_id=(context or {}).get("quote_external_id"),
        )
        external_id = str(response.data["id"])
        username = config.get("username")
        external_url = f"https://twitter.com/{username}/status/{external_id}" if username else None
        return PublishResult(service="twitter", external_id=external_id, external_url=external_url, raw=response.data)
