from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import regex
import requests
import httpx
from sqlalchemy.orm import Session

from app.adapters.base import (
    ConfigurationError,
    DestinationAdapter,
    SourceAdapter,
    get_account_credentials,
    get_account_publish_setting,
)
from app.adapters.common import (
    cutoff_for_initial_poll,
    import_existing_posts_on_first_scan,
    is_initial_sync,
    is_video_attachment,
    service_attachments,
    service_body,
)
from app.domain import (
    CanonicalPostPayload,
    ExternalPostRefPayload,
    MediaItem,
    PendingRelationship,
    PollResult,
    PublishPreview,
    PublishResult,
    ValidationIssue,
)
from app.config import get_settings
from app.models import Account, AccountSyncState, CanonicalPost, Persona
from app.services.media_preparation import ImageLimits, prepare_image
from app.services.video_preparation import VideoLimits, prepare_video
from app.services.storage import download_media, normalize_media_file


BLUESKY_IMAGE_LIMITS = ImageLimits(max_bytes=2_000_000, max_dimension=4000)
BLUESKY_MAX_IMAGES = 10
BLUESKY_MAX_VIDEO_BYTES = 300_000_000
BLUESKY_VIDEO_LIMITS = VideoLimits(max_bytes=BLUESKY_MAX_VIDEO_BYTES, max_duration=600, max_dimension=4000, max_frame_rate=60)


def _get_client(config: dict[str, Any], *, update_session: Callable[[str], None] | None = None):
    from atproto import Client, Request

    client = Client(request=Request(timeout=httpx.Timeout(180.0, connect=10.0, pool=10.0)))
    session_string = config.get("session_string")
    handle = config.get("handle")
    password = config.get("password")
    if session_string:
        try:
            client.login(session_string=session_string)
            return client
        except Exception:
            pass
    if not handle or not password:
        raise ConfigurationError("Bluesky requires either a session string or handle/password.")
    client.login(handle, password)
    if update_session:
        try:
            update_session(client.export_session_string())
        except Exception:
            pass
    return client


def _restore_urls(record: Any) -> str:
    text = record.text
    encoded_text = text.encode("utf-8")
    for facet in record.facets or []:
        if facet.features[0].py_type != "app.bsky.richtext.facet#link":
            continue
        url = facet.features[0].uri
        start = facet.index.byte_start
        end = facet.index.byte_end
        shortened = encoded_text[start:end].decode("utf-8")
        text = text.replace(shortened, url)
    return text


def _parse_mentions(text: str) -> list[dict[str, Any]]:
    spans = []
    urls = _parse_urls(text)
    mention_regex = rb"(@([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)"
    text_bytes = text.encode("utf-8")
    for match in re.finditer(mention_regex, text_bytes):
        if any(match.start(1) < url["end"] and match.end(1) > url["start"] for url in urls):
            continue
        spans.append(
            {
                "start": match.start(1),
                "end": match.end(1),
                "handle": match.group(1).decode("utf-8"),
            }
        )
    return spans


def _parse_urls(text: str) -> list[dict[str, Any]]:
    spans = []
    url_regex = rb"(https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*))"
    text_bytes = text.encode("utf-8")
    for match in re.finditer(url_regex, text_bytes):
        spans.append(
            {
                "start": match.start(1),
                "end": match.end(1),
                "url": match.group(1).decode("utf-8"),
            }
        )
    return spans


def _parse_hashtags(text: str) -> list[dict[str, Any]]:
    spans: list[dict[str, Any]] = []
    urls = _parse_urls(text)
    # Match the official rich-text detector's Unicode tag boundaries, including
    # emoji/full-width hashes, and strip punctuation after the tag.
    whitespace = r"\x09-\x0d \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff"
    excluded = whitespace + r"\u00ad\u2060\u200b\u200c\u200d\u20e2"
    pattern = rf"(?:^|[{whitespace}])[#＃]((?!\ufe0f)[^{excluded}]*[^0-9\p{{P}}{excluded}]+[^{excluded}]*)?"
    for match in regex.finditer(pattern, text):
        tag = regex.sub(r"\p{P}+$", "", match.group(1) or "")
        if not tag:
            continue
        if len(regex.findall(r"\X", tag)) > 64 or len(tag.encode("utf-8")) > 640:
            continue
        # Facets index UTF-8 bytes, not Python Unicode character positions.
        start_index = match.start(1) - 1
        start = len(text[:start_index].encode("utf-8"))
        end = start + len(text[start_index : start_index + 1 + len(tag)].encode("utf-8"))
        if any(start < url["end"] and end > url["start"] for url in urls):
            continue
        spans.append({"start": start, "end": end, "tag": tag})
    return spans


def _build_facets(text: str) -> list[dict[str, Any]]:
    from atproto import client_utils

    spans: list[dict[str, Any]] = []
    for mention in _parse_mentions(text):
        response = requests.get(
            "https://bsky.social/xrpc/com.atproto.identity.resolveHandle",
            params={"handle": mention["handle"][1:]},
            timeout=10,
        )
        if response.status_code == 400:
            continue
        did = response.json()["did"]
        spans.append({**mention, "kind": "mention", "value": did})
    for url in _parse_urls(text):
        spans.append({**url, "kind": "link", "value": url["url"]})
    for tag in _parse_hashtags(text):
        spans.append({**tag, "kind": "tag", "value": tag["tag"]})

    # The SDK owns facet offsets while we preserve the caption byte for byte.
    builder = client_utils.TextBuilder()
    encoded = text.encode("utf-8")
    cursor = 0
    for span in sorted(spans, key=lambda item: item["start"]):
        if span["start"] < cursor:
            continue
        builder.text(encoded[cursor : span["start"]].decode("utf-8"))
        segment = encoded[span["start"] : span["end"]].decode("utf-8")
        getattr(builder, span["kind"])(segment, span["value"])
        cursor = span["end"]
    builder.text(encoded[cursor:].decode("utf-8"))
    return [
        {
            "index": {"byteStart": facet.index.byte_start, "byteEnd": facet.index.byte_end},
            "features": [feature.model_dump(by_alias=True, exclude_none=True) for feature in facet.features],
        }
        for facet in builder.build_facets()
    ]


def _build_preview_facets(text: str) -> tuple[list[dict[str, Any]], list[str]]:
    facets: list[dict[str, Any]] = []
    notes: list[str] = []
    for mention in _parse_mentions(text):
        facets.append(
            {
                "type": "mention",
                "handle": mention["handle"],
                "byteStart": mention["start"],
                "byteEnd": mention["end"],
                "resolution": "skipped in sandbox",
            }
        )
    if any(item["type"] == "mention" for item in facets):
        notes.append("Bluesky mention resolution is skipped in sandbox mode to avoid live network lookups.")
    for url in _parse_urls(text):
        facets.append(
            {
                "type": "link",
                "uri": url["url"],
                "byteStart": url["start"],
                "byteEnd": url["end"],
            }
        )
    for tag in _parse_hashtags(text):
        facets.append(
            {
                "type": "tag",
                "tag": tag["tag"],
                "byteStart": tag["start"],
                "byteEnd": tag["end"],
            }
        )
    return sorted(facets, key=lambda facet: facet["byteStart"]), notes


def _post_id_from_uri(uri: str | None) -> str | None:
    if not uri:
        return None
    return str(uri).split("/")[-1]


def _embedded_images(embed: Any) -> list[Any]:
    media = getattr(embed, "media", embed)
    if getattr(media, "images", None):
        return list(media.images)
    return [item for item in getattr(media, "items", []) if getattr(item, "fullsize", None)]


def _embedded_video(record: Any) -> Any | None:
    embed = getattr(record, "embed", None)
    media = getattr(embed, "media", embed)
    return media if getattr(media, "video", None) is not None else None


def _download_video_blob(client: Any, post_view: Any, video_embed: Any, *, sort_order: int) -> MediaItem:
    blob = video_embed.video
    did = getattr(post_view.author, "did", None) or str(post_view.uri).split("/")[2]
    # The authenticated source is this author's PDS. Fetch the original blob,
    # rather than importing an HLS playlist or a video thumbnail as media.
    data = client.com.atproto.sync.get_blob({"did": did, "cid": str(blob.cid)})
    if not isinstance(data, (bytes, bytearray)) or not data:
        raise ConfigurationError("Bluesky returned an empty or invalid source video blob.")
    directory = get_settings().imported_media_dir
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"bluesky-video-{uuid4().hex}.mp4"
    try:
        path.write_bytes(data)
        path, mime_type, size_bytes, checksum = normalize_media_file(path, blob.mime_type)
        return MediaItem(
            storage_path=path, mime_type=mime_type, alt_text=getattr(video_embed, "alt", None) or "",
            size_bytes=size_bytes, checksum=checksum, sort_order=sort_order,
        )
    except Exception:
        path.unlink(missing_ok=True)
        raise


def _quoted_post_uri(post_view: Any) -> str | None:
    # recordWithMedia wraps its strong reference in an embed.record object.
    for embed in (getattr(post_view.record, "embed", None), post_view.embed):
        quoted = getattr(embed, "record", None)
        quoted = getattr(quoted, "record", quoted)
        uri = getattr(quoted, "uri", None)
        if uri:
            return str(uri)
    return None


class BlueskySourceAdapter(SourceAdapter):
    service = "bluesky"

    def poll(
        self,
        session: Session,
        persona: Persona,
        account: Account,
        sync_state: AccountSyncState | None,
    ) -> PollResult:
        config = get_account_credentials(account)
        handle = config.get("handle")
        if not handle:
            return PollResult(posts=[], next_state=(sync_state.state_json if sync_state else {}), cursor=(sync_state.cursor if sync_state else None))

        def update_session(session_string: str) -> None:
            credentials = dict(account.credentials_json or {})
            credentials["session_string"] = session_string
            account.credentials_json = credentials

        client = _get_client(config, update_session=update_session)
        feed = client.app.bsky.feed.get_author_feed({"actor": handle})
        initial_sync = is_initial_sync(sync_state)
        allow_initial_backfill = import_existing_posts_on_first_scan(persona, account)
        since = cutoff_for_initial_poll(persona, account)
        if sync_state and sync_state.state_json.get("last_seen_at"):
            since = datetime.fromisoformat(str(sync_state.state_json["last_seen_at"]))

        if initial_sync and not allow_initial_backfill:
            newest_seen = since
            for feed_view in feed.feed:
                if feed_view.post.author.handle != handle:
                    continue
                created_at = datetime.fromisoformat(feed_view.post.record.created_at.replace("Z", "+00:00"))
                newest_seen = max(newest_seen, created_at)

            next_state = dict(sync_state.state_json if sync_state else {})
            next_state["last_seen_at"] = newest_seen.isoformat()
            return PollResult(
                posts=[],
                next_state=next_state,
                cursor=(sync_state.cursor if sync_state else None),
                note="Initialized Bluesky sync without importing historical posts.",
            )

        payloads: list[CanonicalPostPayload] = []
        newest_seen = since
        for feed_view in reversed(feed.feed):
            if feed_view.post.author.handle != handle:
                continue
            created_at = datetime.fromisoformat(feed_view.post.record.created_at.replace("Z", "+00:00"))
            if created_at <= since:
                continue
            newest_seen = max(newest_seen, created_at)
            body = feed_view.post.record.text
            if getattr(feed_view.post.record, "facets", None):
                body = _restore_urls(feed_view.post.record)

            metadata: dict[str, Any] = {
                "link": f"https://bsky.app/profile/{handle}/post/{feed_view.post.uri.split('/')[-1]}",
                "visibility": get_account_publish_setting(persona, account, "visibility", "public"),
            }
            if feed_view.post.embed and hasattr(feed_view.post.embed, "external") and hasattr(feed_view.post.embed.external, "uri"):
                uri = feed_view.post.embed.external.uri
                if uri not in body:
                    body += f"\n{uri}"

            attachments = []
            for index, image in enumerate(_embedded_images(feed_view.post.embed)):
                filename_hint = image.fullsize.split("/")[-1] or f"image-{index}.jpg"
                attachments.append(download_media(image.fullsize, filename_hint, image.alt or "", index))
            video_embed = _embedded_video(feed_view.post.record)
            if video_embed is not None:
                attachments.append(_download_video_blob(client, feed_view.post, video_embed, sort_order=len(attachments)))

            reply_to_external = None
            quote_of_external = None
            if getattr(feed_view.post.record, "reply", None) and getattr(feed_view.post.record.reply, "parent", None):
                reply_to_external_id = _post_id_from_uri(getattr(feed_view.post.record.reply.parent, "uri", None))
                if reply_to_external_id:
                    reply_to_external = PendingRelationship(external_id=reply_to_external_id)

            quote_external_id = _post_id_from_uri(_quoted_post_uri(feed_view.post))
            if quote_external_id:
                quote_of_external = PendingRelationship(external_id=quote_external_id)

            payloads.append(
                CanonicalPostPayload(
                    body=body,
                    media=attachments,
                    metadata=metadata,
                    published_at=created_at,
                    external_refs=[
                        ExternalPostRefPayload(
                            external_id=_post_id_from_uri(feed_view.post.uri) or "",
                            external_url=metadata["link"],
                            observed_at=created_at,
                        )
                    ],
                    reply_to_external=reply_to_external,
                    quote_of_external=quote_of_external,
                )
            )

        next_state = dict(sync_state.state_json if sync_state else {})
        next_state["last_seen_at"] = newest_seen.isoformat()
        return PollResult(posts=payloads, next_state=next_state, cursor=(sync_state.cursor if sync_state else None))


class BlueskyDestinationAdapter(DestinationAdapter):
    service = "bluesky"

    def validate(self, post: CanonicalPost, persona: Persona, account: Account) -> list[ValidationIssue]:
        body = service_body(post, account)
        issues: list[ValidationIssue] = []
        attachments = service_attachments(post, account)
        video_attachments = [attachment for attachment in attachments if is_video_attachment(attachment)]
        if len(regex.findall(r"\X", body)) > 300:
            issues.append(ValidationIssue(service="bluesky", field="body", message="Bluesky posts are limited to 300 characters."))
        if len(body.encode("utf-8")) > 3000:
            issues.append(ValidationIssue(service="bluesky", field="body", message="Bluesky post text is limited to 3,000 UTF-8 bytes."))
        if len(attachments) > BLUESKY_MAX_IMAGES:
            issues.append(ValidationIssue(service="bluesky", field="media", message="Bluesky supports up to 10 images per gallery post."))
        if any(
            not str(attachment.mime_type or "").lower().startswith(("image/", "video/"))
            for attachment in attachments
        ):
            issues.append(
                ValidationIssue(
                    service="bluesky",
                    field="media",
                    message="Bluesky attachments must be images or one MP4 video.",
                )
            )
        if len(video_attachments) > 1:
            issues.append(
                ValidationIssue(
                    service="bluesky",
                    field="media",
                    message="Bluesky currently supports only one video attachment per post.",
                )
            )
        if video_attachments and len(attachments) > 1:
            issues.append(
                ValidationIssue(
                    service="bluesky",
                    field="media",
                    message="Bluesky currently supports either up to 10 images or one MP4 video, not mixed or multi-video posts.",
                )
            )
        return issues

    def preview(
        self,
        post: CanonicalPost,
        persona: Persona,
        account: Account,
        *,
        context: dict[str, str | None] | None = None,
    ) -> PublishPreview:
        body = service_body(post, account)
        facets, notes = _build_preview_facets(body)
        attachments = service_attachments(post, account)
        request_shape: dict[str, Any] = {
            "text": body,
            "facets": facets,
        }
        action = "send_post"
        if len(attachments) == 1 and is_video_attachment(attachments[0]):
            attachment = attachments[0]
            action = "send_video"
            request_shape["video"] = {
                "filename": Path(attachment.storage_path).name,
                "mime_type": attachment.mime_type,
                "alt_text": attachment.alt_text,
                "prepared_mime_type": "video/mp4",
                "max_bytes": BLUESKY_VIDEO_LIMITS.max_bytes,
                "max_duration_seconds": BLUESKY_VIDEO_LIMITS.max_duration,
            }
            request_shape["embed"] = {
                "$type": "app.bsky.embed.video",
                "video": {"$type": "blob", "mimeType": "video/mp4", "size": "<prepared-video-bytes>", "ref": {"$link": "<uploaded-video-cid>"}},
                "alt": attachment.alt_text or "",
                "aspectRatio": {"width": "<prepared-width>", "height": "<prepared-height>"},
            }
            notes.append(
                f"Videos are prepared as H.264/AAC MP4 up to {BLUESKY_VIDEO_LIMITS.max_bytes // 1_000_000} MB "
                f"({BLUESKY_VIDEO_LIMITS.max_bytes:,} bytes) and {int(BLUESKY_VIDEO_LIMITS.max_duration)} seconds (10 minutes); "
                "stored originals are preserved."
            )
        elif attachments:
            action = "send_gallery" if len(attachments) > 4 else "send_images"
            request_shape["embed_type"] = "app.bsky.embed.gallery" if len(attachments) > 4 else "app.bsky.embed.images"
            request_shape["image_limits"] = {"max_bytes": BLUESKY_IMAGE_LIMITS.max_bytes, "max_dimension": BLUESKY_IMAGE_LIMITS.max_dimension}
            notes.append(
                f"Each image is prepared to fit {BLUESKY_IMAGE_LIMITS.max_bytes // 1_000_000} MB "
                f"({BLUESKY_IMAGE_LIMITS.max_bytes:,} bytes); stored originals are preserved. "
                f"Use up to {BLUESKY_MAX_IMAGES} images, with gallery embeds for five or more."
            )
            request_shape["images"] = [
                {
                    "filename": Path(attachment.storage_path).name,
                    "mime_type": attachment.mime_type,
                    "alt_text": attachment.alt_text,
                }
                for attachment in attachments
            ]
            image_embeds = [
                {
                    "image": {"$type": "blob", "mimeType": "<prepared-image-mime-type>", "size": "<prepared-image-bytes>", "ref": {"$link": f"<uploaded-image-{index + 1}-cid>"}},
                    "alt": attachment.alt_text or "",
                    "aspectRatio": {"width": "<prepared-width>", "height": "<prepared-height>"},
                }
                for index, attachment in enumerate(attachments)
            ]
            if len(attachments) > 4:
                request_shape["embed"] = {
                    "$type": "app.bsky.embed.gallery",
                    "items": [{"$type": "app.bsky.embed.gallery#image", **item} for item in image_embeds],
                }
            else:
                request_shape["embed"] = {"$type": "app.bsky.embed.images", "images": image_embeds}
        return PublishPreview(
            service="bluesky",
            action=action,
            rendered_body=body,
            endpoint_label="ATProto client method",
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
        config = get_account_credentials(account)

        def update_session(session_string: str) -> None:
            credentials = dict(account.credentials_json or {})
            credentials["session_string"] = session_string
            account.credentials_json = credentials

        client = _get_client(config, update_session=update_session)
        body = service_body(post, account)
        facets = _build_facets(body)
        attachments = service_attachments(post, account)

        response = None
        if len(attachments) == 1 and is_video_attachment(attachments[0]):
            from atproto import models

            attachment = attachments[0]
            with prepare_video(Path(attachment.storage_path), limits=BLUESKY_VIDEO_LIMITS) as prepared:
                response = client.send_video(
                    body, prepared.path.read_bytes(), attachment.alt_text, facets=facets,
                    video_aspect_ratio=models.AppBskyEmbedDefs.AspectRatio(width=prepared.width, height=prepared.height),
                )
        elif attachments:
            from atproto import models

            image_data: list[bytes] = []
            image_alts: list[str] = []
            aspect_ratios = []
            for attachment in attachments:
                with prepare_image(Path(attachment.storage_path), limits=BLUESKY_IMAGE_LIMITS) as prepared:
                    image_data.append(prepared.path.read_bytes())
                    aspect_ratios.append(models.AppBskyEmbedDefs.AspectRatio(width=prepared.width, height=prepared.height))
                image_alts.append(attachment.alt_text or "")
            if len(attachments) > 4:
                items = [
                    models.AppBskyEmbedGallery.Image(image=client.upload_blob(data).blob, alt=alt, aspect_ratio=ratio)
                    for data, alt, ratio in zip(image_data, image_alts, aspect_ratios)
                ]
                response = client.send_post(text=body, embed=models.AppBskyEmbedGallery.Main(items=items), facets=facets)
            else:
                response = client.send_images(body, image_data, image_alts, facets=facets, image_aspect_ratios=aspect_ratios)
        else:
            response = client.send_post(text=body, facets=facets)

        uri = response.uri
        handle = config.get("handle")
        external_id = _post_id_from_uri(uri) or ""
        external_url = f"https://bsky.app/profile/{handle}/post/{external_id}" if handle else None
        return PublishResult(service="bluesky", external_id=external_id, external_url=external_url, raw={"uri": uri})
