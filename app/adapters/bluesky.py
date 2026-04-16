from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import requests
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
    service_body,
)
from app.domain import (
    CanonicalPostPayload,
    ExternalPostRefPayload,
    PendingRelationship,
    PollResult,
    PublishPreview,
    PublishResult,
    ValidationIssue,
)
from app.models import Account, AccountSyncState, CanonicalPost, Persona
from app.services.storage import download_media


def _get_client(config: dict[str, Any], *, update_session: Callable[[str], None] | None = None):
    from atproto import Client

    client = Client()
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
    mention_regex = rb"(@([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)"
    text_bytes = text.encode("utf-8")
    for match in re.finditer(mention_regex, text_bytes):
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


def _extract_hashtags(text: str) -> list[str]:
    return [tag.strip("#") for tag in re.findall(r"#\w+", text)]


def _build_facets(text: str) -> list[dict[str, Any]]:
    facets: list[dict[str, Any]] = []
    for mention in _parse_mentions(text):
        response = requests.get(
            "https://bsky.social/xrpc/com.atproto.identity.resolveHandle",
            params={"handle": mention["handle"]},
            timeout=10,
        )
        if response.status_code == 400:
            continue
        did = response.json()["did"]
        facets.append(
            {
                "index": {"byteStart": mention["start"], "byteEnd": mention["end"]},
                "features": [{"$type": "app.bsky.richtext.facet#mention", "did": did}],
            }
        )
    for url in _parse_urls(text):
        facets.append(
            {
                "index": {"byteStart": url["start"], "byteEnd": url["end"]},
                "features": [{"$type": "app.bsky.richtext.facet#link", "uri": url["url"]}],
            }
        )
    for tag in _extract_hashtags(text):
        start = text.find("#" + tag)
        end = start + len("#" + tag)
        facets.append(
            {
                "index": {"byteStart": start, "byteEnd": end},
                "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": tag}],
            }
        )
    return facets


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
    for tag in _extract_hashtags(text):
        start = text.find("#" + tag)
        end = start + len("#" + tag)
        facets.append(
            {
                "type": "tag",
                "tag": tag,
                "byteStart": start,
                "byteEnd": end,
            }
        )
    return facets, notes


def _post_id_from_uri(uri: str | None) -> str | None:
    if not uri:
        return None
    return str(uri).split("/")[-1]


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
            if feed_view.post.embed and hasattr(feed_view.post.embed, "images"):
                for index, image in enumerate(feed_view.post.embed.images):
                    filename_hint = image.fullsize.split("/")[-1] or f"image-{index}.jpg"
                    attachments.append(download_media(image.fullsize, filename_hint, image.alt or "", index))
            elif feed_view.post.embed and hasattr(feed_view.post.embed, "media") and hasattr(feed_view.post.embed.media, "images"):
                for index, image in enumerate(feed_view.post.embed.media.images):
                    filename_hint = image.fullsize.split("/")[-1] or f"image-{index}.jpg"
                    attachments.append(download_media(image.fullsize, filename_hint, image.alt or "", index))

            reply_to_external = None
            quote_of_external = None
            if getattr(feed_view.post.record, "reply", None) and getattr(feed_view.post.record.reply, "parent", None):
                reply_to_external_id = _post_id_from_uri(getattr(feed_view.post.record.reply.parent, "uri", None))
                if reply_to_external_id:
                    reply_to_external = PendingRelationship(external_id=reply_to_external_id)

            if feed_view.post.embed and hasattr(feed_view.post.embed, "record"):
                quoted_record = feed_view.post.embed.record
                quote_external_id = _post_id_from_uri(getattr(quoted_record, "uri", None))
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
        attachments = sorted(post.attachments, key=lambda item: item.sort_order)
        video_attachments = [attachment for attachment in attachments if is_video_attachment(attachment)]
        if len(body) > 300:
            issues.append(ValidationIssue(service="bluesky", field="body", message="Bluesky posts are limited to 300 characters."))
        if len(attachments) > 4:
            issues.append(ValidationIssue(service="bluesky", field="media", message="Bluesky supports up to 4 images per post."))
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
                    message="Bluesky currently supports either up to 4 images or one MP4 video, not mixed or multi-video posts.",
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
        attachments = sorted(post.attachments, key=lambda item: item.sort_order)
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
            }
        elif attachments:
            action = "send_images"
            request_shape["images"] = [
                {
                    "filename": Path(attachment.storage_path).name,
                    "mime_type": attachment.mime_type,
                    "alt_text": attachment.alt_text,
                }
                for attachment in attachments
            ]
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

        response = None
        if len(post.attachments) == 1 and is_video_attachment(post.attachments[0]):
            attachment = post.attachments[0]
            with Path(attachment.storage_path).open("rb") as handle:
                response = client.send_video(body, handle.read(), attachment.alt_text, facets=facets)
        elif post.attachments:
            image_data: list[bytes] = []
            image_alts: list[str] = []
            for attachment in sorted(post.attachments, key=lambda item: item.sort_order):
                with Path(attachment.storage_path).open("rb") as handle:
                    image_data.append(handle.read())
                image_alts.append(attachment.alt_text)
            response = client.send_images(body, image_data, image_alts, facets=facets)
        else:
            response = client.send_post(text=body, facets=facets)

        uri = response.uri
        handle = config.get("handle")
        external_id = _post_id_from_uri(uri) or ""
        external_url = f"https://bsky.app/profile/{handle}/post/{external_id}" if handle else None
        return PublishResult(service="bluesky", external_id=external_id, external_url=external_url, raw={"uri": uri})
