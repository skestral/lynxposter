from __future__ import annotations

import html
import re
import time
from collections.abc import Mapping
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.adapters.base import (
    ConfigurationError,
    DestinationAdapter,
    SourceAdapter,
    get_account_credentials,
    get_account_publish_setting,
)
from app.adapters.common import cutoff_for_initial_poll, import_existing_posts_on_first_scan, is_initial_sync, now_utc, service_attachments, service_body
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
from app.services.media_preparation import ImageLimits, prepare_image
from app.services.storage import download_media
from app.services.video_preparation import VideoLimits, prepare_video


# Compatibility fallback for instances that do not advertise media limits.
# Advertised configuration takes precedence; this is not a universal Mastodon cap.
MASTODON_FALLBACK_IMAGE_LIMITS = ImageLimits(max_bytes=8 * 1024 * 1024, max_pixels=4096 * 4096)
MASTODON_FALLBACK_VIDEO_LIMITS = VideoLimits(
    max_bytes=40 * 1024 * 1024, max_pixels=1920 * 1080, max_frame_rate=60
)
MASTODON_MEDIA_PROCESSING_TIMEOUT = 120


def _positive_limit(value: Any, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdecimal() and int(value) > 0:
        return int(value)
    return default


def _mastodon_media_configuration(client: Any) -> Mapping[str, Any]:
    try:
        instance = client.instance()
    except Exception:
        return {}
    configuration = instance.get("configuration") if isinstance(instance, Mapping) else None
    media = configuration.get("media_attachments") if isinstance(configuration, Mapping) else None
    if not isinstance(media, Mapping):
        return {}
    return media


def _mastodon_image_limits(client: Any, media: Mapping[str, Any] | None = None) -> ImageLimits:
    if media is None:
        media = _mastodon_media_configuration(client)

    supported_mime_types = MASTODON_FALLBACK_IMAGE_LIMITS.supported_mime_types
    advertised_types = media.get("supported_mime_types")
    if isinstance(advertised_types, (list, tuple)) and all(isinstance(value, str) for value in advertised_types):
        supported_mime_types = tuple(
            value.strip().lower() for value in advertised_types if value.strip().lower().startswith("image/")
        )
    return ImageLimits(
        max_bytes=_positive_limit(media.get("image_size_limit"), MASTODON_FALLBACK_IMAGE_LIMITS.max_bytes),
        max_pixels=_positive_limit(media.get("image_matrix_limit"), MASTODON_FALLBACK_IMAGE_LIMITS.max_pixels),
        supported_mime_types=supported_mime_types,
    )


def _mastodon_video_limits(media: Mapping[str, Any]) -> VideoLimits:
    return VideoLimits(
        max_bytes=_positive_limit(media.get("video_size_limit"), MASTODON_FALLBACK_VIDEO_LIMITS.max_bytes),
        max_pixels=_positive_limit(media.get("video_matrix_limit"), MASTODON_FALLBACK_VIDEO_LIMITS.max_pixels),
        max_frame_rate=_positive_limit(media.get("video_frame_rate_limit"), MASTODON_FALLBACK_VIDEO_LIMITS.max_frame_rate),
    )


def _media_value(media: Any, key: str) -> Any:
    return media.get(key) if isinstance(media, Mapping) else getattr(media, key, None)


def _wait_for_media_processing(client: Any, media: Any) -> Any:
    # Mastodon.py uses /api/v2/media asynchronously for large media. A status
    # cannot attach that upload until the server finishes processing it.
    has_url = "url" in media if isinstance(media, Mapping) else hasattr(media, "url")
    if _media_value(media, "error"):
        raise RuntimeError(f"Mastodon media processing failed: {_media_value(media, 'error')}")
    if not has_url:
        return media
    deadline = time.monotonic() + MASTODON_MEDIA_PROCESSING_TIMEOUT
    while _media_value(media, "url") is None:
        if time.monotonic() >= deadline:
            raise RuntimeError("Mastodon media did not finish processing before the upload timeout.")
        time.sleep(2)
        media = client.media(_media_value(media, "id"))
        if _media_value(media, "error"):
            raise RuntimeError(f"Mastodon media processing failed: {_media_value(media, 'error')}")
    return media


def _strip_html(value: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text).strip()


def _get_client(config: dict[str, str]):
    from mastodon import Mastodon

    if not config.get("token") or not config.get("instance"):
        raise ConfigurationError("Mastodon credentials are incomplete.")
    return Mastodon(access_token=config["token"], api_base_url=config["instance"])


class MastodonSourceAdapter(SourceAdapter):
    service = "mastodon"

    def poll(
        self,
        session: Session,
        persona: Persona,
        account: Account,
        sync_state: AccountSyncState | None,
    ) -> PollResult:
        config = get_account_credentials(account)
        client = _get_client(config)
        me = client.account_verify_credentials()
        initial_sync = is_initial_sync(sync_state)
        allow_initial_backfill = import_existing_posts_on_first_scan(persona, account)
        since_id = sync_state.cursor if sync_state and sync_state.cursor else None
        since = cutoff_for_initial_poll(persona, account)
        if sync_state and sync_state.state_json.get("last_seen_at"):
            since = datetime.fromisoformat(str(sync_state.state_json["last_seen_at"]))
        statuses = client.account_statuses(me["id"], since_id=since_id, exclude_reblogs=True, limit=40)

        newest_id = since_id
        newest_seen = since
        for status in statuses:
            if status.get("reblog"):
                continue
            status_id = str(status["id"])
            created_at = status["created_at"]
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            newest_seen = max(newest_seen, created_at)
            if newest_id is None or int(status_id) > int(str(newest_id)):
                newest_id = status_id

        if initial_sync and not allow_initial_backfill:
            next_state = dict(sync_state.state_json if sync_state else {})
            if newest_id:
                next_state["last_seen_id"] = newest_id
            next_state["last_seen_at"] = newest_seen.isoformat() if statuses else now_utc().isoformat()
            return PollResult(
                posts=[],
                next_state=next_state,
                cursor=str(newest_id) if newest_id else None,
                note="Initialized Mastodon sync without importing historical posts.",
            )

        posts: list[CanonicalPostPayload] = []
        for status in reversed(statuses):
            if status.get("reblog"):
                continue
            status_id = str(status["id"])
            created_at = status["created_at"]
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            if since_id is None and created_at <= since:
                continue

            attachments = []
            for index, media in enumerate(status.get("media_attachments", [])):
                media_url = media.get("url") or media.get("preview_url")
                if not media_url:
                    continue
                attachments.append(
                    download_media(
                        media_url,
                        media_url.split("/")[-1] or f"mastodon-media-{index}",
                        media.get("description") or "",
                        index,
                    )
                )

            reply_to = None
            if status.get("in_reply_to_id"):
                reply_to = PendingRelationship(external_id=str(status["in_reply_to_id"]))

            posts.append(
                CanonicalPostPayload(
                    body=_strip_html(status.get("content", "")),
                    media=attachments,
                    metadata={
                        "link": status.get("url"),
                        "visibility": status.get("visibility"),
                    },
                    published_at=created_at,
                    external_refs=[
                        ExternalPostRefPayload(
                            external_id=status_id,
                            external_url=status.get("url"),
                            observed_at=created_at,
                        )
                    ],
                    reply_to_external=reply_to,
                )
            )

        next_state = dict(sync_state.state_json if sync_state else {})
        if newest_id:
            next_state["last_seen_id"] = newest_id
        next_state["last_seen_at"] = newest_seen.isoformat() if statuses else since.isoformat()
        return PollResult(posts=posts, next_state=next_state, cursor=str(newest_id) if newest_id else None)


class MastodonDestinationAdapter(DestinationAdapter):
    service = "mastodon"
    _allowed_visibilities = ("private", "public", "unlisted", "direct")
    _legacy_visibility_map = {"hybrid": "public"}

    def _resolve_visibility(self, post: CanonicalPost, persona: Persona, account: Account) -> tuple[str, str, str | None]:
        raw_visibility = (post.metadata_json or {}).get("visibility")
        if raw_visibility in (None, ""):
            raw_visibility = get_account_publish_setting(persona, account, "visibility", "public")
        normalized_visibility = str(raw_visibility or "public").strip().lower()
        mapped_visibility = self._legacy_visibility_map.get(normalized_visibility, normalized_visibility)
        return str(raw_visibility or "public"), mapped_visibility, self._legacy_visibility_map.get(normalized_visibility)

    def _resolve_language(self, persona: Persona, account: Account) -> str | None:
        raw_language = get_account_publish_setting(
            persona,
            account,
            "language",
            None,
            fallback_keys=("mastodon_lang",),
        )
        language = str(raw_language or "").strip()
        return language or None

    def validate(self, post: CanonicalPost, persona: Persona, account: Account) -> list[ValidationIssue]:
        body = service_body(post, account)
        issues: list[ValidationIssue] = []
        raw_visibility, visibility, mapped_visibility = self._resolve_visibility(post, persona, account)
        if len(body) > 500:
            issues.append(ValidationIssue(service="mastodon", field="body", message="Mastodon posts are limited to 500 characters by default."))
        if visibility not in self._allowed_visibilities:
            issues.append(
                ValidationIssue(
                    service="mastodon",
                    field="visibility",
                    message=(
                        f"Invalid Mastodon visibility '{raw_visibility}'. "
                        f"Acceptable values are {list(self._allowed_visibilities)}."
                    ),
                )
            )
        elif mapped_visibility:
            issues.append(
                ValidationIssue(
                    service="mastodon",
                    field="visibility",
                    message=f"Legacy Mastodon visibility '{raw_visibility}' was normalized to '{visibility}'.",
                    severity="warning",
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
        raw_visibility, visibility, mapped_visibility = self._resolve_visibility(post, persona, account)
        language = self._resolve_language(persona, account)
        body = service_body(post, account)
        reply_to = (context or {}).get("reply_external_id")
        quote_url = (context or {}).get("quote_external_url")
        if quote_url and not reply_to:
            reply_to = (context or {}).get("quote_external_id")
        elif quote_url and reply_to:
            body = f"{body}\n{quote_url}".strip()
        attachments = service_attachments(post, account)
        request_shape = {
            "status": body,
            "in_reply_to_id": reply_to,
            "visibility": visibility,
            "language": language,
            "media_ids": [f"<uploaded-media-{index + 1}>" for index, _ in enumerate(attachments)],
        }
        notes = []
        if attachments:
            notes.append("Media uploads are skipped in sandbox mode, so media_ids are placeholders.")
        if any(str(attachment.mime_type or "").lower().startswith("image/") for attachment in attachments):
            notes.append(
                "Images are resized or compressed before publishing to fit the destination instance's advertised media limits; "
                "stored originals are preserved. Instance limits are not fetched in sandbox mode."
            )
        if any(str(attachment.mime_type or "").lower().startswith("video/") for attachment in attachments):
            notes.append(
                "Videos are prepared as streaming MP4 with H.264 video and AAC audio to fit the instance's advertised "
                "file size, resolution, and frame rate limits. Stored originals are preserved."
            )
        if mapped_visibility:
            notes.append(f"Normalized legacy Mastodon visibility '{raw_visibility}' to '{visibility}'.")
        return PublishPreview(
            service="mastodon",
            action="status_post",
            rendered_body=body,
            endpoint_label="/api/v1/statuses",
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
        client = _get_client(get_account_credentials(account))
        attachments = service_attachments(post, account)
        media_configuration = (
            _mastodon_media_configuration(client)
            if any(str(attachment.mime_type or "").lower().startswith(("image/", "video/")) for attachment in attachments)
            else {}
        )
        image_limits = _mastodon_image_limits(client, media_configuration)
        video_limits = _mastodon_video_limits(media_configuration)
        advertised_types = media_configuration.get("supported_mime_types")
        if (
            any(str(attachment.mime_type or "").lower().startswith("video/") for attachment in attachments)
            and isinstance(advertised_types, (list, tuple))
            and "video/mp4" not in [str(value).strip().lower() for value in advertised_types]
        ):
            raise ValueError("The Mastodon destination does not accept MP4 video uploads.")
        raw_visibility, visibility, _mapped_visibility = self._resolve_visibility(post, persona, account)
        if visibility not in self._allowed_visibilities:
            raise ValueError(f"Invalid Mastodon visibility '{raw_visibility}'. Acceptable values are {list(self._allowed_visibilities)}.")
        language = self._resolve_language(persona, account)
        body = service_body(post, account)
        reply_to = (context or {}).get("reply_external_id")
        quote_url = (context or {}).get("quote_external_url")
        if quote_url and not reply_to:
            reply_to = (context or {}).get("quote_external_id")
        elif quote_url and reply_to:
            body = f"{body}\n{quote_url}".strip()
        with ExitStack() as stack:
            upload_items = []
            for attachment in attachments:
                path = Path(attachment.storage_path)
                mime_type = str(attachment.mime_type or "").lower()
                if mime_type.startswith("image/"):
                    prepared = stack.enter_context(prepare_image(path, limits=image_limits))
                    upload_items.append((prepared.path, prepared.mime_type, attachment.alt_text))
                elif mime_type.startswith("video/"):
                    prepared = stack.enter_context(prepare_video(path, limits=video_limits))
                    upload_items.append((prepared.path, prepared.mime_type, attachment.alt_text))
                else:
                    if not path.is_file():
                        raise FileNotFoundError(f"Mastodon attachment is missing on disk: {path}")
                    upload_items.append((path, None, attachment.alt_text))

            media_ids = []
            # Finish local preparation for every attachment before creating any
            # remote media, so an invalid later file cannot leave partial uploads.
            for path, mime_type, alt_text in upload_items:
                if mime_type:
                    result = client.media_post(path, mime_type=mime_type, description=alt_text or None)
                elif alt_text:
                    result = client.media_post(path, description=alt_text)
                else:
                    result = client.media_post(path)
                result = _wait_for_media_processing(client, result)
                media_ids.append(_media_value(result, "id"))

            response = client.status_post(body, in_reply_to_id=reply_to, media_ids=media_ids, visibility=visibility, language=language)
        external_id = str(response["id"])
        config = get_account_credentials(account)
        instance = config["instance"].rstrip("/")
        handle = config.get("handle", "").lstrip("@").split("@", 1)[0]
        external_url = response.get("url") or (f"{instance}/@{handle}/{external_id}" if handle else None)
        return PublishResult(service="mastodon", external_id=external_id, external_url=external_url, raw=dict(response))
