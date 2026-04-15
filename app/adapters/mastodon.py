from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from app.adapters.base import (
    ConfigurationError,
    DestinationAdapter,
    SourceAdapter,
    get_account_credentials,
    get_account_publish_setting,
)
from app.adapters.common import cutoff_for_initial_poll, import_existing_posts_on_first_scan, is_initial_sync, now_utc, service_body
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
        request_shape = {
            "status": body,
            "in_reply_to_id": reply_to,
            "visibility": visibility,
            "language": language,
            "media_ids": [f"<uploaded-media-{index + 1}>" for index, _ in enumerate(sorted(post.attachments, key=lambda item: item.sort_order))],
        }
        notes = []
        if post.attachments:
            notes.append("Media uploads are skipped in sandbox mode, so media_ids are placeholders.")
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
        media_ids = []
        for attachment in sorted(post.attachments, key=lambda item: item.sort_order):
            path = Path(attachment.storage_path)
            if attachment.alt_text:
                result = client.media_post(path, description=attachment.alt_text)
            else:
                result = client.media_post(path)
            media_ids.append(result.id)

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
        response = client.status_post(body, in_reply_to_id=reply_to, media_ids=media_ids, visibility=visibility, language=language)
        external_id = str(response["id"])
        config = get_account_credentials(account)
        instance = config["instance"].rstrip("/")
        handle = config.get("handle", "").lstrip("@")
        external_url = f"{instance}/@{handle}/{external_id}" if handle else response.get("url")
        return PublishResult(service="mastodon", external_id=external_id, external_url=external_url, raw=dict(response))
