from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.adapters.base import ConfigurationError, DestinationAdapter, SourceAdapter, get_account_credentials
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


def _telegram_api_url(bot_token: str, method: str) -> str:
    return f"https://api.telegram.org/bot{bot_token}/{method}"


def _telegram_file_url(bot_token: str, file_path: str) -> str:
    return f"https://api.telegram.org/file/bot{bot_token}/{file_path.lstrip('/')}"


def _telegram_request(
    bot_token: str,
    method: str,
    *,
    json_body: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    files: dict[str, Any] | None = None,
    timeout: int = 30,
) -> Any:
    url = _telegram_api_url(bot_token, method)
    if json_body is not None:
        response = requests.post(url, json=json_body, timeout=timeout)
    else:
        response = requests.post(url, data=data, files=files, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("ok"):
        description = payload.get("description") or f"Telegram API request failed for {method}."
        raise RuntimeError(str(description))
    return payload.get("result")


def _configured_chat_value(raw_chat_id: str) -> str:
    return str(raw_chat_id or "").strip()


def _chat_matches(chat: dict[str, Any] | None, configured_chat_id: str) -> bool:
    if not chat:
        return False
    configured = _configured_chat_value(configured_chat_id)
    if not configured:
        return False
    chat_id = str(chat.get("id") or "").strip()
    if configured.lstrip("-").isdigit():
        return chat_id == configured
    username = str(chat.get("username") or "").strip().lstrip("@").lower()
    return username == configured.lstrip("@").lower()


def _message_url(chat: dict[str, Any] | None, message_id: str) -> str | None:
    if not chat:
        return None
    username = str(chat.get("username") or "").strip().lstrip("@")
    if username:
        return f"https://t.me/{username}/{message_id}"
    chat_id = str(chat.get("id") or "").strip()
    if chat_id.startswith("-100") and len(chat_id) > 4:
        return f"https://t.me/c/{chat_id[4:]}/{message_id}"
    return None


def _message_datetime(message: dict[str, Any]) -> datetime:
    return datetime.fromtimestamp(int(message.get("date") or 0), tz=timezone.utc)


def _message_body(message: dict[str, Any]) -> str:
    return str(message.get("text") or message.get("caption") or "").strip()


def _ensure_dedicated_source_token(session: Session, account: Account, bot_token: str) -> None:
    stmt = select(Account).where(
        Account.service == "telegram",
        Account.is_enabled.is_(True),
        Account.source_enabled.is_(True),
    )
    for other in session.scalars(stmt):
        if other.id == account.id:
            continue
        other_token = str((other.credentials_json or {}).get("bot_token") or "").strip()
        if other_token and other_token == bot_token:
            raise ConfigurationError(
                "Telegram source polling requires a dedicated bot token per source account because Telegram update offsets are bot-wide."
            )


def _download_message_media(bot_token: str, message: dict[str, Any], sort_order: int) -> Any | None:
    media_payload: dict[str, Any] | None = None
    filename_hint = f"telegram-media-{sort_order}"

    if message.get("photo"):
        photo = message["photo"][-1]
        media_payload = photo
        filename_hint = f"{photo.get('file_unique_id') or photo.get('file_id')}.jpg"
    elif message.get("video"):
        media_payload = message["video"]
        filename_hint = media_payload.get("file_name") or f"{media_payload.get('file_unique_id') or media_payload.get('file_id')}.mp4"
    elif message.get("document"):
        media_payload = message["document"]
        filename_hint = media_payload.get("file_name") or f"{media_payload.get('file_unique_id') or media_payload.get('file_id')}"
    elif message.get("animation"):
        media_payload = message["animation"]
        filename_hint = media_payload.get("file_name") or f"{media_payload.get('file_unique_id') or media_payload.get('file_id')}.gif"
    elif message.get("audio"):
        media_payload = message["audio"]
        filename_hint = media_payload.get("file_name") or f"{media_payload.get('file_unique_id') or media_payload.get('file_id')}.mp3"
    elif message.get("voice"):
        media_payload = message["voice"]
        filename_hint = f"{media_payload.get('file_unique_id') or media_payload.get('file_id')}.ogg"

    if not media_payload or not media_payload.get("file_id"):
        return None

    file_info = _telegram_request(bot_token, "getFile", json_body={"file_id": media_payload["file_id"]})
    file_path = str(file_info.get("file_path") or "").strip()
    if not file_path:
        return None
    alt_text = str(message.get("caption") or "")
    return download_media(_telegram_file_url(bot_token, file_path), filename_hint, alt_text, sort_order)


def _build_grouped_messages(updates: list[dict[str, Any]], configured_chat_id: str) -> list[list[dict[str, Any]]]:
    groups: list[list[dict[str, Any]]] = []
    grouped: dict[str, list[dict[str, Any]]] = {}

    for update in sorted(updates, key=lambda item: int(item.get("update_id") or 0)):
        message = update.get("channel_post")
        if not isinstance(message, dict) or not _chat_matches(message.get("chat"), configured_chat_id):
            continue
        media_group_id = message.get("media_group_id")
        key = (
            f"group:{message.get('chat', {}).get('id')}:{media_group_id}"
            if media_group_id
            else f"message:{message.get('chat', {}).get('id')}:{message.get('message_id')}"
        )
        if key not in grouped:
            grouped[key] = []
            groups.append(grouped[key])
        grouped[key].append(message)

    for group in groups:
        group.sort(key=lambda message: int(message.get("message_id") or 0))
    return groups


def _build_preview_shape(post: CanonicalPost, account: Account) -> tuple[str, str, dict[str, Any], list[str]]:
    body = service_body(post, account)
    config = get_account_credentials(account)
    chat_id = _configured_chat_value(str(config.get("channel_id") or ""))
    attachments = sorted(post.attachments, key=lambda item: item.sort_order)
    notes: list[str] = ["Bot token and binary file contents are intentionally omitted from sandbox output."]

    if not attachments:
        return (
            "bot_send_message",
            "/sendMessage",
            {
                "chat_id": chat_id,
                "text": body,
            },
            notes,
        )

    if len(attachments) == 1:
        attachment = attachments[0]
        method = "/sendPhoto" if attachment.mime_type.startswith("image/") else "/sendDocument"
        request_shape = {
            "chat_id": chat_id,
            "caption": body,
            "file": {
                "filename": Path(attachment.storage_path).name,
                "mime_type": attachment.mime_type,
                "alt_text": attachment.alt_text,
            },
        }
        return ("bot_upload_media", method, request_shape, notes)

    media = []
    for index, attachment in enumerate(attachments):
        entry = {
            "type": "photo",
            "media": f"attach://file{index}",
            "filename": Path(attachment.storage_path).name,
            "mime_type": attachment.mime_type,
        }
        if index == 0 and body:
            entry["caption"] = body
        media.append(entry)
    notes.append("Telegram multi-attachment publishing uses one media group and currently supports image albums only.")
    return (
        "bot_send_media_group",
        "/sendMediaGroup",
        {
            "chat_id": chat_id,
            "media": media,
        },
        notes,
    )


class TelegramSourceAdapter(SourceAdapter):
    service = "telegram"

    def poll(
        self,
        session: Session,
        persona: Persona,
        account: Account,
        sync_state: AccountSyncState | None,
    ) -> PollResult:
        config = get_account_credentials(account)
        bot_token = str(config.get("bot_token") or "").strip()
        channel_id = _configured_chat_value(str(config.get("channel_id") or ""))
        if not bot_token or not channel_id:
            return PollResult(posts=[], next_state=(sync_state.state_json if sync_state else {}), cursor=(sync_state.cursor if sync_state else None))

        _ensure_dedicated_source_token(session, account, bot_token)
        webhook_info = _telegram_request(bot_token, "getWebhookInfo")
        if webhook_info and webhook_info.get("url"):
            raise ConfigurationError(
                "Telegram getUpdates polling is unavailable while an outgoing webhook is configured for this bot."
            )

        initial_sync = is_initial_sync(sync_state)
        allow_initial_backfill = import_existing_posts_on_first_scan(persona, account)
        since = cutoff_for_initial_poll(persona, account)
        if sync_state and sync_state.state_json.get("last_seen_at"):
            since = datetime.fromisoformat(str(sync_state.state_json["last_seen_at"]))

        raw_offset = sync_state.cursor if sync_state and sync_state.cursor else None
        offset = int(str(raw_offset)) if raw_offset not in (None, "") else None
        updates = _telegram_request(
            bot_token,
            "getUpdates",
            json_body={
                "offset": offset,
                "limit": 100,
                "allowed_updates": ["channel_post"],
            },
        )

        newest_update_id = max((int(update.get("update_id") or 0) for update in updates), default=None)
        next_cursor = str(newest_update_id + 1) if newest_update_id is not None else (str(offset) if offset is not None else None)
        groups = _build_grouped_messages(list(updates), channel_id)
        newest_seen = since
        for group in groups:
            newest_seen = max(newest_seen, _message_datetime(group[-1]))

        if initial_sync and not allow_initial_backfill:
            next_state = dict(sync_state.state_json if sync_state else {})
            next_state["last_seen_at"] = newest_seen.isoformat() if groups else now_utc().isoformat()
            return PollResult(
                posts=[],
                next_state=next_state,
                cursor=next_cursor,
                note="Initialized Telegram sync without importing historical posts.",
            )

        posts: list[CanonicalPostPayload] = []
        for group in groups:
            primary = group[0]
            created_at = _message_datetime(primary)
            if created_at <= since:
                continue

            attachments = []
            external_refs: list[ExternalPostRefPayload] = []
            body = ""
            reply_to = None
            for index, message in enumerate(group):
                message_id = str(message.get("message_id"))
                external_refs.append(
                    ExternalPostRefPayload(
                        external_id=message_id,
                        external_url=_message_url(message.get("chat"), message_id),
                        observed_at=_message_datetime(message),
                    )
                )
                if not body:
                    body = _message_body(message)
                if reply_to is None and isinstance(message.get("reply_to_message"), dict):
                    reply_to = PendingRelationship(external_id=str(message["reply_to_message"].get("message_id")))
                media_item = _download_message_media(bot_token, message, index)
                if media_item is not None:
                    attachments.append(media_item)

            posts.append(
                CanonicalPostPayload(
                    body=body,
                    media=attachments,
                    metadata={"link": external_refs[0].external_url} if external_refs and external_refs[0].external_url else {},
                    published_at=created_at,
                    external_refs=external_refs,
                    reply_to_external=reply_to,
                )
            )

        next_state = dict(sync_state.state_json if sync_state else {})
        next_state["last_seen_at"] = newest_seen.isoformat() if groups else since.isoformat()
        return PollResult(posts=posts, next_state=next_state, cursor=next_cursor)


class TelegramDestinationAdapter(DestinationAdapter):
    service = "telegram"

    def validate(self, post: CanonicalPost, persona: Persona, account: Account) -> list[ValidationIssue]:
        body = service_body(post, account)
        attachments = sorted(post.attachments, key=lambda item: item.sort_order)
        issues: list[ValidationIssue] = []

        if not body and not attachments:
            issues.append(ValidationIssue(service="telegram", field="body", message="Telegram posts need text or at least one attachment."))
        if not attachments and len(body) > 4096:
            issues.append(ValidationIssue(service="telegram", field="body", message="Telegram text messages are limited to 4096 characters."))
        if attachments and len(body) > 1024:
            issues.append(ValidationIssue(service="telegram", field="body", message="Telegram media captions are limited to 1024 characters."))
        if len(attachments) > 10:
            issues.append(ValidationIssue(service="telegram", field="media", message="Telegram media groups support up to 10 attachments."))
        if len(attachments) > 1 and not all(attachment.mime_type.startswith("image/") for attachment in attachments):
            issues.append(
                ValidationIssue(
                    service="telegram",
                    field="media",
                    message="Telegram multi-attachment publishing currently supports image albums only.",
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
        action, endpoint_label, request_shape, notes = _build_preview_shape(post, account)
        return PublishPreview(
            service="telegram",
            action=action,
            rendered_body=service_body(post, account),
            endpoint_label=endpoint_label,
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
        bot_token = str(config.get("bot_token") or "").strip()
        chat_id = _configured_chat_value(str(config.get("channel_id") or ""))
        if not bot_token or not chat_id:
            raise ConfigurationError("Telegram bot token and channel ID are required.")

        attachments = sorted(post.attachments, key=lambda item: item.sort_order)
        body = service_body(post, account)

        if not attachments:
            message = _telegram_request(
                bot_token,
                "sendMessage",
                json_body={"chat_id": chat_id, "text": body},
            )
            external_id = str(message["message_id"])
            return PublishResult(
                service="telegram",
                external_id=external_id,
                external_url=_message_url(message.get("chat"), external_id),
                raw=message if isinstance(message, dict) else {"result": message},
            )

        if len(attachments) == 1:
            attachment = attachments[0]
            method = "sendPhoto" if attachment.mime_type.startswith("image/") else "sendDocument"
            data = {"chat_id": chat_id}
            if body:
                data["caption"] = body
            handles = []
            try:
                handle = Path(attachment.storage_path).open("rb")
                handles.append(handle)
                message = _telegram_request(
                    bot_token,
                    method,
                    data=data,
                    files={("photo" if method == "sendPhoto" else "document"): (Path(attachment.storage_path).name, handle, attachment.mime_type)},
                )
            finally:
                for handle in handles:
                    handle.close()
            external_id = str(message["message_id"])
            return PublishResult(
                service="telegram",
                external_id=external_id,
                external_url=_message_url(message.get("chat"), external_id),
                raw=message if isinstance(message, dict) else {"result": message},
            )

        handles = []
        files: dict[str, Any] = {}
        try:
            media = []
            for index, attachment in enumerate(attachments):
                handle = Path(attachment.storage_path).open("rb")
                handles.append(handle)
                field_name = f"file{index}"
                files[field_name] = (Path(attachment.storage_path).name, handle, attachment.mime_type)
                item: dict[str, Any] = {"type": "photo", "media": f"attach://{field_name}"}
                if index == 0 and body:
                    item["caption"] = body
                media.append(item)
            result = _telegram_request(
                bot_token,
                "sendMediaGroup",
                data={"chat_id": chat_id, "media": json.dumps(media)},
                files=files,
            )
        finally:
            for handle in handles:
                handle.close()

        messages = list(result or [])
        if not messages:
            raise RuntimeError("Telegram sendMediaGroup returned no messages.")

        external_refs = [
            ExternalPostRefPayload(
                external_id=str(message["message_id"]),
                external_url=_message_url(message.get("chat"), str(message["message_id"])),
                observed_at=_message_datetime(message),
            )
            for message in messages
        ]
        return PublishResult(
            service="telegram",
            external_id=external_refs[0].external_id,
            external_url=external_refs[0].external_url,
            raw={"messages": messages},
            external_refs=external_refs,
        )
