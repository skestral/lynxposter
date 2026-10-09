from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import requests
from sqlalchemy.orm import Session

from app.adapters.base import ConfigurationError, DestinationAdapter, get_account_credentials, get_account_publish_setting
from app.adapters.common import is_image_attachment, is_video_attachment, service_attachments, service_body
from app.domain import PublishPreview, PublishResult, ValidationIssue
from app.models import Account, CanonicalPost, DeliveryJob, Persona
from app.services.media_preparation import ImageLimits, prepare_image
from app.services.video_preparation import VideoLimits, prepare_video


def _discord_upload_limit(persona: Persona, account: Account) -> int:
    raw = get_account_publish_setting(persona, account, "max_upload_bytes", 20 * 1024 * 1024)
    try:
        limit = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError("Discord max_upload_bytes must be a positive byte count.") from exc
    if limit <= 0:
        raise ConfigurationError("Discord max_upload_bytes must be a positive byte count.")
    return limit


def discord_link_preference_order(persona: Persona, account: Account) -> list[str]:
    raw_value = get_account_publish_setting(persona, account, "link_preference_order", "source")
    tokens: list[str] = []
    for raw_token in str(raw_value or "").split(","):
        token = raw_token.strip().lower()
        if token and token not in tokens:
            tokens.append(token)
    if not tokens:
        tokens = ["source"]
    if "source" not in tokens:
        tokens.append("source")
    return tokens


def _posted_delivery_urls(post: CanonicalPost) -> dict[str, str]:
    urls: dict[str, str] = {}
    for job in getattr(post, "delivery_jobs", []) or []:
        if not isinstance(job, DeliveryJob):
            continue
        if job.status != "posted" or not job.external_url or not job.target_account:
            continue
        urls.setdefault(job.target_account.service, job.external_url)
    return urls


def discord_selected_link(post: CanonicalPost, persona: Persona, account: Account) -> str | None:
    source_link = str((post.metadata_json or {}).get("link") or "").strip() or None
    posted_urls = _posted_delivery_urls(post)
    for token in discord_link_preference_order(persona, account):
        if token == "source":
            if source_link:
                return source_link
            continue
        preferred_url = posted_urls.get(token)
        if preferred_url:
            return preferred_url
    if source_link:
        return source_link
    return next(iter(posted_urls.values()), None)


def discord_should_wait_for_preferred_links(post: CanonicalPost, persona: Persona, account: Account) -> bool:
    active_statuses = {"draft", "scheduled", "queued"}
    for service in [token for token in discord_link_preference_order(persona, account) if token != "source"]:
        matching_jobs = [
            job
            for job in getattr(post, "delivery_jobs", []) or []
            if isinstance(job, DeliveryJob)
            and job.target_account is not None
            and job.target_account.id != account.id
            and job.target_account.service == service
        ]
        if not matching_jobs:
            continue
        if any(job.status == "posted" and job.external_url for job in matching_jobs):
            return False
        if any(job.status in active_statuses for job in matching_jobs):
            return True
    return False


def render_discord_content(post: CanonicalPost, persona: Persona, account: Account) -> str:
    content = service_body(post, account)
    selected_link = discord_selected_link(post, persona, account)
    if selected_link:
        content = f"{content}\nSource: {selected_link}".strip()
    return content


def _discord_uploadable_attachments(post: CanonicalPost, account: Account) -> list[Any]:
    uploadable_prefixes = ("image/", "video/", "audio/")
    return [
        attachment
        for attachment in service_attachments(post, account)
        if str(attachment.mime_type or "").lower().startswith(uploadable_prefixes)
    ]


class DiscordDestinationAdapter(DestinationAdapter):
    service = "discord"

    def validate(self, post: CanonicalPost, persona: Persona, account: Account) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        if len(render_discord_content(post, persona, account)) > 2000:
            issues.append(ValidationIssue(service="discord", field="body", message="Discord webhook content is limited to 2000 characters."))
        return issues

    def preview(
        self,
        post: CanonicalPost,
        persona: Persona,
        account: Account,
        *,
        context: dict[str, str | None] | None = None,
    ) -> PublishPreview:
        content = render_discord_content(post, persona, account)
        request_shape = {
            "content": content,
            "files": [
                {
                    "filename": Path(attachment.storage_path).name,
                    "mime_type": attachment.mime_type,
                    "alt_text": attachment.alt_text,
                }
                for attachment in _discord_uploadable_attachments(post, account)
            ],
        }
        return PublishPreview(
            service="discord",
            action="webhook_post",
            rendered_body=content,
            endpoint_label="Configured Discord webhook",
            request_shape=request_shape,
            notes=["Webhook URL and binary uploads are intentionally omitted from sandbox output."],
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
        webhook_url = config.get("webhook_url")
        if not webhook_url:
            raise ConfigurationError("Discord webhook URL is missing.")

        payload = {"content": render_discord_content(post, persona, account)}

        files = {}
        with ExitStack() as stack:
            upload_limit = _discord_upload_limit(persona, account)
            for index, attachment in enumerate(_discord_uploadable_attachments(post, account)):
                source_path = Path(attachment.storage_path)
                upload_path = source_path
                mime_type = attachment.mime_type
                if is_video_attachment(attachment):
                    prepared = stack.enter_context(prepare_video(source_path, limits=VideoLimits(max_bytes=upload_limit)))
                    upload_path, mime_type = prepared.path, prepared.mime_type
                elif is_image_attachment(attachment):
                    prepared = stack.enter_context(
                        prepare_image(source_path, limits=ImageLimits(max_bytes=upload_limit, supported_mime_types=("image/jpeg", "image/png", "image/webp", "image/gif", "image/avif")))
                    )
                    upload_path, mime_type = prepared.path, prepared.mime_type
                elif source_path.stat().st_size > upload_limit:
                    raise ConfigurationError(f"Discord audio exceeds the configured {upload_limit:,}-byte file upload limit.")
                suffix = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/gif": ".gif", "image/avif": ".avif", "video/mp4": ".mp4"}.get(mime_type, source_path.suffix)
                handle = stack.enter_context(upload_path.open("rb"))
                files[f"files[{index}]"] = (f"{source_path.stem}{suffix}", handle, mime_type)
            request_kwargs = {
                "params": {"wait": "true"},
                "timeout": 30,
            }
            if files:
                request_kwargs["data"] = {"payload_json": json.dumps(payload)}
                request_kwargs["files"] = files
            else:
                request_kwargs["json"] = payload
            response = requests.post(webhook_url, **request_kwargs)
            response.raise_for_status()

        raw = response.json() if response.content else {}
        message_id = raw.get("id") if isinstance(raw, dict) else None
        return PublishResult(service="discord", external_id=message_id or "posted", external_url=None, raw=raw if isinstance(raw, dict) else {})
