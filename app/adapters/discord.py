from __future__ import annotations

import json
from pathlib import Path

import requests
from sqlalchemy.orm import Session

from app.adapters.base import ConfigurationError, DestinationAdapter, get_account_credentials, get_account_publish_setting
from app.adapters.common import service_body
from app.domain import PublishPreview, PublishResult, ValidationIssue
from app.models import Account, CanonicalPost, DeliveryJob, Persona


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
                for attachment in sorted(post.attachments, key=lambda item: item.sort_order)
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
        handles = []
        try:
            for index, attachment in enumerate(sorted(post.attachments, key=lambda item: item.sort_order)):
                handle = Path(attachment.storage_path).open("rb")
                handles.append(handle)
                files[f"files[{index}]"] = (Path(attachment.storage_path).name, handle, attachment.mime_type)
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
        finally:
            for handle in handles:
                handle.close()

        raw = response.json() if response.content else {}
        message_id = raw.get("id") if isinstance(raw, dict) else None
        return PublishResult(service="discord", external_id=message_id or "posted", external_url=None, raw=raw if isinstance(raw, dict) else {})
