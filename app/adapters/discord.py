from __future__ import annotations

from pathlib import Path

import requests
from sqlalchemy.orm import Session

from app.adapters.base import ConfigurationError, DestinationAdapter, get_account_credentials
from app.adapters.common import service_body
from app.domain import PublishPreview, PublishResult, ValidationIssue
from app.models import Account, CanonicalPost, Persona


class DiscordDestinationAdapter(DestinationAdapter):
    service = "discord"

    def validate(self, post: CanonicalPost, persona: Persona, account: Account) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        if len(service_body(post, account)) > 2000:
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
        content = service_body(post, account)
        source_link = post.metadata_json.get("link")
        if source_link:
            content = f"{content}\nSource: {source_link}".strip()
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

        payload = {"content": service_body(post, account)}
        source_link = post.metadata_json.get("link")
        if source_link:
            payload["content"] = f"{payload['content']}\nSource: {source_link}".strip()

        files = {}
        handles = []
        try:
            for index, attachment in enumerate(sorted(post.attachments, key=lambda item: item.sort_order)):
                handle = Path(attachment.storage_path).open("rb")
                handles.append(handle)
                files[f"files[{index}]"] = (Path(attachment.storage_path).name, handle, attachment.mime_type)
            response = requests.post(webhook_url, data=payload, files=files or None, timeout=30)
            response.raise_for_status()
        finally:
            for handle in handles:
                handle.close()

        raw = response.json() if response.content else {}
        message_id = raw.get("id") if isinstance(raw, dict) else None
        return PublishResult(service="discord", external_id=message_id or "posted", external_url=None, raw=raw if isinstance(raw, dict) else {})
