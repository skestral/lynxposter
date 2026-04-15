from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from app.adapters.base import ConfigurationError, DestinationAdapter, get_account_credentials
from app.adapters.common import service_body
from app.domain import PublishPreview, PublishResult, ValidationIssue
from app.models import Account, CanonicalPost, Persona


class TwitterDestinationAdapter(DestinationAdapter):
    service = "twitter"

    def validate(self, post: CanonicalPost, persona: Persona, account: Account) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        body = service_body(post, account)
        if len(body) > 280:
            issues.append(ValidationIssue(service="twitter", field="body", message="Twitter posts are limited to 280 characters in v1."))
        if len(post.attachments) > 4:
            issues.append(ValidationIssue(service="twitter", field="media", message="Twitter supports up to 4 attachments per post."))
        return issues

    def preview(
        self,
        post: CanonicalPost,
        persona: Persona,
        account: Account,
        *,
        context: dict[str, str | None] | None = None,
    ) -> PublishPreview:
        request_shape = {
            "text": service_body(post, account),
            "media_ids": [f"<uploaded-media-{index + 1}>" for index, _ in enumerate(sorted(post.attachments, key=lambda item: item.sort_order))],
            "in_reply_to_tweet_id": (context or {}).get("reply_external_id"),
            "quote_tweet_id": (context or {}).get("quote_external_id"),
        }
        notes = []
        if post.attachments:
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
        if post.attachments:
            media_ids = []
            for attachment in sorted(post.attachments, key=lambda item: item.sort_order):
                media = api.media_upload(filename=str(Path(attachment.storage_path)))
                if attachment.alt_text:
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
