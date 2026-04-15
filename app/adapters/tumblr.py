from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from app.adapters.base import ConfigurationError, DestinationAdapter, get_account_credentials
from app.adapters.common import service_body
from app.domain import PublishPreview, PublishResult, ValidationIssue
from app.models import Account, CanonicalPost, Persona


class TumblrDestinationAdapter(DestinationAdapter):
    service = "tumblr"

    def validate(self, post: CanonicalPost, persona: Persona, account: Account) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        media_paths = [str(Path(attachment.storage_path)).lower() for attachment in sorted(post.attachments, key=lambda item: item.sort_order)]
        image_count = sum(not path.endswith(".mp4") for path in media_paths)
        video_count = len(media_paths) - image_count

        if video_count > 1:
            issues.append(
                ValidationIssue(
                    service="tumblr",
                    field="media",
                    message="Tumblr publishing currently supports only one video attachment per post.",
                )
            )
        if video_count and image_count:
            issues.append(
                ValidationIssue(
                    service="tumblr",
                    field="media",
                    message="Tumblr publishing currently supports either a photo set or one video, not mixed image and video attachments.",
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
        media_paths = [str(Path(attachment.storage_path)) for attachment in sorted(post.attachments, key=lambda item: item.sort_order)]
        body = service_body(post, account)
        if media_paths:
            image_paths = [path for path in media_paths if not path.lower().endswith(".mp4")]
            video_path = next((path for path in media_paths if path.lower().endswith(".mp4")), None)
            if video_path:
                action = "create_video"
                request_shape = {
                    "state": "published",
                    "caption": body,
                    "data": Path(video_path).name,
                }
            else:
                action = "create_photo"
                request_shape = {
                    "state": "published",
                    "caption": body,
                    "data": [Path(path).name for path in image_paths],
                }
        else:
            action = "create_text"
            request_shape = {
                "state": "published",
                "title": "",
                "body": body,
            }
        return PublishPreview(
            service="tumblr",
            action=action,
            rendered_body=body,
            endpoint_label="TumblrRestClient publish call",
            request_shape=request_shape,
            notes=["Binary uploads are skipped in sandbox mode."],
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
        import pytumblr

        config = get_account_credentials(account)
        required = ("consumer_key", "consumer_secret", "oauth_token", "oauth_secret", "blog_name")
        if not all(config.get(key) for key in required):
            raise ConfigurationError("Tumblr destination credentials are incomplete.")
        client = pytumblr.TumblrRestClient(
            config["consumer_key"],
            config["consumer_secret"],
            config["oauth_token"],
            config["oauth_secret"],
        )
        media_paths = [str(Path(attachment.storage_path)) for attachment in sorted(post.attachments, key=lambda item: item.sort_order)]
        body = service_body(post, account)
        if media_paths:
            image_paths = [path for path in media_paths if not path.lower().endswith(".mp4")]
            video_path = next((path for path in media_paths if path.lower().endswith(".mp4")), None)
            if video_path:
                response = client.create_video(config["blog_name"], state="published", caption=body, data=video_path)
            else:
                response = client.create_photo(config["blog_name"], state="published", caption=body, data=image_paths)
        else:
            response = client.create_text(config["blog_name"], state="published", title="", body=body)
        external_id = str(response.get("id"))
        return PublishResult(service="tumblr", external_id=external_id, external_url=None, raw=dict(response))
