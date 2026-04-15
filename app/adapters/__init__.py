from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.adapters.base import DestinationAdapter, SourceAdapter
from app.adapters.bluesky import BlueskyDestinationAdapter, BlueskySourceAdapter
from app.adapters.discord import DiscordDestinationAdapter
from app.adapters.instagram import InstagramDestinationAdapter, InstagramSourceAdapter
from app.adapters.mastodon import MastodonDestinationAdapter, MastodonSourceAdapter
from app.adapters.telegram import TelegramDestinationAdapter, TelegramSourceAdapter
from app.adapters.tumblr import TumblrDestinationAdapter
from app.adapters.twitter import TwitterDestinationAdapter
from app.models import Account


@dataclass(frozen=True)
class AccountFieldDefinition:
    name: str
    label: str
    input_type: str = "text"
    help_text: str = ""
    fallback_keys: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ComposerConstraintDefinition:
    body_limit: int | None = None
    body_limit_with_media: int | None = None
    max_attachments: int | None = None
    max_images: int | None = None
    max_videos: int | None = None
    requires_media: bool = False
    requires_body_or_media: bool = False
    multi_attachment_images_only: bool = False
    single_video_only: bool = False
    allowed_image_mime_types: tuple[str, ...] = field(default_factory=tuple)
    supported_attachment_kinds: tuple[str, ...] = ("image", "video", "other")
    notes: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ServiceDefinition:
    service: str
    label: str
    source_adapter_cls: type[SourceAdapter] | None = None
    destination_adapter_cls: type[DestinationAdapter] | None = None
    credential_fields: tuple[AccountFieldDefinition, ...] = field(default_factory=tuple)
    source_setting_fields: tuple[AccountFieldDefinition, ...] = field(default_factory=tuple)
    publish_setting_fields: tuple[AccountFieldDefinition, ...] = field(default_factory=tuple)
    composer_constraints: ComposerConstraintDefinition | None = None

    @property
    def source_supported(self) -> bool:
        return self.source_adapter_cls is not None

    @property
    def destination_supported(self) -> bool:
        return self.destination_adapter_cls is not None


SERVICE_REGISTRY: dict[str, ServiceDefinition] = {
    "bluesky": ServiceDefinition(
        service="bluesky",
        label="Bluesky",
        source_adapter_cls=BlueskySourceAdapter,
        destination_adapter_cls=BlueskyDestinationAdapter,
        composer_constraints=ComposerConstraintDefinition(
            body_limit=300,
            max_attachments=4,
            max_images=4,
            max_videos=1,
            single_video_only=True,
            supported_attachment_kinds=("image", "video"),
            notes=("Current adapter supports either up to 4 images or one MP4 video.",),
        ),
        credential_fields=(
            AccountFieldDefinition("handle", "Handle"),
            AccountFieldDefinition("password", "App Password", input_type="password"),
            AccountFieldDefinition("session_string", "Session String"),
        ),
        source_setting_fields=(AccountFieldDefinition("post_time_limit", "Initial Poll Lookback (hours)", input_type="number"),),
    ),
    "instagram": ServiceDefinition(
        service="instagram",
        label="Instagram",
        source_adapter_cls=InstagramSourceAdapter,
        destination_adapter_cls=InstagramDestinationAdapter,
        composer_constraints=ComposerConstraintDefinition(
            max_attachments=10,
            requires_media=True,
            allowed_image_mime_types=("image/jpeg", "image/jpg", "image/pjpeg", "image/png", "image/webp"),
            supported_attachment_kinds=("image", "video"),
            notes=(
                "Instagram destination publishing uses instagrapi direct uploads, so Public Base URL is not required.",
                "Supported inputs are JPEG, PNG, or WEBP images plus MP4 videos for feed posts and carousels.",
            ),
        ),
        credential_fields=(
            AccountFieldDefinition(
                "api_key",
                "Graph Access Token",
                input_type="password",
                help_text="Used for inbound polling and token-lifecycle tracking. It is not used by instagrapi publishing.",
            ),
            AccountFieldDefinition(
                "instagrapi_username",
                "Login Username",
                help_text="Used for Instagram publishing when Session ID is blank.",
            ),
            AccountFieldDefinition(
                "instagrapi_password",
                "Login Password",
                input_type="password",
                help_text="Used together with Login Username for Instagram publishing.",
            ),
            AccountFieldDefinition(
                "instagrapi_sessionid",
                "Session ID",
                input_type="password",
                help_text=(
                    "Preferred for publishing when you want to avoid storing the account password or when challenge flows make password login unreliable. "
                    "If provided, LynxPoster uses this instead of Login Username and Login Password."
                ),
            ),
        ),
        source_setting_fields=(AccountFieldDefinition("post_time_limit", "Initial Poll Lookback (hours)", input_type="number"),),
    ),
    "mastodon": ServiceDefinition(
        service="mastodon",
        label="Mastodon",
        source_adapter_cls=MastodonSourceAdapter,
        destination_adapter_cls=MastodonDestinationAdapter,
        composer_constraints=ComposerConstraintDefinition(
            body_limit=500,
            notes=("Server-specific media limits can vary; LynxPoster only enforces the body cap here.",),
        ),
        credential_fields=(
            AccountFieldDefinition("handle", "Handle"),
            AccountFieldDefinition("instance", "Instance URL"),
            AccountFieldDefinition("token", "Access Token", input_type="password"),
        ),
        source_setting_fields=(AccountFieldDefinition("post_time_limit", "Initial Poll Lookback (hours)", input_type="number"),),
        publish_setting_fields=(
            AccountFieldDefinition("visibility", "Default Visibility"),
            AccountFieldDefinition(
                "language",
                "Default Language",
                help_text="Optional BCP 47 language tag, such as en or en-US.",
                fallback_keys=("mastodon_lang",),
            ),
        ),
    ),
    "twitter": ServiceDefinition(
        service="twitter",
        label="Twitter/X",
        destination_adapter_cls=TwitterDestinationAdapter,
        composer_constraints=ComposerConstraintDefinition(
            body_limit=280,
            max_attachments=4,
            supported_attachment_kinds=("image", "video"),
            notes=("Current adapter enforces tweet length and total attachment count.",),
        ),
        credential_fields=(
            AccountFieldDefinition("username", "Username"),
            AccountFieldDefinition("app_key", "App Key"),
            AccountFieldDefinition("app_secret", "App Secret", input_type="password"),
            AccountFieldDefinition("access_token", "Access Token", input_type="password"),
            AccountFieldDefinition("access_token_secret", "Access Token Secret", input_type="password"),
        ),
        publish_setting_fields=(
            AccountFieldDefinition(
                "language",
                "Tweet Language",
                help_text="Stored per account for compatibility with legacy settings. The current X posting client does not send a language field.",
                fallback_keys=("twitter_lang",),
            ),
        ),
    ),
    "discord": ServiceDefinition(
        service="discord",
        label="Discord",
        destination_adapter_cls=DiscordDestinationAdapter,
        composer_constraints=ComposerConstraintDefinition(
            body_limit=2000,
            notes=("Discord webhook file limits depend on the destination server and webhook configuration.",),
        ),
        credential_fields=(AccountFieldDefinition("webhook_url", "Webhook URL", input_type="password"),),
    ),
    "telegram": ServiceDefinition(
        service="telegram",
        label="Telegram",
        source_adapter_cls=TelegramSourceAdapter,
        destination_adapter_cls=TelegramDestinationAdapter,
        composer_constraints=ComposerConstraintDefinition(
            body_limit=4096,
            body_limit_with_media=1024,
            max_attachments=10,
            requires_body_or_media=True,
            multi_attachment_images_only=True,
            notes=("Multiple Telegram attachments are sent as a single image album.",),
        ),
        credential_fields=(
            AccountFieldDefinition(
                "bot_token",
                "Bot Token",
                input_type="password",
                help_text="Create this with @BotFather and add the bot as an admin in the destination channel.",
            ),
            AccountFieldDefinition(
                "channel_id",
                "Channel ID",
                help_text=(
                    "Use the numeric Telegram channel ID, usually starting with -100. "
                    "If you also enable source polling, keep a dedicated bot token per source account because Telegram offsets are bot-wide."
                ),
            ),
        ),
        source_setting_fields=(AccountFieldDefinition("post_time_limit", "Initial Poll Lookback (hours)", input_type="number"),),
    ),
    "tumblr": ServiceDefinition(
        service="tumblr",
        label="Tumblr",
        destination_adapter_cls=TumblrDestinationAdapter,
        composer_constraints=ComposerConstraintDefinition(
            max_videos=1,
            single_video_only=True,
            supported_attachment_kinds=("image", "video"),
            notes=(
                "Current adapter supports text posts, photo sets, or one video.",
                "Mixed image and video attachments are not supported.",
            ),
        ),
        credential_fields=(
            AccountFieldDefinition("blog_name", "Blog Name"),
            AccountFieldDefinition("consumer_key", "Consumer Key"),
            AccountFieldDefinition("consumer_secret", "Consumer Secret", input_type="password"),
            AccountFieldDefinition("oauth_token", "OAuth Token", input_type="password"),
            AccountFieldDefinition("oauth_secret", "OAuth Secret", input_type="password"),
        ),
    ),
}


def iter_service_definitions() -> list[ServiceDefinition]:
    return [SERVICE_REGISTRY[key] for key in sorted(SERVICE_REGISTRY)]


def get_service_definition(service: str) -> ServiceDefinition:
    return SERVICE_REGISTRY[service]


def service_composer_constraints_context() -> dict[str, dict[str, Any]]:
    context: dict[str, dict[str, Any]] = {}
    for definition in iter_service_definitions():
        if definition.composer_constraints is None:
            continue
        constraints = definition.composer_constraints
        context[definition.service] = {
            "service": definition.service,
            "label": definition.label,
            "body_limit": constraints.body_limit,
            "body_limit_with_media": constraints.body_limit_with_media,
            "max_attachments": constraints.max_attachments,
            "max_images": constraints.max_images,
            "max_videos": constraints.max_videos,
            "requires_media": constraints.requires_media,
            "requires_body_or_media": constraints.requires_body_or_media,
            "multi_attachment_images_only": constraints.multi_attachment_images_only,
            "single_video_only": constraints.single_video_only,
            "allowed_image_mime_types": list(constraints.allowed_image_mime_types),
            "supported_attachment_kinds": list(constraints.supported_attachment_kinds),
            "notes": list(constraints.notes),
        }
    return context


def supports_source(service: str) -> bool:
    return get_service_definition(service).source_supported


def supports_destination(service: str) -> bool:
    return get_service_definition(service).destination_supported


def get_source_adapter_for_account(account: Account) -> SourceAdapter:
    definition = get_service_definition(account.service)
    if not definition.source_adapter_cls:
        raise KeyError(f"{account.service} does not support polling.")
    return definition.source_adapter_cls()


def get_destination_adapter_for_account(account: Account) -> DestinationAdapter:
    definition = get_service_definition(account.service)
    if not definition.destination_adapter_cls:
        raise KeyError(f"{account.service} does not support publishing.")
    return definition.destination_adapter_cls()


def source_configured(service: str, credentials: dict[str, Any]) -> bool:
    if service == "bluesky":
        return bool(credentials.get("session_string") or (credentials.get("handle") and credentials.get("password")))
    if service == "instagram":
        return bool(credentials.get("api_key"))
    if service == "mastodon":
        return bool(credentials.get("instance") and credentials.get("token"))
    if service == "telegram":
        return bool(credentials.get("bot_token") and credentials.get("channel_id"))
    return False


def destination_configured(service: str, credentials: dict[str, Any]) -> bool:
    if service == "bluesky":
        return bool(credentials.get("session_string") or (credentials.get("handle") and credentials.get("password")))
    if service == "instagram":
        return bool(credentials.get("instagrapi_sessionid") or (credentials.get("instagrapi_username") and credentials.get("instagrapi_password")))
    if service == "mastodon":
        return bool(credentials.get("instance") and credentials.get("token"))
    if service == "twitter":
        required = ("app_key", "app_secret", "access_token", "access_token_secret")
        return all(credentials.get(key) for key in required)
    if service == "discord":
        return bool(credentials.get("webhook_url"))
    if service == "telegram":
        return bool(credentials.get("bot_token") and credentials.get("channel_id"))
    if service == "tumblr":
        required = ("consumer_key", "consumer_secret", "oauth_token", "oauth_secret", "blog_name")
        return all(credentials.get(key) for key in required)
    return False


def account_is_configured(account: Account) -> bool:
    credentials = dict(account.credentials_json or {})
    source_ok = not account.source_enabled or (supports_source(account.service) and source_configured(account.service, credentials))
    destination_ok = not account.destination_enabled or (
        supports_destination(account.service) and destination_configured(account.service, credentials)
    )
    return source_ok and destination_ok and (account.source_enabled or account.destination_enabled)
