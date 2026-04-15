from __future__ import annotations

import sys
import shutil
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import requests
from sqlalchemy.orm import Session

from app.adapters.base import ConfigurationError, DestinationAdapter, SourceAdapter, get_account_credentials
from app.adapters.common import cutoff_for_initial_poll, import_existing_posts_on_first_scan, is_initial_sync, now_utc, service_body
from app.domain import CanonicalPostPayload, ExternalPostRefPayload, PollResult, PublishPreview, PublishResult, ValidationIssue
from app.models import Account, AccountSyncState, CanonicalPost, Persona
from app.services.instagram_private_api import apply_instagram_private_settings, get_instagram_private_settings
from app.services.storage import download_media

INSTAGRAM_API_VERSION = "v25.0"
INSTAGRAM_GRAPH_API_BASE_URL = f"https://graph.instagram.com/{INSTAGRAM_API_VERSION}"
INSTAGRAM_SUPPORTED_IMAGE_MIME_TYPES = {"image/jpeg", "image/jpg", "image/pjpeg", "image/png", "image/webp"}
INSTAGRAM_SUPPORTED_VIDEO_MIME_TYPES = {"video/mp4"}


def _load_instagram_dependencies() -> tuple[Any | None, Any | None, type[Exception]]:
    instagrapi_client = None
    image_module = None
    unidentified_error: type[Exception] = Exception

    try:
        from instagrapi import Client as loaded_client

        instagrapi_client = loaded_client
    except ModuleNotFoundError:
        pass

    try:
        from PIL import Image as loaded_image
        from PIL import UnidentifiedImageError as loaded_unidentified_error

        image_module = loaded_image
        unidentified_error = loaded_unidentified_error
    except ModuleNotFoundError:
        pass

    return instagrapi_client, image_module, unidentified_error


def _instagram_destination_dependency_issue() -> str | None:
    instagrapi_client, image_module, _ = _load_instagram_dependencies()
    missing: list[str] = []
    if instagrapi_client is None:
        missing.append("instagrapi")
    if image_module is None:
        missing.append("Pillow")
    if not missing:
        return None
    joined = ", ".join(missing)
    return (
        "Instagram publishing requires optional dependencies that are not installed for "
        f"this Python interpreter ({sys.executable}): {joined}. "
        "Run `pip install -r requirements.txt` with that same interpreter."
    )


def _configured_graph_access_token(config: dict[str, Any]) -> str:
    return str(config.get("api_key") or "").strip()


def _configured_instagrapi_username(config: dict[str, Any]) -> str:
    return str(config.get("instagrapi_username") or "").strip()


def _configured_instagrapi_password(config: dict[str, Any]) -> str:
    return str(config.get("instagrapi_password") or "").strip()


def _configured_instagrapi_sessionid(config: dict[str, Any]) -> str:
    return str(config.get("instagrapi_sessionid") or "").strip()


def _instagrapi_destination_issue(config: dict[str, Any]) -> str | None:
    sessionid = _configured_instagrapi_sessionid(config)
    username = _configured_instagrapi_username(config)
    password = _configured_instagrapi_password(config)
    if sessionid or (username and password):
        return None
    return "Instagram publishing requires Session ID or both Login Username and Login Password."


def _flatten_image_to_jpeg(source_path: Path, target_path: Path) -> None:
    dependency_issue = _instagram_destination_dependency_issue()
    if dependency_issue:
        raise ConfigurationError(dependency_issue)
    _, image_module, unidentified_error = _load_instagram_dependencies()
    try:
        with image_module.open(source_path) as image:
            if image.mode in {"RGBA", "LA"} or (image.mode == "P" and "transparency" in image.info):
                rgba = image.convert("RGBA")
                background = image_module.new("RGBA", rgba.size, (255, 255, 255, 255))
                flattened = image_module.alpha_composite(background, rgba).convert("RGB")
            else:
                flattened = image.convert("RGB")
            flattened.save(target_path, format="JPEG", quality=95)
    except (FileNotFoundError, unidentified_error) as exc:
        raise ConfigurationError(f"Instagram could not open image attachment {source_path}.") from exc


def _prepared_upload_paths(attachments: list[Any], *, temp_dir: str) -> list[Path]:
    prepared: list[Path] = []
    temp_root = Path(temp_dir)
    for index, attachment in enumerate(attachments):
        source_path = Path(str(attachment.storage_path))
        if not source_path.exists():
            raise ConfigurationError(f"Instagram attachment is missing on disk: {source_path}")

        mime_type = str(attachment.mime_type or "").lower()
        if mime_type in INSTAGRAM_SUPPORTED_IMAGE_MIME_TYPES:
            target_path = temp_root / f"{index:02d}-{source_path.stem}.jpg"
            _flatten_image_to_jpeg(source_path, target_path)
            prepared.append(target_path)
            continue
        if mime_type in INSTAGRAM_SUPPORTED_VIDEO_MIME_TYPES:
            target_path = temp_root / f"{index:02d}-{source_path.stem}.mp4"
            shutil.copyfile(source_path, target_path)
            prepared.append(target_path)
            continue
        raise ConfigurationError(f"{source_path} is not a supported Instagram image or MP4 video attachment.")
    return prepared


def _authenticated_publish_client(config: dict[str, Any]) -> Any:
    dependency_issue = _instagram_destination_dependency_issue()
    if dependency_issue:
        raise ConfigurationError(dependency_issue)
    instagrapi_client, _, _ = _load_instagram_dependencies()
    sessionid = _configured_instagrapi_sessionid(config)
    username = _configured_instagrapi_username(config)
    password = _configured_instagrapi_password(config)
    saved_settings = get_instagram_private_settings(config)

    if not sessionid and not (username and password):
        raise ConfigurationError("Instagram publishing requires Session ID or both Login Username and Login Password.")

    client = instagrapi_client()
    if saved_settings:
        client.set_settings(saved_settings)

    if sessionid:
        try:
            client.login_by_sessionid(sessionid)
            return client
        except Exception as exc:
            if not (username and password):
                raise RuntimeError(
                    "Instagram Session ID login failed. Refresh the Session ID or add Login Username and Login Password."
                ) from exc

    if saved_settings:
        try:
            client.account_info()
            return client
        except Exception:
            pass

    try:
        client.login(username, password, relogin=bool(saved_settings))
        return client
    except Exception as exc:
        raise RuntimeError(
            "Instagram login failed. If Instagram is asking for a challenge or MFA, refresh the Session ID and try again."
        ) from exc


def _persist_publish_client_state(account: Account, client: Any) -> None:
    credentials = dict(account.credentials_json or {})
    if not str(credentials.get("instagrapi_username") or "").strip():
        username = str(getattr(client, "username", "") or "").strip()
        if username:
            credentials["instagrapi_username"] = username
    credentials = apply_instagram_private_settings(
        credentials,
        previous_credentials=account.credentials_json,
        settings=client.get_settings(),
    )
    account.credentials_json = credentials


def validate_instagram_account_login(
    credentials: dict[str, Any] | None,
    *,
    previous_credentials: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], str, str | None]:
    config = dict(credentials or {})
    client = _authenticated_publish_client(config)
    settings = client.get_settings()

    resolved_sessionid = str(getattr(client, "sessionid", "") or "").strip()
    if not resolved_sessionid:
        resolved_sessionid = str((settings.get("cookies") or {}).get("sessionid") or "").strip()
    if not resolved_sessionid:
        raise RuntimeError("Instagram login succeeded, but no Session ID was returned.")

    resolved_username = str(getattr(client, "username", "") or "").strip() or None
    if resolved_username:
        config["instagrapi_username"] = resolved_username
    config["instagrapi_sessionid"] = resolved_sessionid
    config = apply_instagram_private_settings(
        config,
        previous_credentials=previous_credentials,
        settings=settings,
    )
    return config, resolved_sessionid, resolved_username


def _published_media_id(media: Any) -> str:
    for field in ("id", "pk"):
        value = str(getattr(media, field, "") or "").strip()
        if value:
            return value
    raise RuntimeError("Instagram did not return a published media identifier.")


def _published_media_url(media: Any) -> str | None:
    code = str(getattr(media, "code", "") or "").strip()
    if not code:
        return None
    product_type = str(getattr(media, "product_type", "") or "").strip().lower()
    if product_type == "clips":
        return f"https://www.instagram.com/reel/{code}/"
    return f"https://www.instagram.com/p/{code}/"


class InstagramSourceAdapter(SourceAdapter):
    service = "instagram"

    def poll(
        self,
        session: Session,
        persona: Persona,
        account: Account,
        sync_state: AccountSyncState | None,
    ) -> PollResult:
        config = get_account_credentials(account)
        api_key = _configured_graph_access_token(config)
        if not api_key:
            return PollResult(posts=[], next_state=(sync_state.state_json if sync_state else {}), cursor=(sync_state.cursor if sync_state else None))

        initial_sync = is_initial_sync(sync_state)
        allow_initial_backfill = import_existing_posts_on_first_scan(persona, account)
        since = cutoff_for_initial_poll(persona, account)
        if sync_state and sync_state.state_json.get("last_seen_at"):
            since = datetime.fromisoformat(str(sync_state.state_json["last_seen_at"]))

        url = f"{INSTAGRAM_GRAPH_API_BASE_URL}/me/media?fields=id,caption,media_url,permalink,timestamp,media_type,children&access_token={api_key}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json().get("data", [])

        if initial_sync and not allow_initial_backfill:
            newest_seen = since
            for media in data:
                newest_seen = max(newest_seen, datetime.fromisoformat(media["timestamp"].replace("Z", "+00:00")))
            next_state = dict(sync_state.state_json if sync_state else {})
            next_state["last_seen_at"] = newest_seen.isoformat() if data else now_utc().isoformat()
            return PollResult(
                posts=[],
                next_state=next_state,
                cursor=(sync_state.cursor if sync_state else None),
                note="Initialized Instagram sync without importing historical posts.",
            )

        posts: list[CanonicalPostPayload] = []
        newest_seen = since
        for media in reversed(data):
            created_at = datetime.fromisoformat(media["timestamp"].replace("Z", "+00:00"))
            if created_at <= since:
                continue
            newest_seen = max(newest_seen, created_at)
            attachments = []
            if media["media_type"] == "CAROUSEL_ALBUM":
                children_url = (
                    f"{INSTAGRAM_GRAPH_API_BASE_URL}/{media['id']}/children?fields=media_url&access_token={api_key}"
                )
                children_response = requests.get(children_url, timeout=30)
                children_response.raise_for_status()
                for index, child in enumerate(children_response.json().get("data", [])):
                    media_url = child.get("media_url")
                    if not media_url:
                        continue
                    attachments.append(download_media(media_url, media_url.split("/")[-1], "", index))
            else:
                media_url = media.get("media_url")
                if media_url:
                    attachments.append(download_media(media_url, media_url.split("/")[-1], "", 0))

            posts.append(
                CanonicalPostPayload(
                    body=media.get("caption", ""),
                    media=attachments,
                    metadata={"link": media.get("permalink", "")},
                    published_at=created_at,
                    external_refs=[
                        ExternalPostRefPayload(
                            external_id=media["id"],
                            external_url=media.get("permalink"),
                            observed_at=created_at,
                        )
                    ],
                )
            )

        next_state = dict(sync_state.state_json if sync_state else {})
        next_state["last_seen_at"] = newest_seen.isoformat()
        return PollResult(posts=posts, next_state=next_state, cursor=(sync_state.cursor if sync_state else None))


class InstagramDestinationAdapter(DestinationAdapter):
    service = "instagram"

    def validate(self, post: CanonicalPost, persona: Persona, account: Account) -> list[ValidationIssue]:
        config = get_account_credentials(account)
        attachments = sorted(post.attachments, key=lambda item: item.sort_order)

        dependency_issue = _instagram_destination_dependency_issue()
        if dependency_issue:
            return [ValidationIssue(service="instagram", field="dependencies", message=dependency_issue)]

        issues: list[ValidationIssue] = []

        auth_issue = _instagrapi_destination_issue(config)
        if auth_issue:
            issues.append(ValidationIssue(service="instagram", field="instagrapi_sessionid", message=auth_issue))

        if not attachments:
            issues.append(ValidationIssue(service="instagram", field="media", message="Instagram publishing requires at least one image or video attachment."))
            return issues
        if len(attachments) > 10:
            issues.append(ValidationIssue(service="instagram", field="media", message="Instagram carousel posts support up to 10 attachments."))

        for attachment in attachments:
            mime_type = str(attachment.mime_type or "").lower()
            if mime_type in INSTAGRAM_SUPPORTED_IMAGE_MIME_TYPES:
                continue
            if mime_type in INSTAGRAM_SUPPORTED_VIDEO_MIME_TYPES:
                continue
            if mime_type.startswith("image/"):
                issues.append(
                    ValidationIssue(
                        service="instagram",
                        field="media",
                        message=f"{attachment.storage_path} must be JPEG, PNG, or WEBP for Instagram publishing.",
                    )
                )
                continue
            if mime_type.startswith("video/"):
                issues.append(
                    ValidationIssue(
                        service="instagram",
                        field="media",
                        message=f"{attachment.storage_path} must be MP4 for Instagram publishing.",
                    )
                )
                continue
            issues.append(
                ValidationIssue(
                    service="instagram",
                    field="media",
                    message=f"{attachment.storage_path} is not a supported Instagram image or MP4 video attachment.",
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
        attachments = sorted(post.attachments, key=lambda item: item.sort_order)
        caption = service_body(post, account)
        dependency_issue = _instagram_destination_dependency_issue()

        if len(attachments) <= 1:
            upload_call = "client.video_upload(path='<local-video-path>.mp4', caption=<caption>)"
            if not attachments or str(attachments[0].mime_type or "").lower().startswith("image/"):
                upload_call = "client.photo_upload(path='<local-image-path>.jpg', caption=<caption>)"
        else:
            upload_call = "client.album_upload(paths=['<local-media-path-1>', '<local-media-path-2>'], caption=<caption>)"

        notes = [
            "Instagram destination publishing uses instagrapi direct uploads from LynxPoster's local media files.",
            "Public Base URL is not required for Instagram destination publishing.",
        ]
        if dependency_issue:
            notes.append(dependency_issue)
        if any(str(attachment.alt_text or "").strip() for attachment in attachments):
            notes.append("Instagram alt text is not currently forwarded through the instagrapi feed upload path.")

        return PublishPreview(
            service="instagram",
            action="instagram_private_api_publish",
            rendered_body=caption,
            endpoint_label="instagrapi Private API",
            request_shape={"call": upload_call},
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
        dependency_issue = _instagram_destination_dependency_issue()
        if dependency_issue:
            raise ConfigurationError(dependency_issue)
        if _instagrapi_destination_issue(config):
            raise ConfigurationError("Instagram publishing requires Session ID or both Login Username and Login Password.")

        attachments = sorted(post.attachments, key=lambda item: item.sort_order)
        if not attachments:
            raise ConfigurationError("Instagram publishing requires at least one image or video attachment.")

        client = _authenticated_publish_client(config)
        _persist_publish_client_state(account, client)

        caption = service_body(post, account)
        with TemporaryDirectory(prefix="lynxposter-instagram-") as temp_dir:
            prepared_paths = _prepared_upload_paths(attachments, temp_dir=temp_dir)
            if len(prepared_paths) == 1:
                if str(attachments[0].mime_type or "").lower().startswith("video/"):
                    media = client.video_upload(prepared_paths[0], caption)
                else:
                    media = client.photo_upload(prepared_paths[0], caption)
            else:
                media = client.album_upload(prepared_paths, caption)

        _persist_publish_client_state(account, client)
        session.flush()

        media_id = _published_media_id(media)
        external_url = _published_media_url(media)
        return PublishResult(
            service="instagram",
            external_id=media_id,
            external_url=external_url,
            raw={
                "id": media_id,
                "code": str(getattr(media, "code", "") or "").strip() or None,
                "product_type": str(getattr(media, "product_type", "") or "").strip() or None,
                "instagrapi_username": str(getattr(client, "username", "") or "").strip() or None,
            },
        )
