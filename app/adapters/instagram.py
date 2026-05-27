from __future__ import annotations

import sys
import shutil
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from sqlalchemy.orm import Session

from app.adapters.base import ConfigurationError, DestinationAdapter, SourceAdapter, get_account_credentials
from app.adapters.common import cutoff_for_initial_poll, import_existing_posts_on_first_scan, is_initial_sync, now_utc, service_attachments, service_body
from app.config import get_settings
from app.domain import CanonicalPostPayload, ExternalPostRefPayload, PollResult, PublishPreview, PublishResult, ValidationIssue
from app.models import Account, AccountSyncState, CanonicalPost, MediaAttachment, Persona
from app.services.instagram_private_api import apply_instagram_private_settings, get_instagram_private_settings
from app.services.instagram_private_policy import INSTAGRAM_PRIVATE_REASON_DIAGNOSTIC, ensure_instagram_private_access_allowed
from app.services.storage import delete_managed_media_file, download_media, normalize_media_file, public_instagram_media_url
from app.utils import detect_mime_type, stable_checksum

INSTAGRAM_API_VERSION = "v25.0"
INSTAGRAM_GRAPH_API_BASE_URL = f"https://graph.instagram.com/{INSTAGRAM_API_VERSION}"
INSTAGRAM_PUBLISH_API_BASE_URL = f"https://graph.facebook.com/{INSTAGRAM_API_VERSION}"
INSTAGRAM_GRAPH_HOST_INSTAGRAM = "instagram"
INSTAGRAM_GRAPH_HOST_FACEBOOK = "facebook"
INSTAGRAM_SUPPORTED_IMAGE_MIME_TYPES = {"image/jpeg", "image/jpg", "image/pjpeg", "image/png", "image/webp"}
INSTAGRAM_SUPPORTED_VIDEO_MIME_TYPES = {"video/mp4"}
INSTAGRAM_GRAPH_IMAGE_MIME_TYPES = {"image/jpeg", "image/jpg", "image/pjpeg"}
INSTAGRAM_GRAPH_VIDEO_MIME_TYPES = {"video/mp4"}
INSTAGRAM_CONTAINER_READY_STATUSES = {"FINISHED", "PUBLISHED"}
INSTAGRAM_CONTAINER_PENDING_STATUSES = {"IN_PROGRESS", "PROCESSING", "CREATED"}
INSTAGRAM_CONTAINER_PUBLISH_ATTEMPTS = 4


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
    token = str(config.get("api_key") or "").strip().strip("\"'")
    if token.lower().startswith("bearer "):
        token = token[7:].strip()

    parsed = urlparse(token)
    if parsed.query:
        query_token = parse_qs(parsed.query).get("access_token", [""])[0]
        if query_token:
            return query_token.strip()

    if token.startswith("access_token="):
        query_token = parse_qs(token).get("access_token", [""])[0]
        if query_token:
            return query_token.strip()

    return token


def _configured_graph_api_host(config: dict[str, Any]) -> str:
    return str(config.get("graph_api_host") or config.get("api_host") or "auto").strip().lower()


def _looks_like_instagram_login_token(access_token: str) -> bool:
    return access_token.upper().startswith("IG")


def _instagram_account_graph_base_url(config: dict[str, Any]) -> str:
    configured_host = _configured_graph_api_host(config)
    if configured_host in {INSTAGRAM_GRAPH_HOST_INSTAGRAM, "instagram_login", "graph.instagram.com"}:
        return INSTAGRAM_GRAPH_API_BASE_URL
    if configured_host in {INSTAGRAM_GRAPH_HOST_FACEBOOK, "facebook_login", "business", "graph.facebook.com"}:
        return INSTAGRAM_PUBLISH_API_BASE_URL
    if _looks_like_instagram_login_token(_configured_graph_access_token(config)):
        return INSTAGRAM_GRAPH_API_BASE_URL
    return INSTAGRAM_PUBLISH_API_BASE_URL


def _configured_instagram_user_id(config: dict[str, Any]) -> str:
    return str(
        config.get("instagram_user_id")
        or config.get("ig_user_id")
        or config.get("professional_account_id")
        or config.get("provider_account_id")
        or ""
    ).strip()


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


def _instagram_graph_destination_issue(config: dict[str, Any]) -> str | None:
    if not _configured_graph_access_token(config):
        return "Instagram Graph publishing requires a Graph access token."
    if not _configured_instagram_user_id(config):
        return "Instagram Graph publishing requires the Instagram professional account ID."
    base_url = get_settings().app_base_url.strip()
    if not base_url:
        return "Instagram Graph publishing requires Public Base URL so Meta can fetch media."
    if not base_url.startswith("https://"):
        return "Instagram Graph publishing requires Public Base URL to be an externally reachable HTTPS URL."
    return None


def _instagram_public_media_url(attachment: MediaAttachment) -> str:
    return public_instagram_media_url(attachment.id, attachment.storage_path, base_url=get_settings().app_base_url)


def _attachment_media_url_for_log(attachment: MediaAttachment) -> str:
    try:
        return _instagram_public_media_url(attachment)
    except Exception:
        return "unavailable"


def _raise_graph_error(response: requests.Response, *, action: str, media_url: str | None = None) -> None:
    try:
        payload = response.json()
    except ValueError:
        payload = {"body": response.text}
    if response.ok:
        return
    error_payload = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error_payload, dict):
        message = error_payload.get("message") or error_payload.get("error_user_msg") or str(error_payload)
    else:
        message = str(payload)
    if media_url:
        message = f"{message} Media URL: {media_url}"
    raise RuntimeError(f"Instagram Graph {action} failed: {message}")


def _raise_instagram_source_error(response: requests.Response, *, action: str) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        error_payload = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error_payload, dict):
            message = error_payload.get("message") or error_payload.get("error_user_msg") or str(error_payload)
        else:
            message = response.text or str(exc)
        raise RuntimeError(f"Instagram Graph {action} failed: {message}") from exc


def _instagram_source_media_endpoint(config: dict[str, Any]) -> tuple[str, str]:
    instagram_user_id = _configured_instagram_user_id(config)
    if instagram_user_id:
        base_url = _instagram_account_graph_base_url(config)
        action = "instagram_media" if base_url == INSTAGRAM_GRAPH_API_BASE_URL else "business_media"
        return f"{base_url}/{instagram_user_id}/media", action
    return f"{INSTAGRAM_GRAPH_API_BASE_URL}/me/media", "login_media"


def _instagram_source_children_endpoint(config: dict[str, Any], media_id: str) -> tuple[str, str]:
    instagram_user_id = _configured_instagram_user_id(config)
    base_url = _instagram_account_graph_base_url(config) if instagram_user_id else INSTAGRAM_GRAPH_API_BASE_URL
    return f"{base_url}/{media_id}/children", "media_children"


def _post_graph(path: str, data: dict[str, Any], *, base_url: str = INSTAGRAM_PUBLISH_API_BASE_URL) -> dict[str, Any]:
    response = requests.post(f"{base_url}/{path.lstrip('/')}", data=data, timeout=60)
    media_url = str(data.get("image_url") or data.get("video_url") or "").strip() or None
    _raise_graph_error(response, action=path, media_url=media_url)
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _get_graph(path: str, params: dict[str, Any], *, base_url: str = INSTAGRAM_PUBLISH_API_BASE_URL) -> dict[str, Any]:
    response = requests.get(f"{base_url}/{path.lstrip('/')}", params=params, timeout=30)
    _raise_graph_error(response, action=path)
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _graph_container_id(payload: dict[str, Any]) -> str:
    container_id = str(payload.get("id") or "").strip()
    if not container_id:
        raise RuntimeError("Instagram Graph did not return a media container id.")
    return container_id


def _wait_for_container_ready(container_id: str, access_token: str, *, base_url: str) -> dict[str, Any]:
    last_payload: dict[str, Any] = {}
    for attempt in range(8):
        payload = _get_graph(
            container_id,
            {"fields": "status_code,status", "access_token": access_token},
            base_url=base_url,
        )
        last_payload = payload
        status_code = str(payload.get("status_code") or "").upper()
        if status_code in INSTAGRAM_CONTAINER_READY_STATUSES or not status_code:
            return payload
        if status_code == "ERROR":
            raise RuntimeError(f"Instagram Graph media container {container_id} failed processing: {payload.get('status') or 'unknown error'}")
        if status_code not in INSTAGRAM_CONTAINER_PENDING_STATUSES:
            return payload
        if attempt < 7:
            time.sleep(2)
    raise RuntimeError(f"Instagram Graph media container {container_id} was not ready before the publish timeout.")


def _graph_create_container(
    *,
    instagram_user_id: str,
    access_token: str,
    data: dict[str, Any],
    base_url: str,
    wait_until_ready: bool = True,
) -> tuple[str, dict[str, Any]]:
    payload = _post_graph(f"{instagram_user_id}/media", {**data, "access_token": access_token}, base_url=base_url)
    container_id = _graph_container_id(payload)
    status_payload = _wait_for_container_ready(container_id, access_token, base_url=base_url) if wait_until_ready else {}
    return container_id, {"create": payload, "status": status_payload}


def _graph_publish_container(instagram_user_id: str, access_token: str, container_id: str, *, base_url: str) -> dict[str, Any]:
    for attempt in range(INSTAGRAM_CONTAINER_PUBLISH_ATTEMPTS):
        try:
            return _post_graph(
                f"{instagram_user_id}/media_publish",
                {"creation_id": container_id, "access_token": access_token},
                base_url=base_url,
            )
        except RuntimeError as exc:
            if "media id is not available" not in str(exc).lower() or attempt == INSTAGRAM_CONTAINER_PUBLISH_ATTEMPTS - 1:
                raise
            _wait_for_container_ready(container_id, access_token, base_url=base_url)
            time.sleep(2)
    raise RuntimeError(f"Instagram Graph media container {container_id} could not be published.")



def _graph_permalink(access_token: str, media_id: str, *, base_url: str) -> str | None:
    if not media_id:
        return None
    payload = _get_graph(media_id, {"fields": "permalink", "access_token": access_token}, base_url=base_url)
    permalink = str(payload.get("permalink") or "").strip()
    return permalink or None


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


def _prepare_graph_attachment(attachment: Any) -> None:
    source_path = Path(str(attachment.storage_path))
    if not source_path.exists():
        return

    try:
        normalized_path, mime_type, size_bytes, checksum = normalize_media_file(source_path, getattr(attachment, "mime_type", None))
    except OSError:
        return

    source_path = normalized_path
    attachment.storage_path = str(normalized_path)
    attachment.mime_type = mime_type
    attachment.size_bytes = size_bytes
    attachment.checksum = checksum

    if mime_type in INSTAGRAM_GRAPH_IMAGE_MIME_TYPES or mime_type in INSTAGRAM_GRAPH_VIDEO_MIME_TYPES:
        return
    if mime_type not in INSTAGRAM_SUPPORTED_IMAGE_MIME_TYPES:
        return

    target_path = source_path.with_name(f"{source_path.stem}-instagram-{stable_checksum(source_path)[:12]}.jpg")
    _flatten_image_to_jpeg(source_path, target_path)
    delete_managed_media_file(source_path)
    attachment.storage_path = str(target_path)
    attachment.mime_type = detect_mime_type(target_path)
    attachment.size_bytes = target_path.stat().st_size
    attachment.checksum = stable_checksum(target_path)


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
    ensure_instagram_private_access_allowed(INSTAGRAM_PRIVATE_REASON_DIAGNOSTIC)
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

        media_url, action = _instagram_source_media_endpoint(config)
        response = requests.get(
            media_url,
            params={
                "fields": "id,caption,media_url,permalink,timestamp,media_type,children",
                "access_token": api_key,
            },
            timeout=30,
        )
        _raise_instagram_source_error(response, action=action)
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
                children_url, children_action = _instagram_source_children_endpoint(config, media["id"])
                children_response = requests.get(
                    children_url,
                    params={"fields": "media_url", "access_token": api_key},
                    timeout=30,
                )
                _raise_instagram_source_error(children_response, action=children_action)
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
        attachments = service_attachments(post, account)

        issues: list[ValidationIssue] = []

        auth_issue = _instagram_graph_destination_issue(config)
        if auth_issue:
            issues.append(ValidationIssue(service="instagram", field="graph", message=auth_issue))

        if not attachments:
            issues.append(ValidationIssue(service="instagram", field="media", message="Instagram publishing requires at least one image or video attachment."))
            return issues
        if len(attachments) > 10:
            issues.append(ValidationIssue(service="instagram", field="media", message="Instagram carousel posts support up to 10 attachments."))

        for attachment in attachments:
            _prepare_graph_attachment(attachment)
            mime_type = str(attachment.mime_type or "").lower()
            if mime_type in INSTAGRAM_GRAPH_IMAGE_MIME_TYPES:
                continue
            if mime_type in INSTAGRAM_GRAPH_VIDEO_MIME_TYPES:
                continue
            if mime_type.startswith("image/"):
                issues.append(
                    ValidationIssue(
                        service="instagram",
                        field="media",
                        message=(
                            f"{attachment.storage_path} must be JPEG for Instagram Graph publishing. "
                            f"Media URL: {_attachment_media_url_for_log(attachment)}"
                        ),
                    )
                )
                continue
            if mime_type.startswith("video/"):
                issues.append(
                    ValidationIssue(
                        service="instagram",
                        field="media",
                        message=(
                            f"{attachment.storage_path} must be MP4 for Instagram publishing. "
                            f"Media URL: {_attachment_media_url_for_log(attachment)}"
                        ),
                    )
                )
                continue
            issues.append(
                ValidationIssue(
                    service="instagram",
                    field="media",
                    message=(
                        f"{attachment.storage_path} is not a supported Instagram image or MP4 video attachment. "
                        f"Media URL: {_attachment_media_url_for_log(attachment)}"
                    ),
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
        attachments = service_attachments(post, account)
        caption = service_body(post, account)

        if len(attachments) <= 1:
            container_shape: dict[str, Any] = {"caption": "<caption>", "video_url": "<public-video-url>", "media_type": "REELS"}
            if not attachments or str(attachments[0].mime_type or "").lower().startswith("image/"):
                container_shape = {"caption": "<caption>", "image_url": "<public-image-url>"}
        else:
            container_shape = {
                "media_type": "CAROUSEL",
                "caption": "<caption>",
                "children": ["<child-container-id-1>", "<child-container-id-2>"],
            }

        notes = [
            "Instagram destination publishing uses the official Graph content publishing flow.",
            "Meta fetches each attachment from LynxPoster's public /media/instagram/... URL.",
        ]
        graph_issue = _instagram_graph_destination_issue(get_account_credentials(account))
        if graph_issue:
            notes.append(graph_issue)
        if any(str(attachment.alt_text or "").strip() for attachment in attachments):
            notes.append("Alt text is sent for single image posts when present.")

        return PublishPreview(
            service="instagram",
            action="instagram_graph_publish",
            rendered_body=caption,
            endpoint_label="Instagram Graph API",
            request_shape={
                "create_container": container_shape,
                "publish_container": {"creation_id": "<container-id>"},
            },
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
        graph_issue = _instagram_graph_destination_issue(config)
        if graph_issue:
            raise ConfigurationError(graph_issue)

        attachments = service_attachments(post, account)
        if not attachments:
            raise ConfigurationError("Instagram publishing requires at least one image or video attachment.")
        if len(attachments) > 10:
            raise ConfigurationError("Instagram carousel posts support up to 10 attachments.")
        for attachment in attachments:
            _prepare_graph_attachment(attachment)

        caption = service_body(post, account)
        access_token = _configured_graph_access_token(config)
        instagram_user_id = _configured_instagram_user_id(config)
        graph_base_url = _instagram_account_graph_base_url(config)
        raw: dict[str, Any] = {"children": []}

        if len(attachments) == 1:
            attachment = attachments[0]
            mime_type = str(attachment.mime_type or "").lower()
            if mime_type in INSTAGRAM_GRAPH_IMAGE_MIME_TYPES:
                container_data = {
                    "image_url": _instagram_public_media_url(attachment),
                    "caption": caption,
                }
                alt_text = str(attachment.alt_text or "").strip()
                if alt_text:
                    container_data["alt_text"] = alt_text
            elif mime_type in INSTAGRAM_GRAPH_VIDEO_MIME_TYPES:
                video_media_type = str((account.publish_settings_json or {}).get("video_media_type") or "REELS").strip().upper()
                if video_media_type not in {"REELS", "STORIES"}:
                    video_media_type = "REELS"
                container_data = {
                    "video_url": _instagram_public_media_url(attachment),
                    "media_type": video_media_type,
                    "caption": caption,
                }
            else:
                raise ConfigurationError(
                    f"{attachment.storage_path} is not a supported Instagram Graph image or MP4 video attachment. "
                    f"Media URL: {_attachment_media_url_for_log(attachment)}"
                )
            container_id, container_raw = _graph_create_container(
                instagram_user_id=instagram_user_id,
                access_token=access_token,
                data=container_data,
                base_url=graph_base_url,
            )
        else:
            child_ids: list[str] = []
            for attachment in attachments:
                mime_type = str(attachment.mime_type or "").lower()
                if mime_type in INSTAGRAM_GRAPH_IMAGE_MIME_TYPES:
                    child_data = {
                        "image_url": _instagram_public_media_url(attachment),
                        "is_carousel_item": "true",
                    }
                elif mime_type in INSTAGRAM_GRAPH_VIDEO_MIME_TYPES:
                    child_data = {
                        "video_url": _instagram_public_media_url(attachment),
                        "media_type": "VIDEO",
                        "is_carousel_item": "true",
                    }
                else:
                    raise ConfigurationError(
                        f"{attachment.storage_path} is not a supported Instagram Graph image or MP4 video attachment. "
                        f"Media URL: {_attachment_media_url_for_log(attachment)}"
                    )
                child_id, child_raw = _graph_create_container(
                    instagram_user_id=instagram_user_id,
                    access_token=access_token,
                    data=child_data,
                    base_url=graph_base_url,
                )
                child_ids.append(child_id)
                raw["children"].append({"id": child_id, **child_raw})
            container_id, container_raw = _graph_create_container(
                instagram_user_id=instagram_user_id,
                access_token=access_token,
                data={
                    "media_type": "CAROUSEL",
                    "children": ",".join(child_ids),
                    "caption": caption,
                },
                base_url=graph_base_url,
            )

        publish_raw = _graph_publish_container(instagram_user_id, access_token, container_id, base_url=graph_base_url)
        media_id = str(publish_raw.get("id") or "").strip()
        if not media_id:
            raise RuntimeError("Instagram Graph did not return a published media id.")
        external_url = _graph_permalink(access_token, media_id, base_url=graph_base_url)
        raw.update(
            {
                "container": {"id": container_id, **container_raw},
                "publish": publish_raw,
                "media_public_base_url": f"{get_settings().app_base_url.rstrip('/')}/media/instagram/",
            }
        )
        return PublishResult(
            service="instagram",
            external_id=media_id,
            external_url=external_url,
            raw=raw,
        )
