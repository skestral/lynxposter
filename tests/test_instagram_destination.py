from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from PIL import Image

from app.adapters.instagram import InstagramDestinationAdapter, validate_instagram_account_login
from app.domain import MediaItem
from app.models import CanonicalPost
from app.schemas import ScheduledPostCreate
from app.services.instagram_private_api import INSTAGRAM_INSTAGRAPI_SETTINGS_KEY
from app.services.personas import create_account, create_persona
from app.services.posts import create_scheduled_post


class _FakeInstagrapiClient:
    instances: list["_FakeInstagrapiClient"] = []

    def __init__(self):
        self.__class__.instances.append(self)
        self.calls: list[tuple[str, object]] = []
        self.loaded_settings: dict[str, object] | None = None
        self.username = ""

    def set_settings(self, settings):
        self.loaded_settings = dict(settings)
        self.calls.append(("set_settings", self.loaded_settings))
        return True

    def account_info(self):
        self.calls.append(("account_info", None))
        return SimpleNamespace(username=self.username or "saved-user")

    def login(self, username, password, relogin=False, verification_code=""):
        self.calls.append(("login", {"username": username, "password": password, "relogin": relogin}))
        self.username = username
        return True

    def login_by_sessionid(self, sessionid):
        self.calls.append(("login_by_sessionid", sessionid))
        self.username = "session-user"
        return True

    def get_settings(self):
        return {"cookies": {"sessionid": "persisted-session"}, "uuids": {"uuid": "device-1"}}

    def photo_upload(self, path, caption):
        self.calls.append(("photo_upload", {"path": Path(path), "caption": caption}))
        return SimpleNamespace(id="media-1", pk="media-1", code="ABC123", product_type="feed")

    def video_upload(self, path, caption):
        self.calls.append(("video_upload", {"path": Path(path), "caption": caption}))
        return SimpleNamespace(id="media-2", pk="media-2", code="DEF456", product_type="feed")

    def album_upload(self, paths, caption):
        self.calls.append(("album_upload", {"paths": [Path(path) for path in paths], "caption": caption}))
        return SimpleNamespace(id="media-3", pk="media-3", code="GHI789", product_type="feed")


def _create_persona(session, *, slug: str = "instagram-destination"):
    return create_persona(
        session,
        {
            "name": "Instagram Persona",
            "slug": slug,
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def _create_instagram_account(session, persona, *, credentials: dict[str, str] | None = None):
    return create_account(
        session,
        persona,
        {
            "service": "instagram",
            "label": "Instagram",
            "handle_or_identifier": "larkyn.lynx",
            "is_enabled": True,
            "source_enabled": False,
            "destination_enabled": True,
            "credentials_json": credentials
            or {
                "api_key": "instagram-token",
                "instagrapi_username": "larkyn.lynx",
                "instagrapi_password": "insta-password",
            },
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


def _create_image(path: Path, *, image_format: str) -> None:
    Image.new("RGB", (4, 4), (40, 90, 180)).save(path, format=image_format)


def test_instagram_destination_validate_requires_login_and_media(session):
    persona = _create_persona(session, slug="instagram-validate")
    account = _create_instagram_account(session, persona, credentials={"api_key": "instagram-token"})
    post = CanonicalPost(persona_id=persona.id, origin_kind="composer", body="No media")
    post.persona = persona

    issues = InstagramDestinationAdapter().validate(post, persona, account)
    messages = [issue.message for issue in issues]

    assert any("requires Session ID or both Login Username and Login Password" in message for message in messages)
    assert any("requires at least one image or video attachment" in message for message in messages)
    assert not any("Public Base URL" in message for message in messages)


def test_instagram_destination_validate_reports_missing_optional_dependencies(session, monkeypatch):
    persona = _create_persona(session, slug="instagram-missing-deps")
    account = _create_instagram_account(session, persona)
    post = CanonicalPost(persona_id=persona.id, origin_kind="composer", body="No media")
    post.persona = persona

    monkeypatch.setattr("app.adapters.instagram._load_instagram_dependencies", lambda: (None, None, Exception))

    issues = InstagramDestinationAdapter().validate(post, persona, account)
    messages = [issue.message for issue in issues]

    assert len(issues) == 1
    assert any("Run `pip install -r requirements.txt` with that same interpreter." in message for message in messages)


def test_validate_instagram_account_login_captures_sessionid_and_settings(monkeypatch):
    _FakeInstagrapiClient.instances.clear()
    monkeypatch.setattr("app.adapters.instagram._load_instagram_dependencies", lambda: (_FakeInstagrapiClient, Image, Exception))

    credentials, sessionid, username = validate_instagram_account_login(
        {
            "api_key": "instagram-token",
            "instagrapi_username": "larkyn.lynx",
            "instagrapi_password": "insta-password",
        }
    )

    assert sessionid == "persisted-session"
    assert username == "larkyn.lynx"
    assert credentials["instagrapi_sessionid"] == "persisted-session"
    assert credentials[INSTAGRAM_INSTAGRAPI_SETTINGS_KEY]["cookies"]["sessionid"] == "persisted-session"


def test_instagram_destination_publish_single_image_uses_instagrapi_and_persists_settings(session, monkeypatch, tmp_path):
    persona = _create_persona(session, slug="instagram-single")
    account = _create_instagram_account(session, persona)
    image_path = tmp_path / "photo.jpg"
    _create_image(image_path, image_format="JPEG")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello Instagram",
                "status": "draft",
                "target_account_ids": [account.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(
                storage_path=image_path,
                mime_type="image/jpeg",
                alt_text="Alt text",
                size_bytes=4,
                checksum="img-1",
                sort_order=0,
            )
        ],
    )
    session.refresh(post)
    _FakeInstagrapiClient.instances.clear()
    monkeypatch.setattr("app.adapters.instagram._load_instagram_dependencies", lambda: (_FakeInstagrapiClient, Image, Exception))

    result = InstagramDestinationAdapter().publish(session, post, persona, account)

    client = _FakeInstagrapiClient.instances[0]
    assert result.external_id == "media-1"
    assert result.external_url == "https://www.instagram.com/p/ABC123/"
    assert client.calls[0][0] == "login"
    upload_call = next(payload for action, payload in client.calls if action == "photo_upload")
    assert upload_call["caption"] == "Hello Instagram"
    assert upload_call["path"].suffix == ".jpg"
    assert account.credentials_json[INSTAGRAM_INSTAGRAPI_SETTINGS_KEY]["cookies"]["sessionid"] == "persisted-session"


def test_instagram_destination_publish_album_uses_sessionid_and_normalizes_media(session, monkeypatch, tmp_path):
    persona = _create_persona(session, slug="instagram-album")
    account = _create_instagram_account(
        session,
        persona,
        credentials={
            "api_key": "instagram-token",
            "instagrapi_sessionid": "12345%3Aabcdef1234567890abcdef1234567890",
        },
    )
    image_path = tmp_path / "one.png"
    video_path = tmp_path / "clip-source.bin"
    _create_image(image_path, image_format="PNG")
    video_path.write_bytes(b"video")

    post = create_scheduled_post(
        session,
        ScheduledPostCreate.model_validate(
            {
                "persona_id": persona.id,
                "body": "Carousel time",
                "status": "draft",
                "target_account_ids": [account.id],
                "publish_overrides_json": {},
                "metadata_json": {},
                "scheduled_for": None,
            }
        ),
        [
            MediaItem(storage_path=image_path, mime_type="image/png", alt_text="", size_bytes=4, checksum="img-1", sort_order=0),
            MediaItem(storage_path=video_path, mime_type="video/mp4", alt_text="", size_bytes=5, checksum="vid-1", sort_order=1),
        ],
    )
    session.refresh(post)
    _FakeInstagrapiClient.instances.clear()
    monkeypatch.setattr("app.adapters.instagram._load_instagram_dependencies", lambda: (_FakeInstagrapiClient, Image, Exception))

    result = InstagramDestinationAdapter().publish(session, post, persona, account)

    client = _FakeInstagrapiClient.instances[0]
    assert result.external_id == "media-3"
    assert client.calls[0] == ("login_by_sessionid", "12345%3Aabcdef1234567890abcdef1234567890")
    album_call = next(payload for action, payload in client.calls if action == "album_upload")
    assert album_call["caption"] == "Carousel time"
    assert [path.suffix for path in album_call["paths"]] == [".jpg", ".mp4"]
    assert account.credentials_json["instagrapi_username"] == "session-user"
