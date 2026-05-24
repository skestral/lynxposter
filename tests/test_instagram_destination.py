from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from app.adapters.instagram import InstagramDestinationAdapter, validate_instagram_account_login
from app.config import reload_settings
from app.domain import MediaItem
from app.models import CanonicalPost
from app.schemas import ScheduledPostCreate
from app.services.instagram_private_api import INSTAGRAM_INSTAGRAPI_SETTINGS_KEY
from app.services.personas import create_account, create_persona
from app.services.posts import create_scheduled_post


@pytest.fixture(autouse=True)
def _reload_settings_after_instagram_destination_test():
    reload_settings()
    yield
    reload_settings()


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
            if credentials is not None
            else {
                "api_key": "instagram-token",
                "instagram_user_id": "17841400000000000",
            },
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


def _create_image(path: Path, *, image_format: str) -> None:
    Image.new("RGB", (4, 4), (40, 90, 180)).save(path, format=image_format)


def test_instagram_destination_validate_requires_login_and_media(session, monkeypatch):
    monkeypatch.delenv("APP_BASE_URL", raising=False)
    reload_settings()
    persona = _create_persona(session, slug="instagram-validate")
    account = _create_instagram_account(session, persona, credentials={})
    post = CanonicalPost(persona_id=persona.id, origin_kind="composer", body="No media")
    post.persona = persona

    issues = InstagramDestinationAdapter().validate(post, persona, account)
    messages = [issue.message for issue in issues]

    assert any("requires a Graph access token" in message for message in messages)
    assert any("requires at least one image or video attachment" in message for message in messages)
    assert not any("Session ID" in message for message in messages)


def test_instagram_destination_validate_requires_https_public_base_url(session, monkeypatch, tmp_path):
    monkeypatch.setenv("APP_BASE_URL", "http://127.0.0.1:8000")
    reload_settings()
    persona = _create_persona(session, slug="instagram-base-url")
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
        [MediaItem(storage_path=image_path, mime_type="image/jpeg", size_bytes=4, checksum="img-1", sort_order=0)],
    )

    issues = InstagramDestinationAdapter().validate(post, persona, account)
    messages = [issue.message for issue in issues]

    assert any("externally reachable HTTPS URL" in message for message in messages)


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


def test_instagram_destination_publish_single_image_uses_graph(session, monkeypatch, tmp_path):
    monkeypatch.setenv("APP_BASE_URL", "https://lynxposter.example.com")
    reload_settings()
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

    calls = []

    class FakeResponse:
        ok = True
        text = ""

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_post(url, data=None, timeout=0):
        calls.append(("post", url, dict(data or {})))
        if url.endswith("/17841400000000000/media"):
            assert data["image_url"].startswith("https://lynxposter.example.com/media/instagram/")
            assert data["caption"] == "Hello Instagram"
            assert data["alt_text"] == "Alt text"
            return FakeResponse({"id": "container-1"})
        if url.endswith("/17841400000000000/media_publish"):
            assert data["creation_id"] == "container-1"
            return FakeResponse({"id": "media-1"})
        raise AssertionError(url)

    def fake_get(url, params=None, timeout=0):
        calls.append(("get", url, dict(params or {})))
        assert url.endswith("/media-1")
        return FakeResponse({"permalink": "https://www.instagram.com/p/ABC123/"})

    monkeypatch.setattr("app.adapters.instagram.requests.post", fake_post)
    monkeypatch.setattr("app.adapters.instagram.requests.get", fake_get)

    result = InstagramDestinationAdapter().publish(session, post, persona, account)

    assert result.external_id == "media-1"
    assert result.external_url == "https://www.instagram.com/p/ABC123/"
    assert [call[0] for call in calls] == ["post", "post", "get"]


def test_instagram_destination_publish_album_uses_graph_children(session, monkeypatch, tmp_path):
    monkeypatch.setenv("APP_BASE_URL", "https://lynxposter.example.com")
    reload_settings()
    persona = _create_persona(session, slug="instagram-album")
    account = _create_instagram_account(
        session,
        persona,
        credentials={
            "api_key": "instagram-token",
            "instagram_user_id": "17841400000000000",
        },
    )
    image_path = tmp_path / "one.jpg"
    video_path = tmp_path / "clip-source.bin"
    _create_image(image_path, image_format="JPEG")
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
            MediaItem(storage_path=image_path, mime_type="image/jpeg", alt_text="", size_bytes=4, checksum="img-1", sort_order=0),
            MediaItem(storage_path=video_path, mime_type="video/mp4", alt_text="", size_bytes=5, checksum="vid-1", sort_order=1),
        ],
    )
    session.refresh(post)

    post_calls = []

    class FakeResponse:
        ok = True
        text = ""

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_post(url, data=None, timeout=0):
        payload = dict(data or {})
        post_calls.append(payload)
        if payload.get("is_carousel_item") == "true":
            return FakeResponse({"id": f"child-{len(post_calls)}"})
        if payload.get("media_type") == "CAROUSEL":
            assert payload["children"] == "child-1,child-2"
            assert payload["caption"] == "Carousel time"
            return FakeResponse({"id": "carousel-container"})
        if payload.get("creation_id") == "carousel-container":
            return FakeResponse({"id": "media-3"})
        raise AssertionError(payload)

    def fake_get(url, params=None, timeout=0):
        if url.endswith("/child-2"):
            return FakeResponse({"status_code": "FINISHED"})
        if url.endswith("/carousel-container"):
            return FakeResponse({"status_code": "FINISHED"})
        if url.endswith("/media-3"):
            return FakeResponse({"permalink": "https://www.instagram.com/p/GHI789/"})
        raise AssertionError(url)

    monkeypatch.setattr("app.adapters.instagram.requests.post", fake_post)
    monkeypatch.setattr("app.adapters.instagram.requests.get", fake_get)

    result = InstagramDestinationAdapter().publish(session, post, persona, account)

    assert result.external_id == "media-3"
    assert result.external_url == "https://www.instagram.com/p/GHI789/"
    assert post_calls[0]["image_url"].startswith("https://lynxposter.example.com/media/instagram/")
    assert post_calls[1]["video_url"].startswith("https://lynxposter.example.com/media/instagram/")
