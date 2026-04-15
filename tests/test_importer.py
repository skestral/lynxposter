from __future__ import annotations

from app.models import Account, AccountRoute, Persona
from app.services.importer import apply_legacy_seed_to_persona


def _create_persona(session, *, name: str, slug: str) -> Persona:
    persona = Persona(
        name=name,
        slug=slug,
        is_enabled=True,
        timezone="UTC",
        settings_json={"mentions": "strip"},
        retry_settings_json={"max_retries": 1},
        throttle_settings_json={"max_per_hour": 1, "overflow_posts": "skip"},
    )
    session.add(persona)
    session.flush()
    return persona


def test_apply_legacy_seed_to_persona_upserts_accounts_and_settings(session, monkeypatch):
    monkeypatch.setenv("BSKY_HANDLE", "lynx.test")
    monkeypatch.setenv("BSKY_PASSWORD", "app-password")
    monkeypatch.setenv("BSKY_SESSION_STRING", "")
    monkeypatch.setenv("MASTODON_CROSSPOSTING", "true")
    monkeypatch.setenv("MASTODON_HANDLE", "lynx@example.social")
    monkeypatch.setenv("MASTODON_INSTANCE", "https://example.social")
    monkeypatch.setenv("MASTODON_TOKEN", "mastodon-token")
    monkeypatch.setenv("MASTODON_LANG", "en-US")
    monkeypatch.setenv("DISCORD_CROSSPOSTING", "true")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/webhook")
    monkeypatch.setenv("INSTAGRAM_CROSSPOSTING", "false")
    monkeypatch.setenv("TWITTER_CROSSPOSTING", "true")
    monkeypatch.setenv("TWITTER_USERNAME", "lynxposter")
    monkeypatch.setenv("TWITTER_APP_KEY", "twitter-app-key")
    monkeypatch.setenv("TWITTER_APP_SECRET", "twitter-app-secret")
    monkeypatch.setenv("TWITTER_ACCESS_TOKEN", "twitter-access-token")
    monkeypatch.setenv("TWITTER_ACCESS_TOKEN_SECRET", "twitter-access-token-secret")
    monkeypatch.setenv("TWITTER_LANG", "en")
    monkeypatch.setenv("TUMBLR_CROSSPOSTING", "false")
    monkeypatch.setenv("MENTIONS", "ignore")
    monkeypatch.setenv("POST_DEFAULT", "false")
    monkeypatch.setenv("QUOTE_POSTS", "false")

    persona = _create_persona(session, name="Savannah", slug="savannah")
    existing_account = Account(
        persona_id=persona.id,
        service="mastodon",
        label="Old Mastodon",
        handle_or_identifier="old@example.social",
        is_enabled=True,
        source_enabled=False,
        destination_enabled=True,
        credentials_json={"token": "old-token"},
        source_settings_json={},
        publish_settings_json={},
    )
    existing_account.persona = persona
    session.add(existing_account)
    session.flush()

    apply_legacy_seed_to_persona(session, persona)

    session.refresh(persona)
    services = {account.service: account for account in persona.accounts}

    assert persona.settings_json["mentions"] == "ignore"
    assert persona.settings_json["post_default"] is False
    assert persona.settings_json["quote_posts"] is False
    assert "mastodon_lang" not in persona.settings_json
    assert "twitter_lang" not in persona.settings_json
    assert services["mastodon"].label == "Mastodon"
    assert services["mastodon"].credentials_json["token"] == "mastodon-token"
    assert services["mastodon"].publish_settings_json["language"] == "en-US"
    assert services["bluesky"].credentials_json["handle"] == "lynx.test"
    assert services["discord"].credentials_json["webhook_url"] == "https://discord.example/webhook"
    assert services["twitter"].publish_settings_json["language"] == "en"

    routes = session.query(AccountRoute).all()
    route_pairs = {(route.source_account.service, route.destination_account.service) for route in routes}
    assert ("bluesky", "mastodon") in route_pairs
    assert ("bluesky", "discord") in route_pairs
    assert ("bluesky", "twitter") in route_pairs


def test_apply_legacy_seed_to_persona_enables_instagram_destination_when_instagrapi_credentials_are_present(session, monkeypatch):
    monkeypatch.setenv("INSTAGRAM_CROSSPOSTING", "true")
    monkeypatch.setenv("INSTAGRAM_API_KEY", "instagram-token")
    monkeypatch.setenv("INSTAGRAM_USER_ID", "17841400000000000")
    monkeypatch.setenv("INSTAGRAPI_USERNAME", "larkyn.lynx")
    monkeypatch.setenv("INSTAGRAPI_PASSWORD", "insta-password")
    monkeypatch.setenv("BSKY_HANDLE", "")
    monkeypatch.setenv("BSKY_PASSWORD", "")
    monkeypatch.setenv("BSKY_SESSION_STRING", "")
    monkeypatch.setenv("MASTODON_CROSSPOSTING", "false")
    monkeypatch.setenv("DISCORD_CROSSPOSTING", "false")
    monkeypatch.setenv("TWITTER_CROSSPOSTING", "false")
    monkeypatch.setenv("TUMBLR_CROSSPOSTING", "false")

    persona = _create_persona(session, name="Lynx", slug="lynx")

    apply_legacy_seed_to_persona(session, persona)

    session.refresh(persona)
    instagram = next(account for account in persona.accounts if account.service == "instagram")

    assert instagram.source_enabled is True
    assert instagram.destination_enabled is True
    assert instagram.credentials_json["api_key"] == "instagram-token"
    assert instagram.credentials_json["instagrapi_username"] == "larkyn.lynx"
    assert instagram.credentials_json["instagrapi_password"] == "insta-password"
