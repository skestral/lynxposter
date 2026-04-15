from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from app.models import AccountPostRef, AccountRoute, AccountSyncState, AlertEvent, CanonicalPost, DeliveryJob, RunEvent
from app.services.instagram_private_api import INSTAGRAM_INSTAGRAPI_SETTINGS_KEY
from app.services.personas import create_account, create_persona, delete_account, get_persona, replace_routes, update_account, update_persona


def _create_persona(session):
    return create_persona(
        session,
        {
            "name": "Personal",
            "slug": "personal",
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def test_one_account_per_service_and_supported_directions(session):
    persona = _create_persona(session)

    create_account(
        session,
        persona,
        {
            "service": "mastodon",
            "label": "Main Mastodon",
            "handle_or_identifier": "@me@example.com",
            "is_enabled": True,
            "source_enabled": True,
            "destination_enabled": True,
            "credentials_json": {"instance": "https://example.social", "token": "secret"},
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )

    with pytest.raises(IntegrityError):
        create_account(
            session,
            persona,
            {
                "service": "mastodon",
                "label": "Duplicate Mastodon",
                "handle_or_identifier": "@duplicate@example.com",
                "is_enabled": True,
                "source_enabled": False,
                "destination_enabled": True,
                "credentials_json": {"instance": "https://example.social", "token": "secret"},
                "source_settings_json": {},
                "publish_settings_json": {},
            },
        )

    session.rollback()

    with pytest.raises(ValueError, match="does not support inbound polling"):
        create_account(
            session,
            persona,
            {
                "service": "twitter",
                "label": "Twitter",
                "handle_or_identifier": "@me",
                "is_enabled": True,
                "source_enabled": True,
                "destination_enabled": True,
                "credentials_json": {
                    "app_key": "a",
                    "app_secret": "b",
                    "access_token": "c",
                    "access_token_secret": "d",
                },
                "source_settings_json": {},
                "publish_settings_json": {},
            },
        )


def test_routes_stay_within_a_persona(session):
    persona = _create_persona(session)
    other_persona = create_persona(
        session,
        {
            "name": "Business",
            "slug": "business",
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )

    source = create_account(
        session,
        persona,
        {
            "service": "bluesky",
            "label": "Bluesky",
            "handle_or_identifier": "me.bsky.social",
            "is_enabled": True,
            "source_enabled": True,
            "destination_enabled": True,
            "credentials_json": {"handle": "me.bsky.social", "password": "pw"},
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )
    destination = create_account(
        session,
        persona,
        {
            "service": "discord",
            "label": "Discord",
            "handle_or_identifier": "Webhook",
            "is_enabled": True,
            "source_enabled": False,
            "destination_enabled": True,
            "credentials_json": {"webhook_url": "https://discord.test"},
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )
    foreign_destination = create_account(
        session,
        other_persona,
        {
            "service": "mastodon",
            "label": "Foreign Mastodon",
            "handle_or_identifier": "@other@example.com",
            "is_enabled": True,
            "source_enabled": True,
            "destination_enabled": True,
            "credentials_json": {"instance": "https://example.social", "token": "secret"},
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )

    replace_routes(
        session,
        get_persona(session, persona.id),
        [{"source_account_id": source.id, "destination_account_id": destination.id, "is_enabled": True}],
    )

    assert session.query(AccountRoute).count() == 1

    with pytest.raises(ValueError, match="within one persona"):
        replace_routes(
            session,
            get_persona(session, persona.id),
            [{"source_account_id": source.id, "destination_account_id": foreign_destination.id, "is_enabled": True}],
        )


def test_update_instagram_account_preserves_instagrapi_settings(session):
    persona = _create_persona(session)
    account = create_account(
        session,
        persona,
        {
            "service": "instagram",
            "label": "Instagram",
            "handle_or_identifier": "larkyn.lynx",
            "is_enabled": True,
            "source_enabled": False,
            "destination_enabled": True,
            "credentials_json": {
                "api_key": "graph-token",
                "instagrapi_username": "larkyn.lynx",
                "instagrapi_password": "insta-password",
                INSTAGRAM_INSTAGRAPI_SETTINGS_KEY: {"cookies": {"sessionid": "persisted-session"}},
            },
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )

    updated = update_account(
        session,
        persona,
        account,
        {
            "label": "Instagram Main",
            "credentials_json": {
                "api_key": "graph-token",
                "instagrapi_username": "larkyn.lynx",
                "instagrapi_password": "insta-password",
            },
        },
    )

    assert updated.label == "Instagram Main"
    assert updated.credentials_json[INSTAGRAM_INSTAGRAPI_SETTINGS_KEY]["cookies"]["sessionid"] == "persisted-session"


def test_update_persona_preserves_hidden_legacy_language_settings(session):
    persona = _create_persona(session)
    persona.settings_json = {
        "mentions": "strip",
        "visibility": "public",
        "mastodon_lang": "en-US",
        "twitter_lang": "en",
    }

    updated = update_persona(
        session,
        persona,
        {
            "settings_json": {
                "mentions": "keep",
                "visibility": "unlisted",
            }
        },
    )

    assert updated.settings_json["mentions"] == "keep"
    assert updated.settings_json["visibility"] == "unlisted"
    assert updated.settings_json["mastodon_lang"] == "en-US"
    assert updated.settings_json["twitter_lang"] == "en"


def test_delete_account_cleans_related_records_and_preserves_logs(session):
    persona = _create_persona(session)
    source = create_account(
        session,
        persona,
        {
            "service": "bluesky",
            "label": "Bluesky",
            "handle_or_identifier": "me.bsky.social",
            "is_enabled": True,
            "source_enabled": True,
            "destination_enabled": True,
            "credentials_json": {"handle": "me.bsky.social", "password": "pw"},
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )
    destination = create_account(
        session,
        persona,
        {
            "service": "discord",
            "label": "Discord",
            "handle_or_identifier": "Webhook",
            "is_enabled": True,
            "source_enabled": False,
            "destination_enabled": True,
            "credentials_json": {"webhook_url": "https://discord.test"},
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )

    replace_routes(
        session,
        persona,
        [{"source_account_id": source.id, "destination_account_id": destination.id, "is_enabled": True}],
    )

    sync_state = AccountSyncState(source_account_id=destination.id, state_json={"cursor": "123"})
    post = CanonicalPost(
        persona_id=persona.id,
        origin_kind="composer",
        origin_account_id=destination.id,
        status="queued",
        body="queued post",
        publish_overrides_json={},
        metadata_json={},
    )
    session.add(sync_state)
    session.add(post)
    session.flush()

    job = DeliveryJob(post_id=post.id, target_account_id=destination.id, status="queued", max_retries=3)
    run_event = RunEvent(run_id="run-1", persona_id=persona.id, account_id=destination.id, operation="publish", message="run")
    alert_event = AlertEvent(
        run_id="run-1",
        fingerprint="fp-1",
        event_type="hard_failure",
        severity="error",
        persona_id=persona.id,
        account_id=destination.id,
        operation="publish",
        message="alert",
        retry_count=1,
        payload_json={},
    )
    session.add_all([job, run_event, alert_event])
    session.flush()

    post_ref = AccountPostRef(post_id=post.id, account_id=destination.id, external_id="ext-1", external_url="https://example.test/post/1")
    session.add(post_ref)
    session.flush()

    delete_account(session, persona, destination)

    assert session.get(type(destination), destination.id) is None
    assert session.query(AccountRoute).count() == 0
    assert session.query(AccountSyncState).count() == 0
    assert session.query(AccountPostRef).count() == 0
    assert session.query(DeliveryJob).count() == 0
    assert session.query(RunEvent).one().account_id is None
    assert session.query(AlertEvent).one().account_id is None

    refreshed_post = session.get(CanonicalPost, post.id)
    assert refreshed_post.origin_account_id is None
    assert refreshed_post.status == "draft"
