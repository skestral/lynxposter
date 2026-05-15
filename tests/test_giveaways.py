from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import reload_settings
from app.database import Base
from app.main import app
from app.models import AlertEvent, GiveawayEntrant, InstagramGiveawayWebhookEvent
from app.schemas import ScheduledPostCreate
from app.services.alerts import AlertDispatcher
from app.services.auth import Principal
from app.services.giveaway_engine import (
    ENTRY_STATUS_ELIGIBLE,
    GIVEAWAY_STATUS_COLLECTING,
    GIVEAWAY_STATUS_REVIEW_REQUIRED,
    GIVEAWAY_STATUS_WINNER_SELECTED,
    collect_bluesky_channel_state,
    process_giveaway_lifecycle,
    serialize_giveaway,
)
from app.services.giveaways import (
    instagram_webhook_observability,
    ingest_instagram_webhook_payload,
    process_instagram_giveaway_lifecycle,
)
from app.services.personas import create_account, create_persona
from app.services.posts import create_scheduled_post, get_post, schedule_post_now


class _DumpableResponse:
    def __init__(self, payload: dict[str, Any]):
        self._payload = payload

    def model_dump(self) -> dict[str, Any]:
        return self._payload


def _create_persona(session: Session, *, slug: str = "giveaway-persona", name: str = "Giveaway Persona"):
    return create_persona(
        session,
        {
            "name": name,
            "slug": slug,
            "is_enabled": True,
            "timezone": "UTC",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def _create_account(session: Session, persona, *, service: str, label: str):
    credentials = {
        "instagram": {
            "api_key": "graph-token",
            "instagrapi_sessionid": "sessionid",
            "instagram_user_id": "17841463479494132",
            "instagram_username": "savannah.ig",
        },
        "bluesky": {
            "handle": "savannah.test",
            "app_password": "app-password",
        },
        "mastodon": {
            "instance": "https://example.social",
            "token": "secret",
            "handle": "@me@example.social",
        },
    }[service]
    return create_account(
        session,
        persona,
        {
            "service": service,
            "label": label,
            "handle_or_identifier": credentials.get("handle") or label,
            "is_enabled": True,
            "source_enabled": False,
            "destination_enabled": True,
            "credentials_json": credentials,
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


def _legacy_instagram_giveaway_payload(
    persona_id: str,
    target_account_ids: list[str],
    *,
    scheduled_for: datetime | None = None,
    giveaway_end_at: datetime | None = None,
) -> ScheduledPostCreate:
    return ScheduledPostCreate.model_validate(
        {
            "persona_id": persona_id,
            "body": "Win a prize by commenting and sharing",
            "post_type": "instagram_giveaway",
            "status": "draft",
            "target_account_ids": target_account_ids,
            "publish_overrides_json": {},
            "metadata_json": {},
            "scheduled_for": scheduled_for,
            "giveaway": {
                "giveaway_end_at": giveaway_end_at,
                "min_friend_mentions": 1,
                "required_keywords": [],
                "required_hashtags": [],
                "require_story_mention": True,
                "require_like": False,
                "require_follow": False,
            },
        }
    )


def _generic_giveaway_payload(
    persona_id: str,
    target_account_ids: list[str],
    *,
    giveaway_end_at: datetime,
    pool_mode: str = "combined",
    channels: list[dict[str, Any]],
) -> ScheduledPostCreate:
    return ScheduledPostCreate.model_validate(
        {
            "persona_id": persona_id,
            "body": "Win a prize across platforms",
            "post_type": "giveaway",
            "status": "draft",
            "target_account_ids": target_account_ids,
            "publish_overrides_json": {},
            "metadata_json": {},
            "scheduled_for": None,
            "giveaway": {
                "giveaway_end_at": giveaway_end_at,
                "pool_mode": pool_mode,
                "channels": channels,
            },
        }
    )


def _mark_posted(post, account_id: str, *, external_id: str, external_url: str | None = None) -> None:
    job = next(job for job in post.delivery_jobs if job.target_account_id == account_id)
    job.status = "posted"
    job.external_id = external_id
    job.external_url = external_url
    post.published_at = datetime.now(timezone.utc) - timedelta(minutes=5)


def test_legacy_instagram_giveaway_requires_exactly_one_instagram_target(session):
    persona = _create_persona(session, slug="legacy-invalid-targets")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon")

    with pytest.raises(ValueError, match="must target exactly one Instagram destination account"):
        create_scheduled_post(
            session,
            _legacy_instagram_giveaway_payload(
                persona.id,
                [instagram.id, mastodon.id],
                giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=2),
            ),
            [],
        )


def test_legacy_instagram_giveaway_rejects_end_before_publish_time(session):
    persona = _create_persona(session, slug="legacy-bad-end")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    scheduled_for = datetime.now(timezone.utc) + timedelta(hours=2)

    with pytest.raises(ValueError, match="must be after the scheduled publish time"):
        create_scheduled_post(
            session,
            _legacy_instagram_giveaway_payload(
                persona.id,
                [instagram.id],
                scheduled_for=scheduled_for,
                giveaway_end_at=scheduled_for - timedelta(minutes=5),
            ),
            [],
        )


def test_publish_now_rejects_giveaway_that_already_ended(session):
    persona = _create_persona(session, slug="giveaway-send-now-expired")
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [bluesky.id],
            giveaway_end_at=datetime.now(timezone.utc) - timedelta(minutes=5),
            channels=[
                {
                    "service": "bluesky",
                    "account_id": bluesky.id,
                    "rules": {
                        "kind": "all",
                        "children": [
                            {"kind": "atom", "atom": "reply_or_quote_present", "params": {}},
                        ],
                    },
                }
            ],
        ),
        [],
    )

    with pytest.raises(ValueError, match="Giveaway end time must be after the scheduled publish time"):
        schedule_post_now(session, post)


def test_legacy_instagram_payload_migrates_to_generic_campaign(session):
    persona = _create_persona(session, slug="legacy-to-generic")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")

    post = create_scheduled_post(
        session,
        _legacy_instagram_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
        ),
        [],
    )

    assert post.post_type == "giveaway"
    assert post.giveaway_campaign is not None
    assert post.giveaway_campaign.pool_mode == "combined"
    assert len(post.giveaway_campaign.channels) == 1
    channel = post.giveaway_campaign.channels[0]
    assert channel.service == "instagram"
    assert channel.account_id == instagram.id
    assert channel.rules_json["kind"] == "all"
    assert any(child["atom"] == "comment_present" for child in channel.rules_json["children"])
    assert any(child["atom"] == "friend_mention_count_gte" for child in channel.rules_json["children"])
    assert any(child["atom"] == "story_mention_present" for child in channel.rules_json["children"])


def test_instagram_webhook_ingest_updates_generic_entrant_state(session):
    persona = _create_persona(session, slug="giveaway-webhook-match")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _legacy_instagram_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-1", external_url="https://instagram.test/p/ig-media-1/")
    session.flush()

    payload = {
        "entry": [
            {
                "id": "17841463479494132",
                "changes": [
                    {
                        "field": "comments",
                        "value": {
                            "media_id": "ig-media-1",
                            "id": "comment-1",
                            "text": "Count me in @friend",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    },
                    {
                        "field": "mentions",
                        "value": {
                            "media_id": "ig-media-1",
                            "story_id": "story-1",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    },
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-1")
    session.flush()
    refreshed = get_post(session, post.id)
    assert refreshed is not None
    assert len(events) == 2
    assert refreshed.giveaway_campaign is not None
    channel = next(item for item in refreshed.giveaway_campaign.channels if item.service == "instagram")
    assert len(channel.entrants) == 1
    entrant = channel.entrants[0]
    assert entrant.provider_username == "entrant.one"
    assert entrant.signal_state_json["comment_count"] == 1
    assert entrant.signal_state_json["friend_mention_count"] == 1
    assert entrant.signal_state_json["story_mention_count"] == 1


def test_instagram_webhook_observability_summarizes_recent_events(session):
    session.add(
        InstagramGiveawayWebhookEvent(
            provider_event_field="comments",
            event_type="comment",
            payload_json={
                "entry": {"id": "instagram-account"},
                "change": {
                    "field": "comments",
                    "value": {
                        "id": "comment-1",
                        "text": "Count me in @friend",
                        "from": {"id": "user-1", "username": "entrant.one"},
                    },
                },
            },
            signature_valid=True,
            processed=True,
        )
    )
    session.add(
        InstagramGiveawayWebhookEvent(
            provider_event_field="messages",
            event_type="message",
                payload_json={
                    "entry": {"id": "instagram-account"},
                    "change": {
                        "field": "messages",
                        "value": {
                            "message": {
                                "mid": "mid-1",
                                "text": "Shared giveaway post",
                                "attachments": [
                                    {
                                        "type": "share",
                                        "payload": {"ig_post_media_id": "ig-media-1", "title": "Giveaway post"},
                                    }
                                ],
                            },
                            "from": {"id": "user-2", "username": "share.user"},
                        },
                    },
                },
            signature_valid=True,
            processed=True,
        )
    )
    session.flush()

    observability = instagram_webhook_observability(session, window_days=7, recent_limit=10, field_limit=5)

    assert observability["total_events"] == 2
    assert observability["giveaway_relevant_events"] >= 2
    assert any(item["key"] == "comments" for item in observability["field_chart"])
    assert any(event["field_label"] == "Shared Post" for event in observability["recent_events"])


def test_process_giveaway_lifecycle_selects_verified_instagram_winner(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-finalize-instagram")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _legacy_instagram_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-finalize")
    session.flush()

    payload = {
        "entry": [
            {
                "id": "17841463479494132",
                "changes": [
                    {
                        "field": "comments",
                        "value": {
                            "media_id": "ig-media-finalize",
                            "id": "comment-1",
                            "text": "Count me in @friend",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    },
                    {
                        "field": "mentions",
                        "value": {
                            "media_id": "ig-media-finalize",
                            "story_id": "story-1",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    },
                ],
            }
        ]
    }
    ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-2")

    class _LiveComment:
        def __init__(self):
            self.pk = "comment-1"
            self.text = "Count me in @friend"
            self.created_at_utc = datetime.now(timezone.utc)
            self.user = SimpleNamespace(pk="user-1", username="entrant.one")

    class _LiveCommentClient:
        def media_comments(self, media_id, amount=0):
            return [_LiveComment()]

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _LiveCommentClient())

    process_instagram_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-3")

    refreshed = get_post(session, post.id)
    assert refreshed is not None
    assert refreshed.giveaway_campaign is not None
    assert refreshed.giveaway_campaign.status == GIVEAWAY_STATUS_WINNER_SELECTED
    pool = refreshed.giveaway_campaign.pools[0]
    assert pool.final_winner_entry is not None
    assert pool.final_winner_entry.provider_username == "entrant.one"
    assert pool.final_winner_entry.eligibility_status == ENTRY_STATUS_ELIGIBLE


def test_process_giveaway_lifecycle_creates_separate_winners_for_mixed_channels(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-mixed-pools")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id, bluesky.id],
            giveaway_end_at=datetime.now(timezone.utc) - timedelta(minutes=5),
            pool_mode="separate",
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "comment_present", "params": {}}],
                    },
                },
                {
                    "service": "bluesky",
                    "account_id": bluesky.id,
                    "rules": {
                        "kind": "all",
                        "children": [
                            {"kind": "atom", "atom": "reply_or_quote_present", "params": {}},
                            {"kind": "atom", "atom": "like_present", "params": {}},
                            {"kind": "atom", "atom": "follow_present", "params": {}},
                        ],
                    },
                },
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-mixed")
    _mark_posted(post, bluesky.id, external_id="bsky-rkey", external_url="https://bsky.app/profile/savannah.test/post/bsky-rkey")
    session.flush()

    campaign = post.giveaway_campaign
    assert campaign is not None
    instagram_channel = next(channel for channel in campaign.channels if channel.service == "instagram")
    bluesky_channel = next(channel for channel in campaign.channels if channel.service == "bluesky")
    instagram_channel.target_post_external_id = "ig-media-mixed"
    bluesky_channel.target_post_external_id = "bsky-rkey"
    bluesky_channel.target_post_uri = "at://did:plc:test/app.bsky.feed.post/bsky-rkey"
    instagram_channel.entrants.append(
        GiveawayEntrant(
            channel=instagram_channel,
            provider_user_id="ig-user-1",
            provider_username="ig.one",
            display_label="ig.one",
            signal_state_json={
                "comments": [{"comment_id": "comment-1", "text": "ready"}],
                "comment_count": 1,
                "friend_mention_count": 0,
                "story_mentions": [],
                "story_mention_count": 0,
            },
        )
    )
    bluesky_channel.entrants.append(
        GiveawayEntrant(
            channel=bluesky_channel,
            provider_user_id="did:plc:user-1",
            provider_username="bsky.one",
            display_label="bsky.one",
            signal_state_json={
                "reply_present": True,
                "quote_present": False,
                "like_present": True,
                "repost_present": False,
                "follow_present": True,
                "reply_posts": [{"uri": "at://did:plc:user-1/app.bsky.feed.post/reply-1", "text": "count me in"}],
                "quote_posts": [],
                "reply_or_quote_mention_count": 1,
            },
        )
    )

    monkeypatch.setattr("app.services.giveaway_engine.hydrate_channel_targets", lambda campaign: None)
    monkeypatch.setattr("app.services.giveaway_engine.refresh_instagram_channel_state", lambda session, channel: None)
    monkeypatch.setattr("app.services.giveaway_engine.collect_bluesky_channel_state", lambda session, channel, run_id: None)

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-mixed")

    refreshed = get_post(session, post.id)
    assert refreshed is not None
    assert refreshed.giveaway_campaign is not None
    pools = {pool.pool_key: pool for pool in refreshed.giveaway_campaign.pools}
    assert set(pools) == {"instagram", "bluesky"}
    assert pools["instagram"].final_winner_entry is not None
    assert pools["instagram"].final_winner_entry.provider_username == "ig.one"
    assert pools["bluesky"].final_winner_entry is not None
    assert pools["bluesky"].final_winner_entry.provider_username == "bsky.one"
    serialized = serialize_giveaway(refreshed.giveaway_campaign)
    assert serialized is not None
    assert serialized.audit_summary.engagement_activities >= 2
    assert serialized.channels[0].summary.engagement_activities >= 1
    assert serialized.pools[0].selection_log is not None
    assert serialized.pools[0].selection_log.candidates


def test_collect_bluesky_channel_state_captures_reply_quote_like_repost_and_follow(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-bluesky-collector")
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [bluesky.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "bluesky",
                    "account_id": bluesky.id,
                    "rules": {
                        "kind": "all",
                        "children": [
                            {"kind": "atom", "atom": "reply_or_quote_present", "params": {}},
                            {"kind": "atom", "atom": "reply_or_quote_mention_count_gte", "params": {"count": 1}},
                            {"kind": "atom", "atom": "like_present", "params": {}},
                            {"kind": "atom", "atom": "follow_present", "params": {}},
                            {"kind": "atom", "atom": "repost_present", "params": {}},
                        ],
                    },
                }
            ],
        ),
        [],
    )
    channel = post.giveaway_campaign.channels[0]
    channel.target_post_uri = "at://did:plc:owner/app.bsky.feed.post/post-1"
    channel.target_post_cid = "cid-1"
    session.flush()

    class _FakeBlueskyClient:
        def __init__(self):
            feed = SimpleNamespace(
                get_likes=lambda params: _DumpableResponse(
                    {
                        "likes": [
                            {"actor": {"did": "did:plc:user-1", "handle": "bsky.one"}},
                        ]
                    }
                ),
                get_reposted_by=lambda params: _DumpableResponse(
                    {
                        "repostedBy": [
                            {"did": "did:plc:user-1", "handle": "bsky.one"},
                        ]
                    }
                ),
                get_quotes=lambda params: _DumpableResponse(
                    {
                        "posts": [
                                {
                                    "uri": "at://did:plc:user-1/app.bsky.feed.post/quote-1",
                                    "record": {"text": "@brand.test entering the giveaway"},
                                    "author": {"did": "did:plc:user-1", "handle": "bsky.one"},
                                }
                            ]
                        }
                ),
                get_post_thread=lambda params: _DumpableResponse(
                    {
                        "thread": {
                            "post": {"cid": "cid-1"},
                            "replies": [
                                {
                                    "post": {
                                        "uri": "at://did:plc:user-1/app.bsky.feed.post/reply-1",
                                            "record": {
                                                "text": "@brand.test count me in",
                                                "reply": {
                                                    "parent": {"uri": "at://did:plc:owner/app.bsky.feed.post/post-1"},
                                                },
                                            },
                                        "author": {"did": "did:plc:user-1", "handle": "bsky.one"},
                                    },
                                    "replies": [],
                                }
                            ],
                        }
                    }
                ),
            )
            graph = SimpleNamespace(
                get_relationships=lambda params: _DumpableResponse(
                    {
                        "relationships": [
                            {"did": "did:plc:user-1", "followedBy": True},
                        ]
                    }
                )
            )
            self.app = SimpleNamespace(bsky=SimpleNamespace(feed=feed, graph=graph))

    monkeypatch.setattr("app.services.giveaway_engine._get_bluesky_client", lambda credentials: _FakeBlueskyClient())

    collect_bluesky_channel_state(session, channel, run_id="run-bsky")

    assert len(channel.entrants) == 1
    entrant = channel.entrants[0]
    assert entrant.provider_username == "bsky.one"
    assert entrant.signal_state_json["reply_present"] is True
    assert entrant.signal_state_json["quote_present"] is True
    assert entrant.signal_state_json["like_present"] is True
    assert entrant.signal_state_json["repost_present"] is True
    assert entrant.signal_state_json["follow_present"] is True
    assert entrant.signal_state_json["reply_or_quote_mention_count"] >= 1


def test_giveaway_lifecycle_records_bluesky_collection_failures(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-bluesky-collection-failure")
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [bluesky.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "bluesky",
                    "account_id": bluesky.id,
                    "rules": {
                        "kind": "all",
                        "children": [
                            {"kind": "atom", "atom": "reply_or_quote_present", "params": {}},
                        ],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(
        post,
        bluesky.id,
        external_id="post-1",
        external_url="https://bsky.app/profile/savannah.test/post/post-1",
    )
    session.flush()

    def fail_collection(session, channel, run_id):
        raise TimeoutError("collector timed out")

    monkeypatch.setattr("app.services.giveaway_engine.collect_bluesky_channel_state", fail_collection)

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-bsky-failure")

    channel = post.giveaway_campaign.channels[0]
    assert post.giveaway_campaign.status == GIVEAWAY_STATUS_COLLECTING
    assert channel.last_error == "Bluesky giveaway collection failed: collector timed out"
    alert = session.query(AlertEvent).filter(AlertEvent.event_type == "giveaway_collection_failed").one()
    assert alert.post_id == post.id
    assert alert.service == "bluesky"


@pytest.fixture()
def giveaway_api_stack(monkeypatch, tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'giveaway-api.db'}", future=True, connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, class_=Session)
    Base.metadata.create_all(engine)

    @contextmanager
    def _db_session_override():
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr("app.main.db_session", _db_session_override)
    monkeypatch.setattr("app.main.bootstrap", lambda: None)
    monkeypatch.setattr("app.main.CrossposterScheduler.start", lambda self: None)
    monkeypatch.setattr("app.main.CrossposterScheduler.stop", lambda self: None)
    monkeypatch.setattr(
        "app.main.build_principal_from_request",
        lambda request: Principal(
            user_id="admin-user",
            display_name="Lynx",
            role="admin",
            timezone="UTC",
            is_authenticated=True,
        ),
    )

    previous = {key: os.environ.get(key) for key in ("APP_ENV_FILE", "INSTAGRAM_WEBHOOKS_ENABLED", "INSTAGRAM_WEBHOOK_VERIFY_TOKEN", "INSTAGRAM_APP_SECRET")}
    env_path = tmp_path / ".env"
    os.environ["APP_ENV_FILE"] = str(env_path)
    os.environ["INSTAGRAM_WEBHOOKS_ENABLED"] = "true"
    os.environ["INSTAGRAM_WEBHOOK_VERIFY_TOKEN"] = "verify-me"
    os.environ["INSTAGRAM_APP_SECRET"] = "webhook-secret"
    reload_settings()

    with TestClient(app) as client:
        yield client, SessionLocal

    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    reload_settings()
    Base.metadata.drop_all(engine)
    engine.dispose()


def test_instagram_webhook_verification_and_signature_api(giveaway_api_stack):
    api_client, SessionLocal = giveaway_api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="giveaway-webhook-api")
        instagram = _create_account(session, persona, service="instagram", label="Instagram")
        post = create_scheduled_post(
            session,
            _legacy_instagram_giveaway_payload(
                persona.id,
                [instagram.id],
                giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            ),
            [],
        )
        _mark_posted(post, instagram.id, external_id="ig-media-4")
        session.commit()

    verify_response = api_client.get(
        "/webhooks/instagram",
        params={"hub.mode": "subscribe", "hub.verify_token": "verify-me", "hub.challenge": "challenge-123"},
    )
    assert verify_response.status_code == 200
    assert verify_response.text == "challenge-123"

    payload = {
        "entry": [
            {
                "id": "17841463479494132",
                "changes": [
                    {
                        "field": "comments",
                        "value": {
                            "media_id": "ig-media-4",
                            "id": "comment-1",
                            "text": "Entry with @friend",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    }
                ],
            }
        ]
    }
    raw_body = json.dumps(payload).encode("utf-8")
    signature = "sha256=" + hmac.new(b"webhook-secret", raw_body, hashlib.sha256).hexdigest()

    bad_signature_response = api_client.post(
        "/webhooks/instagram",
        content=raw_body,
        headers={"X-Hub-Signature-256": "sha256=bad"},
    )
    assert bad_signature_response.status_code == 401
    with SessionLocal() as session:
        alert = session.query(AlertEvent).filter(AlertEvent.event_type == "instagram_webhook_rejected").one()
        assert alert.service == "instagram"
        assert alert.operation == "webhook"

    ok_response = api_client.post(
        "/webhooks/instagram",
        content=raw_body,
        headers={"X-Hub-Signature-256": signature, "Content-Type": "application/json"},
    )
    assert ok_response.status_code == 200
    assert ok_response.json()["stored_events"] == 1

    with SessionLocal() as session:
        stored_event = session.query(InstagramGiveawayWebhookEvent).filter(InstagramGiveawayWebhookEvent.signature_valid.is_(True)).order_by(InstagramGiveawayWebhookEvent.created_at.desc()).first()
        assert stored_event is not None
        refreshed = get_post(session, post.id)
        assert refreshed is not None
        assert refreshed.giveaway_campaign is not None
        channel = next(item for item in refreshed.giveaway_campaign.channels if item.service == "instagram")
        assert len(channel.entrants) == 1
        assert channel.entrants[0].provider_username == "entrant.one"
