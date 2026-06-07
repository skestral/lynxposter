from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
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
from app.domain import MediaItem, PublishResult
from app.models import AlertEvent, GiveawayEntrant, GiveawayEvidenceEvent, InstagramGiveawayWebhookEvent, MediaAttachment, RunEvent
from app.schemas import ScheduledPostCreate
from app.services.alerts import AlertDispatcher
from app.services.auth import Principal
from app.services.giveaway_engine import (
    ENTRY_STATUS_DISQUALIFIED,
    ENTRY_STATUS_ELIGIBLE,
    ENTRY_STATUS_PROVISIONAL,
    GIVEAWAY_STATUS_COLLECTING,
    GIVEAWAY_STATUS_REVIEW_REQUIRED,
    GIVEAWAY_STATUS_WINNER_SELECTED,
    approve_giveaway_entrant,
    clear_giveaway_entrant_approval,
    collect_bluesky_channel_state,
    end_giveaway_campaign,
    evaluate_channel_entrants,
    process_giveaway_lifecycle,
    recalculate_giveaway_entries,
    refresh_instagram_channel_state,
    scan_instagram_giveaway_channels,
    serialize_giveaway,
)
from app.services.giveaways import (
    instagram_webhook_observability,
    ingest_instagram_webhook_payload,
    process_instagram_giveaway_lifecycle,
)
from app.services.personas import create_account, create_persona
from app.services.delivery import process_delivery_queue
from app.services.posts import create_scheduled_post, get_post, schedule_post_now
from app.services.storage import settings as storage_settings


@contextmanager
def _instagram_private_scan_mode(mode: str):
    previous = os.environ.get("INSTAGRAM_PRIVATE_SCAN_MODE")
    os.environ["INSTAGRAM_PRIVATE_SCAN_MODE"] = mode
    reload_settings()
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("INSTAGRAM_PRIVATE_SCAN_MODE", None)
        else:
            os.environ["INSTAGRAM_PRIVATE_SCAN_MODE"] = previous
        reload_settings()


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
    winner_count: int = 1,
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
                "winner_count": winner_count,
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


def test_manual_giveaway_entrant_approval_survives_recalculation(session):
    persona = _create_persona(session, slug="giveaway-manual-entrant-approval")
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
                            {"kind": "atom", "atom": "like_present", "params": {}},
                        ],
                    },
                }
            ],
        ),
        [],
    )
    channel = post.giveaway_campaign.channels[0]
    entrant = GiveawayEntrant(
        channel=channel,
        provider_user_id="did:plc:manual",
        provider_username="manual.test",
        display_label="manual.test",
        signal_state_json={
            "reply_present": True,
            "quote_present": False,
            "like_present": False,
            "repost_present": False,
            "follow_present": None,
            "reply_posts": [{"uri": "at://did:plc:manual/app.bsky.feed.post/reply", "text": "Count me in"}],
            "quote_posts": [],
            "reply_or_quote_mention_count": 0,
        },
    )
    channel.entrants.append(entrant)
    session.flush()

    evaluate_channel_entrants(channel)
    assert entrant.eligibility_status == ENTRY_STATUS_DISQUALIFIED

    approve_giveaway_entrant(
        session,
        post.giveaway_campaign,
        entrant_id=entrant.id,
        run_id="run-manual-approval",
        note="Verified outside the automatic checks.",
        reviewed_by="Savannah",
    )
    session.flush()
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert entrant.signal_state_json["manual_review"]["status"] == "approved"

    recalculate_giveaway_entries(session, post.giveaway_campaign, run_id="run-recalculate-after-manual")
    serialized = serialize_giveaway(post.giveaway_campaign)
    serialized_entrant = serialized.channels[0].entrants[0]
    assert serialized_entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert serialized_entrant.manual_review_status == "approved"
    assert serialized_entrant.manual_review_note == "Verified outside the automatic checks."
    assert any(check.atom == "manual_review" and check.status == "passed" for check in serialized_entrant.checks)

    clear_giveaway_entrant_approval(
        session,
        post.giveaway_campaign,
        entrant_id=entrant.id,
        run_id="run-clear-manual-approval",
        reviewed_by="Savannah",
    )
    assert entrant.eligibility_status == ENTRY_STATUS_DISQUALIFIED
    assert "manual_review" not in entrant.signal_state_json


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
    comment_event = (
        session.query(GiveawayEvidenceEvent)
        .filter(
            GiveawayEvidenceEvent.event_type == "instagram_comment",
            GiveawayEvidenceEvent.source == "webhook_capture",
            GiveawayEvidenceEvent.provider_event_id == "comment-1",
        )
        .one()
    )
    assert comment_event.entrant_id == entrant.id


def test_instagram_webhook_comment_reply_mentions_count_for_parent_thread(session):
    persona = _create_persona(session, slug="giveaway-webhook-comment-reply")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    rules = {
        "kind": "all",
        "children": [
            {"kind": "atom", "atom": "comment_present", "params": {}},
            {"kind": "atom", "atom": "friend_mention_count_gte", "params": {"count": 1}},
        ],
    }
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[{"service": "instagram", "account_id": instagram.id, "rules": rules}],
        ),
        [],
    )
    other_post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[{"service": "instagram", "account_id": instagram.id, "rules": rules}],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-1")
    _mark_posted(other_post, instagram.id, external_id="ig-media-2")
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
                            "text": "Entering this one",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    },
                    {
                        "field": "comments",
                        "value": {
                            "parent_id": "comment-1",
                            "id": "reply-1",
                            "text": "Tagging @friend in the thread",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    },
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-thread-reply")
    session.flush()

    assert len(events) == 2
    assert events[1].matched_post_id == post.id
    assert events[1].matched_post_id != other_post.id
    channel = post.giveaway_campaign.channels[0]
    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="user-1").one()
    assert entrant.signal_state_json["comment_count"] == 2
    assert entrant.signal_state_json["friend_mention_count"] == 1
    assert any(
        item.get("comment_id") == "reply-1" and item.get("is_reply") is True
        for item in entrant.signal_state_json["comments"]
    )

    evaluate_channel_entrants(channel)
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE


def test_instagram_webhook_ingest_updates_generic_like_and_repost_state(session):
    persona = _create_persona(session, slug="giveaway-webhook-like-share")
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
    _mark_posted(post, instagram.id, external_id="ig-media-like-share", external_url="https://instagram.test/p/ig-media-like-share/")
    session.flush()

    payload = {
        "entry": [
            {
                "id": "17841463479494132",
                "changes": [
                    {
                        "field": "likes",
                        "value": {
                            "media_id": "ig-media-like-share",
                            "id": "like-1",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    },
                    {
                        "field": "shares",
                        "value": {
                            "media_id": "ig-media-like-share",
                            "id": "share-1",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    },
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-like-share")
    session.flush()

    assert [event.event_type for event in events] == ["like", "share"]
    channel = post.giveaway_campaign.channels[0]
    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="user-1").one()
    assert entrant.signal_state_json["like_present"] is True
    assert entrant.signal_state_json["repost_present"] is True
    assert len(entrant.signal_state_json["likes"]) == 1
    assert len(entrant.signal_state_json["reposts"]) == 1
    like_event = (
        session.query(GiveawayEvidenceEvent)
        .filter(
            GiveawayEvidenceEvent.event_type == "instagram_like",
            GiveawayEvidenceEvent.source == "webhook_capture",
            GiveawayEvidenceEvent.provider_event_id == "like-1",
        )
        .one()
    )
    repost_event = (
        session.query(GiveawayEvidenceEvent)
        .filter(
            GiveawayEvidenceEvent.event_type == "instagram_repost",
            GiveawayEvidenceEvent.source == "webhook_capture",
            GiveawayEvidenceEvent.provider_event_id == "share-1",
        )
        .one()
    )
    assert like_event.entrant_id == entrant.id
    assert repost_event.entrant_id == entrant.id


def test_instagram_story_mention_message_counts_as_repost_state(session):
    persona = _create_persona(session, slug="giveaway-webhook-story-share")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [
                            {"kind": "atom", "atom": "repost_present", "params": {}},
                        ],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-story-share")
    session.flush()

    payload = {
        "entry": [
            {
                "id": "17841463479494132",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "sender": {"id": "17841463479494132"},
                            "recipient": {"id": "user-story-share", "username": "story.user"},
                            "message": {
                                "mid": "story-share-mid-1",
                                "is_echo": True,
                                "attachments": [
                                    {
                                        "type": "story_mention",
                                        "payload": {"url": "https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=story-share-asset"},
                                    }
                                ],
                            },
                        },
                    }
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-story-share")
    session.flush()
    channel = post.giveaway_campaign.channels[0]
    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="user-story-share").one()

    assert [event.event_type for event in events] == ["story_mention"]
    assert entrant.signal_state_json["story_mention_count"] == 1
    assert entrant.signal_state_json["repost_present"] is True
    assert len(entrant.signal_state_json["reposts"]) == 1
    assert (
        session.query(GiveawayEvidenceEvent)
        .filter(
            GiveawayEvidenceEvent.event_type == "instagram_repost",
            GiveawayEvidenceEvent.source == "message_share_capture",
            GiveawayEvidenceEvent.provider_event_id == "story-share-mid-1",
        )
        .count()
        == 1
    )


def test_instagram_webhook_ingest_accepts_array_like_payload(session):
    persona = _create_persona(session, slug="giveaway-webhook-like-array")
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
    _mark_posted(post, instagram.id, external_id="ig-media-like-array")
    session.flush()

    payload = {
        "entry": [
            {
                "id": "17841463479494132",
                "changes": [
                    {
                        "field": "likes",
                        "value": [
                            {
                                "media_id": "ig-media-like-array",
                                "id": "like-array-1",
                                "from": {"id": "user-array-1", "username": "array.liker"},
                            }
                        ],
                    }
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-like-array")
    session.flush()

    assert len(events) == 1
    assert events[0].event_type == "like"
    channel = post.giveaway_campaign.channels[0]
    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="user-array-1").one()
    assert entrant.provider_username == "array.liker"
    assert entrant.signal_state_json["like_present"] is True
    assert session.query(GiveawayEvidenceEvent).filter_by(
        channel_id=channel.id,
        event_type="instagram_like",
        provider_event_id="like-array-1",
    ).one()


def test_refresh_instagram_channel_state_collects_live_likes(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-live-like-collector")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "like_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-live-like")
    channel = post.giveaway_campaign.channels[0]
    channel.target_post_external_id = "ig-media-live-like"
    session.flush()

    class _FakeInstagramClient:
        def media_comments(self, media_id, amount=0):
            assert media_id == "ig-media-live-like"
            return []

        def media_likers(self, media_id):
            assert media_id == "ig-media-live-like"
            return [SimpleNamespace(pk="ig-user-like", username="liker.one")]

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _FakeInstagramClient())

    refresh_instagram_channel_state(session, channel, force_private_scan=True)

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-like").one()
    assert entrant.provider_username == "liker.one"
    assert entrant.signal_state_json["like_present"] is True
    assert entrant.signal_state_json["likes"][0]["source"] == "live_collection"
    like_event = session.query(GiveawayEvidenceEvent).filter_by(
        channel_id=channel.id,
        event_type="instagram_like",
        source="live_collection",
    ).one()
    assert like_event.entrant_id == entrant.id
    assert like_event.active is True


def test_refresh_instagram_channel_state_uses_permalink_when_graph_media_id_is_unavailable(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-instagram-media-pk-fallback")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "like_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(
        post,
        instagram.id,
        external_id="17841463479494132",
        external_url="https://www.instagram.com/p/DYVeqi8jwhg/",
    )
    channel = post.giveaway_campaign.channels[0]
    channel.target_post_external_id = "17841463479494132"
    channel.target_post_url = "https://www.instagram.com/p/DYVeqi8jwhg/"
    entrant = GiveawayEntrant(
        provider_user_id="2045697446345302",
        provider_username="pawgetsound.studio",
        display_label="pawgetsound.studio",
        signal_state_json={},
    )
    channel.entrants.append(entrant)
    session.flush()

    class _FakeInstagramClient:
        def media_pk_from_url(self, url):
            assert url == "https://www.instagram.com/p/DYVeqi8jwhg/"
            return "private-media-pk"

        def media_comments(self, media_id, amount=0):
            if media_id == "17841463479494132":
                raise RuntimeError("Media not found or unavailable")
            assert media_id == "private-media-pk"
            return []

        def media_likers(self, media_id):
            if media_id == "17841463479494132":
                raise RuntimeError("Media not found or unavailable")
            assert media_id == "private-media-pk"
            return [SimpleNamespace(pk="2045697446345302", username="pawgetsound.studio")]

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _FakeInstagramClient())

    refresh_instagram_channel_state(session, channel, force_private_scan=True)

    assert channel.last_error is None
    assert entrant.signal_state_json["like_present"] is True
    assert entrant.signal_state_json["likes"][0]["media_id"] == "private-media-pk"


def test_giveaway_lifecycle_skips_recent_instagram_private_scan(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-private-scan-throttle")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "like_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-throttle")
    channel = post.giveaway_campaign.channels[0]
    previous_scan_at = datetime.now(timezone.utc)
    channel.last_private_collected_at = previous_scan_at
    session.flush()

    def _fail_private_client(credentials):
        raise AssertionError("Private Instagram scan should be throttled.")

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", _fail_private_client)

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-private-scan-throttle")

    assert session.query(GiveawayEntrant).filter_by(channel_id=channel.id).count() == 0
    assert channel.last_private_collected_at == previous_scan_at


def test_manual_instagram_scan_ignores_private_scan_interval(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-manual-private-scan")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "like_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-manual")
    channel = post.giveaway_campaign.channels[0]
    channel.last_private_collected_at = datetime.now(timezone.utc)
    session.flush()

    class _FakeInstagramClient:
        def media_comments(self, media_id, amount=0):
            assert media_id == "ig-media-manual"
            return []

        def media_likers(self, media_id):
            assert media_id == "ig-media-manual"
            return [SimpleNamespace(pk="ig-user-manual", username="manual.liker")]

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _FakeInstagramClient())

    scan_instagram_giveaway_channels(session, post.giveaway_campaign, run_id="run-manual-private-scan")

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-manual").one()
    assert entrant.provider_username == "manual.liker"
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    scan_event = session.query(RunEvent).filter_by(service="instagram", operation="giveaway_private_scan").order_by(RunEvent.created_at.desc()).first()
    assert scan_event is not None
    assert scan_event.metadata_json["private_scan_reason"] == "manual"


def test_manual_instagram_scan_uses_follower_list_for_follow_verification(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-manual-follow-batch")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "follow_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-follow-batch")
    channel = post.giveaway_campaign.channels[0]
    for index in range(205):
        user_id = f"ig-user-{index:03d}"
        channel.entrants.append(
            GiveawayEntrant(
                provider_user_id=user_id,
                provider_username=f"follower.{index:03d}",
                display_label=f"follower.{index:03d}",
                signal_state_json={},
            )
        )
    session.flush()
    follower_calls: list[tuple[str, bool, int]] = []

    class _BatchFollowClient:
        user_id = "ig-account-id"

        def user_followers(self, user_id, use_cache=True, amount=0):
            follower_calls.append((user_id, use_cache, amount))
            return {
                f"ig-user-{index:03d}": SimpleNamespace(pk=f"ig-user-{index:03d}")
                for index in range(205)
            }

        def user_friendships_v1(self, user_ids):
            raise AssertionError("Private scan should use follower list membership for follow verification.")

        def user_friendship_v1(self, user_id):
            raise AssertionError("Private scan should not verify each follow individually.")

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _BatchFollowClient())

    scan_instagram_giveaway_channels(session, post.giveaway_campaign, run_id="run-manual-follow-batch")

    assert follower_calls == [("ig-account-id", False, 0)]
    assert session.query(GiveawayEntrant).filter_by(channel_id=channel.id, eligibility_status=ENTRY_STATUS_ELIGIBLE).count() == 205


def test_manual_instagram_scan_marks_absent_follower_list_membership_false(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-manual-follow-sparse")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "follow_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-follow-sparse")
    channel = post.giveaway_campaign.channels[0]
    channel.entrants.extend(
        [
            GiveawayEntrant(
                provider_user_id="1000373192696665",
                provider_username="sparse.response",
                display_label="sparse.response",
                signal_state_json={},
            ),
            GiveawayEntrant(
                provider_user_id="1000373192696666",
                provider_username="full.response",
                display_label="full.response",
                signal_state_json={},
            ),
        ]
    )
    session.flush()
    follower_calls: list[tuple[str, bool, int]] = []

    class _SparseFollowClient:
        user_id = "ig-account-id"

        def user_followers(self, user_id, use_cache=True, amount=0):
            follower_calls.append((user_id, use_cache, amount))
            return {
                "1000373192696666": SimpleNamespace(pk="1000373192696666"),
            }

        def private_request(self, endpoint, data=None, with_signature=True):
            raise AssertionError("Follow verification should not use friendships/show_many.")

        def user_friendship_v1(self, user_id):
            raise AssertionError("Private scan should not verify each follow individually.")

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _SparseFollowClient())

    scan_instagram_giveaway_channels(session, post.giveaway_campaign, run_id="run-manual-follow-sparse")

    sparse = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="1000373192696665").one()
    full = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="1000373192696666").one()
    assert channel.last_error is None
    assert sparse.signal_state_json["follow_collection_checked"] is True
    assert sparse.signal_state_json["follow_present"] is False
    assert sparse.eligibility_status == ENTRY_STATUS_DISQUALIFIED
    assert full.signal_state_json["follow_present"] is True
    assert full.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert follower_calls == [("ig-account-id", False, 0)]


def test_instagram_like_follow_private_scan_skips_unneeded_comment_and_story_calls(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-like-follow-skip-story")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [
                            {"kind": "atom", "atom": "like_present", "params": {}},
                            {"kind": "atom", "atom": "follow_present", "params": {}},
                        ],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-like-follow")
    channel = post.giveaway_campaign.channels[0]
    channel.entrants.append(
        GiveawayEntrant(
            provider_user_id="ig-user-like-follow",
            provider_username="liker.follower",
            display_label="liker.follower",
            signal_state_json={},
        )
    )
    session.flush()
    calls = {"likers": 0, "followers": 0}

    class _LikeFollowClient:
        user_id = "ig-account-id"

        def media_comments(self, media_id, amount=0):
            raise AssertionError("Like+follow scans should not fetch comments when no comment rule is present.")

        def media_likers(self, media_id):
            calls["likers"] += 1
            assert media_id == "ig-media-like-follow"
            return [SimpleNamespace(pk="ig-user-like-follow", username="liker.follower")]

        def user_followers(self, user_id, use_cache=True, amount=0):
            calls["followers"] += 1
            assert (user_id, use_cache, amount) == ("ig-account-id", False, 0)
            return {"ig-user-like-follow": SimpleNamespace(pk="ig-user-like-follow")}

        def user_friendships_v1(self, user_ids):
            raise AssertionError("Like+follow scans should use follower list membership.")

        def user_stories(self, user_id):
            raise AssertionError("Like+follow scans should not inspect stories without a repost rule.")

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _LikeFollowClient())

    scan_instagram_giveaway_channels(session, post.giveaway_campaign, run_id="run-like-follow-skip-story")

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-like-follow").one()
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert entrant.signal_state_json["like_present"] is True
    assert entrant.signal_state_json["follow_present"] is True
    assert calls == {"likers": 1, "followers": 1}


def test_end_giveaway_uses_graph_only_by_default_and_logs_blocked_private_scan(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-end-graph-only")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "follow_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-end-graph")
    channel = post.giveaway_campaign.channels[0]
    channel.entrants.append(
        GiveawayEntrant(
            provider_user_id="ig-user-review",
            provider_username="review.one",
            display_label="review.one",
            signal_state_json={},
        )
    )
    session.flush()

    monkeypatch.setattr(
        "app.services.giveaway_engine._authenticated_publish_client",
        lambda credentials: pytest.fail("Default giveaway close should not use private Instagram login."),
    )

    end_giveaway_campaign(session, post.giveaway_campaign, AlertDispatcher(), run_id="run-end-graph-only")

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-review").one()
    assert entrant.eligibility_status == ENTRY_STATUS_PROVISIONAL
    assert channel.last_private_collected_at is None
    blocked_event = (
        session.query(RunEvent)
        .filter_by(service="instagram", operation="giveaway_private_scan", severity="warning")
        .order_by(RunEvent.created_at.desc())
        .first()
    )
    assert blocked_event is not None
    assert blocked_event.metadata_json["private_scan_status"] == "blocked"


def test_due_instagram_scan_preserves_manual_private_scan_evidence(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-due-scan-preserves-manual")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "like_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-preserve")
    channel = post.giveaway_campaign.channels[0]
    channel.last_private_collected_at = datetime.now(timezone.utc) - timedelta(days=8)
    entrant = GiveawayEntrant(
        provider_user_id="ig-user-manual",
        provider_username="manual.liker",
        display_label="manual.liker",
        signal_state_json={
            "likes": [
                {
                    "like_id": "like:ig-user-manual:ig-media-preserve",
                    "media_id": "ig-media-preserve",
                    "actor_id": "ig-user-manual",
                    "source": "live_collection",
                }
            ],
            "like_present": True,
            "like_collection_checked": True,
        },
    )
    channel.entrants.append(entrant)
    session.flush()

    class _FakeInstagramClient:
        def media_comments(self, media_id, amount=0):
            assert media_id == "ig-media-preserve"
            return []

        def media_likers(self, media_id):
            assert media_id == "ig-media-preserve"
            return []

        def user_stories(self, user_id):
            return []

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _FakeInstagramClient())

    with _instagram_private_scan_mode("weekly"):
        process_giveaway_lifecycle(
            session,
            AlertDispatcher(),
            run_id="run-due-preserve-manual",
            allow_instagram_private_scan=True,
        )

    assert entrant.signal_state_json["like_present"] is True
    assert entrant.signal_state_json["likes"][0]["like_id"] == "like:ig-user-manual:ig-media-preserve"
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE


def test_manual_instagram_scan_captures_story_repost_for_existing_entrant(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-manual-private-repost")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [
                            {"kind": "atom", "atom": "comment_present", "params": {}},
                            {"kind": "atom", "atom": "like_present", "params": {}},
                            {"kind": "atom", "atom": "repost_present", "params": {}},
                        ],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(
        post,
        instagram.id,
        external_id="ig-media-manual-repost",
        external_url="https://instagram.test/p/abc123/",
    )
    channel = post.giveaway_campaign.channels[0]
    channel.last_private_collected_at = datetime.now(timezone.utc)
    session.flush()

    class _FakeInstagramClient:
        def media_comments(self, media_id, amount=0):
            assert media_id == "ig-media-manual-repost"
            return [
                SimpleNamespace(
                    pk="comment-1",
                    text="Tagging @friend",
                    user=SimpleNamespace(pk="ig-user-repost", username="repost.user"),
                )
            ]

        def media_likers(self, media_id):
            assert media_id == "ig-media-manual-repost"
            return [SimpleNamespace(pk="ig-user-repost", username="repost.user")]

        def user_stories(self, user_id):
            assert user_id == "ig-user-repost"
            return [
                SimpleNamespace(
                    pk="story-1",
                    medias=[SimpleNamespace(media_pk="ig-media-manual-repost", media_code="abc123")],
                )
            ]

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _FakeInstagramClient())

    scan_instagram_giveaway_channels(session, post.giveaway_campaign, run_id="run-manual-private-repost")

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-repost").one()
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert entrant.signal_state_json["repost_present"] is True
    assert entrant.signal_state_json["reposts"][0]["story_id"] == "story-1"
    repost_event = (
        session.query(GiveawayEvidenceEvent)
        .filter_by(channel_id=channel.id, event_type="instagram_repost", source="live_collection")
        .one()
    )
    assert repost_event.entrant_id == entrant.id


def test_instagram_evaluation_merges_webhook_and_private_ids_for_same_username(session):
    persona = _create_persona(session, slug="giveaway-instagram-merge-same-username")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [
                            {"kind": "atom", "atom": "comment_present", "params": {}},
                            {"kind": "atom", "atom": "like_present", "params": {}},
                        ],
                    },
                }
            ],
        ),
        [],
    )
    channel = post.giveaway_campaign.channels[0]
    webhook_entrant = GiveawayEntrant(
        provider_user_id="2045697446345302",
        provider_username="pawgetsound.studio",
        display_label="pawgetsound.studio",
        signal_state_json={
            "comments": [{"comment_id": "comment-1", "text": "@friend hello"}],
            "comment_count": 1,
            "friend_mention_count": 1,
        },
    )
    private_entrant = GiveawayEntrant(
        provider_user_id="39922605849",
        provider_username="pawgetsound.studio",
        display_label="pawgetsound.studio",
        signal_state_json={
            "likes": [{"like_id": "like-1", "actor_id": "39922605849"}],
            "like_present": True,
        },
    )
    channel.entrants.extend([webhook_entrant, private_entrant])
    session.flush()
    session.add(
        GiveawayEvidenceEvent(
            campaign_id=post.giveaway_campaign.id,
            channel_id=channel.id,
            entrant_id=webhook_entrant.id,
            provider_event_id="comment-1",
            event_type="instagram_comment",
            source="webhook",
            payload_json={},
        )
    )
    session.flush()

    evaluate_channel_entrants(channel)
    session.flush()

    entrants = session.query(GiveawayEntrant).filter_by(channel_id=channel.id).all()
    assert len(entrants) == 1
    entrant = entrants[0]
    assert entrant.provider_username == "pawgetsound.studio"
    assert entrant.signal_state_json["comment_count"] == 1
    assert entrant.signal_state_json["like_present"] is True
    assert set(entrant.signal_state_json["provider_user_id_aliases"]) == {"2045697446345302", "39922605849"}
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert session.query(GiveawayEvidenceEvent).filter_by(entrant_id=entrant.id).count() == 1


def test_giveaway_lifecycle_defers_instagram_follow_verification_without_private_scan(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-follow-private-guard")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "follow_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-follow")
    channel = post.giveaway_campaign.channels[0]
    channel.entrants.append(
        GiveawayEntrant(
            provider_user_id="ig-user-follow",
            provider_username="follower.one",
            display_label="follower.one",
            signal_state_json={},
        )
    )
    session.flush()

    def _fail_private_client(credentials):
        raise AssertionError("Autorun giveaway evaluation should not use private Instagram follow checks.")

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", _fail_private_client)

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-follow-private-guard")

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-follow").one()
    assert entrant.eligibility_status == ENTRY_STATUS_PROVISIONAL
    assert "waiting for a manual" in entrant.inconclusive_reasons_json[0]


def test_giveaway_lifecycle_defers_instagram_repost_without_official_evidence(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-repost-manual-review")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "repost_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-repost-manual-review")
    channel = post.giveaway_campaign.channels[0]
    channel.entrants.append(
        GiveawayEntrant(
            provider_user_id="ig-user-repost",
            provider_username="reposter.one",
            display_label="reposter.one",
            signal_state_json={},
        )
    )
    session.flush()

    def _fail_private_client(credentials):
        raise AssertionError("Graph-only evaluation should not crawl Instagram reposts.")

    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", _fail_private_client)

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-repost-private-guard")

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-repost").one()
    assert entrant.eligibility_status == ENTRY_STATUS_PROVISIONAL
    assert "Official Instagram APIs do not expose public profile repost checks" in entrant.inconclusive_reasons_json[0]


def test_instagram_graph_only_lifecycle_preserves_captured_private_follow_state(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-follow-preserve-autorun")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "follow_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-follow-preserve")
    channel = post.giveaway_campaign.channels[0]
    channel.entrants.append(
        GiveawayEntrant(
            provider_user_id="ig-user-follow",
            provider_username="follower.one",
            display_label="follower.one",
            signal_state_json={"follow_present": True, "follow_collection_checked": True},
        )
    )
    session.flush()

    monkeypatch.setattr(
        "app.services.giveaway_engine._authenticated_publish_client",
        lambda credentials: pytest.fail("Graph-only autorun should not rerun private follow checks."),
    )

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-follow-preserve-autorun")

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-follow").one()
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert entrant.signal_state_json["follow_present"] is True
    assert entrant.signal_state_json["follow_collection_checked"] is True


def test_giveaway_lifecycle_does_not_verify_follow_when_private_scan_not_due(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-follow-due-gate")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "follow_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-follow-due-gate")
    channel = post.giveaway_campaign.channels[0]
    channel.last_private_collected_at = datetime.now(timezone.utc)
    channel.entrants.append(
        GiveawayEntrant(
            provider_user_id="ig-user-follow",
            provider_username="follower.one",
            display_label="follower.one",
            signal_state_json={},
        )
    )
    session.flush()

    def _fail_private_client(credentials):
        raise AssertionError("Private Instagram follow checks should wait until the scan interval is due.")

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", _fail_private_client)

    with _instagram_private_scan_mode("weekly"):
        process_giveaway_lifecycle(
            session,
            AlertDispatcher(),
            run_id="run-follow-due-gate",
            allow_instagram_private_scan=True,
        )

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-follow").one()
    assert entrant.eligibility_status == ENTRY_STATUS_PROVISIONAL
    assert "waiting for a manual" in entrant.inconclusive_reasons_json[0]


def test_instagram_follow_verification_retries_transient_private_api_errors(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-follow-retry")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "follow_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-follow-retry")
    channel = post.giveaway_campaign.channels[0]
    channel.entrants.append(
        GiveawayEntrant(
            provider_user_id="ig-user-follow",
            provider_username="follower.one",
            display_label="follower.one",
            signal_state_json={},
        )
    )
    session.flush()
    calls = {"friendship": 0}

    class _FakeInstagramClient:
        def user_friendship_v1(self, user_id):
            calls["friendship"] += 1
            if calls["friendship"] == 1:
                raise RuntimeError("ResponseError('too many 500 error responses')")
            return SimpleNamespace(followed_by=True)

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _FakeInstagramClient())
    monkeypatch.setattr("app.services.giveaway_engine.time.sleep", lambda seconds: None)

    evaluate_channel_entrants(channel, allow_instagram_private_verification=True)

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-follow").one()
    assert calls["friendship"] == 2
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert entrant.inconclusive_reasons_json == []


def test_instagram_follow_verification_persists_for_public_rechecks(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-follow-persist")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "follow_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    channel = post.giveaway_campaign.channels[0]
    channel.entrants.append(
        GiveawayEntrant(
            provider_user_id="ig-user-follow",
            provider_username="follower.one",
            display_label="follower.one",
            signal_state_json={},
        )
    )
    session.flush()

    class _FollowingClient:
        def user_friendship_v1(self, user_id):
            assert user_id == "ig-user-follow"
            return SimpleNamespace(followed_by=True)

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _FollowingClient())

    evaluate_channel_entrants(channel, allow_instagram_private_verification=True)

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-follow").one()
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert entrant.signal_state_json["follow_present"] is True
    assert entrant.signal_state_json["follow_collection_checked"] is True

    monkeypatch.setattr(
        "app.services.giveaway_engine._authenticated_publish_client",
        lambda credentials: pytest.fail("Public rechecks should reuse the stored follow state."),
    )

    evaluate_channel_entrants(channel, allow_instagram_private_verification=False)

    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert entrant.signal_state_json["follow_present"] is True


def test_instagram_private_scan_can_record_a_real_unfollow(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-follow-unfollow")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "follow_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    channel = post.giveaway_campaign.channels[0]
    channel.entrants.append(
        GiveawayEntrant(
            provider_user_id="ig-user-follow",
            provider_username="follower.one",
            display_label="follower.one",
            signal_state_json={"follow_present": True, "follow_collection_checked": True},
        )
    )
    session.flush()

    class _NotFollowingClient:
        user_id = "ig-account-id"

        def user_followers(self, user_id, use_cache=True, amount=0):
            assert (user_id, use_cache, amount) == ("ig-account-id", False, 0)
            return {}

        def user_friendship_v1(self, user_id):
            raise AssertionError("Private scans should use follower list membership.")

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _NotFollowingClient())

    refresh_instagram_channel_state(session, channel, force_private_scan=True)
    evaluate_channel_entrants(channel, allow_instagram_private_verification=True)

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-follow").one()
    assert entrant.eligibility_status == ENTRY_STATUS_DISQUALIFIED
    assert entrant.signal_state_json["follow_present"] is False
    assert entrant.signal_state_json["follow_collection_checked"] is True


def test_giveaway_lifecycle_updates_qualification_checks_after_collection(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-live-checks")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "like_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-live-checks")
    session.flush()
    media_liker_calls = 0

    class _FakeInstagramClient:
        def media_comments(self, media_id, amount=0):
            assert media_id == "ig-media-live-checks"
            return []

        def media_likers(self, media_id):
            nonlocal media_liker_calls
            media_liker_calls += 1
            assert media_id == "ig-media-live-checks"
            return [SimpleNamespace(pk="ig-user-like", username="liker.one")]

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _FakeInstagramClient())

    with _instagram_private_scan_mode("weekly"):
        process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-live-checks", allow_instagram_private_scan=True)

    channel = post.giveaway_campaign.channels[0]
    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="ig-user-like").one()
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert entrant.rule_match_details_json["children"][0]["atom"] == "like_present"
    assert entrant.rule_match_details_json["children"][0]["result"] is True
    assert media_liker_calls == 1

    serialized = serialize_giveaway(post.giveaway_campaign)
    assert serialized is not None
    checks = serialized.channels[0].entrants[0].checks
    assert checks[0].label == "Like present"
    assert checks[0].status == "passed"


def test_serialize_giveaway_exposes_instagram_private_scan_when_job_is_posted(session):
    persona = _create_persona(session, slug="giveaway-serialize-private-scan")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {"kind": "all", "children": [{"kind": "atom", "atom": "comment_present", "params": {}}]},
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-posted-job", external_url="https://instagram.test/p/posted-job/")
    channel = post.giveaway_campaign.channels[0]
    channel.target_post_external_id = None
    channel.target_post_url = None
    session.flush()

    serialized = serialize_giveaway(post.giveaway_campaign)

    assert serialized is not None
    serialized_channel = serialized.channels[0]
    assert serialized_channel.target_post_external_id == "ig-posted-job"
    assert serialized_channel.target_post_url == "https://instagram.test/p/posted-job/"
    assert serialized_channel.private_scan_available is True


def test_instagram_refresh_backfills_existing_like_and_repost_webhooks(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-webhook-backfill")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [
                            {"kind": "atom", "atom": "like_present", "params": {}},
                            {"kind": "atom", "atom": "repost_present", "params": {}},
                        ],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-backfill", external_url="https://instagram.test/p/ig-media-backfill/")
    session.add_all(
        [
            InstagramGiveawayWebhookEvent(
                provider_event_field="likes",
                event_type="like",
                provider_object_id="like-backfill",
                payload_json={
                    "entry": {"id": "17841463479494132"},
                    "change": {
                        "field": "likes",
                        "value": {
                            "media_id": "ig-media-backfill",
                            "id": "like-backfill",
                            "from": {"id": "user-backfill", "username": "backfill.one"},
                        },
                    },
                },
                signature_valid=True,
                processed=True,
            ),
            InstagramGiveawayWebhookEvent(
                provider_event_field="shares",
                event_type="share",
                provider_object_id="share-backfill",
                payload_json={
                    "entry": {"id": "17841463479494132"},
                    "change": {
                        "field": "shares",
                        "value": {
                            "media_id": "ig-media-backfill",
                            "id": "share-backfill",
                            "from": {"id": "user-backfill", "username": "backfill.one"},
                        },
                    },
                },
                signature_valid=True,
                processed=True,
            ),
        ]
    )
    session.flush()
    channel = post.giveaway_campaign.channels[0]
    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: "Private API unavailable")

    refresh_instagram_channel_state(session, channel)
    evaluate_channel_entrants(channel)
    session.flush()

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="user-backfill").one()
    assert entrant.signal_state_json["like_present"] is True
    assert entrant.signal_state_json["repost_present"] is True
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert (
        session.query(GiveawayEvidenceEvent)
        .filter(GiveawayEvidenceEvent.event_type == "instagram_like", GiveawayEvidenceEvent.provider_event_id == "like-backfill")
        .count()
        == 1
    )
    assert (
        session.query(GiveawayEvidenceEvent)
        .filter(GiveawayEvidenceEvent.event_type == "instagram_repost", GiveawayEvidenceEvent.provider_event_id == "share-backfill")
        .count()
        == 1
    )


def test_instagram_refresh_backfills_story_mention_message_as_repost(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-webhook-story-share-backfill")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {
                        "kind": "all",
                        "children": [
                            {"kind": "atom", "atom": "repost_present", "params": {}},
                        ],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-story-backfill")
    session.add(
        InstagramGiveawayWebhookEvent(
            provider_event_field="messages",
            event_type="story_mention",
            provider_object_id="story-share-backfill-mid",
            payload_json={
                "entry": {"id": "17841463479494132"},
                "change": {
                    "field": "messages",
                    "value": {
                        "sender": {"id": "17841463479494132"},
                        "recipient": {"id": "user-story-backfill", "username": "story.backfill"},
                        "message": {
                            "mid": "story-share-backfill-mid",
                            "is_echo": True,
                            "attachments": [
                                {
                                    "type": "story_mention",
                                    "payload": {"url": "https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=story-backfill-asset"},
                                }
                            ],
                        },
                    },
                },
            },
            signature_valid=True,
            processed=True,
        )
    )
    session.flush()
    channel = post.giveaway_campaign.channels[0]
    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: "Private API unavailable")

    refresh_instagram_channel_state(session, channel)
    evaluate_channel_entrants(channel)
    session.flush()

    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="user-story-backfill").one()
    assert entrant.signal_state_json["story_mention_count"] == 1
    assert entrant.signal_state_json["repost_present"] is True
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert (
        session.query(GiveawayEvidenceEvent)
        .filter(GiveawayEvidenceEvent.event_type == "instagram_repost", GiveawayEvidenceEvent.provider_event_id == "story-share-backfill-mid")
        .count()
        == 1
    )


def test_instagram_webhook_ingest_matches_graph_media_permalink_to_giveaway(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-webhook-graph-match")
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
    _mark_posted(
        post,
        instagram.id,
        external_id="3897155917695617120_63393059983",
        external_url="https://www.instagram.com/p/DYVeqi8jwhg/",
    )
    session.flush()

    def graph_media_match(account, *, media_id):
        if account.id == instagram.id and media_id == "17890000000000000":
            return {
                "id": "17890000000000000",
                "href": "https://www.instagram.com/p/DYVeqi8jwhg/",
                "label": "Test giveaway post",
            }
        return None

    monkeypatch.setattr("app.services.giveaways._instagram_graph_media_match", graph_media_match)

    payload = {
        "entry": [
            {
                "id": "17841463479494132",
                "changes": [
                    {
                        "field": "comments",
                        "value": {
                            "media": {"id": "17890000000000000", "media_product_type": "FEED"},
                            "id": "comment-graph-1",
                            "text": "Graph id comment @friend",
                            "from": {"id": "user-graph", "username": "graph.entrant"},
                        },
                    },
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-graph")
    session.flush()

    assert len(events) == 1
    assert events[0].matched_giveaway_id == post.giveaway_campaign.id
    assert events[0].matched_post_id == post.id
    assert events[0].matched_account_id == instagram.id
    channel = post.giveaway_campaign.channels[0]
    entrant = session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="user-graph").one()
    assert entrant.provider_username == "graph.entrant"
    comment_event = (
        session.query(GiveawayEvidenceEvent)
        .filter(
            GiveawayEvidenceEvent.event_type == "instagram_comment",
            GiveawayEvidenceEvent.source == "webhook_capture",
            GiveawayEvidenceEvent.provider_event_id == "comment-graph-1",
        )
        .one()
    )
    assert comment_event.entrant_id == entrant.id


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
    giveaway_end_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    post = create_scheduled_post(
        session,
        _legacy_instagram_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=giveaway_end_at,
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-finalize")
    post.published_at = giveaway_end_at - timedelta(hours=1)
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
            self.created_at_utc = giveaway_end_at - timedelta(minutes=1)
            self.user = SimpleNamespace(pk="user-1", username="entrant.one")

    class _LiveCommentClient:
        def media_comments(self, media_id, amount=0):
            return [_LiveComment()]

        def media_likers(self, media_id):
            return []

        def user_stories(self, user_id):
            return []

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _LiveCommentClient())

    with _instagram_private_scan_mode("end_only"):
        process_instagram_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-3")

    refreshed = get_post(session, post.id)
    assert refreshed is not None
    assert refreshed.giveaway_campaign is not None
    assert refreshed.giveaway_campaign.status == GIVEAWAY_STATUS_WINNER_SELECTED
    pool = refreshed.giveaway_campaign.pools[0]
    assert pool.final_winner_entry is not None
    assert pool.final_winner_entry.provider_username == "entrant.one"
    assert pool.final_winner_entry.eligibility_status == ENTRY_STATUS_ELIGIBLE
    live_comment_event = (
        session.query(GiveawayEvidenceEvent)
        .filter(
            GiveawayEvidenceEvent.event_type == "instagram_comment",
            GiveawayEvidenceEvent.source == "close_time_live",
            GiveawayEvidenceEvent.provider_event_id == "comment-1",
        )
        .one()
    )
    assert live_comment_event.entrant_id == pool.final_winner_entry.id


def test_instagram_private_scan_ignores_comments_after_giveaway_end(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-private-late-comment")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    giveaway_end_at = datetime.now(timezone.utc) - timedelta(days=1)
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=giveaway_end_at,
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {"kind": "all", "children": [{"kind": "atom", "atom": "comment_present", "params": {}}]},
                }
            ],
        ),
        [],
    )
    _mark_posted(post, instagram.id, external_id="ig-media-late-comment")
    post.published_at = giveaway_end_at - timedelta(days=1)
    channel = post.giveaway_campaign.channels[0]
    session.flush()

    class _LateComment:
        pk = "late-comment-1"
        text = "I am late"
        created_at_utc = giveaway_end_at + timedelta(hours=2)
        user = SimpleNamespace(pk="late-user", username="late.entrant")

    class _LateCommentClient:
        def media_comments(self, media_id, amount=0):
            assert media_id == "ig-media-late-comment"
            return [_LateComment()]

    monkeypatch.setattr("app.services.giveaway_engine._instagram_destination_dependency_issue", lambda: None)
    monkeypatch.setattr("app.services.giveaway_engine._authenticated_publish_client", lambda credentials: _LateCommentClient())

    refresh_instagram_channel_state(session, channel, force_private_scan=True)
    evaluate_channel_entrants(channel, allow_instagram_private_verification=True)

    assert session.query(GiveawayEntrant).filter_by(channel_id=channel.id, provider_user_id="late-user").one_or_none() is None
    assert session.query(GiveawayEvidenceEvent).filter_by(
        channel_id=channel.id,
        event_type="instagram_comment",
        provider_event_id="late-comment-1",
    ).one_or_none() is None


def test_giveaway_lifecycle_collects_ready_channels_before_all_platforms_publish(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-partial-channel-collection")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id, bluesky.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
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
                        "children": [{"kind": "atom", "atom": "reply_or_quote_present", "params": {}}],
                    },
                },
            ],
        ),
        [],
    )
    _mark_posted(
        post,
        instagram.id,
        external_id="ig-media-live",
        external_url="https://instagram.test/p/live/",
    )
    session.flush()
    calls: list[str] = []

    def collect_instagram(session, channel, **kwargs):
        calls.append(channel.service)

    def collect_bluesky(session, channel, run_id):
        raise AssertionError("Bluesky should not collect before its target post is available.")

    monkeypatch.setattr("app.services.giveaway_engine.refresh_instagram_channel_state", collect_instagram)
    monkeypatch.setattr("app.services.giveaway_engine.collect_bluesky_channel_state", collect_bluesky)

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-partial-collect")

    channels = {channel.service: channel for channel in post.giveaway_campaign.channels}
    assert post.giveaway_campaign.status == GIVEAWAY_STATUS_COLLECTING
    assert channels["instagram"].status == GIVEAWAY_STATUS_COLLECTING
    assert channels["bluesky"].status == "scheduled"
    assert calls == ["instagram"]


def test_giveaway_delivery_keeps_media_available_after_initial_publish(session, monkeypatch, tmp_path):
    uploads_dir = tmp_path / "uploads"
    imported_dir = tmp_path / "imported"
    uploads_dir.mkdir()
    imported_dir.mkdir()
    monkeypatch.setattr("app.services.storage.settings", replace(storage_settings, uploads_dir=uploads_dir, imported_media_dir=imported_dir))
    persona = _create_persona(session, slug="giveaway-media-retention")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky")
    image_path = uploads_dir / "giveaway.jpg"
    image_path.write_bytes(b"jpeg")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id, bluesky.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {"kind": "all", "children": [{"kind": "atom", "atom": "comment_present", "params": {}}]},
                },
                {
                    "service": "bluesky",
                    "account_id": bluesky.id,
                    "rules": {"kind": "all", "children": [{"kind": "atom", "atom": "reply_or_quote_present", "params": {}}]},
                },
            ],
        ),
        [
            MediaItem(
                storage_path=image_path,
                mime_type="image/jpeg",
                alt_text="",
                size_bytes=image_path.stat().st_size,
                checksum="img-1",
                sort_order=0,
            ),
        ],
    )
    for job in post.delivery_jobs:
        job.status = "queued"
    session.flush()

    class FakeDestinationAdapter:
        def validate(self, post, persona, account):
            return []

        def publish(self, session, post, persona, account, *, context=None):
            return PublishResult(
                service=account.service,
                external_id=f"{account.service}-post",
                external_url=f"https://example.com/{account.service}/post",
            )

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", lambda account: FakeDestinationAdapter())

    process_delivery_queue(session, AlertDispatcher(), run_id="run-giveaway-media-retention")

    assert {job.status for job in post.delivery_jobs} == {"posted"}
    assert session.query(MediaAttachment).filter_by(post_id=post.id).count() == 1
    assert image_path.exists()


def test_delivery_recovers_requeued_job_that_already_has_external_id(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-requeued-published-job")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {"kind": "all", "children": [{"kind": "atom", "atom": "comment_present", "params": {}}]},
                }
            ],
        ),
        [
            MediaItem(
                storage_path="/tmp/already-posted-giveaway.jpg",
                mime_type="image/jpeg",
                alt_text="",
                size_bytes=4,
                checksum="img-1",
                sort_order=0,
            )
        ],
    )
    for attachment in list(post.attachments):
        session.delete(attachment)
    session.flush()
    job = post.delivery_jobs[0]
    job.status = "queued"
    job.external_id = "ig-media-existing"
    job.external_url = "https://instagram.test/p/existing/"
    session.flush()

    def fail_adapter(account):
        raise AssertionError("Published jobs with external IDs should not be sent again.")

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", fail_adapter)

    process_delivery_queue(session, AlertDispatcher(), run_id="run-recover-published")

    assert job.status == "posted"
    assert post.status == "posted"


def test_delivery_recovers_instagram_giveaway_job_from_channel_target(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-recover-channel-target")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {"kind": "all", "children": [{"kind": "atom", "atom": "comment_present", "params": {}}]},
                }
            ],
        ),
        [
            MediaItem(
                storage_path="/tmp/recover-channel-target.jpg",
                mime_type="image/jpeg",
                alt_text="",
                size_bytes=4,
                checksum="img-1",
                sort_order=0,
            )
        ],
    )
    for attachment in list(post.attachments):
        session.delete(attachment)
    session.flush()
    session.expire(post, ["attachments"])
    channel = post.giveaway_campaign.channels[0]
    channel.target_post_external_id = "ig-existing-channel-target"
    channel.target_post_url = "https://instagram.test/p/channel-target/"
    post.published_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    job = post.delivery_jobs[0]
    job.status = "queued"
    job.external_id = None
    session.flush()

    def fail_adapter(account):
        raise AssertionError("Recovered giveaway jobs should not publish again.")

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", fail_adapter)

    process_delivery_queue(session, AlertDispatcher(), run_id="run-recover-channel-target")

    assert job.status == "posted"
    assert job.external_id == "ig-existing-channel-target"
    assert job.external_url == "https://instagram.test/p/channel-target/"


def test_delivery_cancels_stale_instagram_giveaway_job_without_media(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-cancel-stale-instagram")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            channels=[
                {
                    "service": "instagram",
                    "account_id": instagram.id,
                    "rules": {"kind": "all", "children": [{"kind": "atom", "atom": "comment_present", "params": {}}]},
                }
            ],
        ),
        [
            MediaItem(
                storage_path="/tmp/stale-instagram-giveaway.jpg",
                mime_type="image/jpeg",
                alt_text="",
                size_bytes=4,
                checksum="img-1",
                sort_order=0,
            )
        ],
    )
    for attachment in list(post.attachments):
        session.delete(attachment)
    session.flush()
    session.expire(post, ["attachments"])
    post.published_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    job = post.delivery_jobs[0]
    job.status = "queued"
    job.external_id = None
    session.flush()

    def fail_adapter(account):
        raise AssertionError("Stale giveaway jobs without media should be stopped before validation.")

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", fail_adapter)

    process_delivery_queue(session, AlertDispatcher(), run_id="run-cancel-stale-instagram")

    assert job.status == "cancelled"
    assert job.last_error_class == "StaleGiveawayDelivery"


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
    monkeypatch.setattr("app.services.giveaway_engine.refresh_instagram_channel_state", lambda session, channel, **kwargs: None)
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
    channels_by_service = {channel.service: channel for channel in serialized.channels}
    assert channels_by_service["instagram"].entrants[0].profile_url == "https://www.instagram.com/ig.one/"
    assert channels_by_service["bluesky"].entrants[0].profile_url == "https://bsky.app/profile/bsky.one"
    assert serialized.pools[0].selection_log is not None
    assert serialized.pools[0].selection_log.candidates


def test_process_giveaway_lifecycle_selects_configured_winner_count(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-multiple-winners")
    bluesky = _create_account(session, persona, service="bluesky", label="Bluesky")
    post = create_scheduled_post(
        session,
        _generic_giveaway_payload(
            persona.id,
            [bluesky.id],
            giveaway_end_at=datetime.now(timezone.utc) - timedelta(minutes=5),
            winner_count=2,
            channels=[
                {
                    "service": "bluesky",
                    "account_id": bluesky.id,
                    "rules": {
                        "kind": "all",
                        "children": [{"kind": "atom", "atom": "reply_or_quote_present", "params": {}}],
                    },
                }
            ],
        ),
        [],
    )
    _mark_posted(post, bluesky.id, external_id="bsky-multi", external_url="https://bsky.app/profile/savannah.test/post/bsky-multi")
    campaign = post.giveaway_campaign
    assert campaign is not None
    channel = campaign.channels[0]
    channel.target_post_external_id = "bsky-multi"
    channel.target_post_uri = "at://did:plc:test/app.bsky.feed.post/bsky-multi"
    for index in range(3):
        channel.entrants.append(
            GiveawayEntrant(
                provider_user_id=f"did:plc:user-{index}",
                provider_username=f"bsky.{index}",
                display_label=f"bsky.{index}",
                signal_state_json={
                    "reply_present": True,
                    "quote_present": False,
                    "reply_posts": [{"uri": f"at://did:plc:user-{index}/app.bsky.feed.post/reply", "text": "in"}],
                    "quote_posts": [],
                    "reply_or_quote_mention_count": 0,
                },
            )
        )
    session.flush()

    monkeypatch.setattr("app.services.giveaway_engine.hydrate_channel_targets", lambda campaign: None)
    monkeypatch.setattr("app.services.giveaway_engine.collect_bluesky_channel_state", lambda session, channel, run_id: None)
    monkeypatch.setattr("app.services.giveaway_engine._randomize_entries", lambda entries: list(entries))

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-multiple-winners")

    refreshed = get_post(session, post.id)
    assert refreshed is not None
    pool = refreshed.giveaway_campaign.pools[0]
    assert pool.final_winner_entry is not None
    assert pool.final_winner_entry.provider_username == "bsky.0"
    assert pool.final_winner_entry_ids_json == [channel.entrants[0].id, channel.entrants[1].id]

    serialized = serialize_giveaway(refreshed.giveaway_campaign)
    assert serialized is not None
    assert serialized.winner_count == 2
    assert [winner.provider_username for winner in serialized.pools[0].final_winners] == ["bsky.0", "bsky.1"]
    assert [
        candidate.rank
        for candidate in serialized.pools[0].selection_log.candidates
        if candidate.selected
    ] == [1, 2]


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


def test_collect_bluesky_channel_state_handles_python_client_field_names(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-bluesky-snake-fields")
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
                get_likes=lambda params: _DumpableResponse({"likes": []}),
                get_reposted_by=lambda params: _DumpableResponse(
                    {
                        "reposted_by": [
                            {"did": "did:plc:user-1", "handle": "bsky.one"},
                        ]
                    }
                ),
                get_quotes=lambda params: _DumpableResponse({"posts": []}),
                get_post_thread=lambda params: _DumpableResponse({"thread": {"post": {"cid": "cid-1"}, "replies": []}}),
            )
            graph = SimpleNamespace(
                get_relationships=lambda params: _DumpableResponse(
                    {
                        "relationships": [
                            {"did": "did:plc:user-1", "followed_by": "at://did:plc:user-1/app.bsky.graph.follow/follow-1"},
                        ]
                    }
                )
            )
            self.app = SimpleNamespace(bsky=SimpleNamespace(feed=feed, graph=graph))

    monkeypatch.setattr("app.services.giveaway_engine._get_bluesky_client", lambda credentials: _FakeBlueskyClient())

    collect_bluesky_channel_state(session, channel, run_id="run-bsky-snake-fields")
    evaluate_channel_entrants(channel)

    entrant = channel.entrants[0]
    assert entrant.signal_state_json["repost_present"] is True
    assert entrant.signal_state_json["follow_present"] is True
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert session.query(GiveawayEvidenceEvent).filter_by(channel_id=channel.id, event_type="bluesky_repost").count() == 1
    assert session.query(GiveawayEvidenceEvent).filter_by(channel_id=channel.id, event_type="bluesky_follow").count() == 1


def test_collect_bluesky_channel_state_handles_nested_repost_actor(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-bluesky-nested-repost")
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
                            {"actor": {"did": "did:plc:user-1", "handle": "furkarufam.bsky.social"}},
                        ]
                    }
                ),
                get_reposted_by=lambda params: _DumpableResponse(
                    {
                        "reposted_by": [
                            {"actor": {"did": "did:plc:user-1", "handle": "furkarufam.bsky.social"}},
                        ]
                    }
                ),
                get_quotes=lambda params: _DumpableResponse({"posts": []}),
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
                                        "author": {"did": "did:plc:user-1", "handle": "furkarufam.bsky.social"},
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

    collect_bluesky_channel_state(session, channel, run_id="run-bsky-nested-repost")
    evaluate_channel_entrants(channel)

    entrant = channel.entrants[0]
    assert entrant.provider_username == "furkarufam.bsky.social"
    assert entrant.signal_state_json["repost_present"] is True
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert session.query(GiveawayEvidenceEvent).filter_by(channel_id=channel.id, event_type="bluesky_repost").count() == 1


def test_collect_bluesky_channel_state_falls_back_to_author_feed_reposts(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-bluesky-author-feed-repost")
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
                            {"actor": {"did": "did:plc:user-1", "handle": "furkarufam.bsky.social"}},
                        ]
                    }
                ),
                get_reposted_by=lambda params: _DumpableResponse({"repostedBy": []}),
                get_quotes=lambda params: _DumpableResponse({"posts": []}),
                get_post_thread=lambda params: _DumpableResponse(
                    {
                        "thread": {
                            "post": {"cid": "cid-1"},
                            "replies": [
                                {
                                    "post": {
                                        "uri": "at://did:plc:user-1/app.bsky.feed.post/reply-1",
                                        "record": {
                                            "text": "count me in",
                                            "reply": {
                                                "parent": {"uri": "at://did:plc:owner/app.bsky.feed.post/post-1"},
                                            },
                                        },
                                        "author": {"did": "did:plc:user-1", "handle": "furkarufam.bsky.social"},
                                    },
                                    "replies": [],
                                }
                            ],
                        }
                    }
                ),
                get_author_feed=lambda params: _DumpableResponse(
                    {
                        "feed": [
                            {
                                "post": {"uri": "at://did:plc:owner/app.bsky.feed.post/post-1", "cid": "cid-1"},
                                "reason": {"$type": "app.bsky.feed.defs#reasonRepost"},
                            }
                        ]
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

    collect_bluesky_channel_state(session, channel, run_id="run-bsky-author-feed-repost")
    evaluate_channel_entrants(channel)

    entrant = channel.entrants[0]
    assert entrant.provider_username == "furkarufam.bsky.social"
    assert entrant.signal_state_json["repost_present"] is True
    assert entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE
    assert session.query(GiveawayEvidenceEvent).filter_by(channel_id=channel.id, event_type="bluesky_repost").count() == 1


def test_collect_bluesky_channel_state_retries_transient_timeouts(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-bluesky-timeout-retry")
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
                        "children": [{"kind": "atom", "atom": "like_present", "params": {}}],
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
    calls = {"likes": 0}

    class InvokeTimeoutError(Exception):
        pass

    def flaky_get_likes(params):
        calls["likes"] += 1
        if calls["likes"] == 1:
            raise InvokeTimeoutError("timed out waiting for Bluesky")
        return _DumpableResponse({"likes": [{"actor": {"did": "did:plc:user-1", "handle": "bsky.one"}}]})

    class _FakeBlueskyClient:
        def __init__(self):
            feed = SimpleNamespace(
                get_likes=flaky_get_likes,
                get_reposted_by=lambda params: _DumpableResponse({"repostedBy": []}),
                get_quotes=lambda params: _DumpableResponse({"posts": []}),
                get_post_thread=lambda params: _DumpableResponse({"thread": {"post": {"cid": "cid-1"}, "replies": []}}),
            )
            graph = SimpleNamespace(get_relationships=lambda params: _DumpableResponse({"relationships": []}))
            self.app = SimpleNamespace(bsky=SimpleNamespace(feed=feed, graph=graph))

    monkeypatch.setattr("app.services.giveaway_engine._get_bluesky_client", lambda credentials: _FakeBlueskyClient())
    monkeypatch.setattr("app.services.giveaway_engine.time.sleep", lambda seconds: None)

    collect_bluesky_channel_state(session, channel, run_id="run-bsky-timeout-retry")

    assert calls["likes"] == 2
    assert channel.last_error is None
    assert len(channel.entrants) == 1
    assert channel.entrants[0].signal_state_json["like_present"] is True


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
        raise RuntimeError("collector stopped")

    monkeypatch.setattr("app.services.giveaway_engine.collect_bluesky_channel_state", fail_collection)

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-bsky-failure")

    channel = post.giveaway_campaign.channels[0]
    assert post.giveaway_campaign.status == GIVEAWAY_STATUS_COLLECTING
    assert channel.last_error == "Bluesky giveaway collection failed: collector stopped"
    alert = session.query(AlertEvent).filter(AlertEvent.event_type == "giveaway_collection_failed").one()
    assert alert.post_id == post.id
    assert alert.service == "bluesky"


def test_giveaway_lifecycle_treats_bluesky_timeouts_as_transient_warnings(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-bluesky-transient-timeout")
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

    class InvokeTimeoutError(Exception):
        pass

    def fail_collection(session, channel, run_id):
        raise InvokeTimeoutError("collector timed out")

    monkeypatch.setattr("app.services.giveaway_engine.collect_bluesky_channel_state", fail_collection)

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-bsky-timeout-warning")

    channel = post.giveaway_campaign.channels[0]
    assert channel.last_error == "Bluesky giveaway collection failed: collector timed out"
    assert session.query(AlertEvent).filter(AlertEvent.event_type == "giveaway_collection_failed").count() == 0

    def collect_success(session, channel, run_id):
        channel.last_error = None

    monkeypatch.setattr("app.services.giveaway_engine.collect_bluesky_channel_state", collect_success)

    process_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-bsky-timeout-cleared")

    assert channel.last_error is None
    assert post.giveaway_campaign.last_error is None


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
