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

from app.database import Base
from app.main import app
from app.schemas import ScheduledPostCreate
from app.services.alerts import AlertDispatcher
from app.services.auth import Principal
from app.services.giveaways import (
    GIVEAWAY_STATUS_FAILED,
    GIVEAWAY_STATUS_REVIEW_REQUIRED,
    GIVEAWAY_STATUS_WINNER_SELECTED,
    advance_giveaway_winner,
    instagram_webhook_observability,
    ingest_instagram_webhook_payload,
    process_instagram_giveaway_lifecycle,
)
from app.services.personas import create_account, create_persona
from app.services.posts import create_scheduled_post, get_post
from app.config import reload_settings
from app.models import AlertEvent, InstagramGiveawayWebhookEvent, RunEvent


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, Any]:
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
            "handle_or_identifier": label,
            "is_enabled": True,
            "source_enabled": False,
            "destination_enabled": True,
            "credentials_json": credentials,
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


def _giveaway_payload(persona_id: str, target_account_ids: list[str], *, scheduled_for: datetime | None = None, giveaway_end_at: datetime | None = None):
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


def test_create_instagram_giveaway_rejects_non_instagram_or_multiple_targets(session):
    persona = _create_persona(session, slug="giveaway-invalid-targets")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    mastodon = _create_account(session, persona, service="mastodon", label="Mastodon")

    with pytest.raises(ValueError, match="must target exactly one Instagram destination account"):
        create_scheduled_post(
            session,
            _giveaway_payload(
                persona.id,
                [instagram.id, mastodon.id],
                giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=2),
            ),
            [],
        )


def test_create_instagram_giveaway_requires_end_time(session):
    persona = _create_persona(session, slug="giveaway-missing-end")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")

    with pytest.raises(ValueError, match="require a giveaway end time"):
        create_scheduled_post(
            session,
            _giveaway_payload(persona.id, [instagram.id], giveaway_end_at=None),
            [],
        )


def test_create_instagram_giveaway_rejects_end_before_publish_time(session):
    persona = _create_persona(session, slug="giveaway-bad-end")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    scheduled_for = datetime.now(timezone.utc) + timedelta(hours=2)

    with pytest.raises(ValueError, match="must be after the scheduled publish time"):
        create_scheduled_post(
            session,
            _giveaway_payload(
                persona.id,
                [instagram.id],
                scheduled_for=scheduled_for,
                giveaway_end_at=scheduled_for - timedelta(minutes=5),
            ),
            [],
        )


def test_instagram_webhook_ingest_matches_comment_and_story_mention(session):
    persona = _create_persona(session, slug="giveaway-webhook-match")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
        ),
        [],
    )
    job = post.delivery_jobs[0]
    job.status = "posted"
    job.external_id = "ig-media-1"
    job.external_url = "https://instagram.test/p/ig-media-1/"
    post.published_at = datetime.now(timezone.utc) - timedelta(minutes=5)
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
    assert refreshed.instagram_giveaway is not None
    assert refreshed.instagram_giveaway.last_webhook_received_at is not None
    assert len(refreshed.instagram_giveaway.entries) == 1
    entry = refreshed.instagram_giveaway.entries[0]
    assert entry.instagram_username == "entrant.one"
    assert entry.comment_count == 1
    assert entry.mention_count == 1
    assert len(entry.story_mentions_json) == 1


def test_instagram_webhook_ingest_treats_shared_post_message_as_story_evidence(session):
    persona = _create_persona(session, slug="giveaway-webhook-share-message")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
        ),
        [],
    )
    job = post.delivery_jobs[0]
    job.status = "posted"
    job.external_id = "ig-media-share-1"
    job.external_url = "https://instagram.test/p/ig-media-share-1/"
    post.published_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    session.flush()

    payload = {
        "entry": [
            {
                "id": "17841463479494132",
                "messaging": [
                    {
                        "sender": {"id": "user-1"},
                        "recipient": {"id": "17841463479494132"},
                        "timestamp": 1776276436783,
                        "message": {
                            "mid": "message-share-1",
                            "attachments": [
                                {
                                    "type": "share",
                                    "payload": {
                                        "ig_post_media_id": "ig-media-share-1",
                                        "title": "Shared giveaway post",
                                    },
                                },
                                {
                                    "type": "ig_post",
                                    "payload": {
                                        "ig_post_media_id": "ig-media-share-1",
                                        "title": "Shared giveaway post",
                                    },
                                },
                            ],
                        },
                    }
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-share-message")
    session.flush()
    refreshed = get_post(session, post.id)
    assert refreshed is not None
    assert len(events) == 1
    assert events[0].event_type == "message"
    assert events[0].matched_giveaway_id == refreshed.instagram_giveaway.id
    assert events[0].matched_post_id == post.id
    assert refreshed.instagram_giveaway is not None
    assert refreshed.instagram_giveaway.last_webhook_received_at is not None
    assert len(refreshed.instagram_giveaway.entries) == 1
    entry = refreshed.instagram_giveaway.entries[0]
    assert entry.instagram_user_id == "user-1"
    assert len(entry.story_mentions_json) == 1
    assert entry.story_mentions_json[0]["media_id"] == "ig-media-share-1"
    assert entry.story_mentions_json[0]["source"] == "message_share_capture"


def test_instagram_webhook_ingest_stores_unmatched_events_for_diagnostics(session):
    payload = {
        "entry": [
            {
                "id": "unknown-account",
                "changes": [
                    {
                        "field": "comments",
                        "value": {
                            "media_id": "missing-media",
                            "id": "comment-1",
                            "text": "Hello @friend",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    }
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-2")

    assert len(events) == 1
    event = session.query(InstagramGiveawayWebhookEvent).one()
    assert event.matched_giveaway_id is None
    assert event.processed is True
    assert event.event_type == "comment"


def test_instagram_webhook_ingest_supports_messaging_envelope_events(session):
    payload = {
        "entry": [
            {
                "id": "ig-provider-account",
                "messaging": [
                    {
                        "sender": {"id": "user-42", "username": "dm.user"},
                        "recipient": {"id": "ig-provider-account"},
                        "timestamp": "2026-04-15T18:05:00Z",
                        "message": {
                            "mid": "m_123",
                            "text": "hello from inbox",
                        },
                    }
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-msg-envelope")

    assert len(events) == 1
    event = session.query(InstagramGiveawayWebhookEvent).one()
    assert event.provider_event_field == "messages"
    assert event.event_type == "message"
    assert event.provider_object_id == "m_123"
    assert event.payload_json["container"] == "messaging"
    assert event.payload_json["change"]["value"]["message"]["text"] == "hello from inbox"


def test_instagram_webhook_observability_summarizes_fields_and_recent_payloads(session):
    persona = _create_persona(session, slug="giveaway-observability")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
        ),
        [],
    )
    job = post.delivery_jobs[0]
    job.status = "posted"
    job.external_id = "ig-media-observable"
    job.external_url = "https://instagram.test/p/post-1/"
    post.published_at = datetime.now(timezone.utc) - timedelta(minutes=2)
    session.flush()

    payload = {
        "entry": [
            {
                "id": instagram.id,
                "changes": [
                    {
                        "field": "comments",
                        "value": {
                            "media_id": "ig-media-observable",
                            "id": "comment-1",
                            "text": "Count me in @friend",
                            "from": {"id": "user-1", "username": "entrant.one"},
                        },
                    },
                ],
            },
            {
                "id": "unknown-account",
                "changes": [
                    {
                        "field": "live_comments",
                        "value": {
                            "id": "live-1",
                            "text": "Live stream hello",
                            "from": {"id": "user-2", "username": "viewer.two"},
                        },
                    },
                    {
                        "field": "messages",
                        "value": {
                            "id": "message-1",
                            "message": "Hi from inbox",
                            "from": {"id": "user-3", "username": "dm.three"},
                        },
                    },
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-observability")
    stats = instagram_webhook_observability(session, window_days=7, recent_limit=10, field_limit=10)

    assert len(events) == 3
    assert {event.event_type for event in events} == {"comment", "live_comment", "message"}
    assert stats["total_events"] == 3
    assert stats["matched_events"] == 1
    assert stats["unmatched_events"] == 2
    assert stats["giveaway_relevant_events"] == 2
    assert stats["unique_fields"] == 3
    field_counts = {item["label"]: item["count"] for item in stats["field_chart"]}
    assert field_counts["Comments"] == 1
    assert field_counts["Live Comments"] == 1
    assert field_counts["Messages"] == 1
    assert stats["recent_events"][0]["payload_json"]["change"]["field"] in {"comments", "live_comments", "messages"}
    assert any(event["actor_username"] == "entrant.one" for event in stats["recent_events"])
    matched_event = next(event for event in stats["recent_events"] if event["matched"])
    assert matched_event["parent_post"]["post_id"] == post.id
    assert matched_event["parent_post"]["href"] == f"/scheduled-posts/{post.id}/page"
    assert "Win a prize" in matched_event["parent_post"]["label"]
    assert matched_event["matched_local_account"]["label"] == "Instagram"
    assert matched_event["matched_local_account"]["persona_name"] == "Giveaway Persona"
    assert matched_event["provider_local_account"]["label"] == "Instagram"
    assert matched_event["summary_text"] == (
        "Giveaway Persona received a new comment from @entrant.one on "
        "Win a prize by commenting and sharing."
    )
    assert matched_event["activity_href"] == "https://instagram.test/p/post-1/"
    assert matched_event["activity_href_label"] == "Open Instagram post"


def test_instagram_webhook_observability_builds_friendly_dm_summary_and_chat_link(session, monkeypatch):
    recipient_persona = _create_persona(session, slug="larkyn-lynx", name="Larkyn Lynx")
    sender_persona = _create_persona(session, slug="pawgetsound-studio", name="PawgetSound.Studio")
    recipient_ig_user_id = "17841463479494132"
    sender_ig_user_id = "2045697446345302"
    recipient = _create_account(session, recipient_persona, service="instagram", label="Instagram")
    recipient.credentials_json["instagrapi_username"] = "larkyn.lynx"
    recipient.credentials_json["instagram_user_id"] = recipient_ig_user_id
    sender = _create_account(session, sender_persona, service="instagram", label="Instagram")
    sender.credentials_json["instagrapi_username"] = "pawgetsound.studio"
    sender.credentials_json["instagram_user_id"] = sender_ig_user_id
    session.flush()

    def fake_get(url, params=None, timeout=None):  # noqa: ARG001
        if url.endswith(f"/{sender_ig_user_id}"):
            return _FakeResponse(
                200,
                {
                    "id": sender_ig_user_id,
                    "username": "pawgetsound.studio",
                    "name": "Pawget Sound Studio",
                    "profile_pic": "https://cdn.test/pawget.jpg",
                },
            )
        if url.endswith(f"/{recipient_ig_user_id}"):
            return _FakeResponse(
                200,
                {
                    "id": recipient_ig_user_id,
                    "username": "larkyn.lynx",
                    "name": "Larkyn Lynx",
                    "profile_pic": "https://cdn.test/larkyn.jpg",
                },
            )
        if url.endswith(f"/{recipient_ig_user_id}/conversations"):
            return _FakeResponse(
                200,
                {
                    "data": [
                        {
                            "id": "conversation-123",
                            "updated_time": "2026-04-15T18:07:17+0000",
                            "participants": {
                                "data": [
                                    {"id": recipient_ig_user_id, "username": "larkyn.lynx"},
                                    {"id": sender_ig_user_id, "username": "pawgetsound.studio"},
                                ]
                            },
                            "messages": {
                                "data": [
                                    {
                                        "id": "message-1",
                                        "message": "tes",
                                        "from": {"id": sender_ig_user_id, "username": "pawgetsound.studio"},
                                    }
                                ]
                            },
                        }
                    ]
                },
            )
        raise AssertionError(f"Unexpected Graph request: {url} {params}")

    monkeypatch.setattr("app.services.giveaways.requests.get", fake_get)

    payload = {
        "entry": [
            {
                "id": recipient_ig_user_id,
                "messaging": [
                    {
                        "sender": {"id": sender_ig_user_id},
                        "recipient": {"id": recipient_ig_user_id},
                        "timestamp": 1776276436783,
                        "message": {
                            "mid": "message-1",
                            "text": "tes",
                        },
                    }
                ],
            }
        ]
    }

    events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-dm-summary")
    stats = instagram_webhook_observability(session, window_days=7, recent_limit=10, field_limit=10)

    assert len(events) == 1
    message_event = stats["recent_events"][0]
    assert message_event["event_type"] == "message"
    assert message_event["provider_local_account"]["persona_name"] == "Larkyn Lynx"
    assert message_event["actor_local_account"]["persona_name"] == "PawgetSound.Studio"
    assert message_event["recipient_label"] == "Larkyn Lynx (@larkyn.lynx)"
    assert message_event["actor_label"] == "PawgetSound.Studio (@pawgetsound.studio)"
    assert message_event["actor_profile_href"] == "https://www.instagram.com/pawgetsound.studio/"
    assert message_event["actor_profile_image_url"] == "https://cdn.test/pawget.jpg"
    assert message_event["recipient_profile_image_url"] == "https://cdn.test/larkyn.jpg"
    assert message_event["summary_text"] == (
        "Larkyn Lynx (@larkyn.lynx) received a direct message from "
        "PawgetSound.Studio (@pawgetsound.studio)."
    )
    assert message_event["chat_href"] == "https://www.instagram.com/direct/t/conversation-123/"
    assert message_event["chat_href_label"] == "Open Instagram conversation"


def test_instagram_webhook_observability_recognizes_shared_post_messages(session, monkeypatch):
    recipient_persona = _create_persona(session, slug="share-recipient", name="Larkyn Lynx")
    sender_persona = _create_persona(session, slug="share-sender", name="PawgetSound.Studio")
    recipient_ig_user_id = "17841463479494132"
    sender_ig_user_id = "2045697446345302"
    recipient = _create_account(session, recipient_persona, service="instagram", label="Instagram")
    recipient.credentials_json["instagrapi_username"] = "larkyn.lynx"
    recipient.credentials_json["instagram_user_id"] = recipient_ig_user_id
    sender = _create_account(session, sender_persona, service="instagram", label="Instagram")
    sender.credentials_json["instagrapi_username"] = "pawgetsound.studio"
    sender.credentials_json["instagram_user_id"] = sender_ig_user_id
    post = create_scheduled_post(
        session,
        _giveaway_payload(
            recipient_persona.id,
            [recipient.id],
            giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
        ),
        [],
    )
    job = post.delivery_jobs[0]
    job.status = "posted"
    job.external_id = "ig-media-share-observed"
    job.external_url = "https://instagram.test/p/ig-media-share-observed/"
    post.published_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    session.flush()

    def fake_get(url, params=None, timeout=None):  # noqa: ARG001
        if url.endswith(f"/{sender_ig_user_id}"):
            return _FakeResponse(
                200,
                {
                    "id": sender_ig_user_id,
                    "username": "pawgetsound.studio",
                    "name": "Pawget Sound Studio",
                    "profile_pic": "https://cdn.test/pawget.jpg",
                },
            )
        if url.endswith(f"/{recipient_ig_user_id}"):
            return _FakeResponse(
                200,
                {
                    "id": recipient_ig_user_id,
                    "username": "larkyn.lynx",
                    "name": "Larkyn Lynx",
                    "profile_pic": "https://cdn.test/larkyn.jpg",
                },
            )
        if url.endswith(f"/{recipient_ig_user_id}/conversations"):
            return _FakeResponse(200, {"data": []})
        raise AssertionError(f"Unexpected Graph request: {url} {params}")

    monkeypatch.setattr("app.services.giveaways.requests.get", fake_get)

    payload = {
        "entry": [
            {
                "id": recipient_ig_user_id,
                "messaging": [
                    {
                        "sender": {"id": sender_ig_user_id},
                        "recipient": {"id": recipient_ig_user_id},
                        "timestamp": 1776276436783,
                        "message": {
                            "mid": "message-share-2",
                            "attachments": [
                                {
                                    "type": "share",
                                    "payload": {
                                        "ig_post_media_id": "ig-media-share-observed",
                                        "title": "Giveaway post share",
                                    },
                                },
                                {
                                    "type": "ig_post",
                                    "payload": {
                                        "ig_post_media_id": "ig-media-share-observed",
                                        "title": "Giveaway post share",
                                    },
                                },
                            ],
                        },
                    }
                ],
            }
        ]
    }

    ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-share-observability")
    stats = instagram_webhook_observability(session, window_days=7, recent_limit=10, field_limit=10)

    shared_event = stats["recent_events"][0]
    assert shared_event["event_type"] == "message"
    assert shared_event["field_label"] == "Shared Post"
    assert shared_event["event_type_label"] == "Shared Post"
    assert shared_event["matched"] is True
    assert shared_event["matched_post_id"] == post.id
    assert shared_event["account_context_label"] == "Account"
    assert shared_event["actor_context_label"] == "Shared By"
    assert shared_event["provider_object_label"] == "Provider Share Message ID"
    assert shared_event["summary_text"] == (
        "Larkyn Lynx (@larkyn.lynx) received a shared Instagram post from "
        "PawgetSound.Studio (@pawgetsound.studio) for Win a prize by commenting and sharing."
    )
    assert shared_event["activity_href"] == "https://instagram.test/p/ig-media-share-observed/"
    assert shared_event["activity_href_label"] == "Open shared Instagram post"


def test_instagram_webhook_observability_enriches_comment_profile_and_media_link(session, monkeypatch):
    recipient_persona = _create_persona(session, slug="comment-recipient", name="Larkyn Lynx")
    recipient_ig_user_id = "17841463479494132"
    commenter_ig_user_id = "2045697446345302"
    recipient = _create_account(session, recipient_persona, service="instagram", label="Instagram")
    recipient.credentials_json["instagrapi_username"] = "larkyn.lynx"
    recipient.credentials_json["instagram_user_id"] = recipient_ig_user_id
    session.flush()

    def fake_get(url, params=None, timeout=None):  # noqa: ARG001
        if url.endswith(f"/{commenter_ig_user_id}"):
            return _FakeResponse(
                200,
                {
                    "id": commenter_ig_user_id,
                    "username": "pawgetsound.studio",
                    "name": "Pawget Sound Studio",
                    "profile_pic": "https://cdn.test/pawget.jpg",
                },
            )
        if url.endswith(f"/{recipient_ig_user_id}"):
            return _FakeResponse(
                200,
                {
                    "id": recipient_ig_user_id,
                    "username": "larkyn.lynx",
                    "name": "Larkyn Lynx",
                    "profile_pic": "https://cdn.test/larkyn.jpg",
                },
            )
        if url.endswith(f"/{recipient_ig_user_id}/media"):
            return _FakeResponse(
                200,
                {
                    "data": [
                        {
                            "id": "media-123",
                            "caption": "Comment target post",
                            "permalink": "https://www.instagram.com/p/media-123/",
                            "timestamp": "2026-04-15T18:07:17+0000",
                            "media_type": "IMAGE",
                        }
                    ]
                },
            )
        raise AssertionError(f"Unexpected Graph request: {url} {params}")

    monkeypatch.setattr("app.services.giveaways.requests.get", fake_get)

    payload = {
        "entry": [
            {
                "id": recipient_ig_user_id,
                "changes": [
                    {
                        "field": "comments",
                        "value": {
                            "from": {"id": commenter_ig_user_id, "username": "pawgetsound.studio"},
                            "media": {"id": "media-123", "media_product_type": "FEED"},
                            "id": "comment-123",
                            "parent_id": "comment-parent-456",
                            "text": "Love this one",
                        },
                    }
                ],
            }
        ]
    }

    ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id="run-comment-enrichment")
    stats = instagram_webhook_observability(session, window_days=7, recent_limit=10, field_limit=10)

    comment_event = stats["recent_events"][0]
    assert comment_event["event_type"] == "comment"
    assert comment_event["actor_label"] == "Pawget Sound Studio (@pawgetsound.studio)"
    assert comment_event["actor_profile_href"] == "https://www.instagram.com/pawgetsound.studio/"
    assert comment_event["actor_profile_image_url"] == "https://cdn.test/pawget.jpg"
    assert comment_event["activity_href"] == "https://www.instagram.com/p/media-123/"
    assert comment_event["activity_href_label"] == "Open Instagram post"
    assert comment_event["related_media"]["label"] == "Comment target post"
    assert comment_event["summary_text"] == (
        "Larkyn Lynx (@larkyn.lynx) received a new comment from "
        "Pawget Sound Studio (@pawgetsound.studio) on Comment target post."
    )


def test_process_instagram_giveaway_lifecycle_selects_verified_winner(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-finalize-success")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        ),
        [],
    )
    job = post.delivery_jobs[0]
    job.status = "posted"
    job.external_id = "ig-media-2"
    post.published_at = datetime.now(timezone.utc) - timedelta(hours=1)
    session.flush()

    class _LiveCommentClient:
        def media_comments(self, media_id, amount=0):  # noqa: ARG002
            return [
                SimpleNamespace(
                    pk="comment-1",
                    text="Entering with @friend",
                    user=SimpleNamespace(pk="user-1", username="entrant.one"),
                    created_at_utc=datetime.now(timezone.utc),
                )
            ]

    monkeypatch.setattr("app.services.giveaways._authenticated_publish_client", lambda config: _LiveCommentClient())
    monkeypatch.setattr("app.services.giveaways._verify_like_and_follow", lambda giveaway, entry: ("not_required", "not_required", [], []))
    ingest_instagram_webhook_payload(
        session,
        {
            "entry": [
                {
                    "id": instagram.id,
                    "changes": [
                        {
                            "field": "comments",
                            "value": {
                                "media_id": "ig-media-2",
                                "id": "comment-1",
                                "text": "Entering with @friend",
                                "from": {"id": "user-1", "username": "entrant.one"},
                            },
                        },
                        {
                            "field": "mentions",
                            "value": {
                                "media_id": "ig-media-2",
                                "story_id": "story-1",
                                "from": {"id": "user-1", "username": "entrant.one"},
                            },
                        },
                    ],
                }
            ]
        },
        signature_valid=True,
        run_id="run-3",
    )

    process_instagram_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-3")

    refreshed = get_post(session, post.id)
    assert refreshed is not None
    assert refreshed.instagram_giveaway is not None
    assert refreshed.instagram_giveaway.status == GIVEAWAY_STATUS_WINNER_SELECTED
    assert refreshed.instagram_giveaway.final_winner_rank == 1
    assert refreshed.instagram_giveaway.entries[0].eligibility_status == "eligible"


def test_process_instagram_giveaway_lifecycle_marks_review_required_and_advance_uses_frozen_rank(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-finalize-review")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        ),
        [],
    )
    job = post.delivery_jobs[0]
    job.status = "posted"
    job.external_id = "ig-media-3"
    post.published_at = datetime.now(timezone.utc) - timedelta(hours=1)
    session.flush()

    class _LiveCommentClient:
        def media_comments(self, media_id, amount=0):  # noqa: ARG002
            return [
                SimpleNamespace(
                    pk="comment-1",
                    text="Entry from @friend.one",
                    user=SimpleNamespace(pk="user-1", username="entrant.one"),
                    created_at_utc=datetime.now(timezone.utc),
                ),
                SimpleNamespace(
                    pk="comment-2",
                    text="Entry from @friend.two",
                    user=SimpleNamespace(pk="user-2", username="entrant.two"),
                    created_at_utc=datetime.now(timezone.utc),
                ),
            ]

    monkeypatch.setattr("app.services.giveaways._authenticated_publish_client", lambda config: _LiveCommentClient())
    monkeypatch.setattr(
        "app.services.giveaways._verify_like_and_follow",
        lambda giveaway, entry: ("inconclusive", "not_required", ["Like verification could not be completed."], []),
    )
    ingest_instagram_webhook_payload(
        session,
        {
            "entry": [
                {
                    "id": instagram.id,
                    "changes": [
                        {
                            "field": "comments",
                            "value": {
                                "media_id": "ig-media-3",
                                "id": "comment-1",
                                "text": "Entry from @friend.one",
                                "from": {"id": "user-1", "username": "entrant.one"},
                            },
                        },
                        {
                            "field": "comments",
                            "value": {
                                "media_id": "ig-media-3",
                                "id": "comment-2",
                                "text": "Entry from @friend.two",
                                "from": {"id": "user-2", "username": "entrant.two"},
                            },
                        },
                        {
                            "field": "mentions",
                            "value": {
                                "media_id": "ig-media-3",
                                "story_id": "story-1",
                                "from": {"id": "user-1", "username": "entrant.one"},
                            },
                        },
                        {
                            "field": "mentions",
                            "value": {
                                "media_id": "ig-media-3",
                                "story_id": "story-2",
                                "from": {"id": "user-2", "username": "entrant.two"},
                            },
                        },
                    ],
                }
            ]
        },
        signature_valid=True,
        run_id="run-4",
    )

    process_instagram_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-4")
    refreshed = get_post(session, post.id)
    assert refreshed is not None
    assert refreshed.instagram_giveaway is not None
    assert refreshed.instagram_giveaway.status == GIVEAWAY_STATUS_REVIEW_REQUIRED
    original_rank = refreshed.instagram_giveaway.provisional_winner_rank
    assert original_rank in {1, 2}

    advance_giveaway_winner(session, refreshed.instagram_giveaway, run_id="run-5")
    assert refreshed.instagram_giveaway.provisional_winner_rank in {1, 2}
    assert refreshed.instagram_giveaway.provisional_winner_rank != original_rank


def test_process_instagram_giveaway_disqualifies_deleted_comment_at_close(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-close-time-comment")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        ),
        [],
    )
    job = post.delivery_jobs[0]
    job.status = "posted"
    job.external_id = "ig-media-deleted-comment"
    post.published_at = datetime.now(timezone.utc) - timedelta(hours=1)
    session.flush()

    class _DeletedCommentClient:
        def media_comments(self, media_id, amount=0):  # noqa: ARG002
            return []

    monkeypatch.setattr("app.services.giveaways._authenticated_publish_client", lambda config: _DeletedCommentClient())
    monkeypatch.setattr("app.services.giveaways._verify_like_and_follow", lambda giveaway, entry: ("not_required", "not_required", [], []))

    ingest_instagram_webhook_payload(
        session,
        {
            "entry": [
                {
                    "id": instagram.id,
                    "changes": [
                        {
                            "field": "comments",
                            "value": {
                                "media_id": "ig-media-deleted-comment",
                                "id": "comment-1",
                                "text": "Entering with @friend",
                                "from": {"id": "user-1", "username": "entrant.one"},
                            },
                        },
                        {
                            "field": "mentions",
                            "value": {
                                "media_id": "ig-media-deleted-comment",
                                "story_id": "story-1",
                                "from": {"id": "user-1", "username": "entrant.one"},
                            },
                        },
                    ],
                }
            ]
        },
        signature_valid=True,
        run_id="run-deleted-comment",
    )

    process_instagram_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-deleted-comment")

    refreshed = get_post(session, post.id)
    assert refreshed is not None
    assert refreshed.instagram_giveaway is not None
    assert refreshed.instagram_giveaway.status == GIVEAWAY_STATUS_FAILED
    assert refreshed.instagram_giveaway.entries[0].eligibility_status == "disqualified"
    assert refreshed.instagram_giveaway.entries[0].disqualification_reasons_json == [
        "No current giveaway comment was found at close time."
    ]


def test_process_instagram_giveaway_logs_captured_comment_fallback_when_live_revalidation_fails(session, monkeypatch):
    persona = _create_persona(session, slug="giveaway-comment-fallback")
    instagram = _create_account(session, persona, service="instagram", label="Instagram")
    post = create_scheduled_post(
        session,
        _giveaway_payload(
            persona.id,
            [instagram.id],
            giveaway_end_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        ),
        [],
    )
    job = post.delivery_jobs[0]
    job.status = "posted"
    job.external_id = "ig-media-comment-fallback"
    post.published_at = datetime.now(timezone.utc) - timedelta(hours=1)
    session.flush()

    monkeypatch.setattr(
        "app.services.giveaways._authenticated_publish_client",
        lambda config: (_ for _ in ()).throw(RuntimeError("challenge required")),
    )
    monkeypatch.setattr("app.services.giveaways._verify_like_and_follow", lambda giveaway, entry: ("not_required", "not_required", [], []))

    ingest_instagram_webhook_payload(
        session,
        {
            "entry": [
                {
                    "id": instagram.id,
                    "changes": [
                        {
                            "field": "comments",
                            "value": {
                                "media_id": "ig-media-comment-fallback",
                                "id": "comment-1",
                                "text": "Entering with @friend",
                                "from": {"id": "user-1", "username": "entrant.one"},
                            },
                        },
                        {
                            "field": "mentions",
                            "value": {
                                "media_id": "ig-media-comment-fallback",
                                "story_id": "story-1",
                                "from": {"id": "user-1", "username": "entrant.one"},
                            },
                        },
                    ],
                }
            ]
        },
        signature_valid=True,
        run_id="run-comment-fallback",
    )

    process_instagram_giveaway_lifecycle(session, AlertDispatcher(), run_id="run-comment-fallback")

    refreshed = get_post(session, post.id)
    assert refreshed is not None
    assert refreshed.instagram_giveaway is not None
    assert refreshed.instagram_giveaway.status == GIVEAWAY_STATUS_WINNER_SELECTED

    evaluation_event = (
        session.query(RunEvent)
        .filter(RunEvent.run_id == "run-comment-fallback", RunEvent.message.like("Instagram giveaway for post%evaluated using%"))
        .order_by(RunEvent.created_at.desc())
        .first()
    )
    assert evaluation_event is not None
    assert evaluation_event.metadata_json["comment_evidence_mode"] == "captured_comment_fallback"
    assert evaluation_event.metadata_json["story_evidence_mode"] == "captured_story_mentions"
    assert "captured comment evidence fallback" in evaluation_event.message


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
            _giveaway_payload(
                persona.id,
                [instagram.id],
                giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=1),
            ),
            [],
        )
        post.delivery_jobs[0].status = "posted"
        post.delivery_jobs[0].external_id = "ig-media-4"
        post.published_at = datetime.now(timezone.utc)
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
                "id": instagram.id,
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
        assert alert.payload_json["payload"]["x_hub_signature_256_present"] is True
        assert alert.payload_json["payload"]["x_hub_signature_present"] is False

    ok_response = api_client.post(
        "/webhooks/instagram",
        content=raw_body,
        headers={"X-Hub-Signature-256": signature, "Content-Type": "application/json"},
    )
    assert ok_response.status_code == 200
    assert ok_response.json()["stored_events"] == 1

    legacy_signature = "sha1=" + hmac.new(b"webhook-secret", raw_body, hashlib.sha1).hexdigest()
    legacy_response = api_client.post(
        "/webhooks/instagram",
        content=raw_body,
        headers={"X-Hub-Signature": legacy_signature, "Content-Type": "application/json"},
    )
    assert legacy_response.status_code == 200
    assert legacy_response.json()["stored_events"] == 1


def test_instagram_webhook_api_accepts_messaging_envelope_payload(giveaway_api_stack):
    api_client, _SessionLocal = giveaway_api_stack

    payload = {
        "entry": [
            {
                "id": "ig-provider-account",
                "messaging": [
                    {
                        "sender": {"id": "user-55", "username": "dm.user"},
                        "recipient": {"id": "ig-provider-account"},
                        "timestamp": "2026-04-15T18:10:00Z",
                        "message": {
                            "mid": "mid-55",
                            "text": "new inbox message",
                        },
                    }
                ],
            }
        ]
    }
    raw_body = json.dumps(payload).encode("utf-8")
    signature = "sha256=" + hmac.new(b"webhook-secret", raw_body, hashlib.sha256).hexdigest()

    response = api_client.post(
        "/webhooks/instagram",
        content=raw_body,
        headers={"X-Hub-Signature-256": signature, "Content-Type": "application/json"},
    )

    assert response.status_code == 200
    assert response.json()["stored_events"] == 1
