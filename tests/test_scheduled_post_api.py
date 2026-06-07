from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.models import AlertEvent, GiveawayEntrant
from app.main import app
from app.services.auth import Principal
from app.services.personas import create_account, create_persona
from app.services.posts import create_scheduled_post, get_post
from app.domain import MediaItem
from app.schemas import ScheduledPostCreate


def _create_persona(
    session,
    *,
    slug: str = "scheduled-post-api",
    owner_user_id: str | None = "admin-user",
):
    return create_persona(
        session,
        {
            "name": "Scheduled API Persona",
            "slug": slug,
            "owner_user_id": owner_user_id,
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def _create_destination_account(session, persona, *, service: str = "mastodon", label: str = "Mastodon"):
    default_credentials = {
        "bluesky": {"handle": "me.bsky.social", "password": "pw"},
        "mastodon": {"instance": "https://example.social", "token": "secret", "handle": "@me@example.social"},
        "discord": {"webhook_url": "https://discord.test"},
    }
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
            "credentials_json": default_credentials[service],
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


@pytest.fixture()
def api_stack(monkeypatch, tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'scheduled-post-api.db'}", future=True, connect_args={"check_same_thread": False})
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

    uploads_dir = tmp_path / "uploads"

    def _ensure_storage_dirs_override():
        uploads_dir.mkdir(parents=True, exist_ok=True)

    def _unique_path_override(directory, original_name):
        target = uploads_dir / original_name
        if target.exists():
            target = uploads_dir / f"{target.stem}-copy{target.suffix}"
        return target

    monkeypatch.setattr("app.services.storage.ensure_storage_dirs", _ensure_storage_dirs_override)
    monkeypatch.setattr("app.services.storage._unique_path", _unique_path_override)

    with TestClient(app) as client:
        yield client, SessionLocal

    Base.metadata.drop_all(engine)
    engine.dispose()


def test_create_scheduled_post_api_persists_multipart_attachments(api_stack):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-create")
        destination = _create_destination_account(session, persona)
        session.commit()
        persona_id = persona.id
        destination_id = destination.id

    response = api_client.post(
        "/scheduled-posts",
        data={
            "persona_id": persona_id,
            "body": "Hello from API",
            "status": "draft",
            "target_account_ids": json.dumps([destination_id]),
            "publish_overrides_json": json.dumps({}),
            "metadata_json": json.dumps({}),
            "scheduled_for": "",
            "alt_texts": json.dumps(["Cat alt"]),
        },
        files=[("uploads", ("cat.jpg", b"fake-jpeg", "image/jpeg"))],
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["attachments"]) == 1
    assert payload["attachments"][0]["mime_type"] == "image/jpeg"
    assert payload["attachments"][0]["alt_text"] == "Cat alt"

    with SessionLocal() as session:
        saved = get_post(session, payload["id"])
        assert saved is not None
        assert len(saved.attachments) == 1
        assert saved.attachments[0].mime_type == "image/jpeg"
        assert saved.attachments[0].alt_text == "Cat alt"


def test_update_scheduled_post_api_appends_multipart_attachments(api_stack, tmp_path):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-update")
        destination = _create_destination_account(session, persona)

        existing_path = tmp_path / "existing.jpg"
        existing_path.write_bytes(b"existing-jpeg")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Draft body",
                    "status": "draft",
                    "target_account_ids": [destination.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                }
            ),
            [
                MediaItem(
                    storage_path=Path(existing_path),
                    mime_type="image/jpeg",
                    alt_text="Existing image",
                    size_bytes=existing_path.stat().st_size,
                    checksum="existing-1",
                    sort_order=0,
                )
            ],
        )
        session.commit()
        post_id = post.id
        destination_id = destination.id

    response = api_client.put(
        f"/scheduled-posts/{post_id}",
        data={
            "body": "Draft body updated",
            "status": "draft",
            "target_account_ids": json.dumps([destination_id]),
            "publish_overrides_json": json.dumps({}),
            "metadata_json": json.dumps({}),
            "scheduled_for": "",
            "alt_texts": json.dumps(["New image"]),
        },
        files=[("uploads", ("new.jpg", b"new-jpeg", "image/jpeg"))],
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["body"] == "Draft body updated"
    assert len(payload["attachments"]) == 2
    assert [attachment["sort_order"] for attachment in payload["attachments"]] == [0, 1]
    assert payload["attachments"][1]["alt_text"] == "New image"

    with SessionLocal() as session:
        saved = get_post(session, post_id)
        assert saved is not None
        assert len(saved.attachments) == 2
        assert [attachment.sort_order for attachment in saved.attachments] == [0, 1]
        assert saved.attachments[1].alt_text == "New image"


def test_update_scheduled_post_api_reorders_and_deletes_attachments(api_stack, tmp_path):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-reorder-media")
        destination = _create_destination_account(session, persona)

        paths = [tmp_path / "one.jpg", tmp_path / "two.jpg", tmp_path / "three.jpg"]
        for index, path in enumerate(paths):
            path.write_bytes(f"image-{index}".encode())
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Draft body",
                    "status": "draft",
                    "target_account_ids": [destination.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                }
            ),
            [
                MediaItem(
                    storage_path=Path(path),
                    mime_type="image/jpeg",
                    alt_text=path.stem,
                    size_bytes=path.stat().st_size,
                    checksum=f"checksum-{index}",
                    sort_order=index,
                )
                for index, path in enumerate(paths)
            ],
        )
        session.commit()
        post_id = post.id
        destination_id = destination.id
        attachment_ids = {attachment.alt_text: attachment.id for attachment in post.attachments}

    response = api_client.put(
        f"/scheduled-posts/{post_id}",
        data={
            "body": "Draft body",
            "status": "draft",
            "target_account_ids": json.dumps([destination_id]),
            "publish_overrides_json": json.dumps({}),
            "metadata_json": json.dumps({}),
            "scheduled_for": "",
            "attachment_order": json.dumps([attachment_ids["three"], attachment_ids["one"]]),
            "deleted_attachment_ids": json.dumps([attachment_ids["two"]]),
            "alt_texts": json.dumps([]),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert [attachment["id"] for attachment in payload["attachments"]] == [attachment_ids["three"], attachment_ids["one"]]
    assert [attachment["sort_order"] for attachment in payload["attachments"]] == [0, 1]

    with SessionLocal() as session:
        saved = get_post(session, post_id)
        assert saved is not None
        saved_attachments = sorted(saved.attachments, key=lambda attachment: attachment.sort_order)
        assert [attachment.id for attachment in saved_attachments] == [attachment_ids["three"], attachment_ids["one"]]


def test_update_scheduled_post_api_updates_attachment_alt_text(api_stack, tmp_path):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-alt-text")
        destination = _create_destination_account(session, persona)

        image_path = tmp_path / "one.jpg"
        image_path.write_bytes(b"image-one")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Draft body",
                    "status": "draft",
                    "target_account_ids": [destination.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                }
            ),
            [
                MediaItem(
                    storage_path=Path(image_path),
                    mime_type="image/jpeg",
                    alt_text="Old description",
                    size_bytes=image_path.stat().st_size,
                    checksum="checksum-one",
                    sort_order=0,
                )
            ],
        )
        session.commit()
        post_id = post.id
        destination_id = destination.id
        attachment_id = post.attachments[0].id

    response = api_client.put(
        f"/scheduled-posts/{post_id}",
        data={
            "body": "Draft body",
            "status": "draft",
            "target_account_ids": json.dumps([destination_id]),
            "publish_overrides_json": json.dumps({}),
            "metadata_json": json.dumps({}),
            "scheduled_for": "",
            "attachment_alt_texts": json.dumps({attachment_id: "A product photo on a pink background"}),
            "alt_texts": json.dumps([]),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["attachments"][0]["alt_text"] == "A product photo on a pink background"

    with SessionLocal() as session:
        saved = get_post(session, post_id)
        assert saved is not None
        assert saved.attachments[0].alt_text == "A product photo on a pink background"


def test_update_scheduled_post_api_accepts_json_schedule_moves(api_stack):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-json-update")
        destination = _create_destination_account(session, persona)
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Ready to move",
                    "status": "draft",
                    "target_account_ids": [destination.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                }
            ),
            [],
        )
        session.commit()
        post_id = post.id

    schedule_response = api_client.put(
        f"/scheduled-posts/{post_id}",
        json={
            "status": "scheduled",
            "scheduled_for": "2026-04-15T14:30",
        },
    )

    assert schedule_response.status_code == 200
    assert schedule_response.json()["status"] == "scheduled"
    assert schedule_response.json()["scheduled_for"] == "2026-04-15T14:30:00"

    clear_response = api_client.put(
        f"/scheduled-posts/{post_id}",
        json={
            "status": "draft",
            "scheduled_for": None,
        },
    )

    assert clear_response.status_code == 200
    assert clear_response.json()["status"] == "draft"
    assert clear_response.json()["scheduled_for"] is None

    with SessionLocal() as session:
        saved = get_post(session, post_id)
        assert saved is not None
        assert saved.status == "draft"
        assert saved.scheduled_for is None


def test_update_scheduled_giveaway_api_moves_time(api_stack):
    api_client, SessionLocal = api_stack
    giveaway_end_at = datetime(2026, 5, 16, 20, 0, tzinfo=timezone.utc)
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-giveaway-update")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        giveaway = {
            "giveaway_end_at": giveaway_end_at.isoformat(),
            "pool_mode": "combined",
            "winner_count": 3,
            "channels": [
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
        }
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Move this giveaway",
                    "post_type": "giveaway",
                    "status": "draft",
                    "target_account_ids": [bluesky.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": "2026-05-16T12:00:00+00:00",
                    "giveaway": giveaway,
                }
            ),
            [],
        )
        session.commit()
        post_id = post.id
        bluesky_id = bluesky.id

    response = api_client.put(
        f"/scheduled-posts/{post_id}",
        data={
            "body": "Move this giveaway",
            "post_type": "giveaway",
            "status": "draft",
            "target_account_ids": json.dumps([bluesky_id]),
            "publish_overrides_json": json.dumps({}),
            "metadata_json": json.dumps({}),
            "scheduled_for": "2026-05-16T13:30",
            "giveaway_json": json.dumps(giveaway),
            "alt_texts": json.dumps([]),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["scheduled_for"] == "2026-05-16T13:30:00"
    assert payload["giveaway"]["giveaway_end_at"] == "2026-05-16T20:00:00"
    assert payload["giveaway"]["winner_count"] == 3

    with SessionLocal() as session:
        saved = get_post(session, post_id)
        assert saved is not None
        assert saved.scheduled_for is not None
        assert saved.scheduled_for.replace(tzinfo=timezone.utc) == datetime(2026, 5, 16, 13, 30, tzinfo=timezone.utc)
        assert saved.giveaway_campaign is not None
        assert saved.giveaway_campaign.giveaway_end_at.replace(tzinfo=timezone.utc) == giveaway_end_at
        assert saved.giveaway_campaign.winner_count == 3


def test_end_giveaway_api_collects_and_selects_winner(api_stack, monkeypatch):
    api_client, SessionLocal = api_stack
    collection_calls = []

    def fake_collect_bluesky(session, channel, *, run_id):
        collection_calls.append((run_id, channel.id))
        entrant = GiveawayEntrant(
            channel=channel,
            provider_user_id="did:plc:entrant",
            provider_username="entrant.test",
            display_label="entrant.test",
            signal_state_json={
                "reply_present": True,
                "quote_present": False,
                "like_present": False,
                "repost_present": False,
                "follow_present": None,
                "reply_posts": [{"uri": "at://did:plc:entrant/app.bsky.feed.post/reply", "text": "I am in"}],
                "quote_posts": [],
                "reply_or_quote_mention_count": 0,
            },
        )
        channel.entrants.append(entrant)

    monkeypatch.setattr("app.services.giveaway_engine.collect_bluesky_channel_state", fake_collect_bluesky)

    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-end-giveaway")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "End this giveaway",
                    "post_type": "giveaway",
                    "status": "posted",
                    "target_account_ids": [bluesky.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                    "giveaway": {
                        "giveaway_end_at": datetime.now(timezone.utc) + timedelta(days=1),
                        "pool_mode": "combined",
                        "channels": [
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
                    },
                }
            ),
            [],
        )
        job = next(job for job in post.delivery_jobs if job.target_account_id == bluesky.id)
        job.status = "posted"
        job.external_id = "3k-test"
        channel = post.giveaway_campaign.channels[0]
        channel.target_post_uri = "at://did:plc:owner/app.bsky.feed.post/3k-test"
        session.commit()
        post_id = post.id

    response = api_client.post(f"/scheduled-posts/{post_id}/giveaway/end-now")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "winner_selected"
    assert payload["pools"][0]["status"] == "winner_selected"
    assert payload["pools"][0]["final_winner"]["provider_username"] == "entrant.test"
    assert collection_calls and collection_calls[0][0]
    with SessionLocal() as session:
        saved = get_post(session, post_id)
        assert saved.giveaway_campaign.giveaway_end_at.replace(tzinfo=timezone.utc) <= datetime.now(timezone.utc)
        assert saved.giveaway_campaign.frozen_at is not None


def test_recalculate_giveaway_api_applies_updated_rules(api_stack):
    api_client, SessionLocal = api_stack
    giveaway_end_at = datetime(2026, 5, 16, 20, 0, tzinfo=timezone.utc)
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-recalculate-giveaway")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Recalculate this giveaway",
                    "post_type": "giveaway",
                    "status": "draft",
                    "target_account_ids": [bluesky.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                    "giveaway": {
                        "giveaway_end_at": giveaway_end_at,
                        "pool_mode": "combined",
                        "channels": [
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
                    },
                }
            ),
            [],
        )
        channel = post.giveaway_campaign.channels[0]
        channel.entrants.append(
            GiveawayEntrant(
                channel=channel,
                provider_user_id="did:plc:entrant",
                provider_username="entrant.test",
                display_label="entrant.test",
                signal_state_json={
                    "reply_present": True,
                    "quote_present": False,
                    "like_present": False,
                    "repost_present": False,
                    "follow_present": None,
                    "reply_posts": [{"uri": "at://did:plc:entrant/app.bsky.feed.post/reply", "text": "Count me in"}],
                    "quote_posts": [],
                    "reply_or_quote_mention_count": 0,
                },
            )
        )
        session.commit()
        post_id = post.id
        bluesky_id = bluesky.id

    updated_giveaway = {
        "giveaway_end_at": giveaway_end_at.isoformat(),
        "pool_mode": "combined",
        "channels": [
            {
                "service": "bluesky",
                "account_id": bluesky_id,
                "rules": {
                    "kind": "all",
                    "children": [
                        {"kind": "atom", "atom": "like_present", "params": {}},
                    ],
                },
            }
        ],
    }

    update_response = api_client.put(
        f"/scheduled-posts/{post_id}",
        json={"post_type": "giveaway", "giveaway": updated_giveaway},
    )
    assert update_response.status_code == 200

    response = api_client.post(f"/scheduled-posts/{post_id}/giveaway/recalculate")

    assert response.status_code == 200
    entrant = response.json()["channels"][0]["entrants"][0]
    checks = {check["atom"]: check for check in entrant["checks"]}
    assert entrant["eligibility_status"] == "disqualified"
    assert checks["like_present"]["status"] == "failed"


def test_manual_giveaway_entrant_approval_api_overrides_checks(api_stack):
    api_client, SessionLocal = api_stack
    giveaway_end_at = datetime(2026, 5, 16, 20, 0, tzinfo=timezone.utc)
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-manual-approval")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Approve this entrant",
                    "post_type": "giveaway",
                    "status": "draft",
                    "target_account_ids": [bluesky.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                    "giveaway": {
                        "giveaway_end_at": giveaway_end_at,
                        "pool_mode": "combined",
                        "channels": [
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
                    },
                }
            ),
            [],
        )
        channel = post.giveaway_campaign.channels[0]
        entrant = GiveawayEntrant(
            channel=channel,
            provider_user_id="did:plc:entrant",
            provider_username="entrant.test",
            display_label="entrant.test",
            signal_state_json={
                "reply_present": True,
                "quote_present": False,
                "like_present": False,
                "repost_present": False,
                "follow_present": None,
                "reply_posts": [{"uri": "at://did:plc:entrant/app.bsky.feed.post/reply", "text": "Count me in"}],
                "quote_posts": [],
                "reply_or_quote_mention_count": 0,
            },
        )
        channel.entrants.append(entrant)
        session.flush()
        post_id = post.id
        entrant_id = entrant.id
        session.commit()

    approve_response = api_client.post(
        f"/scheduled-posts/{post_id}/giveaway/entrants/{entrant_id}/approve",
        json={"note": "Verified manually."},
    )

    assert approve_response.status_code == 200
    approved_entrant = approve_response.json()["channels"][0]["entrants"][0]
    assert approved_entrant["eligibility_status"] == "eligible"
    assert approved_entrant["manual_review_status"] == "approved"
    assert approved_entrant["manual_review_note"] == "Verified manually."
    assert any(check["atom"] == "manual_review" for check in approved_entrant["checks"])

    clear_response = api_client.delete(f"/scheduled-posts/{post_id}/giveaway/entrants/{entrant_id}/approval")

    assert clear_response.status_code == 200
    cleared_entrant = clear_response.json()["channels"][0]["entrants"][0]
    assert cleared_entrant["eligibility_status"] == "disqualified"
    assert cleared_entrant["manual_review_status"] is None


def test_rerun_giveaway_api_selects_new_winner_from_current_evidence(api_stack, monkeypatch):
    api_client, SessionLocal = api_stack
    giveaway_end_at = datetime(2026, 5, 16, 20, 0, tzinfo=timezone.utc)

    def reverse_entries(entries):
        return list(reversed(entries))

    monkeypatch.setattr("app.services.giveaway_engine._randomize_entries", reverse_entries)

    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-rerun-giveaway")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Rerun this giveaway",
                    "post_type": "giveaway",
                    "status": "draft",
                    "target_account_ids": [bluesky.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                    "giveaway": {
                        "giveaway_end_at": giveaway_end_at,
                        "pool_mode": "combined",
                        "channels": [
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
                    },
                }
            ),
            [],
        )
        channel = post.giveaway_campaign.channels[0]
        entrant_one = GiveawayEntrant(
            channel=channel,
            provider_user_id="did:plc:first",
            provider_username="first.test",
            display_label="first.test",
            signal_state_json={
                "reply_present": True,
                "quote_present": False,
                "like_present": False,
                "repost_present": False,
                "follow_present": None,
                "reply_posts": [{"uri": "at://did:plc:first/app.bsky.feed.post/reply", "text": "First"}],
                "quote_posts": [],
                "reply_or_quote_mention_count": 0,
            },
        )
        entrant_two = GiveawayEntrant(
            channel=channel,
            provider_user_id="did:plc:second",
            provider_username="second.test",
            display_label="second.test",
            signal_state_json={
                "reply_present": True,
                "quote_present": False,
                "like_present": False,
                "repost_present": False,
                "follow_present": None,
                "reply_posts": [{"uri": "at://did:plc:second/app.bsky.feed.post/reply", "text": "Second"}],
                "quote_posts": [],
                "reply_or_quote_mention_count": 0,
            },
        )
        channel.entrants.extend([entrant_one, entrant_two])
        session.flush()
        pool = post.giveaway_campaign.pools[0]
        pool.status = "winner_selected"
        pool.candidate_entry_ids_json = [entrant_one.id, entrant_two.id]
        pool.final_winner_entry = entrant_one
        post.giveaway_campaign.status = "winner_selected"
        post.giveaway_campaign.frozen_at = datetime.now(timezone.utc)
        session.commit()
        post_id = post.id

    response = api_client.post(f"/scheduled-posts/{post_id}/giveaway/rerun-raffle")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "winner_selected"
    assert payload["pools"][0]["candidate_count"] == 2
    assert payload["pools"][0]["final_winner"]["provider_username"] == "second.test"


def test_reopen_giveaway_api_returns_closed_giveaway_to_collecting(api_stack):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-reopen-giveaway")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Reopen this giveaway",
                    "post_type": "giveaway",
                    "status": "draft",
                    "target_account_ids": [bluesky.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                    "giveaway": {
                        "giveaway_end_at": datetime.now(timezone.utc) - timedelta(hours=1),
                        "pool_mode": "combined",
                        "channels": [
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
                    },
                }
            ),
            [],
        )
        post.status = "posted"
        job = next(job for job in post.delivery_jobs if job.target_account_id == bluesky.id)
        job.status = "posted"
        job.external_id = "3k-reopen"
        channel = post.giveaway_campaign.channels[0]
        channel.status = "winner_selected"
        channel.target_post_uri = "at://did:plc:owner/app.bsky.feed.post/3k-reopen"
        entrant = GiveawayEntrant(
            channel=channel,
            provider_user_id="did:plc:first",
            provider_username="first.test",
            display_label="first.test",
            eligibility_status="eligible",
            signal_state_json={
                "reply_present": True,
                "quote_present": False,
                "like_present": False,
                "repost_present": False,
                "follow_present": None,
                "reply_posts": [{"uri": "at://did:plc:first/app.bsky.feed.post/reply", "text": "First"}],
                "quote_posts": [],
                "reply_or_quote_mention_count": 0,
            },
        )
        channel.entrants.append(entrant)
        session.flush()
        campaign = post.giveaway_campaign
        campaign.status = "winner_selected"
        campaign.frozen_at = datetime.now(timezone.utc)
        campaign.last_evaluated_at = datetime.now(timezone.utc)
        pool = campaign.pools[0]
        pool.status = "winner_selected"
        pool.candidate_entry_ids_json = [entrant.id]
        pool.final_winner_entry = entrant
        pool.frozen_at = datetime.now(timezone.utc)
        pool.last_evaluated_at = datetime.now(timezone.utc)
        session.commit()
        post_id = post.id

    new_end_at = (datetime.now(timezone.utc) + timedelta(days=2)).replace(second=0, microsecond=0)
    response = api_client.post(
        f"/scheduled-posts/{post_id}/giveaway/reopen",
        json={"giveaway_end_at": new_end_at.strftime("%Y-%m-%dT%H:%M")},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "collecting"
    assert payload["giveaway_end_at"] == new_end_at.isoformat().replace("+00:00", "Z")
    assert payload["pools"][0]["status"] == "collecting"
    assert payload["pools"][0]["candidate_count"] == 0
    assert payload["pools"][0]["final_winner"] is None

    with SessionLocal() as session:
        saved = get_post(session, post_id)
        assert saved.giveaway_campaign.status == "collecting"
        assert saved.giveaway_campaign.frozen_at is None
        assert saved.giveaway_campaign.giveaway_end_at.replace(tzinfo=timezone.utc) == new_end_at
        saved_pool = saved.giveaway_campaign.pools[0]
        assert saved_pool.status == "collecting"
        assert saved_pool.candidate_entry_ids_json == []
        assert saved_pool.final_winner_entry is None


def test_reopen_giveaway_api_rejects_past_end_time(api_stack):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-reopen-giveaway-past")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Reopen this giveaway",
                    "post_type": "giveaway",
                    "status": "draft",
                    "target_account_ids": [bluesky.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                    "giveaway": {
                        "giveaway_end_at": datetime.now(timezone.utc) - timedelta(hours=1),
                        "pool_mode": "combined",
                        "channels": [
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
                    },
                }
            ),
            [],
        )
        post.status = "posted"
        job = next(job for job in post.delivery_jobs if job.target_account_id == bluesky.id)
        job.status = "posted"
        job.external_id = "3k-reopen-past"
        channel = post.giveaway_campaign.channels[0]
        channel.target_post_uri = "at://did:plc:owner/app.bsky.feed.post/3k-reopen-past"
        post.giveaway_campaign.status = "failed"
        session.commit()
        post_id = post.id

    response = api_client.post(
        f"/scheduled-posts/{post_id}/giveaway/reopen",
        json={"giveaway_end_at": "2026-01-01T12:00"},
    )

    assert response.status_code == 400
    assert "future" in response.json()["detail"]


def test_scheduled_post_api_exposes_delivery_outcome_breakdown(api_stack):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-outcome")
        mastodon = _create_destination_account(session, persona, service="mastodon", label="Mastodon")
        discord = _create_destination_account(session, persona, service="discord", label="Discord")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Mixed results",
                    "status": "queued",
                    "target_account_ids": [mastodon.id, discord.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                }
            ),
            [],
        )
        jobs = {job.target_account_id: job for job in post.delivery_jobs}
        jobs[mastodon.id].status = "posted"
        jobs[mastodon.id].external_url = "https://example.social/@me/1"
        jobs[discord.id].status = "failed"
        jobs[discord.id].last_error = "Webhook rejected the payload."
        session.commit()
        post_id = post.id

    response = api_client.get(f"/scheduled-posts/{post_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["display_status"] == "partial_failure"
    assert [item["label"] for item in payload["delivery_breakdown"]["succeeded"]] == ["Mastodon"]
    assert payload["delivery_breakdown"]["failed"][0]["label"] == "Discord"
    assert payload["delivery_breakdown"]["failed"][0]["last_error"] == "Webhook rejected the payload."


def test_send_now_api_processes_giveaway_delivery_in_background(api_stack, monkeypatch):
    api_client, SessionLocal = api_stack
    delivery_calls = []
    lifecycle_calls = []

    def fake_process_delivery_queue(session, alerts, *, run_id=None, post_id=None):
        delivery_calls.append((run_id, post_id))
        return run_id or "run-send-now"

    def fake_process_giveaway_lifecycle(session, alerts, *, run_id, post_id=None, allow_instagram_private_scan=False):
        lifecycle_calls.append((run_id, post_id, allow_instagram_private_scan))
        return run_id

    monkeypatch.setattr("app.main.process_delivery_queue", fake_process_delivery_queue)
    monkeypatch.setattr("app.main.process_giveaway_lifecycle", fake_process_giveaway_lifecycle)

    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-send-now-giveaway")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Giveaway body",
                    "post_type": "giveaway",
                    "status": "draft",
                    "target_account_ids": [bluesky.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                    "giveaway": {
                        "giveaway_end_at": datetime.now(timezone.utc) + timedelta(days=1),
                        "pool_mode": "combined",
                        "channels": [
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
                    },
                }
            ),
            [],
        )
        session.commit()
        post_id = post.id

    response = api_client.post(f"/scheduled-posts/{post_id}/send-now")

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert len(delivery_calls) == 1
    assert len(lifecycle_calls) == 1
    assert delivery_calls[0][0]
    assert delivery_calls[0][1] == post_id
    assert lifecycle_calls[0] == (delivery_calls[0][0], delivery_calls[0][1], False)


def test_send_now_api_returns_success_when_background_delivery_fails(api_stack, monkeypatch):
    api_client, SessionLocal = api_stack

    def fail_delivery(session, alerts, *, run_id=None, post_id=None):
        raise TimeoutError("delivery timed out")

    monkeypatch.setattr("app.main.process_delivery_queue", fail_delivery)

    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-send-now-background-failure")
        destination = _create_destination_account(session, persona)
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Background delivery failure",
                    "status": "draft",
                    "target_account_ids": [destination.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                }
            ),
            [],
        )
        session.commit()
        post_id = post.id

    response = api_client.post(f"/scheduled-posts/{post_id}/send-now")

    assert response.status_code == 200
    assert response.json()["status"] == "queued"

    with SessionLocal() as session:
        saved = get_post(session, post_id)
        assert saved is not None
        assert saved.last_error == "delivery timed out"
        alert = session.query(AlertEvent).filter(AlertEvent.operation == "publish_now").one()
        assert alert.post_id == post_id
        assert alert.error_class == "TimeoutError"


def test_delete_scheduled_post_api_removes_draft(api_stack):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-delete")
        destination = _create_destination_account(session, persona)
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Delete this draft",
                    "status": "draft",
                    "target_account_ids": [destination.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                }
            ),
            [],
        )
        session.commit()
        post_id = post.id

    response = api_client.delete(f"/scheduled-posts/{post_id}")

    assert response.status_code == 200
    assert response.json() == {"deleted_post_id": post_id}

    with SessionLocal() as session:
        assert get_post(session, post_id) is None


def test_delete_scheduled_post_api_removes_giveaway_draft(api_stack):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-delete-giveaway")
        bluesky = _create_destination_account(session, persona, service="bluesky", label="Bluesky")
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Delete this giveaway",
                    "post_type": "giveaway",
                    "status": "draft",
                    "target_account_ids": [bluesky.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": None,
                    "giveaway": {
                        "giveaway_end_at": datetime(2026, 5, 16, 20, 0, tzinfo=timezone.utc),
                        "pool_mode": "combined",
                        "channels": [
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
                    },
                }
            ),
            [],
        )
        session.commit()
        post_id = post.id

    response = api_client.delete(f"/scheduled-posts/{post_id}")

    assert response.status_code == 200
    assert response.json() == {"deleted_post_id": post_id}

    with SessionLocal() as session:
        assert get_post(session, post_id) is None


def test_delete_scheduled_post_api_removes_needs_attention_post(api_stack):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-delete-attention")
        destination = _create_destination_account(session, persona)
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Failed post",
                    "status": "scheduled",
                    "target_account_ids": [destination.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": "2026-04-15T12:00:00+00:00",
                }
            ),
            [],
        )
        for job in post.delivery_jobs:
            job.status = "failed"
            job.last_error = "Delivery failed."
        post.status = "failed"
        session.commit()
        post_id = post.id

    detail_response = api_client.get(f"/scheduled-posts/{post_id}")
    response = api_client.delete(f"/scheduled-posts/{post_id}")

    assert detail_response.status_code == 200
    assert detail_response.json()["display_status"] == "failure"
    assert detail_response.json()["can_delete"] is True
    assert response.status_code == 200
    assert response.json() == {"deleted_post_id": post_id}

    with SessionLocal() as session:
        assert get_post(session, post_id) is None


def test_delete_scheduled_post_api_rejects_active_post(api_stack):
    api_client, SessionLocal = api_stack
    with SessionLocal() as session:
        persona = _create_persona(session, slug="scheduled-post-api-delete-reject")
        destination = _create_destination_account(session, persona)
        post = create_scheduled_post(
            session,
            ScheduledPostCreate.model_validate(
                {
                    "persona_id": persona.id,
                    "body": "Scheduled post",
                    "status": "scheduled",
                    "target_account_ids": [destination.id],
                    "publish_overrides_json": {},
                    "metadata_json": {},
                    "scheduled_for": "2026-04-15T12:00:00+00:00",
                }
            ),
            [],
        )
        session.commit()
        post_id = post.id

    response = api_client.delete(f"/scheduled-posts/{post_id}")

    assert response.status_code == 400
    assert response.json()["detail"] == "Only draft or needs-attention scheduled posts can be deleted."

    with SessionLocal() as session:
        assert get_post(session, post_id) is not None
