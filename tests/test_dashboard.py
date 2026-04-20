from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from starlette.requests import Request
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.main import app
from app.models import (
    Account,
    AlertEvent,
    CanonicalPost,
    GiveawayCampaign,
    GiveawayChannel,
    GiveawayEntrant,
    GiveawayEvidenceEvent,
    InstagramGiveawayWebhookEvent,
    Persona,
    RunEvent,
)
from app.services.auth import Principal
from app.main import _dismiss_dashboard_alerts, _visible_dashboard_alerts


def _request_with_session() -> Request:
    return Request({"type": "http", "headers": [], "session": {}})


def _create_persona(
    session,
    *,
    name: str = "Dashboard",
    slug: str = "dashboard",
    owner_user_id: str | None = "admin-user",
) -> Persona:
    persona = Persona(
        name=name,
        slug=slug,
        owner_user_id=owner_user_id,
        is_enabled=True,
        timezone="UTC",
        settings_json={},
        retry_settings_json={"max_retries": 3},
        throttle_settings_json={"max_per_hour": 0, "overflow_posts": "retry"},
    )
    session.add(persona)
    session.flush()
    return persona


def _create_alert(session, persona: Persona, *, run_id: str, message: str) -> AlertEvent:
    alert = AlertEvent(
        run_id=run_id,
        fingerprint=f"{run_id}-fingerprint",
        event_type="publish_failed",
        severity="error",
        persona_id=persona.id,
        operation="publish",
        message=message,
        retry_count=0,
        payload_json={},
    )
    session.add(alert)
    session.flush()
    return alert


def _create_run_event(session, persona: Persona, *, run_id: str, message: str) -> RunEvent:
    event = RunEvent(
        run_id=run_id,
        persona_id=persona.id,
        operation="poll",
        severity="info",
        message=message,
        metadata_json={},
    )
    session.add(event)
    session.flush()
    return event


def _create_account(session, persona: Persona, *, service: str, label: str, handle: str = "") -> Account:
    account = Account(
        persona_id=persona.id,
        service=service,
        label=label,
        handle_or_identifier=handle,
        is_enabled=True,
        source_enabled=False,
        destination_enabled=True,
        credentials_json={},
        source_settings_json={},
        publish_settings_json={},
    )
    session.add(account)
    session.flush()
    return account


def test_dashboard_alert_clear_only_hides_alerts_from_dashboard(session):
    persona = _create_persona(session)
    first = _create_alert(session, persona, run_id="run-1", message="First alert")
    second = _create_alert(session, persona, run_id="run-2", message="Second alert")
    request = _request_with_session()

    visible_before = _visible_dashboard_alerts(request, [second, first])
    dismissed_count = _dismiss_dashboard_alerts(request, visible_before)
    visible_after = _visible_dashboard_alerts(request, [second, first])

    assert [alert.id for alert in visible_before] == [second.id, first.id]
    assert dismissed_count == 2
    assert visible_after == []
    assert session.get(AlertEvent, first.id) is not None
    assert session.get(AlertEvent, second.id) is not None


def test_dashboard_alert_clear_does_not_hide_newer_alerts(session):
    persona = _create_persona(session)
    older = _create_alert(session, persona, run_id="run-older", message="Older alert")
    request = _request_with_session()

    _dismiss_dashboard_alerts(request, [older])

    newer = _create_alert(session, persona, run_id="run-newer", message="Newer alert")
    visible = _visible_dashboard_alerts(request, [newer, older])

    assert [alert.id for alert in visible] == [newer.id]


def test_dashboard_route_renders_persona_names_for_alerts_and_run_events(monkeypatch, tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'dashboard.db'}",
        future=True,
        connect_args={"check_same_thread": False},
    )
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

    try:
        with SessionLocal() as session:
            persona = _create_persona(session, name="Savannah", slug="savannah-dashboard")
            _create_alert(session, persona, run_id="run-alert", message="Dashboard alert")
            _create_run_event(session, persona, run_id="run-event", message="Dashboard event")
            instagram_account = _create_account(session, persona, service="instagram", label="Savannah IG", handle="savannah.ig")
            bluesky_account = _create_account(session, persona, service="bluesky", label="Savannah Bsky", handle="savannah.test")
            post = CanonicalPost(
                persona_id=persona.id,
                origin_kind="manual",
                post_type="giveaway",
                status="scheduled",
                body="Spring giveaway with Instagram and Bluesky entrants",
                publish_overrides_json={},
                metadata_json={},
                scheduled_for=datetime.now(timezone.utc) + timedelta(hours=2),
            )
            session.add(post)
            session.flush()
            campaign = GiveawayCampaign(
                post_id=post.id,
                giveaway_end_at=datetime.now(timezone.utc) + timedelta(hours=6),
                pool_mode="separate",
                status="collecting",
            )
            session.add(campaign)
            session.flush()
            instagram_channel = GiveawayChannel(
                campaign_id=campaign.id,
                service="instagram",
                account_id=instagram_account.id,
                rules_json={"kind": "all", "children": []},
                status="collecting",
                target_post_url="https://instagram.example/p/giveaway",
            )
            bluesky_channel = GiveawayChannel(
                campaign_id=campaign.id,
                service="bluesky",
                account_id=bluesky_account.id,
                rules_json={"kind": "all", "children": []},
                status="collecting",
                target_post_url="https://bsky.app/profile/savannah.test/post/giveaway",
            )
            session.add_all([instagram_channel, bluesky_channel])
            session.flush()
            instagram_entrant = GiveawayEntrant(
                channel_id=instagram_channel.id,
                provider_user_id="ig-user-1",
                provider_username="entrant.one",
                display_label="entrant.one",
                signal_state_json={"comment_count": 1},
                rule_match_details_json={},
                eligibility_status="pending",
                inconclusive_reasons_json=[],
                disqualification_reasons_json=[],
            )
            bluesky_entrant = GiveawayEntrant(
                channel_id=bluesky_channel.id,
                provider_user_id="did:plc:user-1",
                provider_username="bsky.one",
                display_label="bsky.one",
                signal_state_json={"reply_present": True},
                rule_match_details_json={},
                eligibility_status="pending",
                inconclusive_reasons_json=[],
                disqualification_reasons_json=[],
            )
            session.add_all([instagram_entrant, bluesky_entrant])
            session.flush()
            session.add_all(
                [
                    GiveawayEvidenceEvent(
                        campaign_id=campaign.id,
                        channel_id=instagram_channel.id,
                        entrant_id=instagram_entrant.id,
                        provider_event_id="comment-1",
                        event_type="instagram_comment",
                        source="webhook_capture",
                        payload_json={
                            "change": {
                                "value": {
                                    "text": "Joined via giveaway card",
                                    "from": {"id": "ig-user-1", "username": "entrant.one"},
                                }
                            }
                        },
                    ),
                    GiveawayEvidenceEvent(
                        campaign_id=campaign.id,
                        channel_id=bluesky_channel.id,
                        entrant_id=bluesky_entrant.id,
                        provider_event_id="at://did:plc:user-1/app.bsky.feed.post/reply-1",
                        event_type="bluesky_reply",
                        source="collector",
                        payload_json={
                            "actor_did": "did:plc:user-1",
                            "actor_handle": "bsky.one",
                            "actor_display_label": "bsky.one",
                            "text": "ready to join",
                            "uri": "at://did:plc:user-1/app.bsky.feed.post/reply-1",
                            "last_seen_at": datetime.now(timezone.utc).isoformat(),
                        },
                    ),
                ]
            )
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
                                "message": {"mid": "test-mid", "text": "Inbox ping"},
                                "from": {"id": "user-2", "username": "dm.user"},
                            },
                        },
                    },
                    signature_valid=True,
                    processed=True,
                )
            )
            session.commit()

        with TestClient(app) as client:
            response = client.get("/")
            bluesky_filtered = client.get("/?activity_service=bluesky&activity_event_type=bluesky_reply")

        assert response.status_code == 200
        assert bluesky_filtered.status_code == 200
        assert "Savannah" in response.text
        assert "Dashboard alert" in response.text
        assert "Dashboard event" in response.text
        assert "Giveaway Activity Monitor" in response.text
        assert "Tracked Entrants" in response.text
        assert "Joined via giveaway card" in response.text
        assert "Count me in @friend" in response.text
        assert "ready to join" in response.text
        assert "Bluesky" in response.text
        assert "Instagram Webhooks" in response.text
        assert "Comments" in response.text
        assert "Count me in @friend" in response.text
        assert "Messages" in response.text
        assert "No persona" not in response.text
        assert "ready to join" in bluesky_filtered.text
        assert "Joined via giveaway card" not in bluesky_filtered.text
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()
