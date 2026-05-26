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
from app.main import _visible_dashboard_alerts


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


def test_dashboard_alerts_are_limited_to_recent_items(session):
    persona = _create_persona(session)
    alerts = [_create_alert(session, persona, run_id=f"run-{index}", message=f"Alert {index}") for index in range(12)]
    request = _request_with_session()

    visible = _visible_dashboard_alerts(request, list(reversed(alerts)))

    assert len(visible) == 10
    assert visible[0].message == "Alert 11"
    assert visible[-1].message == "Alert 2"


def _install_dashboard_test_app(monkeypatch, tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'dashboard-{id(tmp_path)}.db'}",
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
    return engine, SessionLocal


def test_dashboard_alert_clear_route_deletes_alerts(monkeypatch, tmp_path):
    engine, SessionLocal = _install_dashboard_test_app(monkeypatch, tmp_path)

    try:
        with SessionLocal() as session:
            persona = _create_persona(session, name="Savannah", slug="savannah-dashboard-clear")
            alert = _create_alert(session, persona, run_id="run-alert", message="Dashboard alert")
            alert_id = alert.id
            session.commit()

        with TestClient(app) as client:
            response = client.post("/dashboard/alerts/clear", follow_redirects=True)

        assert response.status_code == 200
        assert "Cleared 1 dashboard alert" in response.text
        with SessionLocal() as session:
            assert session.get(AlertEvent, alert_id) is None
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_dashboard_route_renders_persona_names_for_alerts_and_run_events(monkeypatch, tmp_path):
    engine, SessionLocal = _install_dashboard_test_app(monkeypatch, tmp_path)

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


def test_dashboard_v2_route_renders_ops_health_for_admin(monkeypatch, tmp_path):
    engine, SessionLocal = _install_dashboard_test_app(monkeypatch, tmp_path)

    try:
        with SessionLocal() as session:
            persona = _create_persona(session, name="Savannah", slug="savannah-dashboard-v2")
            _create_alert(session, persona, run_id="run-alert-v2", message="Dashboard V2 alert")
            _create_run_event(session, persona, run_id="run-event-v2", message="Dashboard V2 event")
            post = CanonicalPost(
                persona_id=persona.id,
                origin_kind="composer",
                post_type="standard",
                status="scheduled",
                body="Dashboard V2 scheduled post",
                publish_overrides_json={},
                metadata_json={},
                scheduled_for=datetime.now(timezone.utc) + timedelta(hours=2),
            )
            session.add(post)
            session.add(
                InstagramGiveawayWebhookEvent(
                    provider_event_field="comments",
                    event_type="comment",
                    payload_json={"entry": {"id": "instagram-account"}, "change": {"field": "comments", "value": {"id": "comment-1"}}},
                    signature_valid=True,
                    processed=True,
                )
            )
            session.commit()

        with TestClient(app) as client:
            response = client.get("/dashboard-v2")

        assert response.status_code == 200
        assert "Dashboard V2" in response.text
        assert "Ops Health" in response.text
        assert "Dashboard V2 scheduled post" in response.text
        assert "Dashboard V2 alert" in response.text
        assert "Instagram Webhooks" in response.text
        assert "dashboard-v2-tabs" not in response.text
        assert "dashboard-v2-live-update-status" in response.text
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_dashboard_v2_route_respects_user_scope(monkeypatch, tmp_path):
    engine, SessionLocal = _install_dashboard_test_app(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "app.main.build_principal_from_request",
        lambda request: Principal(
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
            is_authenticated=True,
        ),
    )

    try:
        with SessionLocal() as session:
            owned_persona = _create_persona(session, name="Owned", slug="owned-dashboard-v2", owner_user_id="user-1")
            other_persona = _create_persona(session, name="Other", slug="other-dashboard-v2", owner_user_id="other-user")
            session.add_all(
                [
                    CanonicalPost(
                        persona_id=owned_persona.id,
                        origin_kind="composer",
                        post_type="standard",
                        status="scheduled",
                        body="Owned user post",
                        publish_overrides_json={},
                        metadata_json={},
                        scheduled_for=datetime.now(timezone.utc) + timedelta(hours=2),
                    ),
                    CanonicalPost(
                        persona_id=other_persona.id,
                        origin_kind="composer",
                        post_type="standard",
                        status="scheduled",
                        body="Other user post",
                        publish_overrides_json={},
                        metadata_json={},
                        scheduled_for=datetime.now(timezone.utc) + timedelta(hours=3),
                    ),
                ]
            )
            session.commit()

        with TestClient(app) as client:
            response = client.get("/dashboard-v2")

        assert response.status_code == 200
        assert "Owned user post" in response.text
        assert "Other user post" not in response.text
        assert "Instagram Webhooks" in response.text
        assert "Admin Only" in response.text
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()
