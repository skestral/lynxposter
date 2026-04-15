from __future__ import annotations

from app.models import AlertEvent, RunEvent
from app.services.alerts import AlertDispatcher
from app.services.events import log_run_event
from app.services.logs import clear_alert_events, serialize_alert_event, serialize_run_event, summarize_run_events
from app.services.personas import create_account, create_persona


def _create_persona(session):
    return create_persona(
        session,
        {
            "name": "Logs",
            "slug": "logs",
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def test_warning_run_event_sends_structured_notifications(session, monkeypatch):
    sent_webhook_payloads = []
    sent_discord_payloads = []
    monkeypatch.setattr("app.services.events.send_webhook_payload", lambda payload: sent_webhook_payloads.append(payload))
    monkeypatch.setattr("app.services.events.send_discord_webhook_payload", lambda payload: sent_discord_payloads.append(payload))
    monkeypatch.setattr("app.services.events.severity_meets_threshold", lambda severity, threshold: True)

    persona = _create_persona(session)
    event = log_run_event(
        session,
        run_id="run-warning",
        persona_id=persona.id,
        persona_name=persona.name,
        service="scheduler",
        operation="poll",
        severity="warning",
        message="Warning event",
        metadata={"imported_count": 2},
    )

    assert event.id
    assert sent_webhook_payloads
    assert sent_discord_payloads
    assert sent_webhook_payloads[0]["event_type"] == "run_event"
    assert sent_webhook_payloads[0]["persona_id"] == persona.id
    assert sent_webhook_payloads[0]["persona_name"] == persona.name
    assert sent_webhook_payloads[0]["payload"]["imported_count"] == 2
    assert sent_discord_payloads[0]["event_type"] == "run_event"
    assert sent_discord_payloads[0]["persona_id"] == persona.id
    assert sent_discord_payloads[0]["persona_name"] == persona.name


def test_alert_persists_when_webhook_send_fails(session, monkeypatch):
    monkeypatch.setattr("app.services.events.send_webhook_payload", lambda payload: (_ for _ in ()).throw(RuntimeError("offline")))

    persona = _create_persona(session)
    account = create_account(
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

    alert = AlertDispatcher().emit_hard_failure(
        session,
        run_id="run-error",
        persona=persona,
        account=account,
        service=account.service,
        operation="publish",
        message="Publish failed",
        error_class="RuntimeError",
        retry_count=1,
    )

    assert alert is not None
    assert alert.id
    assert alert.message == "Publish failed"


def test_log_serializers_include_persona_and_account_context(session):
    persona = _create_persona(session)
    account = create_account(
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

    run_event = log_run_event(
        session,
        run_id="run-context",
        persona_id=persona.id,
        persona_name=persona.name,
        account_id=account.id,
        service=account.service,
        operation="publish",
        severity="warning",
        message="Context event",
    )
    alert_event = AlertDispatcher().emit_hard_failure(
        session,
        run_id="run-context-alert",
        persona=persona,
        account=account,
        service=account.service,
        operation="publish",
        message="Alert context",
        error_class="RuntimeError",
        retry_count=1,
    )

    run_read = serialize_run_event(run_event)
    alert_read = serialize_alert_event(alert_event)

    assert run_read.persona_name == persona.name
    assert run_read.account_label == account.label
    assert alert_read.persona_name == persona.name
    assert alert_read.account_label == account.label


def test_log_serializers_fall_back_to_account_persona_when_direct_persona_is_missing(session):
    persona = _create_persona(session)
    account = create_account(
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

    run_event = RunEvent(
        run_id="run-account-fallback",
        account_id=account.id,
        service=account.service,
        operation="publish",
        severity="info",
        message="Fallback run event",
        metadata_json={},
    )
    alert_event = AlertEvent(
        run_id="run-account-fallback",
        fingerprint="alert-fallback",
        event_type="publish_failed",
        severity="error",
        account_id=account.id,
        service=account.service,
        operation="publish",
        message="Fallback alert event",
        error_class="RuntimeError",
        retry_count=0,
        payload_json={},
    )
    session.add_all([run_event, alert_event])
    session.flush()

    run_read = serialize_run_event(run_event)
    alert_read = serialize_alert_event(alert_event)

    assert run_read.persona_name == persona.name
    assert alert_read.persona_name == persona.name


def test_clear_alert_events_respects_filters(session):
    persona = _create_persona(session)
    account = create_account(
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

    dispatcher = AlertDispatcher()
    dispatcher.emit_hard_failure(
        session,
        run_id="run-error-1",
        persona=persona,
        account=account,
        service="mastodon",
        operation="publish",
        message="Publish failed",
        error_class="RuntimeError",
        retry_count=1,
    )
    dispatcher.emit_hard_failure(
        session,
        run_id="run-error-2",
        persona=persona,
        account=account,
        service="discord",
        operation="publish",
        message="Discord failed",
        error_class="RuntimeError",
        retry_count=1,
    )

    cleared_count = clear_alert_events(session, filters={"service": "mastodon"})

    assert cleared_count == 1
    remaining = session.query(AlertEvent).all()
    assert len(remaining) == 1
    assert remaining[0].service == "discord"


def test_summarize_run_events_rolls_up_run_and_persona_metrics(session):
    first_persona = create_persona(
        session,
        {
            "name": "Lynx",
            "slug": "lynx-rollup",
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )
    second_persona = create_persona(
        session,
        {
            "name": "Savannah",
            "slug": "savannah-rollup",
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )
    first_account = create_account(
        session,
        first_persona,
        {
            "service": "bluesky",
            "label": "Lynx Bluesky",
            "handle_or_identifier": "@lynx",
            "is_enabled": True,
            "source_enabled": True,
            "destination_enabled": True,
            "credentials_json": {"handle": "me.bsky.social", "password": "pw"},
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )
    second_account = create_account(
        session,
        second_persona,
        {
            "service": "discord",
            "label": "Savannah Discord",
            "handle_or_identifier": "Webhook",
            "is_enabled": True,
            "source_enabled": False,
            "destination_enabled": True,
            "credentials_json": {"webhook_url": "https://discord.test"},
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )

    run_events = [
        log_run_event(
            session,
            run_id="run-rollup",
            operation="automation_cycle",
            message="Autorun automation cycle started.",
            metadata={"trigger": "autorun"},
        ),
        log_run_event(
            session,
            run_id="run-rollup",
            persona_id=first_persona.id,
            persona_name=first_persona.name,
            account_id=first_account.id,
            service=first_account.service,
            operation="poll",
            message="Imported 3 posts from Lynx Bluesky",
            metadata={"trigger": "autorun", "imported_count": 3},
        ),
        log_run_event(
            session,
            run_id="run-rollup",
            persona_id=second_persona.id,
            persona_name=second_persona.name,
            account_id=second_account.id,
            service=second_account.service,
            operation="publish",
            message="Published post post-1 to Savannah Discord",
            metadata={"trigger": "autorun"},
        ),
        log_run_event(
            session,
            run_id="run-rollup",
            persona_id=second_persona.id,
            persona_name=second_persona.name,
            account_id=second_account.id,
            service=second_account.service,
            operation="schedule",
            severity="error",
            message="Post could not be queued",
            metadata={"trigger": "autorun"},
        ),
        log_run_event(
            session,
            run_id="run-rollup",
            operation="automation_cycle",
            message="Autorun automation cycle completed.",
            metadata={"trigger": "autorun"},
        ),
    ]

    grouped = summarize_run_events([serialize_run_event(event) for event in run_events])

    assert len(grouped) == 1
    run = grouped[0]
    assert run["trigger"] == "autorun"
    assert run["counts"]["personas"] == 2
    assert run["counts"]["accounts"] == 2
    assert run["counts"]["posts_found"] == 3
    assert run["counts"]["reposted"] == 1
    assert run["counts"]["queued"] == 0
    assert run["counts"]["errors"] == 1
    assert len(run["persona_summaries"]) == 2
    assert run["persona_summaries"][0]["persona_name"] == "Savannah"
    assert run["persona_summaries"][1]["persona_name"] == "Lynx"
