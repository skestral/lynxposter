from __future__ import annotations

import pytest

from app.domain import CanonicalPostPayload, ExternalPostRefPayload, PublishResult
from app.models import AccountPostRef, CanonicalPost, DeliveryJob
from app.schemas import ScheduledPostCreate
from app.services.alerts import AlertDispatcher
from app.services.delivery import process_delivery_queue
from app.services.personas import create_account, create_persona, get_persona, replace_routes
from app.services.posts import create_scheduled_post, upsert_polled_post


def _persona(session, slug="dedupe"):
    return create_persona(session, {
        "name": slug, "slug": slug, "is_enabled": True, "timezone": "server",
        "settings_json": {}, "retry_settings_json": {"max_retries": 3},
        "throttle_settings_json": {"max_per_hour": 0},
    })


def _account(session, persona, label, *, service="mastodon", source=False, destination=True):
    credentials = {"instance": "https://example.social", "token": "test-token", "handle": "@me@example.social"}
    if service == "bluesky":
        credentials = {"handle": "me.bsky.social", "password": "test-password"}
    return create_account(session, persona, {
        "service": service, "label": label, "handle_or_identifier": label,
        "is_enabled": True, "source_enabled": source, "destination_enabled": destination,
        "credentials_json": credentials, "source_settings_json": {}, "publish_settings_json": {},
    })


def _scheduled(session, persona, target):
    return create_scheduled_post(session, ScheduledPostCreate.model_validate({
        "persona_id": persona.id, "body": "Scheduled original", "status": "queued",
        "target_account_ids": [target.id], "publish_overrides_json": {}, "metadata_json": {},
    }), [])


def _poll(session, persona, account, external_id="101", url="https://example.social/@me/101"):
    return upsert_polled_post(session, get_persona(session, persona.id), account, CanonicalPostPayload(
        body="Scheduled original", external_refs=[ExternalPostRefPayload(external_id=external_id, external_url=url)],
    ))


def _adapter(monkeypatch, calls):
    class FakeDestinationAdapter:
        def validate(self, post, persona, account):
            return []

        def publish(self, session, post, persona, account, *, context=None):
            calls.append((post.id, account.id))
            return PublishResult(service=account.service, external_id="101", external_url="https://example.social/@me/101")

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", lambda account: FakeDestinationAdapter())


def test_published_reference_survives_later_cycle_rollback_and_rescan(session, monkeypatch):
    persona = _persona(session)
    mastodon = _account(session, persona, "Mastodon", source=True)
    bluesky = _account(session, persona, "Bluesky", service="bluesky")
    replace_routes(session, get_persona(session, persona.id), [{
        "source_account_id": mastodon.id, "destination_account_id": bluesky.id, "is_enabled": True,
    }])
    post = _scheduled(session, persona, mastodon)
    session.commit()
    calls = []
    _adapter(monkeypatch, calls)

    process_delivery_queue(session, AlertDispatcher(), run_id="first-cycle")
    # A failure in a later scheduler step rolls back the remaining cycle work.
    session.rollback()
    imported = _poll(session, persona, mastodon)
    process_delivery_queue(session, AlertDispatcher(), run_id="rescan")

    assert imported.id == post.id
    assert session.query(CanonicalPost).count() == 1
    assert calls == [(post.id, mastodon.id)]
    assert session.query(DeliveryJob).one().status == "posted"
    assert session.query(AccountPostRef).one().external_id == "101"


def test_local_cleanup_failure_keeps_successful_remote_delivery_posted(session, monkeypatch):
    persona = _persona(session)
    mastodon = _account(session, persona, "Mastodon", source=True)
    post = _scheduled(session, persona, mastodon)
    session.commit()
    calls = []
    _adapter(monkeypatch, calls)

    def fail_cleanup(session, post):
        raise OSError("local cleanup failed")

    monkeypatch.setattr("app.services.delivery.remove_post_media_files", fail_cleanup)
    process_delivery_queue(session, AlertDispatcher(), run_id="cleanup-failed")
    session.rollback()

    job = session.query(DeliveryJob).one()
    assert calls == [(post.id, mastodon.id)]
    assert job.status == "posted"
    assert job.last_error is None
    assert job.attempts[0].status == "posted"
    assert _poll(session, persona, mastodon).id == post.id


def test_source_rescan_recovers_reference_from_saved_successful_delivery(session):
    persona = _persona(session)
    mastodon = _account(session, persona, "Mastodon", source=True)
    post = _scheduled(session, persona, mastodon)
    job = post.delivery_jobs[0]
    job.status = "posted"
    job.external_id = "101"
    job.external_url = "https://example.social/@me/101"
    session.commit()

    assert _poll(session, persona, mastodon).id == post.id
    assert session.query(CanonicalPost).count() == 1
    assert session.query(AccountPostRef).one().post_id == post.id


def test_queue_recovers_known_external_id_without_republishing(session, monkeypatch):
    persona = _persona(session)
    mastodon = _account(session, persona, "Mastodon", source=True)
    post = _scheduled(session, persona, mastodon)
    post.delivery_jobs[0].external_id = "101"
    post.delivery_jobs[0].external_url = "https://example.social/@me/101"
    session.commit()
    calls = []
    _adapter(monkeypatch, calls)

    process_delivery_queue(session, AlertDispatcher(), run_id="recover")
    session.rollback()

    assert calls == []
    assert session.query(DeliveryJob).one().status == "posted"
    assert session.query(AccountPostRef).one().post_id == post.id
    assert _poll(session, persona, mastodon).id == post.id


def test_successful_delivery_keeps_id_when_response_serialization_fails(session, monkeypatch):
    persona = _persona(session)
    mastodon = _account(session, persona, "Mastodon", source=True)
    post = _scheduled(session, persona, mastodon)
    session.commit()
    calls = []
    _adapter(monkeypatch, calls)

    def fail_serialization(raw):
        raise TypeError("unserializable response")

    monkeypatch.setattr("app.services.delivery.to_json_compatible", fail_serialization)
    process_delivery_queue(session, AlertDispatcher(), run_id="response-error")
    session.rollback()

    job = session.query(DeliveryJob).one()
    assert job.status == "posted"
    assert job.attempts[0].response_payload == {"serialization_error": "TypeError"}
    assert _poll(session, persona, mastodon).id == post.id


def test_delivery_saves_primary_id_and_unique_additional_references(session, monkeypatch):
    persona = _persona(session)
    mastodon = _account(session, persona, "Mastodon", source=True)
    post = _scheduled(session, persona, mastodon)

    class FakeDestinationAdapter:
        def validate(self, post, persona, account):
            return []

        def publish(self, session, post, persona, account, *, context=None):
            return PublishResult(service=account.service, external_id="101", external_refs=[
                ExternalPostRefPayload(external_id=" 102 "),
                ExternalPostRefPayload(external_id="102"),
            ])

    monkeypatch.setattr("app.services.delivery.get_destination_adapter_for_account", lambda account: FakeDestinationAdapter())
    process_delivery_queue(session, AlertDispatcher(), run_id="multiple-refs")
    session.rollback()

    assert session.query(DeliveryJob).one().status == "posted"
    assert {ref.external_id for ref in session.query(AccountPostRef)} == {"101", "102"}
    assert _poll(session, persona, mastodon, external_id="102", url=None).id == post.id


def test_same_remote_url_with_new_local_id_reuses_original_post(session, monkeypatch):
    persona = _persona(session)
    source = _account(session, persona, "Mastodon", source=True)
    bluesky = _account(session, persona, "Bluesky", service="bluesky")
    replace_routes(session, get_persona(session, persona.id), [{
        "source_account_id": source.id, "destination_account_id": bluesky.id, "is_enabled": True,
    }])
    post = _scheduled(session, persona, source)
    calls = []
    _adapter(monkeypatch, calls)
    process_delivery_queue(session, AlertDispatcher(), run_id="scheduled")

    # Federation can assign a different local ID to the same canonical status URL.
    assert _poll(session, persona, source, external_id="202").id == post.id
    assert session.query(CanonicalPost).count() == 1
    assert session.query(DeliveryJob).count() == 1
    assert {ref.external_id for ref in session.query(AccountPostRef).filter_by(account_id=source.id)} == {"101", "202"}
    assert _poll(session, persona, source, external_id="202").id == post.id
    assert calls == [(post.id, source.id)]


@pytest.mark.parametrize("difference", ["instance", "persona", "service"])
def test_source_deduplication_does_not_merge_unrelated_remote_posts(session, difference):
    persona = _persona(session)
    destination = _account(session, persona, "Mastodon destination")
    post = _scheduled(session, persona, destination)
    session.add(AccountPostRef(post_id=post.id, account_id=destination.id, external_id="101", external_url="https://example.social/@me/101"))
    source_persona = _persona(session, "other-persona") if difference == "persona" else persona
    service = "bluesky" if difference == "service" else "mastodon"
    source = destination if difference == "instance" else _account(session, source_persona, "source", service=service, source=True, destination=False)
    session.commit()
    url = "https://different.social/@me/101" if difference == "instance" else "https://example.social/@me/101"

    imported = _poll(session, source_persona, source, url=url)

    assert imported.id != post.id
    assert session.query(CanonicalPost).count() == 2
