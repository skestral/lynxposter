from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.main import app
from app.models import User
from app.services.auth import Principal
from app.services.personas import (
    accept_persona_access_for_user,
    account_to_read,
    create_account,
    create_persona,
    create_persona_access,
    decline_persona_access_for_user,
    get_persona,
    get_account,
    link_pending_persona_access_for_user,
    list_persona_access,
    list_persona_access_with_owner,
    list_pending_persona_access_for_user,
    list_personas,
    user_can_manage_persona_access,
)


def _user(session, user_id: str, email: str) -> User:
    user = User(
        id=user_id,
        oidc_sub=f"oidc:{user_id}",
        email=email,
        display_name=user_id.title(),
        role="user",
        timezone="UTC",
    )
    session.add(user)
    session.flush()
    return user


def _persona(session, *, owner_user_id: str):
    return create_persona(
        session,
        {
            "name": "Shared Persona",
            "slug": "shared-persona",
            "owner_user_id": owner_user_id,
            "is_enabled": True,
            "timezone": "UTC",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def test_persona_access_filters_owner_edit_view_and_unrelated_users(session):
    owner = _user(session, "owner-user", "owner@example.com")
    editor = _user(session, "edit-user", "edit@example.com")
    viewer = _user(session, "view-user", "view@example.com")
    outsider = _user(session, "other-user", "other@example.com")
    persona = _persona(session, owner_user_id=owner.id)

    create_persona_access(session, persona, {"user_id": editor.id, "permission": "edit"}, created_by_user_id=owner.id)
    create_persona_access(session, persona, {"user_id": viewer.id, "permission": "view"}, created_by_user_id=owner.id)

    assert [item.id for item in list_personas(session, owner_user_id=owner.id, access_user_id=owner.id)] == [persona.id]
    assert [item.id for item in list_personas(session, owner_user_id=editor.id, access_user_id=editor.id, min_permission="edit")] == [persona.id]
    assert [item.id for item in list_personas(session, owner_user_id=viewer.id, access_user_id=viewer.id, min_permission="view")] == [persona.id]
    assert list_personas(session, owner_user_id=viewer.id, access_user_id=viewer.id, min_permission="edit") == []
    assert list_personas(session, owner_user_id=outsider.id, access_user_id=outsider.id) == []

    assert get_persona(session, persona.id, owner_user_id=editor.id, access_user_id=editor.id, min_permission="edit")
    assert get_persona(session, persona.id, owner_user_id=viewer.id, access_user_id=viewer.id, min_permission="view")
    assert get_persona(session, persona.id, owner_user_id=viewer.id, access_user_id=viewer.id, min_permission="edit") is None
    assert user_can_manage_persona_access(persona, owner.id)
    assert not user_can_manage_persona_access(persona, editor.id)
    assert user_can_manage_persona_access(persona, outsider.id, is_admin=True)


def test_persona_access_listing_includes_original_owner(session):
    owner = _user(session, "owner-user", "owner@example.com")
    editor = _user(session, "edit-user", "edit@example.com")
    persona = _persona(session, owner_user_id=owner.id)
    create_persona_access(session, persona, {"user_id": editor.id, "permission": "edit"}, created_by_user_id=owner.id)

    access_rows = list_persona_access_with_owner(session, persona)

    assert access_rows[0].is_owner is True
    assert access_rows[0].user_id == owner.id
    assert access_rows[0].email == owner.email
    assert access_rows[0].permission == "edit"
    assert access_rows[0].status == "active"
    assert [row.user_id for row in access_rows] == [owner.id, editor.id]


def test_persona_access_can_store_pending_email_invites(session):
    owner = _user(session, "owner-user", "owner@example.com")
    persona = _persona(session, owner_user_id=owner.id)

    access = create_persona_access(
        session,
        persona,
        {"email": "Pending@Example.com", "permission": "view"},
        created_by_user_id=owner.id,
    )

    assert access.user_id is None
    assert access.email == "pending@example.com"
    assert access.status == "pending"
    assert list_persona_access(session, persona)[0].id == access.id


def test_pending_persona_access_links_to_matching_user_and_waits_for_acceptance(session):
    owner = _user(session, "owner-user", "owner@example.com")
    persona = _persona(session, owner_user_id=owner.id)
    access = create_persona_access(
        session,
        persona,
        {"email": "Pending@Example.com", "permission": "edit"},
        created_by_user_id=owner.id,
    )
    invited = _user(session, "pending-user", "pending@example.com")

    assert link_pending_persona_access_for_user(session, invited) == 1
    assert access.user_id == invited.id
    assert access.status == "pending"
    assert list_personas(session, owner_user_id=invited.id, access_user_id=invited.id) == []
    assert [entry.id for entry in list_pending_persona_access_for_user(session, invited)] == [access.id]

    accepted = accept_persona_access_for_user(session, access.id, invited)

    assert accepted.status == "active"
    assert accepted.user_id == invited.id
    assert [item.id for item in list_personas(session, owner_user_id=invited.id, access_user_id=invited.id)] == [persona.id]


def test_pending_persona_access_can_be_declined_by_matching_user(session):
    owner = _user(session, "owner-user", "owner@example.com")
    persona = _persona(session, owner_user_id=owner.id)
    access = create_persona_access(
        session,
        persona,
        {"email": "Pending@Example.com", "permission": "view"},
        created_by_user_id=owner.id,
    )
    invited = _user(session, "pending-user", "pending@example.com")

    decline_persona_access_for_user(session, access.id, invited)

    assert list_persona_access(session, persona) == []
    assert list_pending_persona_access_for_user(session, invited) == []


def test_shared_persona_account_reads_can_hide_credentials(session):
    owner = _user(session, "owner-user", "owner@example.com")
    persona = _persona(session, owner_user_id=owner.id)
    account = create_account(
        session,
        persona,
        {
            "service": "mastodon",
            "label": "Mastodon",
            "handle_or_identifier": "@me@example.social",
            "is_enabled": True,
            "source_enabled": False,
            "destination_enabled": True,
            "credentials_json": {"token": "secret", "instance": "https://example.social"},
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )

    assert account_to_read(account).credentials_json["token"] == "secret"
    assert account_to_read(account, include_credentials=False).credentials_json == {}


@pytest.fixture()
def shared_persona_api(monkeypatch, tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'shared-persona-api.db'}", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    Base.metadata.create_all(engine)

    @contextmanager
    def test_db_session():
        with SessionLocal() as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    monkeypatch.setattr("app.main.db_session", test_db_session)
    monkeypatch.setattr("app.main.bootstrap", lambda: None)
    monkeypatch.setattr("app.main.CrossposterScheduler.start", lambda self: None)
    monkeypatch.setattr("app.main.CrossposterScheduler.stop", lambda self: None)
    monkeypatch.setattr(
        "app.main.build_principal_from_request",
        lambda request: Principal(
            user_id=request.headers.get("x-test-user", "editor"),
            display_name="Test user",
            role="admin" if request.headers.get("x-test-user") == "admin" else "user",
            timezone="UTC",
            is_authenticated=True,
        ),
    )
    with SessionLocal() as session:
        owner = _user(session, "owner", "owner@example.com")
        persona = _persona(session, owner_user_id=owner.id)
        invite = create_persona_access(session, persona, {"email": "editor@example.com", "permission": "edit"}, created_by_user_id=owner.id)
        editor = _user(session, "editor", "editor@example.com")
        viewer = _user(session, "viewer", "viewer@example.com")
        _user(session, "outsider", "outsider@example.com")
        _user(session, "admin", "admin@example.com")
        create_persona_access(session, persona, {"user_id": viewer.id, "permission": "view"}, created_by_user_id=owner.id)
        account = create_account(session, persona, {
            "service": "mastodon", "label": "Mastodon", "handle_or_identifier": "@original@example.social",
            "is_enabled": True, "source_enabled": False, "destination_enabled": True,
            "credentials_json": {"token": "test-secret-token", "instance": "https://example.social"},
            "source_settings_json": {}, "publish_settings_json": {},
        })
        session.commit()
        state = SimpleNamespace(persona_id=persona.id, account_id=account.id, invite_id=invite.id, SessionLocal=SessionLocal)
    try:
        with TestClient(app) as client:
            state.client = client
            yield state
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def _accept_edit_share(state):
    response = state.client.post(f"/persona-access/inbox/{state.invite_id}/accept")
    assert response.status_code == 200
    assert response.json()["status"] == "active"


def test_accepted_edit_share_can_save_connection_settings_and_persona(shared_persona_api):
    state = shared_persona_api
    _accept_edit_share(state)
    response = state.client.put(f"/personas/{state.persona_id}/accounts/{state.account_id}", json={
        "label": "Edited connection", "handle_or_identifier": "@edited@example.social",
        "publish_settings_json": {"visibility": "unlisted"},
    })
    assert response.status_code == 200
    assert response.json()["handle_or_identifier"] == "@edited@example.social"
    assert response.json()["credentials_json"] == {}
    persona_response = state.client.put(f"/personas/{state.persona_id}", json={"name": "Edited shared persona"})
    assert persona_response.status_code == 200
    with state.SessionLocal() as session:
        account = get_account(session, state.account_id)
        assert account.credentials_json["token"] == "test-secret-token"
        assert account.publish_settings_json == {"visibility": "unlisted"}
        assert account.label == "Edited connection"
        assert get_persona(session, state.persona_id).owner_user_id == "owner"


def test_edit_share_page_shows_basic_editing_without_credential_fields(shared_persona_api):
    state = shared_persona_api
    _accept_edit_share(state)
    response = state.client.get(f"/personas/{state.persona_id}/page")
    assert response.status_code == 200
    assert 'class="account-update-form tab-pane-stack"' in response.text
    assert 'data-can-manage-credentials="false"' in response.text
    assert 'name="handle_or_identifier"' in response.text
    assert 'data-scope="credentials"' not in response.text
    assert "test-secret-token" not in response.text
    assert 'class="btn btn-outline-danger delete-account-button"' not in response.text
    assert '<form id="account-create-form"' not in response.text
    payload_builder = response.text.split("function buildAccountPayload(form) {", 1)[1].split("function toggleCreateServiceFields()", 1)[0]
    assert 'source_settings_json: scoped.sourceSettings' in payload_builder
    assert 'publish_settings_json: scoped.publishSettings' in payload_builder
    assert 'credentials_json: scoped.credentials' not in payload_builder
    assert 'if (form.dataset.canManageCredentials === "true") {\n    payload.credentials_json = scoped.credentials;\n  }' in payload_builder


def test_view_share_can_read_connections_but_cannot_mutate_them(shared_persona_api):
    state = shared_persona_api
    headers = {"x-test-user": "viewer"}
    read = state.client.get(f"/personas/{state.persona_id}/accounts", headers=headers)
    assert read.status_code == 200
    assert read.json()[0]["credentials_json"] == {}
    response = state.client.put(f"/personas/{state.persona_id}/accounts/{state.account_id}", headers=headers, json={"handle_or_identifier": "forbidden"})
    assert response.status_code == 404
    page = state.client.get(f"/personas/{state.persona_id}/page", headers=headers)
    assert 'class="account-update-form tab-pane-stack"' not in page.text
    with state.SessionLocal() as session:
        assert get_account(session, state.account_id).handle_or_identifier == "@original@example.social"


@pytest.mark.parametrize("access_state", ["pending", "revoked"])
def test_pending_or_revoked_edit_share_cannot_save_connection(shared_persona_api, access_state):
    state = shared_persona_api
    if access_state == "revoked":
        _accept_edit_share(state)
        revoked = state.client.delete(f"/personas/{state.persona_id}/access/{state.invite_id}", headers={"x-test-user": "owner"})
        assert revoked.status_code == 200
    response = state.client.put(f"/personas/{state.persona_id}/accounts/{state.account_id}", json={"handle_or_identifier": "forbidden"})
    assert response.status_code == 404
    with state.SessionLocal() as session:
        assert get_account(session, state.account_id).handle_or_identifier == "@original@example.social"


@pytest.mark.parametrize("credentials", [{}, {"token": "replacement"}, None])
def test_shared_editor_cannot_replace_or_clear_owner_credentials(shared_persona_api, credentials):
    state = shared_persona_api
    _accept_edit_share(state)
    response = state.client.put(f"/personas/{state.persona_id}/accounts/{state.account_id}", json={"credentials_json": credentials, "label": "Must roll back"})
    assert response.status_code == 403
    with state.SessionLocal() as session:
        account = get_account(session, state.account_id)
        assert account.credentials_json["token"] == "test-secret-token"
        assert account.label == "Mastodon"


def test_shared_editor_form_save_omits_credentials_and_preserves_secret(shared_persona_api):
    state = shared_persona_api
    _accept_edit_share(state)
    response = state.client.put(f"/personas/{state.persona_id}/accounts/{state.account_id}", data={
        "label": "Mastodon", "handle_or_identifier": "@form@example.social", "is_enabled": "true",
        "source_enabled": "false", "destination_enabled": "true", "source_settings_json": "{}", "publish_settings_json": "{}",
    })
    assert response.status_code == 200
    assert response.json()["credentials_json"] == {}
    with state.SessionLocal() as session:
        account = get_account(session, state.account_id)
        assert account.handle_or_identifier == "@form@example.social"
        assert account.credentials_json["token"] == "test-secret-token"


def test_edit_share_is_scoped_to_persona_and_its_connections(shared_persona_api):
    state = shared_persona_api
    _accept_edit_share(state)
    with state.SessionLocal() as session:
        foreign = create_persona(session, {"name": "Foreign", "slug": "foreign", "owner_user_id": "outsider", "settings_json": {}})
        foreign_account = create_account(session, foreign, {
            "service": "discord", "label": "Foreign", "source_enabled": False, "destination_enabled": True,
            "credentials_json": {"webhook_url": "https://example.test/webhook"},
        })
        session.commit()
    forbidden_persona = state.client.put(f"/personas/{foreign.id}/accounts/{foreign_account.id}", json={"label": "Forbidden"})
    forbidden_account = state.client.put(f"/personas/{state.persona_id}/accounts/{foreign_account.id}", json={"label": "Forbidden"})
    assert forbidden_persona.status_code == 404
    assert forbidden_account.status_code == 404
    outsider = state.client.put(f"/personas/{state.persona_id}/accounts/{state.account_id}", headers={"x-test-user": "outsider"}, json={"label": "Forbidden"})
    assert outsider.status_code == 404
    with state.SessionLocal() as session:
        assert get_account(session, foreign_account.id).label == "Foreign"


@pytest.mark.parametrize("operation", ["create", "delete", "token", "validate", "share"])
def test_regular_edit_share_cannot_manage_credentials_or_sharing(shared_persona_api, operation):
    state = shared_persona_api
    _accept_edit_share(state)
    base = f"/personas/{state.persona_id}/accounts"
    if operation == "create":
        response = state.client.post(base, json={"service": "telegram", "label": "Telegram", "destination_enabled": True})
    elif operation == "delete":
        response = state.client.delete(f"{base}/{state.account_id}")
    elif operation == "token":
        response = state.client.post(f"{base}/{state.account_id}/instagram-token/record-refresh")
    elif operation == "validate":
        response = state.client.post(f"{base}/{state.account_id}/instagram-login/validate", json={})
    else:
        response = state.client.post(f"/personas/{state.persona_id}/access", json={"user_id": "outsider", "permission": "edit"})
    assert response.status_code == 403
    with state.SessionLocal() as session:
        assert get_account(session, state.account_id) is not None


@pytest.mark.parametrize("actor", ["owner", "admin"])
def test_owner_and_administrator_edit_share_retain_credential_management(shared_persona_api, actor):
    state = shared_persona_api
    if actor == "admin":
        with state.SessionLocal() as session:
            persona = get_persona(session, state.persona_id)
            create_persona_access(session, persona, {"user_id": "admin", "permission": "edit"}, created_by_user_id="owner")
            session.commit()
    headers = {"x-test-user": actor}
    updated = state.client.put(f"/personas/{state.persona_id}/accounts/{state.account_id}", headers=headers, json={
        "credentials_json": {"token": "updated-test-token", "instance": "https://example.social"},
    })
    assert updated.status_code == 200
    assert updated.json()["credentials_json"]["token"] == "updated-test-token"
    created = state.client.post(f"/personas/{state.persona_id}/accounts", headers=headers, json={
        "service": "telegram", "label": "Telegram", "destination_enabled": True,
        "credentials_json": {"bot_token": "test-token", "channel_id": "@test_channel"},
    })
    assert created.status_code == 200
    deleted = state.client.delete(f"/personas/{state.persona_id}/accounts/{created.json()['id']}", headers=headers)
    assert deleted.status_code == 200


def test_admin_role_without_edit_share_does_not_bypass_tenant_access(shared_persona_api):
    state = shared_persona_api
    headers = {"x-test-user": "admin"}
    unrelated = state.client.put(f"/personas/{state.persona_id}/accounts/{state.account_id}", headers=headers, json={"label": "Forbidden"})
    assert unrelated.status_code == 404
    with state.SessionLocal() as session:
        create_persona_access(session, get_persona(session, state.persona_id), {"user_id": "admin", "permission": "view"}, created_by_user_id="owner")
        session.commit()
    viewer = state.client.put(f"/personas/{state.persona_id}/accounts/{state.account_id}", headers=headers, json={"label": "Forbidden"})
    assert viewer.status_code == 404
