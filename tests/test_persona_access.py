from __future__ import annotations

from app.models import User
from app.services.personas import (
    account_to_read,
    create_account,
    create_persona,
    create_persona_access,
    get_persona,
    list_persona_access,
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
