from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

from starlette.requests import Request

from app.main import _datetime_display, _local_timezone_to_utc, _owner_user_id_for_principal
from app.models import Persona
from app.services.auth import Principal, _enforce_role, build_principal_from_request
from app.services.personas import get_persona, list_personas
from app.services.users import (
    admin_update_user,
    claim_unowned_personas_for_user,
    create_local_user,
    create_or_update_user,
    ensure_local_admin_user,
    list_local_users,
    update_user_settings,
)


def _create_persona(session, *, name: str, slug: str, owner_user_id: str | None = None) -> Persona:
    persona = Persona(
        name=name,
        slug=slug,
        owner_user_id=owner_user_id,
        is_enabled=True,
        timezone="server",
        settings_json={},
        retry_settings_json={"max_retries": 5},
        throttle_settings_json={"max_per_hour": 0, "overflow_posts": "retry"},
    )
    session.add(persona)
    session.flush()
    return persona


@contextmanager
def _session_scope(session):
    yield session


def test_claim_unowned_personas_only_assigns_to_first_regular_user(session):
    _create_persona(session, name="One", slug="one")
    _create_persona(session, name="Two", slug="two")

    first_user = create_or_update_user(
        session,
        oidc_sub="sub-1",
        email="one@example.com",
        username="one",
        display_name="One",
        role="user",
        timezone="UTC",
        groups=["users"],
    )
    second_user = create_or_update_user(
        session,
        oidc_sub="sub-2",
        email="two@example.com",
        username="two",
        display_name="Two",
        role="user",
        timezone="UTC",
        groups=["users"],
    )

    claimed = claim_unowned_personas_for_user(session, first_user)
    claimed_again = claim_unowned_personas_for_user(session, second_user)

    assert claimed == 2
    assert claimed_again == 0
    assert len(list_personas(session, owner_user_id=first_user.id)) == 2
    assert len(list_personas(session, owner_user_id=second_user.id)) == 0


def test_claim_unowned_personas_can_assign_to_admin_user(session):
    _create_persona(session, name="Admin Owned", slug="admin-owned")

    admin_user = create_or_update_user(
        session,
        oidc_sub="admin-sub-1",
        email="admin@example.com",
        username="admin",
        display_name="Admin",
        role="admin",
        timezone="UTC",
        groups=["admins"],
    )

    claimed = claim_unowned_personas_for_user(session, admin_user)

    assert claimed == 1
    assert len(list_personas(session, owner_user_id=admin_user.id)) == 1


def test_persona_queries_respect_owner_user_id(session):
    first_user = create_or_update_user(
        session,
        oidc_sub="sub-a",
        email="a@example.com",
        username="alpha",
        display_name="Alpha",
        role="user",
        timezone="UTC",
        groups=["users"],
    )
    second_user = create_or_update_user(
        session,
        oidc_sub="sub-b",
        email="b@example.com",
        username="beta",
        display_name="Beta",
        role="user",
        timezone="UTC",
        groups=["users"],
    )
    first_persona = _create_persona(session, name="Alpha Persona", slug="alpha-persona", owner_user_id=first_user.id)
    _create_persona(session, name="Beta Persona", slug="beta-persona", owner_user_id=second_user.id)

    personas = list_personas(session, owner_user_id=first_user.id)
    persona = get_persona(session, first_persona.id, owner_user_id=second_user.id)

    assert [item.name for item in personas] == ["Alpha Persona"]
    assert persona is None


def test_local_admin_is_seeded_as_lynx(session):
    user = ensure_local_admin_user(session)

    assert user.display_name == "Lynx"
    assert user.role == "admin"
    assert user.is_enabled is True
    assert user.ui_theme == "skylight"
    assert user.ui_mode == "light"


def test_create_local_user_claims_unowned_personas(session):
    _create_persona(session, name="Unowned", slug="unowned")

    user = create_local_user(session, display_name="Local User", timezone="America/Chicago")

    assert user.display_name == "Local User"
    assert user.role == "user"
    assert user.timezone == "America/Chicago"
    assert user.ui_theme == "skylight"
    assert user.ui_mode == "light"
    assert len(list_personas(session, owner_user_id=user.id)) == 1


def test_local_user_listing_only_returns_local_accounts(session):
    ensure_local_admin_user(session)
    create_local_user(session, display_name="Local User", timezone="UTC")
    create_or_update_user(
        session,
        oidc_sub="oidc:sub-123",
        email="oidc@example.com",
        username="oidc-user",
        display_name="OIDC User",
        role="user",
        timezone="UTC",
        groups=["users"],
    )

    local_users = list_local_users(session)

    assert [user.display_name for user in local_users] == ["Lynx", "Local User"]


def test_update_user_settings_normalizes_timezone_theme_and_mode(session):
    user = create_or_update_user(
        session,
        oidc_sub="sub-timezone",
        email="tz@example.com",
        username="timezone",
        display_name="Timezone",
        role="user",
        timezone="UTC",
        groups=["users"],
    )

    update_user_settings(session, user, timezone="America/New_York", ui_theme="lagoon", ui_mode="dark")

    assert user.timezone == "America/New_York"
    assert user.ui_theme == "lagoon"
    assert user.ui_mode == "dark"


def test_update_user_settings_can_store_and_clear_preferred_name(session):
    user = create_or_update_user(
        session,
        oidc_sub="sub-preferred",
        email="preferred@example.com",
        username="preferred",
        display_name="Savannah",
        role="user",
        timezone="UTC",
        groups=["users"],
    )

    update_user_settings(session, user, timezone="UTC", preferred_name="Sav", apply_preferred_name=True)

    assert user.preferred_name == "Sav"
    assert user.effective_display_name == "Sav"

    update_user_settings(session, user, timezone="UTC", preferred_name="   ", apply_preferred_name=True)

    assert user.preferred_name is None
    assert user.effective_display_name == "Savannah"


def test_admin_update_user_can_disable_access(session):
    user = create_local_user(session, display_name="Disable Me", timezone="UTC")

    admin_update_user(session, user, timezone="Europe/Berlin", is_enabled=False)

    assert user.timezone == "Europe/Berlin"
    assert user.is_enabled is False


def test_legacy_role_guard_allows_single_user_admin(monkeypatch):
    principal = Principal(
        user_id=None,
        display_name="Local Admin",
        role="admin",
        timezone="UTC",
        is_authenticated=True,
    )
    monkeypatch.setattr("app.services.auth.auth_enabled", lambda: False)

    _enforce_role(principal, "user")
    _enforce_role(principal, "admin")


def test_admin_role_also_satisfies_user_guard_in_oidc_mode(monkeypatch):
    principal = Principal(
        user_id="admin-1",
        display_name="Admin",
        role="admin",
        timezone="UTC",
        is_authenticated=True,
    )
    monkeypatch.setattr("app.services.auth.auth_enabled", lambda: True)

    _enforce_role(principal, "user")
    _enforce_role(principal, "admin")


def test_admin_principal_is_scoped_to_owned_personas():
    principal = Principal(
        user_id="admin-1",
        display_name="Admin",
        role="admin",
        timezone="UTC",
        is_authenticated=True,
    )

    assert _owner_user_id_for_principal(principal) == "admin-1"


def test_local_mode_builds_principal_from_selected_local_user(session, monkeypatch):
    user = ensure_local_admin_user(session)
    user.preferred_name = "Savannah"
    request = Request({"type": "http", "headers": [], "session": {"user_id": user.id}})
    monkeypatch.setattr("app.services.auth.auth_enabled", lambda: False)
    monkeypatch.setattr("app.services.auth.db_session", lambda: _session_scope(session))

    principal = build_principal_from_request(request)

    assert principal.is_authenticated is True
    assert principal.display_name == "Savannah"
    assert principal.is_admin is True


def test_local_mode_clears_disabled_user_session(session, monkeypatch):
    user = create_local_user(session, display_name="Disabled Local", timezone="UTC")
    admin_update_user(session, user, timezone="UTC", is_enabled=False)
    request = Request({"type": "http", "headers": [], "session": {"user_id": user.id}})
    monkeypatch.setattr("app.services.auth.auth_enabled", lambda: False)
    monkeypatch.setattr("app.services.auth.db_session", lambda: _session_scope(session))

    principal = build_principal_from_request(request)

    assert principal.is_authenticated is False
    assert request.session == {}


def test_oidc_mode_clears_local_session(session, monkeypatch):
    user = create_local_user(session, display_name="Local Only", timezone="UTC")
    request = Request({"type": "http", "headers": [], "session": {"user_id": user.id}})
    monkeypatch.setattr("app.services.auth.auth_enabled", lambda: True)
    monkeypatch.setattr("app.services.auth.db_session", lambda: _session_scope(session))

    principal = build_principal_from_request(request)

    assert principal.is_authenticated is False
    assert request.session == {}


def test_timezone_helpers_render_and_parse_user_timezone():
    request = Request({"type": "http", "headers": []})
    request.state.principal = Principal(
        user_id="user-1",
        display_name="User",
        role="user",
        timezone="America/New_York",
        is_authenticated=True,
    )
    context = {"request": request}
    timestamp = datetime(2026, 4, 15, 13, 30, tzinfo=timezone.utc)

    rendered = _datetime_display(SimpleNamespace(get=context.get), timestamp)
    parsed = _local_timezone_to_utc("2026-04-15T09:30", "America/New_York")

    assert "Apr 15, 2026" in rendered
    assert rendered.endswith("EDT")
    assert parsed == timestamp
