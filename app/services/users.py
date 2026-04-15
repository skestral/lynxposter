from __future__ import annotations

from functools import lru_cache
from uuid import uuid4
from zoneinfo import ZoneInfo, available_timezones

from sqlalchemy import Select, select
from sqlalchemy.orm import Session, selectinload

from app.models import Persona, User
from app.schemas import UserRead
from app.services.ui import DEFAULT_UI_MODE, DEFAULT_UI_THEME, normalize_ui_mode, normalize_ui_theme

LOCAL_USER_PREFIX = "local:"
LOCAL_ADMIN_SUBJECT = f"{LOCAL_USER_PREFIX}lynx"


@lru_cache(maxsize=2)
def timezone_options(include_server: bool = False) -> list[str]:
    zones = sorted(zone for zone in available_timezones() if zone and ("/" in zone or zone == "UTC"))
    ordered = ["UTC", *[zone for zone in zones if zone != "UTC"]]
    if include_server:
        return ["server", *ordered]
    return ordered


def normalize_timezone(value: str | None, *, allow_server: bool = False) -> str:
    candidate = (value or "").strip()
    if allow_server and candidate == "server":
        return "server"
    if not candidate:
        return "UTC"
    try:
        ZoneInfo(candidate)
    except Exception:
        return "UTC"
    return candidate


def normalize_preferred_name(value: str | None) -> str | None:
    candidate = str(value or "").strip()
    return candidate or None


def list_users(session: Session) -> list[User]:
    stmt = select(User).options(selectinload(User.personas)).order_by(User.display_name, User.email)
    return list(session.scalars(stmt))


def get_user(session: Session, user_id: str) -> User | None:
    return session.get(User, user_id)


def get_user_by_sub(session: Session, oidc_sub: str) -> User | None:
    stmt: Select[tuple[User]] = select(User).where(User.oidc_sub == oidc_sub)
    return session.scalar(stmt)


def is_local_user(user: User) -> bool:
    return str(user.oidc_sub or "").startswith(LOCAL_USER_PREFIX)


def list_local_users(session: Session) -> list[User]:
    stmt = (
        select(User)
        .options(selectinload(User.personas))
        .where(User.oidc_sub.like(f"{LOCAL_USER_PREFIX}%"))
        .order_by(User.role.asc(), User.display_name, User.email)
    )
    return list(session.scalars(stmt))


def create_or_update_user(
    session: Session,
    *,
    oidc_sub: str,
    email: str | None,
    username: str | None,
    display_name: str,
    role: str,
    timezone: str | None,
    groups: list[str],
) -> User:
    user = get_user_by_sub(session, oidc_sub)
    normalized_timezone = normalize_timezone(timezone)
    if user is None:
        user = User(
            oidc_sub=oidc_sub,
            email=email,
            username=username,
            display_name=display_name,
            preferred_name=None,
            role=role,
            is_enabled=True,
            timezone=normalized_timezone,
            ui_theme=DEFAULT_UI_THEME,
            ui_mode=DEFAULT_UI_MODE,
            groups_json=groups,
        )
        session.add(user)
        session.flush()
        return user

    user.email = email
    user.username = username
    user.display_name = display_name
    user.role = role
    user.groups_json = groups
    if user.timezone in {"", "UTC"} and normalized_timezone != "UTC":
        user.timezone = normalized_timezone
    if not user.ui_theme:
        user.ui_theme = DEFAULT_UI_THEME
    if not user.ui_mode:
        user.ui_mode = DEFAULT_UI_MODE
    session.flush()
    return user


def update_user_settings(
    session: Session,
    user: User,
    *,
    timezone: str,
    ui_theme: str | None = None,
    ui_mode: str | None = None,
    preferred_name: str | None = None,
    apply_preferred_name: bool = False,
) -> User:
    user.timezone = normalize_timezone(timezone)
    if ui_theme is not None:
        user.ui_theme = normalize_ui_theme(ui_theme)
    if ui_mode is not None:
        user.ui_mode = normalize_ui_mode(ui_mode)
    if apply_preferred_name:
        user.preferred_name = normalize_preferred_name(preferred_name)
    session.flush()
    return user


def admin_update_user(session: Session, user: User, *, timezone: str, is_enabled: bool) -> User:
    user.timezone = normalize_timezone(timezone)
    user.is_enabled = bool(is_enabled)
    session.flush()
    return user


def ensure_local_admin_user(session: Session) -> User:
    user = get_user_by_sub(session, LOCAL_ADMIN_SUBJECT)
    if user is None:
        user = User(
            oidc_sub=LOCAL_ADMIN_SUBJECT,
            email=None,
            username="lynx",
            display_name="Lynx",
            preferred_name=None,
            role="admin",
            is_enabled=True,
            timezone="UTC",
            ui_theme=DEFAULT_UI_THEME,
            ui_mode=DEFAULT_UI_MODE,
            groups_json=["local-admin"],
        )
        session.add(user)
        session.flush()
        return user

    user.display_name = "Lynx"
    user.username = "lynx"
    user.role = "admin"
    user.is_enabled = True
    if not user.timezone:
        user.timezone = "UTC"
    if not user.ui_theme:
        user.ui_theme = DEFAULT_UI_THEME
    if not user.ui_mode:
        user.ui_mode = DEFAULT_UI_MODE
    session.flush()
    return user


def create_local_user(session: Session, *, display_name: str, timezone: str) -> User:
    cleaned_name = (display_name or "").strip()
    if not cleaned_name:
        raise ValueError("Display name is required.")
    user = User(
        oidc_sub=f"{LOCAL_USER_PREFIX}{uuid4()}",
        email=None,
        username=None,
        display_name=cleaned_name,
        preferred_name=None,
        role="user",
        is_enabled=True,
        timezone=normalize_timezone(timezone),
        ui_theme=DEFAULT_UI_THEME,
        ui_mode=DEFAULT_UI_MODE,
        groups_json=["local-user"],
    )
    session.add(user)
    session.flush()
    claim_unowned_personas_for_user(session, user)
    return user


def claim_unowned_personas_for_user(session: Session, user: User) -> int:
    if user.role != "user":
        return 0
    owned_persona = session.scalar(select(Persona.id).where(Persona.owner_user_id == user.id).limit(1))
    if owned_persona:
        return 0
    personas = list(session.scalars(select(Persona).where(Persona.owner_user_id.is_(None)).order_by(Persona.created_at)))
    for persona in personas:
        persona.owner_user_id = user.id
    session.flush()
    return len(personas)


def user_to_read(user: User) -> UserRead:
    return UserRead(
        id=user.id,
        oidc_sub=user.oidc_sub,
        email=user.email,
        username=user.username,
        display_name=user.display_name,
        preferred_name=user.preferred_name,
        effective_display_name=user.effective_display_name,
        role=user.role,
        is_enabled=user.is_enabled,
        timezone=user.timezone,
        ui_theme=user.ui_theme or DEFAULT_UI_THEME,
        ui_mode=user.ui_mode or DEFAULT_UI_MODE,
        groups_json=list(user.groups_json or []),
        last_login_at=user.last_login_at,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )
