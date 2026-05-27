from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import exists, or_, select
from sqlalchemy.orm import Session, selectinload

from app.adapters import account_is_configured, get_service_definition, supports_destination, supports_source
from app.models import Account, AccountPostRef, AccountRoute, AccountSyncState, AlertEvent, CanonicalPost, DeliveryJob, Persona, PersonaAccess, RunEvent, User
from app.services.instagram_private_api import apply_instagram_private_settings
from app.schemas import AccountRead
from app.services.instagram_tokens import apply_instagram_token_tracking, record_instagram_token_refresh

_LEGACY_PERSONA_PUBLISH_KEYS = ("mastodon_lang", "twitter_lang")
PERSONA_PERMISSION_ORDER = {"view": 1, "edit": 2}


@dataclass(frozen=True)
class PersonaAccessView:
    id: str
    persona_id: str
    user_id: str | None
    email: str | None
    display_name: str | None
    permission: str
    status: str
    created_by_user_id: str | None
    is_owner: bool
    created_at: datetime
    updated_at: datetime
    user: User | None = None
    created_by_user: User | None = None


def normalize_persona_permission(value: str | None) -> str:
    normalized = str(value or "view").strip().lower()
    if normalized not in PERSONA_PERMISSION_ORDER:
        raise ValueError("Persona access permission must be view or edit.")
    return normalized


def _allowed_persona_permissions(min_permission: str) -> list[str]:
    minimum = PERSONA_PERMISSION_ORDER[normalize_persona_permission(min_permission)]
    return [permission for permission, level in PERSONA_PERMISSION_ORDER.items() if level >= minimum]


def _active_persona_access_clause(access_user_id: str | None, min_permission: str = "view"):
    if access_user_id is None:
        return None
    return exists().where(
        PersonaAccess.persona_id == Persona.id,
        PersonaAccess.user_id == access_user_id,
        PersonaAccess.status == "active",
        PersonaAccess.permission.in_(_allowed_persona_permissions(min_permission)),
    )


def _matching_persona_access_clause(user: User):
    clauses = [PersonaAccess.user_id == user.id]
    normalized_email = _normalize_access_email(user.email)
    if normalized_email:
        clauses.append(PersonaAccess.email == normalized_email)
    return or_(*clauses)


def _persona_visibility_clause(
    *,
    owner_user_id: str | None = None,
    access_user_id: str | None = None,
    min_permission: str = "view",
):
    clauses = []
    if owner_user_id is not None:
        clauses.append(Persona.owner_user_id == owner_user_id)
    access_clause = _active_persona_access_clause(access_user_id, min_permission)
    if access_clause is not None:
        clauses.append(access_clause)
    if not clauses:
        return None
    return or_(*clauses)


def list_personas(
    session: Session,
    *,
    owner_user_id: str | None = None,
    access_user_id: str | None = None,
    min_permission: str = "view",
) -> list[Persona]:
    stmt = (
        select(Persona)
        .options(
            selectinload(Persona.accounts),
            selectinload(Persona.owner_user),
            selectinload(Persona.access_entries),
        )
        .order_by(Persona.name)
    )
    visibility_clause = _persona_visibility_clause(
        owner_user_id=owner_user_id,
        access_user_id=access_user_id,
        min_permission=min_permission,
    )
    if visibility_clause is not None:
        stmt = stmt.where(visibility_clause)
    return list(session.scalars(stmt))


def get_persona(
    session: Session,
    persona_id: str,
    *,
    owner_user_id: str | None = None,
    access_user_id: str | None = None,
    min_permission: str = "view",
) -> Persona | None:
    stmt = (
        select(Persona)
        .options(
            selectinload(Persona.accounts),
            selectinload(Persona.owner_user),
            selectinload(Persona.access_entries),
        )
        .where(Persona.id == persona_id)
        .execution_options(populate_existing=True)
    )
    visibility_clause = _persona_visibility_clause(
        owner_user_id=owner_user_id,
        access_user_id=access_user_id,
        min_permission=min_permission,
    )
    if visibility_clause is not None:
        stmt = stmt.where(visibility_clause)
    return session.scalar(stmt)


def user_can_manage_persona_access(persona: Persona, user_id: str | None, *, is_admin: bool = False) -> bool:
    if is_admin:
        return True
    if user_id is None:
        return persona.owner_user_id is None
    return persona.owner_user_id == user_id


def persona_user_permission(persona: Persona, user_id: str | None, *, is_admin: bool = False) -> str | None:
    if is_admin or user_can_manage_persona_access(persona, user_id):
        return "owner"
    if user_id is None:
        return None
    for access in persona.access_entries or []:
        if access.user_id == user_id and access.status == "active":
            return normalize_persona_permission(access.permission)
    return None


def _normalize_access_email(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    return normalized or None


def _find_user_for_access(session: Session, *, user_id: str | None = None, email: str | None = None) -> User | None:
    if user_id:
        user = session.get(User, user_id)
        if user:
            return user
    normalized_email = _normalize_access_email(email)
    if normalized_email:
        return session.scalar(select(User).where(User.email == normalized_email))
    return None


def list_persona_access(session: Session, persona: Persona) -> list[PersonaAccess]:
    stmt = (
        select(PersonaAccess)
        .where(PersonaAccess.persona_id == persona.id)
        .options(selectinload(PersonaAccess.user), selectinload(PersonaAccess.created_by_user))
        .order_by(PersonaAccess.status, PersonaAccess.email, PersonaAccess.user_id)
    )
    return list(session.scalars(stmt))


def _persona_owner_access_view(persona: Persona) -> PersonaAccessView:
    owner = persona.owner_user
    owner_display_name = owner.effective_display_name if owner else None
    return PersonaAccessView(
        id=f"owner:{persona.id}",
        persona_id=persona.id,
        user_id=persona.owner_user_id,
        email=owner.email if owner else None,
        display_name=owner_display_name or persona.owner_user_id or "Original owner",
        permission="edit",
        status="active",
        created_by_user_id=None,
        is_owner=True,
        created_at=persona.created_at,
        updated_at=persona.updated_at,
        user=owner,
        created_by_user=None,
    )


def list_persona_access_with_owner(session: Session, persona: Persona) -> list[PersonaAccess | PersonaAccessView]:
    return [_persona_owner_access_view(persona), *list_persona_access(session, persona)]


def list_pending_persona_access_for_user(session: Session, user: User) -> list[PersonaAccess]:
    stmt = (
        select(PersonaAccess)
        .where(PersonaAccess.status == "pending", _matching_persona_access_clause(user))
        .options(
            selectinload(PersonaAccess.persona).selectinload(Persona.owner_user),
            selectinload(PersonaAccess.created_by_user),
        )
        .order_by(PersonaAccess.created_at.desc())
    )
    return list(session.scalars(stmt))


def count_pending_persona_access_for_user(session: Session, user: User) -> int:
    return len(list_pending_persona_access_for_user(session, user))


def link_pending_persona_access_for_user(session: Session, user: User) -> int:
    normalized_email = _normalize_access_email(user.email)
    if not normalized_email:
        return 0
    pending_entries = list(
        session.scalars(
            select(PersonaAccess).where(
                PersonaAccess.status == "pending",
                PersonaAccess.email == normalized_email,
                or_(PersonaAccess.user_id.is_(None), PersonaAccess.user_id == user.id),
            )
        )
    )
    changed = 0
    for access in pending_entries:
        if access.user_id == user.id:
            continue
        existing = session.scalar(
            select(PersonaAccess).where(
                PersonaAccess.persona_id == access.persona_id,
                PersonaAccess.user_id == user.id,
                PersonaAccess.id != access.id,
            )
        )
        if existing is not None:
            if existing.status == "active":
                session.delete(access)
                changed += 1
            continue
        access.user_id = user.id
        changed += 1
    if changed:
        session.flush()
    return changed


def accept_persona_access_for_user(session: Session, access_id: str, user: User) -> PersonaAccess:
    access = session.get(PersonaAccess, access_id)
    normalized_email = _normalize_access_email(user.email)
    matches_user = bool(
        access
        and (
            access.user_id == user.id
            or (normalized_email and _normalize_access_email(access.email) == normalized_email)
        )
    )
    if access is None or not matches_user:
        raise ValueError("Persona share invitation not found.")
    if access.status == "active":
        return access

    existing = session.scalar(
        select(PersonaAccess).where(
            PersonaAccess.persona_id == access.persona_id,
            PersonaAccess.user_id == user.id,
            PersonaAccess.id != access.id,
        )
    )
    if existing is not None:
        existing.permission = normalize_persona_permission(existing.permission)
        existing.status = "active"
        if normalized_email and not existing.email:
            existing.email = normalized_email
        session.delete(access)
        session.flush()
        return existing

    access.user_id = user.id
    if normalized_email:
        access.email = normalized_email
    access.status = "active"
    session.flush()
    return access


def decline_persona_access_for_user(session: Session, access_id: str, user: User) -> None:
    access = session.get(PersonaAccess, access_id)
    normalized_email = _normalize_access_email(user.email)
    matches_user = bool(
        access
        and access.status == "pending"
        and (
            access.user_id == user.id
            or (normalized_email and _normalize_access_email(access.email) == normalized_email)
        )
    )
    if access is None or not matches_user:
        raise ValueError("Persona share invitation not found.")
    session.delete(access)
    session.flush()


def create_persona_access(
    session: Session,
    persona: Persona,
    payload: dict,
    *,
    created_by_user_id: str | None = None,
) -> PersonaAccess:
    permission = normalize_persona_permission(payload.get("permission"))
    requested_email = _normalize_access_email(payload.get("email"))
    requested_user_id = str(payload.get("user_id") or "").strip() or None
    user = _find_user_for_access(session, user_id=requested_user_id, email=requested_email)
    user_id = user.id if user else requested_user_id
    email = _normalize_access_email(user.email if user and user.email else requested_email)

    if not user_id and not email:
        raise ValueError("Choose a user or enter an email address to share with.")
    if user_id and user_id == persona.owner_user_id:
        raise ValueError("The owner already has full control of this persona.")

    existing = None
    if user_id:
        existing = session.scalar(
            select(PersonaAccess).where(PersonaAccess.persona_id == persona.id, PersonaAccess.user_id == user_id)
        )
    if existing is None and email:
        existing = session.scalar(
            select(PersonaAccess).where(PersonaAccess.persona_id == persona.id, PersonaAccess.email == email)
        )

    if existing is not None:
        existing.user_id = user_id or existing.user_id
        existing.email = email or existing.email
        existing.permission = permission
        existing.status = "active" if user_id else "pending"
        session.flush()
        return existing

    access = PersonaAccess(
        persona_id=persona.id,
        user_id=user_id,
        email=email,
        permission=permission,
        status="active" if user_id else "pending",
        created_by_user_id=created_by_user_id,
    )
    session.add(access)
    session.flush()
    return access


def update_persona_access(session: Session, persona: Persona, access_id: str, payload: dict) -> PersonaAccess:
    access = session.get(PersonaAccess, access_id)
    if access is None or access.persona_id != persona.id:
        raise ValueError("Persona sharing entry not found.")
    if payload.get("permission") is not None:
        access.permission = normalize_persona_permission(payload.get("permission"))
    if payload.get("status") is not None:
        status = str(payload.get("status") or "").strip().lower()
        if status not in {"pending", "active"}:
            raise ValueError("Persona sharing status must be pending or active.")
        if status == "active" and not access.user_id:
            user = _find_user_for_access(session, email=access.email)
            if user is None:
                raise ValueError("A pending email invite can only become active after it matches a local user.")
            access.user_id = user.id
        access.status = status
    session.flush()
    return access


def delete_persona_access(session: Session, persona: Persona, access_id: str) -> None:
    access = session.get(PersonaAccess, access_id)
    if access is None or access.persona_id != persona.id:
        raise ValueError("Persona sharing entry not found.")
    session.delete(access)
    session.flush()


def get_account(session: Session, account_id: str) -> Account | None:
    stmt = select(Account).options(selectinload(Account.persona)).where(Account.id == account_id)
    return session.scalar(stmt)


def list_routes(session: Session, persona: Persona) -> list[AccountRoute]:
    persona_account_ids = [account.id for account in persona.accounts]
    if not persona_account_ids:
        return []
    stmt = (
        select(AccountRoute)
        .where(
            AccountRoute.source_account_id.in_(persona_account_ids),
            AccountRoute.destination_account_id.in_(persona_account_ids),
        )
        .options(
            selectinload(AccountRoute.source_account),
            selectinload(AccountRoute.destination_account),
        )
    )
    return list(session.scalars(stmt))


def create_persona(session: Session, payload: dict) -> Persona:
    persona = Persona(**payload)
    session.add(persona)
    session.flush()
    return get_persona(session, persona.id) or persona


def update_persona(session: Session, persona: Persona, payload: dict) -> Persona:
    if "settings_json" in payload and payload["settings_json"] is not None:
        settings_json = dict(payload["settings_json"])
        existing_settings = dict(persona.settings_json or {})
        for key in _LEGACY_PERSONA_PUBLISH_KEYS:
            if key not in settings_json and key in existing_settings:
                settings_json[key] = existing_settings[key]
        payload = dict(payload)
        payload["settings_json"] = settings_json
    for field, value in payload.items():
        setattr(persona, field, value)
    session.flush()
    return get_persona(session, persona.id) or persona


def _validate_account_payload(payload: dict) -> None:
    service = payload["service"]
    source_enabled = bool(payload.get("source_enabled"))
    destination_enabled = bool(payload.get("destination_enabled"))
    if source_enabled and not supports_source(service):
        raise ValueError(f"{service} does not support inbound polling.")
    if destination_enabled and not supports_destination(service):
        raise ValueError(f"{service} does not support outbound publishing.")
    if not source_enabled and not destination_enabled:
        raise ValueError("An account must enable at least one direction.")


def _normalize_optional_settings(payload: dict) -> dict:
    normalized: dict = dict(payload)
    for field in ("source_settings_json", "publish_settings_json"):
        if field not in normalized or normalized[field] is None:
            continue
        cleaned = {}
        for key, value in dict(normalized[field]).items():
            if value is None:
                continue
            if isinstance(value, str):
                value = value.strip()
                if value == "":
                    continue
            cleaned[key] = value
        normalized[field] = cleaned
    return normalized


def create_account(session: Session, persona: Persona, payload: dict) -> Account:
    payload = _normalize_optional_settings(payload)
    _validate_account_payload(payload)
    if payload.get("service") == "instagram":
        credentials = apply_instagram_token_tracking(payload.get("credentials_json"))
        payload["credentials_json"] = apply_instagram_private_settings(credentials)
    account = Account(persona_id=persona.id, **payload)
    account.persona = persona
    session.add(account)
    session.flush()
    return get_account(session, account.id) or account


def update_account(session: Session, persona: Persona, account: Account, payload: dict) -> Account:
    payload = _normalize_optional_settings(payload)
    candidate = {
        "service": account.service,
        "source_enabled": account.source_enabled,
        "destination_enabled": account.destination_enabled,
    }
    candidate.update(payload)
    _validate_account_payload(candidate)
    if account.service == "instagram" and "credentials_json" in payload:
        credentials = apply_instagram_token_tracking(
            payload.get("credentials_json"),
            previous_credentials=account.credentials_json,
        )
        payload["credentials_json"] = apply_instagram_private_settings(
            credentials,
            previous_credentials=account.credentials_json,
        )
    for field, value in payload.items():
        setattr(account, field, value)
    session.flush()
    return get_account(session, account.id) or account


def record_account_token_refresh(session: Session, persona: Persona, account: Account) -> Account:
    if account.persona_id != persona.id:
        raise ValueError("Account not found in this persona.")
    if account.service != "instagram":
        raise ValueError("Only Instagram accounts support token refresh tracking.")
    if not str((account.credentials_json or {}).get("api_key") or "").strip():
        raise ValueError("Add an Instagram access token before recording a refresh.")
    record_instagram_token_refresh(account)
    session.flush()
    return get_account(session, account.id) or account


def delete_account(session: Session, persona: Persona, account: Account) -> None:
    from app.services.posts import refresh_post_status

    if account.persona_id != persona.id:
        raise ValueError("Account not found in this persona.")

    jobs = list(
        session.scalars(
            select(DeliveryJob)
            .options(selectinload(DeliveryJob.post).selectinload(CanonicalPost.delivery_jobs))
            .where(DeliveryJob.target_account_id == account.id)
        )
    )
    affected_posts = {job.post for job in jobs if job.post is not None}
    for job in jobs:
        session.delete(job)

    for route in session.scalars(
        select(AccountRoute).where(
            (AccountRoute.source_account_id == account.id) | (AccountRoute.destination_account_id == account.id)
        )
    ):
        session.delete(route)

    for post_ref in session.scalars(select(AccountPostRef).where(AccountPostRef.account_id == account.id)):
        session.delete(post_ref)

    for sync_state in session.scalars(select(AccountSyncState).where(AccountSyncState.source_account_id == account.id)):
        session.delete(sync_state)

    for post in session.scalars(select(CanonicalPost).where(CanonicalPost.origin_account_id == account.id)):
        post.origin_account_id = None

    for event in session.scalars(select(RunEvent).where(RunEvent.account_id == account.id)):
        event.account_id = None

    for alert in session.scalars(select(AlertEvent).where(AlertEvent.account_id == account.id)):
        alert.account_id = None

    session.delete(account)
    session.flush()

    for post in affected_posts:
        session.expire(post, ["delivery_jobs"])
        refresh_post_status(post)
        remaining_job = session.scalar(select(DeliveryJob.id).where(DeliveryJob.post_id == post.id).limit(1))
        if post.origin_kind == "composer" and remaining_job is None and post.status != "posted":
            post.status = "draft"
            post.last_error = "Select at least one destination account."
    session.flush()


def replace_routes(session: Session, persona: Persona, routes: list[dict]) -> list[AccountRoute]:
    persona_account_ids = {account.id for account in persona.accounts}
    existing_by_pair = {(route.source_account_id, route.destination_account_id): route for route in list_routes(session, persona)}
    desired_pairs = set()

    for payload in routes:
        source_id = payload["source_account_id"]
        destination_id = payload["destination_account_id"]
        if source_id == destination_id:
            raise ValueError("Routes cannot point an account to itself.")
        if source_id not in persona_account_ids or destination_id not in persona_account_ids:
            raise ValueError("Routes must stay within one persona.")

        source_account = next(account for account in persona.accounts if account.id == source_id)
        destination_account = next(account for account in persona.accounts if account.id == destination_id)
        if not source_account.source_enabled:
            raise ValueError(f"{source_account.label} is not enabled as a source account.")
        if not destination_account.destination_enabled:
            raise ValueError(f"{destination_account.label} is not enabled as a destination account.")

        pair = (source_id, destination_id)
        desired_pairs.add(pair)
        route = existing_by_pair.get(pair)
        if route:
            route.is_enabled = bool(payload.get("is_enabled", True))
        else:
            session.add(
                AccountRoute(
                    source_account_id=source_id,
                    destination_account_id=destination_id,
                    is_enabled=bool(payload.get("is_enabled", True)),
                )
            )

    for pair, route in existing_by_pair.items():
        if pair not in desired_pairs:
            session.delete(route)

    session.flush()
    return list_routes(session, persona)


def routed_destination_accounts(session: Session, source_account: Account) -> list[Account]:
    stmt = (
        select(Account)
        .join(AccountRoute, AccountRoute.destination_account_id == Account.id)
        .where(
            AccountRoute.source_account_id == source_account.id,
            AccountRoute.is_enabled.is_(True),
            Account.is_enabled.is_(True),
            Account.destination_enabled.is_(True),
        )
        .order_by(Account.label, Account.service)
    )
    return list(session.scalars(stmt))


def persona_destination_accounts(persona: Persona) -> list[Account]:
    return [account for account in sorted(persona.accounts, key=lambda item: (item.label, item.service)) if account.is_enabled and account.destination_enabled]


def account_to_read(account: Account, *, include_credentials: bool = True) -> AccountRead:
    definition = get_service_definition(account.service)
    return AccountRead(
        id=account.id,
        persona_id=account.persona_id,
        service=account.service,
        label=account.label,
        handle_or_identifier=account.handle_or_identifier,
        is_enabled=account.is_enabled,
        source_enabled=account.source_enabled,
        destination_enabled=account.destination_enabled,
        credentials_json=dict(account.credentials_json or {}) if include_credentials else {},
        source_settings_json=dict(account.source_settings_json or {}),
        publish_settings_json=dict(account.publish_settings_json or {}),
        last_health_status=account.last_health_status,
        last_error=account.last_error,
        source_supported=definition.source_supported,
        destination_supported=definition.destination_supported,
        configured=account_is_configured(account),
        created_at=account.created_at,
        updated_at=account.updated_at,
    )
