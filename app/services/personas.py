from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.adapters import account_is_configured, get_service_definition, supports_destination, supports_source
from app.models import Account, AccountPostRef, AccountRoute, AccountSyncState, AlertEvent, CanonicalPost, DeliveryJob, Persona, RunEvent
from app.services.instagram_private_api import apply_instagram_private_settings
from app.schemas import AccountRead
from app.services.instagram_tokens import apply_instagram_token_tracking, record_instagram_token_refresh

_LEGACY_PERSONA_PUBLISH_KEYS = ("mastodon_lang", "twitter_lang")


def list_personas(session: Session, *, owner_user_id: str | None = None) -> list[Persona]:
    stmt = select(Persona).options(selectinload(Persona.accounts)).order_by(Persona.name)
    if owner_user_id is not None:
        stmt = stmt.where(Persona.owner_user_id == owner_user_id)
    return list(session.scalars(stmt))


def get_persona(session: Session, persona_id: str, *, owner_user_id: str | None = None) -> Persona | None:
    stmt = (
        select(Persona)
        .options(selectinload(Persona.accounts))
        .where(Persona.id == persona_id)
        .execution_options(populate_existing=True)
    )
    if owner_user_id is not None:
        stmt = stmt.where(Persona.owner_user_id == owner_user_id)
    return session.scalar(stmt)


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


def account_to_read(account: Account) -> AccountRead:
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
        credentials_json=dict(account.credentials_json or {}),
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
