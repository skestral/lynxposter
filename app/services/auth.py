from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import HTTPException, Request
from fastapi.responses import RedirectResponse

from app.config import get_settings
from app.database import db_session
from app.models import User
from app.services.oidc import normalize_oidc_scope
from app.services.ui import DEFAULT_UI_MODE, DEFAULT_UI_THEME, normalize_ui_mode, normalize_ui_theme
from app.services.users import (
    claim_unowned_personas_for_user,
    create_or_update_user,
    ensure_local_admin_user,
    get_user,
    is_local_user,
)


@dataclass(slots=True)
class Principal:
    user_id: str | None
    display_name: str
    role: str
    timezone: str
    ui_theme: str = DEFAULT_UI_THEME
    ui_mode: str = DEFAULT_UI_MODE
    email: str | None = None
    username: str | None = None
    groups: list[str] = field(default_factory=list)
    is_authenticated: bool = False

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @property
    def is_user(self) -> bool:
        return self.role in {"user", "admin"}


def auth_enabled() -> bool:
    settings = get_settings()
    return bool(settings.auth_oidc_enabled and settings.auth_oidc_issuer_url and settings.auth_oidc_client_id)


def _legacy_principal() -> Principal:
    return Principal(
        user_id=None,
        display_name="Guest",
        role="guest",
        timezone="UTC",
        ui_theme=DEFAULT_UI_THEME,
        ui_mode=DEFAULT_UI_MODE,
        is_authenticated=False,
    )


def _login_redirect(request: Request) -> RedirectResponse:
    next_path = request.url.path
    if request.url.query:
        next_path = f"{next_path}?{request.url.query}"
    target = "/auth/login" if auth_enabled() else "/auth/select"
    return RedirectResponse(url=f"{target}?next={quote(next_path)}", status_code=303)


def require_html_access(request: Request, *, role: str = "any") -> Principal | RedirectResponse:
    principal = get_request_principal(request)
    if not principal.is_authenticated:
        return _login_redirect(request)
    _enforce_role(principal, role)
    return principal


def require_api_access(request: Request, *, role: str = "any") -> Principal:
    principal = get_request_principal(request)
    if not principal.is_authenticated:
        raise HTTPException(status_code=401, detail="Authentication required.")
    _enforce_role(principal, role)
    return principal


def _enforce_role(principal: Principal, role: str) -> None:
    if not auth_enabled() and principal.is_admin:
        return
    if role == "admin" and not principal.is_admin:
        raise HTTPException(status_code=403, detail="Admin access is required.")
    if role == "user" and not principal.is_user:
        raise HTTPException(status_code=403, detail="User access is required.")


def get_request_principal(request: Request) -> Principal:
    principal = getattr(request.state, "principal", None)
    session_user_id = (request.scope.get("session") or {}).get("user_id")
    if isinstance(principal, Principal):
        if principal.is_authenticated:
            return principal
        if not session_user_id:
            return principal
    principal = build_principal_from_request(request)
    request.state.principal = principal
    return principal


def build_principal_from_request(request: Request) -> Principal:
    session_state = request.scope.get("session")
    if not auth_enabled():
        session_user_id = (session_state or {}).get("user_id")
        if not session_user_id:
            return _legacy_principal()
        with db_session() as session:
            ensure_local_admin_user(session)
            user = get_user(session, session_user_id)
            if user is None or not user.is_enabled or not is_local_user(user):
                if session_state is not None:
                    session_state.clear()
                return _legacy_principal()
            return _user_to_principal(user)

    session_user_id = (session_state or {}).get("user_id")
    if not session_user_id:
        return _legacy_principal()

    with db_session() as session:
        user = get_user(session, session_user_id)
        if user is None or not user.is_enabled or is_local_user(user):
            if session_state is not None:
                session_state.clear()
            return _legacy_principal()
        return _user_to_principal(user)


def _user_to_principal(user: User) -> Principal:
    return Principal(
        user_id=user.id,
        display_name=user.effective_display_name,
        role=user.role,
        timezone=user.timezone or "UTC",
        ui_theme=normalize_ui_theme(user.ui_theme),
        ui_mode=normalize_ui_mode(user.ui_mode),
        email=user.email,
        username=user.username,
        groups=list(user.groups_json or []),
        is_authenticated=True,
    )


def _auth_client():
    from authlib.integrations.starlette_client import OAuth

    settings = get_settings()
    issuer = settings.auth_oidc_issuer_url.rstrip("/")
    oauth = OAuth()
    oauth.register(
        name="authelia",
        client_id=settings.auth_oidc_client_id,
        client_secret=settings.auth_oidc_client_secret,
        server_metadata_url=f"{issuer}/.well-known/openid-configuration",
        client_kwargs={"scope": normalize_oidc_scope(settings.auth_oidc_scope)},
    )
    return oauth.create_client("authelia")


def _callback_url(request: Request) -> str:
    settings = get_settings()
    if settings.app_base_url:
        return f"{settings.app_base_url.rstrip('/')}/auth/callback"
    return str(request.url_for("auth_callback"))


def _normalize_groups(raw_value: Any) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, str):
        return [item.strip() for item in raw_value.split(",") if item.strip()]
    if isinstance(raw_value, (list, tuple, set)):
        return [str(item).strip() for item in raw_value if str(item).strip()]
    return [str(raw_value).strip()]


def _sanitize_auth_debug_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(key): _sanitize_auth_debug_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_auth_debug_value(item) for item in value]
    return str(value)


def _decode_jwt_segment(segment: str) -> Any:
    padding = "=" * (-len(segment) % 4)
    decoded = base64.urlsafe_b64decode(f"{segment}{padding}".encode("ascii"))
    return json.loads(decoded.decode("utf-8"))


def _jwt_debug_snapshot(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str):
        return None

    token = value.strip()
    if not token:
        return None

    parts = token.split(".")
    if len(parts) != 3:
        return {
            "raw": token,
            "is_jwt": False,
        }

    snapshot: dict[str, Any] = {
        "raw": token,
        "is_jwt": True,
    }
    try:
        snapshot["decoded_header"] = _sanitize_auth_debug_value(_decode_jwt_segment(parts[0]))
    except Exception as exc:
        snapshot["decoded_header_error"] = str(exc)
    try:
        snapshot["decoded_payload"] = _sanitize_auth_debug_value(_decode_jwt_segment(parts[1]))
    except Exception as exc:
        snapshot["decoded_payload_error"] = str(exc)
    return snapshot


def _mapping_to_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    try:
        return {str(key): item for key, item in dict(value).items()}
    except Exception:
        return {}


def clear_auth_debug_snapshot(request: Request) -> None:
    app = request.scope.get("app")
    if app is None:
        return
    app.state.last_auth_debug = None


def store_auth_debug_snapshot(request: Request, *, claims: dict[str, Any], groups: list[str], token: dict[str, Any] | None = None) -> None:
    app = request.scope.get("app")
    if app is None:
        return

    settings = get_settings()
    raw_group_value = claims.get(settings.auth_oidc_groups_claim)
    if raw_group_value is None and settings.auth_oidc_groups_claim != "groups":
        raw_group_value = claims.get("groups")

    app.state.last_auth_debug = {
        "lynxposter_config": {
            "requested_scope": normalize_oidc_scope(settings.auth_oidc_scope),
            "groups_claim_name": settings.auth_oidc_groups_claim,
            "username_claim_name": settings.auth_oidc_username_claim,
            "admin_groups": sorted(_configured_group_set(settings.auth_oidc_admin_groups)),
            "user_groups": sorted(_configured_group_set(settings.auth_oidc_user_groups)),
        },
        "provider_claims": {
            "claim_keys": sorted(str(key) for key in claims.keys()),
            "raw_group_claim_value": _sanitize_auth_debug_value(raw_group_value),
            "resolved_groups": sorted(groups),
            "merged_claims": _sanitize_auth_debug_value(claims),
        },
    }
    if token:
        userinfo_fetch = token.get("_lynxposter_userinfo_fetch")
        if userinfo_fetch:
            app.state.last_auth_debug["provider_claims"]["userinfo_fetch"] = _sanitize_auth_debug_value(userinfo_fetch)
        provider_tokens: dict[str, Any] = {
            "token_keys": sorted(str(key) for key in token.keys()),
        }
        id_token_snapshot = _jwt_debug_snapshot(token.get("id_token"))
        if id_token_snapshot is not None:
            provider_tokens["id_token"] = id_token_snapshot
        access_token_snapshot = _jwt_debug_snapshot(token.get("access_token"))
        if access_token_snapshot is not None:
            provider_tokens["access_token"] = access_token_snapshot
        for key in ("token_type", "expires_at", "expires_in", "scope"):
            if key in token:
                provider_tokens[key] = _sanitize_auth_debug_value(token.get(key))
        app.state.last_auth_debug["provider_tokens"] = provider_tokens


def describe_auth_failure(error: str | None, description: str | None = None) -> str:
    normalized_error = (error or "").strip()
    normalized_description = (description or "").strip()
    if normalized_error == "invalid_scope" and "groups" in normalized_description.lower():
        return (
            "OIDC login failed because the configured scope includes 'groups', but your provider or client is not "
            "allowed to request that scope. Set OIDC Scope to 'openid profile email' unless your provider explicitly "
            "supports the 'groups' scope."
        )
    if normalized_description:
        return f"OIDC login failed: {normalized_description}"
    if normalized_error:
        return f"OIDC login failed with error '{normalized_error}'."
    return "OIDC login failed due to an unknown authentication error."


def describe_oidc_connectivity_failure(exc: Exception) -> str:
    settings = get_settings()
    issuer = settings.auth_oidc_issuer_url.rstrip("/")
    metadata_url = f"{issuer}/.well-known/openid-configuration" if issuer else "the OIDC discovery document"
    if isinstance(exc, httpx.TimeoutException):
        return (
            "OIDC login failed because LynxPoster could not reach the provider discovery endpoint before timing out: "
            f"{metadata_url}. Check that this URL is reachable from the machine or dev container running LynxPoster."
        )
    if isinstance(exc, httpx.HTTPError):
        return (
            "OIDC login failed because LynxPoster could not reach the provider discovery or token endpoint. "
            f"Check that {metadata_url} is reachable from the machine or dev container running LynxPoster. "
            f"Original error: {exc}"
        )
    return describe_auth_failure(None, str(exc))


def _configured_group_set(raw_value: str) -> set[str]:
    return {item.strip() for item in raw_value.split(",") if item.strip()}


def describe_group_mapping_failure(groups: list[str]) -> str:
    settings = get_settings()
    admin_groups = sorted(_configured_group_set(settings.auth_oidc_admin_groups))
    user_groups = sorted(_configured_group_set(settings.auth_oidc_user_groups))
    expected_groups = admin_groups + [group for group in user_groups if group not in admin_groups]

    if not groups:
        expected_text = ", ".join(expected_groups) if expected_groups else "none"
        return (
            "Authenticated user does not belong to an allowed group. No groups were received from the OIDC claims. "
            f"Configured admin/user groups: {expected_text}. If using Authelia, either allow the 'groups' scope for "
            "this client or configure Authelia to return the groups claim without that scope."
        )

    received_text = ", ".join(sorted(groups))
    expected_text = ", ".join(expected_groups) if expected_groups else "none"
    return (
        "Authenticated user does not belong to an allowed group. "
        f"Received groups: {received_text}. Configured admin/user groups: {expected_text}."
    )


def _resolve_role(groups: list[str]) -> str | None:
    settings = get_settings()
    normalized_groups = {group.lower() for group in groups}
    admin_groups = {group.lower() for group in _configured_group_set(settings.auth_oidc_admin_groups)}
    user_groups = {group.lower() for group in _configured_group_set(settings.auth_oidc_user_groups)}
    if admin_groups and normalized_groups & admin_groups:
        return "admin"
    if user_groups and normalized_groups & user_groups:
        return "user"
    if not admin_groups and not user_groups:
        return "user"
    return None


def _merged_claims(token: dict[str, Any]) -> dict[str, Any]:
    claims: dict[str, Any] = {}
    claims.update(_mapping_to_dict(token.get("userinfo")))
    for key in ("sub", "email", "name", "preferred_username", "username", "groups"):
        if key in token and key not in claims:
            claims[key] = token[key]
    return claims


async def _claims_with_userinfo(client, token: dict[str, Any]) -> dict[str, Any]:
    claims = _merged_claims(token)
    if "access_token" not in token:
        token["_lynxposter_userinfo_fetch"] = {"status": "skipped", "reason": "missing_access_token"}
        return claims

    try:
        userinfo = await client.userinfo(token=token)
    except Exception as exc:
        token["_lynxposter_userinfo_fetch"] = {"status": "error", "error": str(exc)}
        return claims

    fetched_userinfo = _mapping_to_dict(userinfo)
    if not fetched_userinfo:
        token["_lynxposter_userinfo_fetch"] = {"status": "empty"}
        return claims

    token["userinfo"] = fetched_userinfo
    token["_lynxposter_userinfo_fetch"] = {
        "status": "fetched",
        "claim_keys": sorted(fetched_userinfo.keys()),
    }
    claims.update(fetched_userinfo)
    return claims


async def begin_oidc_login(request: Request) -> RedirectResponse:
    client = _auth_client()
    clear_auth_debug_snapshot(request)
    request.session["post_login_redirect"] = request.query_params.get("next") or "/"
    try:
        return await client.authorize_redirect(request, _callback_url(request))
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=describe_oidc_connectivity_failure(exc)) from exc


async def complete_oidc_login(request: Request) -> Principal:
    client = _auth_client()
    try:
        token = await client.authorize_access_token(request)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=describe_oidc_connectivity_failure(exc)) from exc
    claims = await _claims_with_userinfo(client, token)
    settings = get_settings()
    groups = _normalize_groups(claims.get(settings.auth_oidc_groups_claim) or claims.get("groups"))
    store_auth_debug_snapshot(request, claims=claims, groups=groups, token=token)
    role = _resolve_role(groups)
    if role is None:
        raise HTTPException(status_code=403, detail=describe_group_mapping_failure(groups))

    username_claim = settings.auth_oidc_username_claim.strip() or "preferred_username"
    username = claims.get(username_claim) or claims.get("preferred_username") or claims.get("username")
    email = claims.get("email")
    display_name = claims.get("name") or username or email or claims.get("sub")
    oidc_sub = str(claims.get("sub") or "").strip()
    if not oidc_sub:
        raise HTTPException(status_code=400, detail="OIDC login did not include a subject claim.")

    with db_session() as session:
        user = create_or_update_user(
            session,
            oidc_sub=oidc_sub,
            email=str(email).strip() if email else None,
            username=str(username).strip() if username else None,
            display_name=str(display_name).strip(),
            role=role,
            timezone=None,
            groups=groups,
        )
        if not user.is_enabled:
            raise HTTPException(status_code=403, detail="This user has been disabled by an administrator.")
        user.last_login_at = datetime.now(timezone.utc)
        session.flush()
        claim_unowned_personas_for_user(session, user)
        principal = _user_to_principal(user)

    request.session["user_id"] = principal.user_id
    clear_auth_debug_snapshot(request)
    return principal


def logout(request: Request) -> None:
    request.session.clear()
