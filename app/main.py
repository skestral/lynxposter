from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, RedirectResponse
from jinja2 import pass_context
from starlette.middleware.sessions import SessionMiddleware
from starlette.datastructures import UploadFile as StarletteUploadFile
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.adapters import iter_service_definitions, service_composer_constraints_context
from app.adapters.base import get_account_publish_setting
from app.adapters.instagram import validate_instagram_account_login
from app.config import get_settings
from app.database import db_session
from app.models import AccountRoute, CanonicalPost, MediaAttachment
from app.schemas import (
    AccountCreate,
    AccountRead,
    AccountRouteRead,
    AccountRouteReplaceRequest,
    AccountUpdate,
    AdminUserUpdate,
    AppSettingsRead,
    AppSettingsUpdate,
    AlertEventRead,
    PersonaCreate,
    PersonaRead,
    PersonaUpdate,
    RunEventRead,
    SandboxPreviewRead,
    SandboxPreviewRequest,
    DeliveryBreakdownRead,
    ScheduledPostCreate,
    ScheduledPostRead,
    ScheduledPostUpdate,
    UserRead,
    UserSettingsUpdate,
)
from app.services.auth import (
    Principal,
    auth_enabled,
    begin_oidc_login,
    build_principal_from_request,
    complete_oidc_login,
    describe_auth_failure,
    get_request_principal,
    logout,
    require_api_access,
    require_html_access,
)
from app.services.alerts import AlertDispatcher
from app.services.app_settings import read_app_settings, send_settings_test_webhook, update_app_settings
from app.services.delivery import new_run_id
from app.services.events import log_run_event
from app.services.bootstrap import bootstrap
from app.services.giveaways import (
    POST_TYPE_INSTAGRAM_GIVEAWAY,
    advance_giveaway_winner,
    confirm_giveaway_winner,
    instagram_webhook_callback_url,
    instagram_webhook_observability,
    ingest_instagram_webhook_payload,
    latest_instagram_webhook_event,
    serialize_giveaway,
    verify_instagram_webhook_signature,
)
from app.services.logs import list_alert_events, list_run_events, summarize_run_events
from app.services.logs import clear_alert_events
from app.services.logs import serialize_alert_event, serialize_run_event
from app.services.instagram_tokens import build_instagram_token_status
from app.services.live_updates import (
    LIVE_UPDATE_POLL_INTERVAL_MS,
    LIVE_UPDATE_TOPIC_ALERT_EVENTS,
    LIVE_UPDATE_TOPIC_DASHBOARD,
    LIVE_UPDATE_TOPIC_INSTAGRAM_WEBHOOKS,
    LIVE_UPDATE_TOPIC_LOGS,
    LIVE_UPDATE_TOPIC_RUN_EVENTS,
    LIVE_UPDATE_TOPIC_SCHEDULED_POSTS,
    live_update_snapshot,
    publish_live_update,
)
from app.services.oidc import oidc_group_mapping_enabled, oidc_scope_includes_groups
from app.services.personas import (
    account_to_read,
    create_account,
    create_persona,
    delete_account,
    get_account,
    get_persona,
    list_personas,
    list_routes,
    persona_destination_accounts,
    record_account_token_refresh,
    replace_routes,
    update_account,
    update_persona,
)
from app.services.posts import (
    build_delivery_states,
    create_scheduled_post,
    delete_scheduled_post,
    get_post,
    list_scheduled_posts,
    schedule_post_now,
    scheduled_post_delivery_breakdown,
    scheduled_post_display_status,
    update_scheduled_post,
)
from app.services.scheduler import CrossposterScheduler
from app.services.sandbox import build_sandbox_preview
from app.services.storage import store_upload
from app.services.ui import (
    ui_mode_label,
    ui_theme_catalog_for_client,
    ui_theme_definition,
    ui_theme_options,
    ui_theme_runtime_style,
)
from app.services.users import (
    admin_update_user,
    create_local_user,
    ensure_local_admin_user,
    get_user,
    is_local_user,
    list_local_users,
    list_users,
    normalize_timezone,
    timezone_options,
    update_user_settings,
    user_to_read,
)


settings = get_settings()
templates = Jinja2Templates(directory=str(settings.project_root / "app" / "templates"))


def _coerce_datetime(value: datetime | str | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _principal_timezone(principal: Principal | None) -> str:
    timezone_name = (principal.timezone if principal else "") or "UTC"
    if timezone_name == "server":
        return datetime.now().astimezone().tzinfo.key if hasattr(datetime.now().astimezone().tzinfo, "key") else "UTC"
    return normalize_timezone(timezone_name)


@pass_context
def _datetime_display(context, value: datetime | str | None, timezone_name: str | None = None) -> str:
    dt = _coerce_datetime(value)
    if dt is None:
        return "-"
    request = context.get("request")
    principal = getattr(getattr(request, "state", None), "principal", None)
    tz_name = normalize_timezone(timezone_name or _principal_timezone(principal))
    local_dt = dt.astimezone(ZoneInfo(tz_name))
    return local_dt.strftime("%b %d, %Y %I:%M %p %Z")


@pass_context
def _datetime_local_input(context, value: datetime | str | None, timezone_name: str | None = None) -> str:
    dt = _coerce_datetime(value)
    if dt is None:
        return ""
    request = context.get("request")
    principal = getattr(getattr(request, "state", None), "principal", None)
    tz_name = normalize_timezone(timezone_name or _principal_timezone(principal))
    local_dt = dt.astimezone(ZoneInfo(tz_name))
    return local_dt.strftime("%Y-%m-%dT%H:%M")


templates.env.filters["datetime_display"] = _datetime_display
templates.env.filters["datetime_local_input"] = _datetime_local_input
templates.env.globals["timezone_options"] = timezone_options
templates.env.globals["ui_theme_options"] = ui_theme_options
templates.env.globals["ui_theme_definition"] = ui_theme_definition
templates.env.globals["ui_theme_catalog_for_client"] = ui_theme_catalog_for_client
templates.env.globals["ui_theme_runtime_style"] = ui_theme_runtime_style
templates.env.globals["ui_mode_label"] = ui_mode_label
templates.env.globals["live_update_poll_interval_ms"] = LIVE_UPDATE_POLL_INTERVAL_MS
templates.env.globals["auth_enabled"] = auth_enabled

_DASHBOARD_DISMISSED_ALERT_IDS_KEY = "dashboard_dismissed_alert_ids"
_MAX_DASHBOARD_DISMISSED_ALERT_IDS = 200


def _template_context(request: Request, **context: Any) -> dict[str, Any]:
    return {
        "current_principal": get_request_principal(request),
        "auth_enabled": auth_enabled(),
        "current_path": request.url.path,
        **context,
    }


def _page_guard(request: Request, *, role: str = "any") -> Principal | RedirectResponse:
    result = require_html_access(request, role=role)
    return result


def _owner_user_id_for_principal(principal: Principal) -> str | None:
    if principal.is_admin:
        return None
    return principal.user_id if principal.is_user else None


def _load_user_settings_subject(session, principal: Principal):
    if not principal.user_id:
        raise HTTPException(status_code=404, detail="User settings are only available when OIDC authentication is enabled.")
    user = get_user(session, principal.user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    return user


def _load_admin_managed_user(session, user_id: str):
    user = get_user(session, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    return user


def _scheduler_service(request: Request) -> CrossposterScheduler:
    scheduler = getattr(request.app.state, "scheduler", None)
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler is not available.")
    return scheduler


def _alert_dispatcher(request: Request) -> AlertDispatcher:
    dispatcher = getattr(request.app.state, "alerts", None)
    if isinstance(dispatcher, AlertDispatcher):
        return dispatcher
    return AlertDispatcher()


def _dashboard_dismissed_alert_ids(request: Request) -> set[str]:
    raw_ids = request.session.get(_DASHBOARD_DISMISSED_ALERT_IDS_KEY, [])
    if not isinstance(raw_ids, list):
        return set()
    return {str(item).strip() for item in raw_ids if str(item).strip()}


def _store_dashboard_dismissed_alert_ids(request: Request, alert_ids: list[str]) -> None:
    unique_ids = list(dict.fromkeys(alert_ids))
    request.session[_DASHBOARD_DISMISSED_ALERT_IDS_KEY] = unique_ids[-_MAX_DASHBOARD_DISMISSED_ALERT_IDS:]


def _visible_dashboard_alerts(request: Request, alerts: list[Any], *, limit: int = 10) -> list[Any]:
    dismissed_ids = _dashboard_dismissed_alert_ids(request)
    return [alert for alert in alerts if getattr(alert, "id", None) not in dismissed_ids][:limit]


def _dismiss_dashboard_alerts(request: Request, alerts: list[Any]) -> int:
    existing_ids = list(request.session.get(_DASHBOARD_DISMISSED_ALERT_IDS_KEY, []))
    dismissed_ids = [alert_id for alert_id in existing_ids if str(alert_id).strip()]
    new_ids = [str(alert.id) for alert in alerts if getattr(alert, "id", None)]
    _store_dashboard_dismissed_alert_ids(request, dismissed_ids + new_ids)
    return len(new_ids)


def _coerce_int_query_param(value: str | None, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _local_timezone_to_utc(value: str | None, timezone_name: str) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(normalize_timezone(timezone_name)))
    return parsed.astimezone(timezone.utc)


def _parse_json_value(raw: str | None, default: Any) -> Any:
    if raw is None or not raw.strip():
        return default
    return json.loads(raw)


def _serialize_account(account) -> AccountRead:
    return account_to_read(account)


def _serialize_route(route: AccountRoute) -> AccountRouteRead:
    return AccountRouteRead.model_validate(route)


def _serialize_post(post: CanonicalPost) -> ScheduledPostRead:
    target_account_ids = [
        job.target_account_id
        for job in sorted(post.delivery_jobs, key=lambda item: item.target_account_id)
        if job.status != "cancelled"
    ]
    delivery_breakdown = scheduled_post_delivery_breakdown(post)
    return ScheduledPostRead(
        id=post.id,
        persona_id=post.persona_id,
        body=post.body,
        post_type=post.post_type,
        status=post.status,
        display_status=scheduled_post_display_status(post),
        target_account_ids=target_account_ids,
        publish_overrides_json=post.publish_overrides_json,
        metadata_json=post.metadata_json,
        scheduled_for=post.scheduled_for,
        giveaway=serialize_giveaway(post.instagram_giveaway),
        origin_kind=post.origin_kind,
        origin_account_id=post.origin_account_id,
        published_at=post.published_at,
        last_error=post.last_error,
        created_at=post.created_at,
        updated_at=post.updated_at,
        deliveries=build_delivery_states(post),
        delivery_breakdown=DeliveryBreakdownRead(**delivery_breakdown),
        attachments=list(post.attachments),
    )


def _persona_targets_context(personas) -> dict[str, list[dict[str, Any]]]:
    return {
        persona.id: [_serialize_account(account).model_dump(mode="json") for account in persona_destination_accounts(persona)]
        for persona in personas
    }


def _sandbox_seed_from_post(post: CanonicalPost) -> dict[str, Any]:
    serialized_post = _serialize_post(post)
    return {
        "post_id": post.id,
        "persona_id": post.persona_id,
        "body": post.body,
        "target_account_ids": serialized_post.target_account_ids,
        "publish_overrides_json": dict(post.publish_overrides_json or {}),
        "metadata_json": dict(post.metadata_json or {}),
        "attachment_inputs": [
            {
                "filename": Path(attachment.storage_path).name,
                "mime_type": attachment.mime_type,
                "alt_text": attachment.alt_text,
                "size_bytes": attachment.size_bytes,
                "sort_order": attachment.sort_order,
                "storage_path": attachment.storage_path,
            }
            for attachment in sorted(post.attachments, key=lambda item: item.sort_order)
        ],
    }


def _account_template_context(account) -> dict[str, Any]:
    definition = next(defn for defn in iter_service_definitions() if defn.service == account.service)
    return {
        "account": account,
        "account_read": _serialize_account(account),
        "definition": definition,
        "publish_field_values": {
            field.name: get_account_publish_setting(
                account.persona,
                account,
                field.name,
                "",
                fallback_keys=field.fallback_keys,
            )
            for field in definition.publish_setting_fields
        },
        "instagram_token_status": build_instagram_token_status(account),
    }


async def _read_persona_payload(request: Request) -> dict[str, Any]:
    if request.headers.get("content-type", "").startswith("application/json"):
        return await request.json()
    form = await request.form()
    return {
        "name": form.get("name"),
        "slug": form.get("slug"),
        "is_enabled": str(form.get("is_enabled", "true")).lower() in {"1", "true", "on", "yes"},
        "timezone": form.get("timezone", "server"),
        "settings_json": _parse_json_value(form.get("settings_json"), {}),
        "retry_settings_json": _parse_json_value(form.get("retry_settings_json"), {}),
        "throttle_settings_json": _parse_json_value(form.get("throttle_settings_json"), {}),
    }


async def _read_account_payload(request: Request) -> dict[str, Any]:
    if request.headers.get("content-type", "").startswith("application/json"):
        return await request.json()
    form = await request.form()
    return {
        "service": form.get("service"),
        "label": form.get("label"),
        "handle_or_identifier": form.get("handle_or_identifier", ""),
        "is_enabled": str(form.get("is_enabled", "true")).lower() in {"1", "true", "on", "yes"},
        "source_enabled": str(form.get("source_enabled", "false")).lower() in {"1", "true", "on", "yes"},
        "destination_enabled": str(form.get("destination_enabled", "false")).lower() in {"1", "true", "on", "yes"},
        "credentials_json": _parse_json_value(form.get("credentials_json"), {}),
        "source_settings_json": _parse_json_value(form.get("source_settings_json"), {}),
        "publish_settings_json": _parse_json_value(form.get("publish_settings_json"), {}),
    }


async def _read_routes_payload(request: Request) -> dict[str, Any]:
    if request.headers.get("content-type", "").startswith("application/json"):
        return await request.json()
    form = await request.form()
    return {"routes": _parse_json_value(form.get("routes_json"), [])}


async def _read_app_settings_payload(request: Request) -> dict[str, Any]:
    if request.headers.get("content-type", "").startswith("application/json"):
        return await request.json()
    form = await request.form()
    return {
        "instance_name": form.get("instance_name", ""),
        "app_base_url": form.get("app_base_url", ""),
        "app_port": int(form.get("app_port", 8000)),
        "scheduler_automation_interval_seconds": int(form.get("scheduler_automation_interval_seconds", 300)),
        "webhook_logging_enabled": str(form.get("webhook_logging_enabled", "false")).lower() in {"1", "true", "on", "yes"},
        "webhook_logging_endpoint": form.get("webhook_logging_endpoint", ""),
        "webhook_logging_bearer_token": form.get("webhook_logging_bearer_token", ""),
        "webhook_logging_timeout_seconds": int(form.get("webhook_logging_timeout_seconds", 10)),
        "webhook_logging_retry_count": int(form.get("webhook_logging_retry_count", 2)),
        "webhook_logging_min_severity": form.get("webhook_logging_min_severity", "warning"),
        "discord_notification_enabled": str(form.get("discord_notification_enabled", "false")).lower() in {"1", "true", "on", "yes"},
        "discord_notification_webhook_url": form.get("discord_notification_webhook_url", ""),
        "discord_notification_username": form.get("discord_notification_username", "LynxPoster"),
        "discord_notification_min_severity": form.get("discord_notification_min_severity", "warning"),
        "auth_oidc_enabled": str(form.get("auth_oidc_enabled", "false")).lower() in {"1", "true", "on", "yes"},
        "auth_oidc_issuer_url": form.get("auth_oidc_issuer_url", ""),
        "auth_oidc_client_id": form.get("auth_oidc_client_id", ""),
        "auth_oidc_client_secret": form.get("auth_oidc_client_secret", ""),
        "auth_oidc_scope": form.get("auth_oidc_scope", "openid profile email"),
        "auth_oidc_groups_claim": form.get("auth_oidc_groups_claim", "groups"),
        "auth_oidc_username_claim": form.get("auth_oidc_username_claim", "preferred_username"),
        "auth_oidc_admin_groups": form.get("auth_oidc_admin_groups", ""),
        "auth_oidc_user_groups": form.get("auth_oidc_user_groups", ""),
        "auth_session_secret": form.get("auth_session_secret", ""),
        "instagram_webhooks_enabled": str(form.get("instagram_webhooks_enabled", "false")).lower() in {"1", "true", "on", "yes"},
        "instagram_webhook_verify_token": form.get("instagram_webhook_verify_token", ""),
        "instagram_app_secret": form.get("instagram_app_secret", ""),
    }


async def _read_admin_user_payload(request: Request) -> dict[str, Any]:
    if request.headers.get("content-type", "").startswith("application/json"):
        return await request.json()
    form = await request.form()
    return {
        "is_enabled": str(form.get("is_enabled", "false")).lower() in {"1", "true", "on", "yes"},
        "timezone": form.get("timezone", "UTC"),
    }


async def _read_local_user_payload(request: Request) -> dict[str, Any]:
    form = await request.form()
    return {
        "display_name": form.get("display_name", ""),
        "timezone": form.get("timezone", "UTC"),
        "next": form.get("next", "/"),
    }


async def _read_scheduled_post_payload(request: Request) -> tuple[dict[str, Any], list[UploadFile], list[str]]:
    principal = get_request_principal(request)
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/json"):
        payload = await request.json()
        scheduled_for = payload.get("scheduled_for")
        if isinstance(scheduled_for, str):
            payload["scheduled_for"] = _local_timezone_to_utc(scheduled_for, _principal_timezone(principal))
        giveaway = payload.get("giveaway")
        if isinstance(giveaway, dict) and isinstance(giveaway.get("giveaway_end_at"), str):
            giveaway["giveaway_end_at"] = _local_timezone_to_utc(giveaway.get("giveaway_end_at"), _principal_timezone(principal))
        return payload, [], []

    form = await request.form()
    uploads: list[UploadFile] = []
    for _, value in form.multi_items():
        if isinstance(value, (UploadFile, StarletteUploadFile)) and value.filename:
            uploads.append(value)
    alt_texts = _parse_json_value(form.get("alt_texts"), [])
    payload = {
        "persona_id": form.get("persona_id"),
        "body": form.get("body", ""),
        "post_type": form.get("post_type", "standard"),
        "status": form.get("status", "draft"),
        "target_account_ids": _parse_json_value(form.get("target_account_ids"), []),
        "publish_overrides_json": _parse_json_value(form.get("publish_overrides_json"), {}),
        "metadata_json": _parse_json_value(form.get("metadata_json"), {}),
        "scheduled_for": _local_timezone_to_utc(form.get("scheduled_for"), _principal_timezone(principal)),
        "giveaway": _parse_json_value(form.get("giveaway_json"), None),
    }
    if isinstance(payload.get("giveaway"), dict):
        payload["giveaway"]["giveaway_end_at"] = _local_timezone_to_utc(
            payload["giveaway"].get("giveaway_end_at"),
            _principal_timezone(principal),
        )
    return payload, uploads, alt_texts


def _log_filters_from_request(request: Request) -> dict[str, str | None]:
    principal = get_request_principal(request)
    since = request.query_params.get("since")
    if since:
        since_dt = _local_timezone_to_utc(since, _principal_timezone(principal))
        since = since_dt.isoformat() if since_dt else since
    return {
        "persona_id": request.query_params.get("persona_id"),
        "account_id": request.query_params.get("account_id"),
        "service": request.query_params.get("service"),
        "severity": request.query_params.get("severity"),
        "operation": request.query_params.get("operation"),
        "since": since,
    }


def _settings_page_redirect(*, saved: bool = False, tested: bool = False, error_message: str | None = None) -> RedirectResponse:
    params: dict[str, str] = {}
    if saved:
        params["saved"] = "1"
    if tested:
        params["tested"] = "1"
    if error_message:
        params["error"] = error_message
    query = urlencode(params)
    url = "/settings/page"
    if query:
        url = f"{url}?{query}"
    return RedirectResponse(url=url, status_code=303)


def _oidc_settings_warning(app_settings: AppSettingsRead) -> str | None:
    if not app_settings.auth_oidc_enabled:
        return None

    groups_in_scope = oidc_scope_includes_groups(app_settings.auth_oidc_scope)
    groups_mapped = oidc_group_mapping_enabled(app_settings.auth_oidc_admin_groups, app_settings.auth_oidc_user_groups)

    if groups_mapped and not groups_in_scope:
        return (
            "Group-based role mapping is configured, but OIDC Scope does not include 'groups'. If your provider "
            "does not return a groups claim without that scope, sign-in may fail after authentication."
        )
    return None


def _admin_users_page_redirect(*, saved_user_id: str | None = None, error_message: str | None = None) -> RedirectResponse:
    params: dict[str, str] = {}
    if saved_user_id:
        params["saved_user_id"] = saved_user_id
    if error_message:
        params["error"] = error_message
    query = urlencode(params)
    url = "/admin/users/page"
    if query:
        url = f"{url}?{query}"
    return RedirectResponse(url=url, status_code=303)


def _auth_select_redirect(*, next_path: str = "/", error_message: str | None = None) -> RedirectResponse:
    params: dict[str, str] = {}
    if next_path:
        params["next"] = next_path
    if error_message:
        params["error"] = error_message
    query = urlencode(params)
    url = "/auth/select"
    if query:
        url = f"{url}?{query}"
    return RedirectResponse(url=url, status_code=303)


def _auth_error_redirect(message: str) -> RedirectResponse:
    return RedirectResponse(url=f"/auth/error?{urlencode({'message': message})}", status_code=303)


@asynccontextmanager
async def lifespan(app: FastAPI):
    alerts = AlertDispatcher()
    scheduler = CrossposterScheduler(alerts)
    try:
        bootstrap()
        scheduler.start()
        app.state.alerts = alerts
        app.state.scheduler = scheduler
        yield
    finally:
        scheduler.stop()


app = FastAPI(title="LynxPoster", lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    secret_key=get_settings().auth_session_secret or "lynxposter-dev-session-secret",
    same_site="lax",
    https_only=bool(get_settings().app_base_url.startswith("https://")),
)
app.mount("/static", StaticFiles(directory=str(settings.static_dir)), name="static")


@app.middleware("http")
async def attach_principal(request: Request, call_next):
    request.state.principal = build_principal_from_request(request)
    return await call_next(request)


def _public_attachment_path(attachment: MediaAttachment) -> Path | None:
    file_path = Path(attachment.storage_path).resolve()
    allowed_roots = [settings.uploads_dir.resolve(), settings.imported_media_dir.resolve()]
    if not any(file_path.is_relative_to(root) for root in allowed_roots):
        return None
    if not file_path.is_file():
        return None
    return file_path


@app.get("/media/attachments/{attachment_id}")
def public_attachment(attachment_id: str):
    with db_session() as session:
        attachment = session.get(MediaAttachment, attachment_id)
        if attachment is None:
            raise HTTPException(status_code=404, detail="Attachment not found.")
        file_path = _public_attachment_path(attachment)
        if file_path is None:
            raise HTTPException(status_code=404, detail="Attachment file not found.")
        return FileResponse(path=file_path, media_type=attachment.mime_type or None)


@app.get("/auth/login")
async def auth_login(request: Request) -> RedirectResponse:
    if not auth_enabled():
        next_target = request.query_params.get("next")
        url = "/auth/select"
        if next_target:
            url = f"{url}?{urlencode({'next': next_target})}"
        return RedirectResponse(url=url, status_code=303)
    principal = get_request_principal(request)
    if principal.is_authenticated:
        return RedirectResponse(url=request.query_params.get("next") or "/", status_code=303)
    try:
        return await begin_oidc_login(request)
    except HTTPException as exc:
        return _auth_error_redirect(describe_auth_failure(str(exc.status_code), exc.detail))
    except Exception as exc:
        return _auth_error_redirect(describe_auth_failure(None, str(exc)))


@app.get("/auth/select", response_class=HTMLResponse)
def auth_select_page(request: Request) -> HTMLResponse:
    if auth_enabled():
        principal = get_request_principal(request)
        if principal.is_authenticated:
            return RedirectResponse(url=request.query_params.get("next") or "/", status_code=303)
        return RedirectResponse(url=f"/auth/login?{urlencode({'next': request.query_params.get('next') or '/'})}", status_code=303)
    principal = get_request_principal(request)
    if principal.is_authenticated:
        return RedirectResponse(url=request.query_params.get("next") or "/", status_code=303)
    with db_session() as session:
        ensure_local_admin_user(session)
        users = list_local_users(session)
        return templates.TemplateResponse(
            name="auth_select.html",
            request=request,
            context=_template_context(
                request,
                local_users=users,
                next_path=request.query_params.get("next") or "/",
                error_message=request.query_params.get("error"),
            ),
        )


@app.get("/auth/error", response_class=HTMLResponse)
def auth_error_page(request: Request) -> HTMLResponse:
    auth_debug = getattr(request.app.state, "last_auth_debug", None)
    return templates.TemplateResponse(
        name="auth_error.html",
        request=request,
        context=_template_context(
            request,
            error_message=request.query_params.get("message") or "OIDC login failed.",
            auth_is_enabled=auth_enabled(),
            auth_debug_json=json.dumps(auth_debug, indent=2, sort_keys=True) if auth_debug else None,
        ),
    )


@app.post("/auth/select")
async def auth_select_existing_user(request: Request) -> RedirectResponse:
    if auth_enabled():
        return RedirectResponse(url="/auth/login", status_code=303)
    form = await request.form()
    user_id = str(form.get("user_id", "")).strip()
    next_path = str(form.get("next", "/")).strip() or "/"
    try:
        with db_session() as session:
            ensure_local_admin_user(session)
            user = get_user(session, user_id)
            if user is None or not user.is_enabled or not is_local_user(user):
                return _auth_select_redirect(next_path=next_path, error_message="That local user is not available.")
            request.session["user_id"] = user.id
        return RedirectResponse(url=next_path, status_code=303)
    except Exception as exc:
        return _auth_select_redirect(next_path=next_path, error_message=str(exc))


@app.post("/auth/select/create")
async def auth_create_local_user(request: Request) -> RedirectResponse:
    if auth_enabled():
        return RedirectResponse(url="/auth/login", status_code=303)
    payload = await _read_local_user_payload(request)
    next_path = str(payload.get("next", "/")).strip() or "/"
    try:
        with db_session() as session:
            ensure_local_admin_user(session)
            user = create_local_user(session, display_name=payload["display_name"], timezone=payload["timezone"])
            request.session["user_id"] = user.id
        return RedirectResponse(url=next_path, status_code=303)
    except Exception as exc:
        return _auth_select_redirect(next_path=next_path, error_message=str(exc))


@app.get("/auth/callback")
async def auth_callback(request: Request) -> RedirectResponse:
    if not auth_enabled():
        return RedirectResponse(url="/auth/select", status_code=303)
    try:
        await complete_oidc_login(request)
        return RedirectResponse(url=request.session.pop("post_login_redirect", "/"), status_code=303)
    except HTTPException as exc:
        request.session.pop("post_login_redirect", None)
        return _auth_error_redirect(describe_auth_failure(str(exc.status_code), exc.detail))
    except Exception as exc:
        request.session.pop("post_login_redirect", None)
        error_name = getattr(exc, "error", None)
        description = getattr(exc, "description", None) or str(exc)
        return _auth_error_redirect(describe_auth_failure(error_name, description))


@app.get("/auth/logout")
def auth_logout(request: Request) -> RedirectResponse:
    logout(request)
    return RedirectResponse(url="/auth/select" if not auth_enabled() else "/", status_code=303)


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    guarded = _page_guard(request)
    if isinstance(guarded, RedirectResponse):
        return guarded
    principal = guarded
    with db_session() as session:
        owner_user_id = _owner_user_id_for_principal(principal)
        personas = list_personas(session, owner_user_id=owner_user_id)
        posts = list_scheduled_posts(session, owner_user_id=owner_user_id)[:10]
        run_events = [serialize_run_event(event) for event in list_run_events(session, limit=60, owner_user_id=owner_user_id)]
        alert_events = [
            serialize_alert_event(event)
            for event in _visible_dashboard_alerts(request, list_alert_events(session, limit=100, owner_user_id=owner_user_id))
        ]
        run_groups = summarize_run_events(run_events, limit_runs=6)
        account_count = sum(len(persona.accounts) for persona in personas)
        webhook_observability = instagram_webhook_observability(session, window_days=7, recent_limit=12, field_limit=6) if principal.is_admin else None
        return templates.TemplateResponse(
            name="dashboard.html",
            request=request,
            context=_template_context(
                request,
                personas=personas,
                account_count=account_count,
                posts=[_serialize_post(post) for post in posts],
                persona_name_by_id={persona.id: persona.name for persona in personas},
                run_groups=run_groups,
                alert_events=alert_events,
                instagram_webhook_observability=webhook_observability,
                scheduler_status=_scheduler_service(request).get_status(),
                admin_mode=bool(auth_enabled() and principal.is_admin),
                cleared_dashboard_alert_count=_coerce_int_query_param(request.query_params.get("alerts_cleared")),
            ),
        )


@app.post("/dashboard/alerts/clear")
async def clear_dashboard_alerts(request: Request) -> RedirectResponse:
    guarded = _page_guard(request)
    if isinstance(guarded, RedirectResponse):
        return guarded
    principal = guarded
    with db_session() as session:
        owner_user_id = _owner_user_id_for_principal(principal)
        current_alerts = _visible_dashboard_alerts(request, list_alert_events(session, limit=100, owner_user_id=owner_user_id))
    dismissed_count = _dismiss_dashboard_alerts(request, current_alerts)
    return RedirectResponse(url=f"/?alerts_cleared={dismissed_count}", status_code=303)


@app.get("/profiles/page")
def legacy_profiles_page() -> RedirectResponse:
    return RedirectResponse(url="/personas/page", status_code=307)


@app.get("/personas/page", response_class=HTMLResponse)
def personas_page(request: Request) -> HTMLResponse:
    guarded = _page_guard(request, role="user")
    if isinstance(guarded, RedirectResponse):
        return guarded
    principal = guarded
    with db_session() as session:
        personas = list_personas(session, owner_user_id=_owner_user_id_for_principal(principal))
        return templates.TemplateResponse(
            name="personas.html",
            request=request,
            context=_template_context(request, personas=personas),
        )


@app.get("/personas/{persona_id}/page", response_class=HTMLResponse)
def persona_detail_page(persona_id: str, request: Request) -> HTMLResponse:
    guarded = _page_guard(request, role="user")
    if isinstance(guarded, RedirectResponse):
        return guarded
    principal = guarded
    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        accounts = [_account_template_context(account) for account in sorted(persona.accounts, key=lambda item: (item.label, item.service))]
        routes = list_routes(session, persona)
        source_accounts = [account for account in persona.accounts if account.source_enabled]
        destination_accounts = [account for account in persona.accounts if account.destination_enabled]
        routes_map = {(route.source_account_id, route.destination_account_id): route for route in routes}
        return templates.TemplateResponse(
            name="persona_detail.html",
            request=request,
            context=_template_context(
                request,
                persona=persona,
                accounts=accounts,
                routes=routes,
                routes_map=routes_map,
                source_accounts=source_accounts,
                destination_accounts=destination_accounts,
                service_definitions=iter_service_definitions(),
            ),
        )


@app.get("/scheduled-posts/page", response_class=HTMLResponse)
def scheduled_posts_page(request: Request) -> HTMLResponse:
    guarded = _page_guard(request, role="user")
    if isinstance(guarded, RedirectResponse):
        return guarded
    principal = guarded
    with db_session() as session:
        owner_user_id = _owner_user_id_for_principal(principal)
        posts = list_scheduled_posts(session, owner_user_id=owner_user_id)
        personas = list_personas(session, owner_user_id=owner_user_id)
        return templates.TemplateResponse(
            name="scheduled_posts.html",
            request=request,
            context=_template_context(
                request,
                posts=[_serialize_post(post) for post in posts],
                personas=personas,
                persona_targets=_persona_targets_context(personas),
                persona_name_by_id={persona.id: persona.name for persona in personas},
                service_post_guidance=service_composer_constraints_context(),
            ),
        )


@app.get("/scheduled-posts/{post_id}/page", response_class=HTMLResponse)
def scheduled_post_detail_page(post_id: str, request: Request) -> HTMLResponse:
    guarded = _page_guard(request, role="user")
    if isinstance(guarded, RedirectResponse):
        return guarded
    principal = guarded
    with db_session() as session:
        owner_user_id = _owner_user_id_for_principal(principal)
        post = get_post(session, post_id, owner_user_id=owner_user_id)
        if not post or post.origin_kind != "composer":
            raise HTTPException(status_code=404, detail="Scheduled post not found.")
        persona = get_persona(session, post.persona_id, owner_user_id=owner_user_id)
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        return templates.TemplateResponse(
            name="scheduled_post_detail.html",
            request=request,
            context=_template_context(
                request,
                post=_serialize_post(post),
                persona=persona,
                accounts=[_serialize_account(account) for account in persona_destination_accounts(persona)],
                service_post_guidance=service_composer_constraints_context(),
            ),
        )


@app.get("/account/page", response_class=HTMLResponse)
def account_settings_page(request: Request) -> HTMLResponse:
    guarded = _page_guard(request)
    if isinstance(guarded, RedirectResponse):
        return guarded
    principal = guarded
    with db_session() as session:
        user = _load_user_settings_subject(session, principal)
        return templates.TemplateResponse(
            name="account_settings.html",
            request=request,
            context=_template_context(
                request,
                user=user_to_read(user),
            ),
        )


@app.get("/admin/users/page", response_class=HTMLResponse)
def admin_users_page(request: Request) -> HTMLResponse:
    guarded = _page_guard(request, role="admin")
    if isinstance(guarded, RedirectResponse):
        return guarded
    with db_session() as session:
        users = list_users(session)
        return templates.TemplateResponse(
            name="admin_users.html",
            request=request,
            context=_template_context(
                request,
                users=users,
                saved_user_id=request.query_params.get("saved_user_id"),
                error_message=request.query_params.get("error"),
            ),
        )


@app.post("/admin/users/{user_id}/page")
async def save_admin_user_page(user_id: str, request: Request) -> RedirectResponse:
    try:
        principal = require_api_access(request, role="admin")
        payload = AdminUserUpdate.model_validate(await _read_admin_user_payload(request))
        if principal.user_id == user_id and not payload.is_enabled:
            return _admin_users_page_redirect(error_message="You cannot disable your own admin account from the web UI.")
        with db_session() as session:
            user = _load_admin_managed_user(session, user_id)
            admin_update_user(session, user, timezone=payload.timezone, is_enabled=payload.is_enabled)
        return _admin_users_page_redirect(saved_user_id=user_id)
    except HTTPException as exc:
        return _admin_users_page_redirect(error_message=exc.detail)
    except Exception as exc:
        return _admin_users_page_redirect(error_message=str(exc))


@app.get("/settings/page", response_class=HTMLResponse)
def settings_page(request: Request) -> HTMLResponse:
    guarded = _page_guard(request, role="admin")
    if isinstance(guarded, RedirectResponse):
        return guarded
    app_settings = read_app_settings()
    save_status = request.query_params.get("saved")
    test_status = request.query_params.get("tested")
    error_message = request.query_params.get("error")
    local_tunnel_target = f"http://127.0.0.1:{app_settings.app_port}"
    webhook_callback_url = instagram_webhook_callback_url(app_settings.app_base_url)
    webhook_verify_probe = None
    if webhook_callback_url and app_settings.instagram_webhook_verify_token:
        verify_query = urlencode(
            {
                "hub.mode": "subscribe",
                "hub.verify_token": app_settings.instagram_webhook_verify_token,
                "hub.challenge": "test123",
            }
        )
        webhook_verify_probe = f'curl "{webhook_callback_url}?{verify_query}"'
    with db_session() as session:
        latest_webhook = latest_instagram_webhook_event(session)
        return templates.TemplateResponse(
            name="settings.html",
            request=request,
            context=_template_context(
                request,
                app_settings=app_settings,
                saved=save_status == "1",
                tested=test_status == "1",
                error_message=error_message,
                oidc_warning_message=_oidc_settings_warning(app_settings),
                scheduler_status=_scheduler_service(request).get_status(),
                instagram_webhook_callback_url=webhook_callback_url,
                instagram_webhook_latest_received_at=latest_webhook.created_at if latest_webhook else None,
                instagram_webhook_latest_event_type=latest_webhook.event_type if latest_webhook else None,
                instagram_webhook_required_fields=["comments", "mentions"],
                instagram_tunnel_local_target=local_tunnel_target,
                instagram_tunnel_cloudflared_command=f"cloudflared tunnel --url {local_tunnel_target}",
                instagram_tunnel_ngrok_command=f"ngrok http {app_settings.app_port}",
                instagram_tunnel_verify_probe=webhook_verify_probe,
                instagram_webhook_logs_href="/logs/page#instagram-webhooks",
                instagram_webhook_dashboard_href="/#instagram-webhooks",
            ),
        )


@app.get("/sandbox/page", response_class=HTMLResponse)
def sandbox_page(request: Request, post_id: str | None = None) -> HTMLResponse:
    guarded = _page_guard(request, role="user")
    if isinstance(guarded, RedirectResponse):
        return guarded
    principal = guarded
    with db_session() as session:
        owner_user_id = _owner_user_id_for_principal(principal)
        personas = list_personas(session, owner_user_id=owner_user_id)
        sandbox_seed: dict[str, Any] | None = None
        sandbox_source_label: str | None = None
        if post_id:
            post = get_post(session, post_id, owner_user_id=owner_user_id)
            if not post:
                raise HTTPException(status_code=404, detail="Scheduled post not found.")
            sandbox_seed = _sandbox_seed_from_post(post)
            sandbox_source_label = f"Loaded from post {post.id}"

        return templates.TemplateResponse(
            name="sandbox.html",
            request=request,
            context=_template_context(
                request,
                personas=personas,
                persona_targets=_persona_targets_context(personas),
                sandbox_seed=sandbox_seed or {},
                sandbox_source_label=sandbox_source_label,
            ),
        )


@app.get("/logs/page", response_class=HTMLResponse)
def logs_page(request: Request) -> HTMLResponse:
    guarded = _page_guard(request)
    if isinstance(guarded, RedirectResponse):
        return guarded
    principal = guarded
    filters = _log_filters_from_request(request)
    cleared_count = request.query_params.get("cleared")
    with db_session() as session:
        owner_user_id = _owner_user_id_for_principal(principal)
        personas = list_personas(session, owner_user_id=owner_user_id)
        accounts = [_serialize_account(account) for persona in personas for account in persona.accounts]
        run_events = [serialize_run_event(event) for event in list_run_events(session, filters=filters, limit=150, owner_user_id=owner_user_id)]
        run_groups = summarize_run_events(run_events)
        alert_events = [serialize_alert_event(event) for event in list_alert_events(session, filters=filters, limit=100, owner_user_id=owner_user_id)]
        webhook_observability = instagram_webhook_observability(session, window_days=7, recent_limit=12, field_limit=10) if principal.is_admin else None
        return templates.TemplateResponse(
            name="logs.html",
            request=request,
            context=_template_context(
                request,
                filters=filters,
                personas=personas,
                accounts=accounts,
                run_groups=run_groups,
                alert_events=alert_events,
                instagram_webhook_observability=webhook_observability,
                cleared_count=int(cleared_count) if cleared_count is not None and cleared_count.isdigit() else None,
            ),
        )


@app.post("/logs/alerts/clear")
async def clear_logs_alerts(request: Request) -> RedirectResponse:
    principal = require_api_access(request)
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/json"):
        raw_filters = await request.json()
    else:
        form = await request.form()
        raw_filters = dict(form)

    filters = {
        "persona_id": raw_filters.get("persona_id") or None,
        "account_id": raw_filters.get("account_id") or None,
        "service": raw_filters.get("service") or None,
        "severity": raw_filters.get("severity") or None,
        "operation": raw_filters.get("operation") or None,
        "since": raw_filters.get("since") or None,
    }

    with db_session() as session:
        cleared_count = clear_alert_events(session, filters=filters, owner_user_id=_owner_user_id_for_principal(principal))

    query = {key: value for key, value in filters.items() if value}
    query["cleared"] = str(cleared_count)
    return RedirectResponse(url=f"/logs/page?{urlencode(query)}", status_code=303)


@app.get("/health")
def health() -> dict[str, Any]:
    current_settings = get_settings()
    with db_session() as session:
        return {
            "status": "ok",
            "instance": current_settings.instance_name,
            "database": str(current_settings.database_path),
            "app_data_dir": str(current_settings.app_data_dir),
            "persona_count": len(list_personas(session)),
            "scheduler": {
                "autorun_interval_seconds": current_settings.scheduler_automation_interval_seconds,
            },
        }


@app.get("/scheduler/status")
def api_scheduler_status(request: Request) -> dict[str, Any]:
    require_api_access(request, role="admin")
    return _scheduler_service(request).get_status()


@app.post("/scheduler/pause")
def api_pause_scheduler(request: Request) -> dict[str, Any]:
    require_api_access(request, role="admin")
    return _scheduler_service(request).pause_automation()


@app.post("/scheduler/start")
def api_start_scheduler(request: Request) -> dict[str, Any]:
    require_api_access(request, role="admin")
    return _scheduler_service(request).resume_automation()


@app.get("/live-updates/status")
def api_live_update_status(request: Request) -> dict[str, Any]:
    require_api_access(request)
    topics = request.query_params.get("topics", "")
    return live_update_snapshot(topics)


@app.get("/settings")
def api_get_settings(request: Request) -> AppSettingsRead:
    require_api_access(request, role="admin")
    return read_app_settings()


@app.get("/account")
def api_get_account_settings(request: Request) -> UserRead:
    principal = require_api_access(request)
    with db_session() as session:
        user = _load_user_settings_subject(session, principal)
        return user_to_read(user)


@app.get("/admin/users")
def api_list_users(request: Request) -> list[UserRead]:
    require_api_access(request, role="admin")
    with db_session() as session:
        return [user_to_read(user) for user in list_users(session)]


@app.put("/admin/users/{user_id}")
async def api_update_admin_user(user_id: str, request: Request) -> UserRead:
    principal = require_api_access(request, role="admin")
    payload = AdminUserUpdate.model_validate(await request.json())
    if principal.user_id == user_id and not payload.is_enabled:
        raise HTTPException(status_code=400, detail="You cannot disable your own admin account from the web UI.")
    with db_session() as session:
        user = _load_admin_managed_user(session, user_id)
        updated = admin_update_user(session, user, timezone=payload.timezone, is_enabled=payload.is_enabled)
        return user_to_read(updated)


@app.put("/account")
async def api_update_account_settings(request: Request) -> UserRead:
    principal = require_api_access(request)
    raw_payload = await request.json()
    payload = UserSettingsUpdate.model_validate(raw_payload)
    with db_session() as session:
        user = _load_user_settings_subject(session, principal)
        updated = update_user_settings(
            session,
            user,
            timezone=payload.timezone,
            ui_theme=payload.ui_theme,
            ui_mode=payload.ui_mode,
            preferred_name=payload.preferred_name,
            apply_preferred_name="preferred_name" in raw_payload,
        )
        principal.display_name = updated.effective_display_name
        principal.timezone = updated.timezone
        principal.ui_theme = updated.ui_theme
        principal.ui_mode = updated.ui_mode
        return user_to_read(updated)


@app.put("/settings")
async def api_update_settings(request: Request) -> AppSettingsRead:
    require_api_access(request, role="admin")
    payload = AppSettingsUpdate.model_validate(await request.json())
    updated = update_app_settings(payload)
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler:
        scheduler.refresh_configuration()
    return updated


@app.post("/settings/page")
async def save_settings_page(request: Request) -> RedirectResponse:
    try:
        require_api_access(request, role="admin")
    except HTTPException as exc:
        return _settings_page_redirect(error_message=exc.detail)
    try:
        payload = AppSettingsUpdate.model_validate(await _read_app_settings_payload(request))
        update_app_settings(payload)
        _scheduler_service(request).refresh_configuration()
        return _settings_page_redirect(saved=True)
    except Exception as exc:
        return _settings_page_redirect(error_message=str(exc))


@app.post("/settings/test-webhook")
async def test_settings_webhook_page(request: Request) -> RedirectResponse:
    try:
        require_api_access(request, role="admin")
    except HTTPException as exc:
        return _settings_page_redirect(error_message=exc.detail)
    try:
        payload = AppSettingsUpdate.model_validate(await _read_app_settings_payload(request))
        update_app_settings(payload)
        _scheduler_service(request).refresh_configuration()
        send_settings_test_webhook()
        return _settings_page_redirect(saved=True, tested=True)
    except Exception as exc:
        return _settings_page_redirect(error_message=str(exc))


@app.get("/webhooks/instagram")
def instagram_webhook_verify(request: Request) -> PlainTextResponse:
    app_settings = read_app_settings()
    if not app_settings.instagram_webhooks_enabled:
        raise HTTPException(status_code=404, detail="Instagram webhooks are not enabled.")
    if request.query_params.get("hub.mode") != "subscribe":
        raise HTTPException(status_code=400, detail="Invalid Instagram webhook verification mode.")
    if request.query_params.get("hub.verify_token") != app_settings.instagram_webhook_verify_token:
        raise HTTPException(status_code=403, detail="Instagram webhook verify token did not match.")
    challenge = request.query_params.get("hub.challenge", "")
    return PlainTextResponse(challenge)


@app.post("/webhooks/instagram")
async def instagram_webhook_callback(request: Request) -> dict[str, Any]:
    app_settings = read_app_settings()
    if not app_settings.instagram_webhooks_enabled:
        raise HTTPException(status_code=404, detail="Instagram webhooks are not enabled.")
    signature_256 = request.headers.get("X-Hub-Signature-256")
    signature_legacy = request.headers.get("X-Hub-Signature")
    signature = signature_256 or signature_legacy
    raw_body = await request.body()
    if not verify_instagram_webhook_signature(raw_body, signature, app_settings.instagram_app_secret):
        run_id = new_run_id()
        client_host = request.client.host if request.client else None
        with db_session() as session:
            _alert_dispatcher(request).emit_hard_failure(
                session,
                run_id=run_id,
                service="instagram",
                operation="webhook",
                event_type="instagram_webhook_rejected",
                severity="warning",
                message="Rejected Instagram webhook delivery because signature validation failed.",
                error_class="InvalidWebhookSignature",
                payload={
                    "client_host": client_host,
                    "content_type": request.headers.get("content-type"),
                    "body_length": len(raw_body),
                    "x_hub_signature_256_present": bool(signature_256),
                    "x_hub_signature_present": bool(signature_legacy),
                    "user_agent": request.headers.get("user-agent"),
                },
            )
        raise HTTPException(status_code=401, detail="Instagram webhook signature validation failed.")
    try:
        payload = json.loads(raw_body.decode("utf-8") or "{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Instagram webhook payload was not valid JSON.") from exc
    run_id = new_run_id()
    with db_session() as session:
        events = ingest_instagram_webhook_payload(session, payload, signature_valid=True, run_id=run_id)
    return {"ok": True, "stored_events": len(events)}


@app.post("/scheduler/run")
def api_run_scheduler(request: Request) -> dict[str, Any]:
    require_api_access(request, role="admin")
    result = _scheduler_service(request).run_manual_cycle()
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    if result["status"] == "busy":
        raise HTTPException(status_code=409, detail=result["message"])
    return result


@app.get("/personas")
def api_personas(request: Request) -> list[PersonaRead]:
    principal = require_api_access(request, role="user")
    with db_session() as session:
        return [PersonaRead.model_validate(persona) for persona in list_personas(session, owner_user_id=_owner_user_id_for_principal(principal))]


@app.post("/personas")
async def api_create_persona(request: Request) -> PersonaRead:
    principal = require_api_access(request, role="user")
    raw_payload = await _read_persona_payload(request)
    payload = PersonaCreate.model_validate(raw_payload)
    with db_session() as session:
        persona_payload = payload.model_dump()
        if principal.user_id:
            persona_payload["owner_user_id"] = principal.user_id
        persona = create_persona(session, persona_payload)
        return PersonaRead.model_validate(persona)


@app.get("/personas/{persona_id}")
def api_persona_detail(persona_id: str, request: Request) -> PersonaRead:
    principal = require_api_access(request, role="user")
    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        return PersonaRead.model_validate(persona)


@app.put("/personas/{persona_id}")
async def api_update_persona(persona_id: str, request: Request) -> PersonaRead:
    principal = require_api_access(request, role="user")
    raw_payload = await _read_persona_payload(request)
    payload = PersonaUpdate.model_validate(raw_payload)
    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        updated = update_persona(session, persona, payload.model_dump(exclude_unset=True))
        return PersonaRead.model_validate(updated)


@app.get("/personas/{persona_id}/accounts")
def api_persona_accounts(persona_id: str, request: Request) -> list[AccountRead]:
    principal = require_api_access(request, role="user")
    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        return [_serialize_account(account) for account in sorted(persona.accounts, key=lambda item: (item.label, item.service))]


@app.post("/personas/{persona_id}/accounts")
async def api_create_account(persona_id: str, request: Request) -> AccountRead:
    principal = require_api_access(request, role="user")
    raw_payload = await _read_account_payload(request)
    payload = AccountCreate.model_validate(raw_payload)
    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        try:
            account = create_account(session, persona, payload.model_dump())
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _serialize_account(account)


@app.put("/personas/{persona_id}/accounts/{account_id}")
async def api_update_account(persona_id: str, account_id: str, request: Request) -> AccountRead:
    principal = require_api_access(request, role="user")
    raw_payload = await _read_account_payload(request)
    payload = AccountUpdate.model_validate(raw_payload)
    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        account = get_account(session, account_id)
        if not account or account.persona_id != persona_id:
            raise HTTPException(status_code=404, detail="Account not found.")
        try:
            updated = update_account(session, persona, account, payload.model_dump(exclude_unset=True))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _serialize_account(updated)


@app.post("/personas/{persona_id}/accounts/{account_id}/instagram-login/validate")
async def api_validate_instagram_account_login(persona_id: str, account_id: str, request: Request) -> dict[str, Any]:
    principal = require_api_access(request, role="user")
    raw_payload = await _read_account_payload(request)
    payload = AccountUpdate.model_validate(raw_payload)
    run_id = new_run_id()

    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        account = get_account(session, account_id)
        if not account or account.persona_id != persona_id:
            raise HTTPException(status_code=404, detail="Account not found.")
        if account.service != "instagram":
            raise HTTPException(status_code=400, detail="Instagram login validation is only available for Instagram accounts.")

        payload_data = payload.model_dump(exclude_unset=True)
        candidate_credentials = dict(payload_data.get("credentials_json") or {})

        try:
            validated_credentials, _, instagram_username = validate_instagram_account_login(
                candidate_credentials,
                previous_credentials=account.credentials_json,
            )
            payload_data["credentials_json"] = validated_credentials
            updated = update_account(session, persona, account, payload_data)
            updated.last_health_status = "ok"
            updated.last_error = None
            session.flush()

            log_run_event(
                session,
                run_id=run_id,
                persona_id=persona.id,
                persona_name=persona.name,
                account_id=updated.id,
                service=updated.service,
                operation="instagram_auth_validate",
                message=f"Validated Instagram login for {updated.label}",
                metadata={
                    "session_id_saved": True,
                    "instagram_username": instagram_username or str((validated_credentials or {}).get("instagrapi_username") or "").strip() or None,
                },
            )
            return {"message": "Instagram login validated. Session ID captured and saved.", "session_id_saved": True}
        except Exception as exc:
            account.last_health_status = "error"
            account.last_error = str(exc)
            session.flush()
            _alert_dispatcher(request).emit_hard_failure(
                session,
                run_id=run_id,
                persona=persona,
                account=account,
                service=account.service,
                operation="instagram_auth_validate",
                message=str(exc),
                error_class=exc.__class__.__name__,
                event_type="account_validation_failure",
            )
            session.commit()
            raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/personas/{persona_id}/accounts/{account_id}")
def api_delete_account(persona_id: str, account_id: str, request: Request) -> dict[str, Any]:
    principal = require_api_access(request, role="user")
    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        account = get_account(session, account_id)
        if not account or account.persona_id != persona_id:
            raise HTTPException(status_code=404, detail="Account not found.")
        try:
            delete_account(session, persona, account)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"deleted_account_id": account_id}


@app.post("/personas/{persona_id}/accounts/{account_id}/instagram-token/record-refresh")
def api_record_instagram_token_refresh(persona_id: str, account_id: str, request: Request) -> AccountRead:
    principal = require_api_access(request, role="user")
    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        account = get_account(session, account_id)
        if not account or account.persona_id != persona_id:
            raise HTTPException(status_code=404, detail="Account not found.")
        try:
            updated = record_account_token_refresh(session, persona, account)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _serialize_account(updated)


@app.get("/personas/{persona_id}/routes")
def api_persona_routes(persona_id: str, request: Request) -> list[AccountRouteRead]:
    principal = require_api_access(request, role="user")
    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        return [_serialize_route(route) for route in list_routes(session, persona)]


@app.put("/personas/{persona_id}/routes")
async def api_replace_routes(persona_id: str, request: Request) -> list[AccountRouteRead]:
    principal = require_api_access(request, role="user")
    raw_payload = await _read_routes_payload(request)
    payload = AccountRouteReplaceRequest.model_validate(raw_payload)
    with db_session() as session:
        persona = get_persona(session, persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        try:
            routes = replace_routes(session, persona, [route.model_dump() for route in payload.routes if route.is_enabled])
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return [_serialize_route(route) for route in routes]


@app.get("/scheduled-posts")
def api_scheduled_posts(request: Request) -> list[ScheduledPostRead]:
    principal = require_api_access(request, role="user")
    with db_session() as session:
        return [_serialize_post(post) for post in list_scheduled_posts(session, owner_user_id=_owner_user_id_for_principal(principal))]


@app.post("/scheduled-posts")
async def api_create_scheduled_post(request: Request) -> ScheduledPostRead:
    principal = require_api_access(request, role="user")
    raw_payload, uploads, alt_texts = await _read_scheduled_post_payload(request)
    payload = ScheduledPostCreate.model_validate(raw_payload)
    media_items = []
    for index, upload in enumerate(uploads):
        alt_text = alt_texts[index] if index < len(alt_texts) else ""
        media_items.append(await store_upload(upload, alt_text=alt_text, sort_order=index))
    with db_session() as session:
        persona = get_persona(session, payload.persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        try:
            post = create_scheduled_post(session, payload, media_items)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        publish_live_update(
            LIVE_UPDATE_TOPIC_SCHEDULED_POSTS,
            LIVE_UPDATE_TOPIC_DASHBOARD,
        )
        return _serialize_post(post)


@app.get("/scheduled-posts/{post_id}")
def api_scheduled_post_detail(post_id: str, request: Request) -> ScheduledPostRead:
    principal = require_api_access(request, role="user")
    with db_session() as session:
        post = get_post(session, post_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not post or post.origin_kind != "composer":
            raise HTTPException(status_code=404, detail="Scheduled post not found.")
        return _serialize_post(post)


@app.get("/scheduled-posts/{post_id}/giveaway")
def api_scheduled_post_giveaway(post_id: str, request: Request):
    principal = require_api_access(request, role="user")
    with db_session() as session:
        post = get_post(session, post_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not post or post.origin_kind != "composer" or post.post_type != POST_TYPE_INSTAGRAM_GIVEAWAY:
            raise HTTPException(status_code=404, detail="Instagram giveaway not found.")
        if post.instagram_giveaway is None:
            raise HTTPException(status_code=404, detail="Instagram giveaway not found.")
        return serialize_giveaway(post.instagram_giveaway)


@app.put("/scheduled-posts/{post_id}")
async def api_update_scheduled_post(post_id: str, request: Request) -> ScheduledPostRead:
    principal = require_api_access(request, role="user")
    raw_payload, uploads, alt_texts = await _read_scheduled_post_payload(request)
    payload = ScheduledPostUpdate.model_validate(raw_payload)
    media_items = []
    with db_session() as session:
        post = get_post(session, post_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not post or post.origin_kind != "composer":
            raise HTTPException(status_code=404, detail="Scheduled post not found.")
        next_sort_order = len(post.attachments)
        for index, upload in enumerate(uploads):
            alt_text = alt_texts[index] if index < len(alt_texts) else ""
            media_items.append(await store_upload(upload, alt_text=alt_text, sort_order=next_sort_order + index))
    with db_session() as session:
        post = get_post(session, post_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not post or post.origin_kind != "composer":
            raise HTTPException(status_code=404, detail="Scheduled post not found.")
        try:
            updated = update_scheduled_post(session, post, payload, media_items)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        publish_live_update(
            LIVE_UPDATE_TOPIC_SCHEDULED_POSTS,
            LIVE_UPDATE_TOPIC_DASHBOARD,
        )
        return _serialize_post(updated)


@app.delete("/scheduled-posts/{post_id}")
def api_delete_scheduled_post(post_id: str, request: Request) -> dict[str, Any]:
    principal = require_api_access(request, role="user")
    with db_session() as session:
        post = get_post(session, post_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not post or post.origin_kind != "composer":
            raise HTTPException(status_code=404, detail="Scheduled post not found.")
        try:
            delete_scheduled_post(session, post)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        publish_live_update(
            LIVE_UPDATE_TOPIC_SCHEDULED_POSTS,
            LIVE_UPDATE_TOPIC_DASHBOARD,
        )
        return {"deleted_post_id": post_id}


@app.post("/scheduled-posts/{post_id}/giveaway/review/confirm")
def api_confirm_giveaway_winner(post_id: str, request: Request):
    principal = require_api_access(request, role="user")
    with db_session() as session:
        post = get_post(session, post_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not post or post.origin_kind != "composer" or post.instagram_giveaway is None:
            raise HTTPException(status_code=404, detail="Instagram giveaway not found.")
        try:
            updated = confirm_giveaway_winner(session, post.instagram_giveaway, run_id=new_run_id())
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        publish_live_update(
            LIVE_UPDATE_TOPIC_SCHEDULED_POSTS,
            LIVE_UPDATE_TOPIC_DASHBOARD,
            LIVE_UPDATE_TOPIC_LOGS,
        )
        return serialize_giveaway(updated)


@app.post("/scheduled-posts/{post_id}/giveaway/review/advance")
def api_advance_giveaway_winner(post_id: str, request: Request):
    principal = require_api_access(request, role="user")
    with db_session() as session:
        post = get_post(session, post_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not post or post.origin_kind != "composer" or post.instagram_giveaway is None:
            raise HTTPException(status_code=404, detail="Instagram giveaway not found.")
        try:
            updated = advance_giveaway_winner(session, post.instagram_giveaway, run_id=new_run_id())
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        publish_live_update(
            LIVE_UPDATE_TOPIC_SCHEDULED_POSTS,
            LIVE_UPDATE_TOPIC_DASHBOARD,
            LIVE_UPDATE_TOPIC_LOGS,
        )
        return serialize_giveaway(updated)


@app.post("/scheduled-posts/{post_id}/send-now")
def api_send_now(post_id: str, request: Request) -> ScheduledPostRead:
    principal = require_api_access(request, role="user")
    with db_session() as session:
        post = get_post(session, post_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not post or post.origin_kind != "composer":
            raise HTTPException(status_code=404, detail="Scheduled post not found.")
        try:
            updated = schedule_post_now(session, post)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        publish_live_update(
            LIVE_UPDATE_TOPIC_SCHEDULED_POSTS,
            LIVE_UPDATE_TOPIC_DASHBOARD,
        )
        return _serialize_post(updated)


@app.post("/sandbox/preview")
async def api_sandbox_preview(request: Request) -> SandboxPreviewRead:
    principal = require_api_access(request, role="user")
    raw_payload = await request.json()
    payload = SandboxPreviewRequest.model_validate(raw_payload)
    with db_session() as session:
        persona = get_persona(session, payload.persona_id, owner_user_id=_owner_user_id_for_principal(principal))
        if not persona:
            raise HTTPException(status_code=404, detail="Persona not found.")
        try:
            return build_sandbox_preview(session, payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/runs/recent")
def api_recent_runs(request: Request) -> list[RunEventRead]:
    principal = require_api_access(request)
    with db_session() as session:
        filters = _log_filters_from_request(request)
        runs = list_run_events(session, filters=filters, limit=100, owner_user_id=_owner_user_id_for_principal(principal))
        return [serialize_run_event(run) for run in runs]


@app.get("/errors/recent")
def api_recent_errors(request: Request) -> list[AlertEventRead]:
    principal = require_api_access(request)
    with db_session() as session:
        filters = _log_filters_from_request(request)
        alerts = list_alert_events(session, filters=filters, limit=100, owner_user_id=_owner_user_id_for_principal(principal))
        return [serialize_alert_event(alert) for alert in alerts]


@app.post("/errors/clear")
async def api_clear_recent_errors(request: Request) -> dict[str, Any]:
    principal = require_api_access(request)
    content_type = request.headers.get("content-type", "")
    raw_filters = await request.json() if content_type.startswith("application/json") else {}
    filters = {
        "persona_id": raw_filters.get("persona_id"),
        "account_id": raw_filters.get("account_id"),
        "service": raw_filters.get("service"),
        "severity": raw_filters.get("severity"),
        "operation": raw_filters.get("operation"),
        "since": raw_filters.get("since"),
    }
    with db_session() as session:
        cleared_count = clear_alert_events(session, filters=filters, owner_user_id=_owner_user_id_for_principal(principal))
    return {"cleared_count": cleared_count, "filters": {key: value for key, value in filters.items() if value}}


def main() -> None:
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=settings.app_port, reload=False)
