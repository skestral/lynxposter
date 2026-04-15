from __future__ import annotations

import ast
import copy
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Account, AccountPostRef, AccountRoute, CanonicalPost, DeliveryJob, Persona
from app.utils import slugify


settings = get_settings()


class LiteralEvaluator(ast.NodeVisitor):
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def visit_Assign(self, node: ast.Assign) -> None:
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            return
        target = node.targets[0].id
        try:
            self.values[target] = self._eval(node.value)
        except ValueError:
            return

    def _eval(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            if node.id not in self.values:
                raise ValueError(node.id)
            return self.values[node.id]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self._eval(node.left) + self._eval(node.right)
        raise ValueError(ast.dump(node))


def _read_legacy_assignments(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    tree = ast.parse(path.read_text(encoding="utf-8"))
    evaluator = LiteralEvaluator()
    for node in tree.body:
        evaluator.visit(node)
    return evaluator.values


def _parse_bool(value: str | bool | None, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str | int | None, default: int) -> int:
    if isinstance(value, int):
        return value
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _build_account_payload(
    *,
    service: str,
    label: str,
    handle_or_identifier: str,
    source_enabled: bool,
    destination_enabled: bool,
    credentials_json: dict[str, Any],
    source_settings_json: dict[str, Any] | None = None,
    publish_settings_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "service": service,
        "label": label,
        "handle_or_identifier": handle_or_identifier,
        "is_enabled": True,
        "source_enabled": source_enabled,
        "destination_enabled": destination_enabled,
        "credentials_json": {key: value for key, value in credentials_json.items() if value not in (None, "")},
        "source_settings_json": source_settings_json or {},
        "publish_settings_json": publish_settings_json or {},
    }


def build_default_persona_seed() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    configured_settings_dir = settings.config_dir / "settings"
    settings_dir = configured_settings_dir if configured_settings_dir.exists() else settings.project_root / "settings"

    auth_defaults = _read_legacy_assignments(settings_dir / "auth.py")
    settings_defaults = _read_legacy_assignments(settings_dir / "settings.py")

    persona_payload = {
        "name": "Default Persona",
        "slug": slugify("default-persona"),
        "is_enabled": True,
        "timezone": "server",
        "settings_json": {
            "post_time_limit": _parse_int(os.getenv("POST_TIME_LIMIT"), int(settings_defaults.get("post_time_limit", 12))),
            "visibility": os.getenv("MASTODON_VISIBILITY", settings_defaults.get("visibility", "public")),
            "mentions": os.getenv("MENTIONS", settings_defaults.get("mentions", "strip")),
            "quote_posts": _parse_bool(os.getenv("QUOTE_POSTS"), settings_defaults.get("quote_posts", True)),
            "post_default": _parse_bool(os.getenv("POST_DEFAULT"), settings_defaults.get("post_default", True)),
            "import_existing_posts": False,
        },
        "retry_settings_json": {
            "max_retries": _parse_int(os.getenv("MAX_RETRIES"), int(settings_defaults.get("max_retries", 5))),
        },
        "throttle_settings_json": {
            "max_per_hour": _parse_int(os.getenv("MAX_PER_HOUR"), int(settings_defaults.get("max_per_hour", 0))),
            "overflow_posts": os.getenv("OVERFLOW_POST", settings_defaults.get("overflow_posts", "retry")),
        },
    }

    accounts: list[dict[str, Any]] = []

    bluesky_handle = os.getenv("BSKY_HANDLE", auth_defaults.get("BSKY_HANDLE", ""))
    bluesky_password = os.getenv("BSKY_PASSWORD", auth_defaults.get("BSKY_PASSWORD", ""))
    bluesky_session = os.getenv("BSKY_SESSION_STRING", auth_defaults.get("BSKY_SESSION_STRING", ""))
    accounts.append(
        _build_account_payload(
            service="bluesky",
            label="Bluesky",
            handle_or_identifier=bluesky_handle,
            source_enabled=True,
            destination_enabled=True,
            credentials_json={
                "handle": bluesky_handle,
                "password": bluesky_password,
                "session_string": bluesky_session,
            },
        )
    )

    if _parse_bool(os.getenv("INSTAGRAM_CROSSPOSTING"), _parse_bool(settings_defaults.get("Instagram"))):
        instagram_key = os.getenv("INSTAGRAM_API_KEY", auth_defaults.get("INSTAGRAM_API_KEY", ""))
        instagram_user_id = os.getenv("INSTAGRAM_USER_ID", auth_defaults.get("INSTAGRAM_USER_ID", ""))
        instagrapi_username = os.getenv("INSTAGRAPI_USERNAME", auth_defaults.get("INSTAGRAPI_USERNAME", ""))
        instagrapi_password = os.getenv("INSTAGRAPI_PASSWORD", auth_defaults.get("INSTAGRAPI_PASSWORD", ""))
        instagrapi_sessionid = os.getenv("INSTAGRAPI_SESSIONID", auth_defaults.get("INSTAGRAPI_SESSIONID", ""))
        accounts.append(
            _build_account_payload(
                service="instagram",
                label="Instagram",
                handle_or_identifier=instagrapi_username or instagram_user_id or "Instagram",
                source_enabled=bool(instagram_key),
                destination_enabled=bool(instagrapi_sessionid or (instagrapi_username and instagrapi_password)),
                credentials_json={
                    "api_key": instagram_key,
                    "instagrapi_username": instagrapi_username,
                    "instagrapi_password": instagrapi_password,
                    "instagrapi_sessionid": instagrapi_sessionid,
                },
            )
        )

    if _parse_bool(os.getenv("MASTODON_CROSSPOSTING"), _parse_bool(settings_defaults.get("Mastodon"))):
        mastodon_handle = os.getenv("MASTODON_HANDLE", auth_defaults.get("MASTODON_HANDLE", ""))
        mastodon_instance = os.getenv("MASTODON_INSTANCE", auth_defaults.get("MASTODON_INSTANCE", ""))
        mastodon_token = os.getenv("MASTODON_TOKEN", auth_defaults.get("MASTODON_TOKEN", ""))
        accounts.append(
            _build_account_payload(
                service="mastodon",
                label="Mastodon",
                handle_or_identifier=mastodon_handle or mastodon_instance,
                source_enabled=True,
                destination_enabled=True,
                credentials_json={
                    "handle": mastodon_handle,
                    "instance": mastodon_instance,
                    "token": mastodon_token,
                },
                publish_settings_json={
                    "visibility": persona_payload["settings_json"]["visibility"],
                    "language": os.getenv("MASTODON_LANG", settings_defaults.get("mastodon_lang", "")),
                },
            )
        )

    if _parse_bool(os.getenv("TWITTER_CROSSPOSTING"), _parse_bool(settings_defaults.get("Twitter"))):
        twitter_username = os.getenv("TWITTER_USERNAME", auth_defaults.get("TWITTER_USERNAME", ""))
        accounts.append(
            _build_account_payload(
                service="twitter",
                label="Twitter/X",
                handle_or_identifier=twitter_username,
                source_enabled=False,
                destination_enabled=True,
                credentials_json={
                    "app_key": os.getenv("TWITTER_APP_KEY", auth_defaults.get("TWITTER_APP_KEY", "")),
                    "app_secret": os.getenv("TWITTER_APP_SECRET", auth_defaults.get("TWITTER_APP_SECRET", "")),
                    "access_token": os.getenv("TWITTER_ACCESS_TOKEN", auth_defaults.get("TWITTER_ACCESS_TOKEN", "")),
                    "access_token_secret": os.getenv(
                        "TWITTER_ACCESS_TOKEN_SECRET",
                        auth_defaults.get("TWITTER_ACCESS_TOKEN_SECRET", ""),
                    ),
                    "username": twitter_username,
                },
                publish_settings_json={"language": os.getenv("TWITTER_LANG", settings_defaults.get("twitter_lang", ""))},
            )
        )

    if _parse_bool(os.getenv("DISCORD_CROSSPOSTING"), _parse_bool(settings_defaults.get("Discord"))):
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL", auth_defaults.get("DISCORD_WEBHOOK_URL", ""))
        accounts.append(
            _build_account_payload(
                service="discord",
                label="Discord",
                handle_or_identifier="Webhook",
                source_enabled=False,
                destination_enabled=True,
                credentials_json={"webhook_url": webhook_url},
            )
        )

    if _parse_bool(os.getenv("TELEGRAM_CROSSPOSTING"), _parse_bool(settings_defaults.get("Telegram"))):
        telegram_channel_id = os.getenv("TELEGRAM_CHANNEL_ID", auth_defaults.get("TELEGRAM_CHANNEL_ID", ""))
        accounts.append(
            _build_account_payload(
                service="telegram",
                label="Telegram",
                handle_or_identifier=telegram_channel_id,
                source_enabled=True,
                destination_enabled=True,
                credentials_json={
                    "bot_token": os.getenv("TELEGRAM_BOT_TOKEN", auth_defaults.get("TELEGRAM_BOT_TOKEN", "")),
                    "channel_id": telegram_channel_id,
                },
            )
        )

    if _parse_bool(os.getenv("TUMBLR_CROSSPOSTING"), _parse_bool(settings_defaults.get("Tumblr"))):
        blog_name = os.getenv("TUMBLR_BLOG_NAME", auth_defaults.get("TUMBLR_BLOG_NAME", ""))
        accounts.append(
            _build_account_payload(
                service="tumblr",
                label="Tumblr",
                handle_or_identifier=blog_name,
                source_enabled=False,
                destination_enabled=True,
                credentials_json={
                    "consumer_key": os.getenv("TUMBLR_CONSUMER_KEY", auth_defaults.get("TUMBLR_CONSUMER_KEY", "")),
                    "consumer_secret": os.getenv(
                        "TUMBLR_CONSUMER_SECRET",
                        auth_defaults.get("TUMBLR_CONSUMER_SECRET", ""),
                    ),
                    "oauth_token": os.getenv("TUMBLR_OAUTH_TOKEN", auth_defaults.get("TUMBLR_OAUTH_TOKEN", "")),
                    "oauth_secret": os.getenv("TUMBLR_OAUTH_SECRET", auth_defaults.get("TUMBLR_OAUTH_SECRET", "")),
                    "blog_name": blog_name,
                },
            )
        )

    return persona_payload, accounts


def _copy_legacy_artifact(path: Path) -> None:
    if not path.exists():
        return
    settings.backups_dir.mkdir(parents=True, exist_ok=True)
    target = settings.backups_dir / path.name
    if not target.exists():
        shutil.copy2(path, target)


def _default_routes_for_accounts(accounts: list[Account]) -> list[AccountRoute]:
    source_accounts = [account for account in accounts if account.source_enabled and account.is_enabled]
    destination_accounts = [account for account in accounts if account.destination_enabled and account.is_enabled]
    routes: list[AccountRoute] = []
    for source_account in source_accounts:
        for destination_account in destination_accounts:
            if destination_account.id == source_account.id:
                continue
            routes.append(
                AccountRoute(
                    source_account_id=source_account.id,
                    destination_account_id=destination_account.id,
                    is_enabled=True,
                )
            )
    return routes


def _update_account_from_payload(account: Account, payload: dict[str, Any]) -> None:
    for field in (
        "label",
        "handle_or_identifier",
        "is_enabled",
        "source_enabled",
        "destination_enabled",
        "credentials_json",
        "source_settings_json",
        "publish_settings_json",
    ):
        setattr(account, field, copy.deepcopy(payload[field]))


def _apply_persona_payload(persona: Persona, payload: dict[str, Any]) -> None:
    for field in ("is_enabled", "timezone", "settings_json", "retry_settings_json", "throttle_settings_json"):
        setattr(persona, field, copy.deepcopy(payload[field]))


def _ensure_missing_default_routes(session: Session, persona: Persona) -> None:
    existing_pairs = {
        (route.source_account_id, route.destination_account_id)
        for route in session.query(AccountRoute)
        .join(Account, Account.id == AccountRoute.source_account_id)
        .filter(Account.persona_id == persona.id)
    }
    for route in _default_routes_for_accounts(list(persona.accounts)):
        pair = (route.source_account_id, route.destination_account_id)
        if pair in existing_pairs:
            continue
        session.add(route)
        existing_pairs.add(pair)


def apply_legacy_seed_to_persona(session: Session, persona: Persona) -> Persona:
    persona_payload, account_payloads = build_default_persona_seed()
    _apply_persona_payload(persona, persona_payload)

    existing_by_service = {account.service: account for account in persona.accounts}
    for payload in account_payloads:
        account = existing_by_service.get(payload["service"])
        if account is None:
            account = Account(persona_id=persona.id, **copy.deepcopy(payload))
            account.persona = persona
            session.add(account)
            existing_by_service[payload["service"]] = account
        else:
            _update_account_from_payload(account, payload)

    session.flush()
    _ensure_missing_default_routes(session, persona)
    session.flush()
    return persona


def import_legacy_install(session: Session) -> Persona | None:
    if session.query(Persona).first():
        return None

    persona_payload, account_payloads = build_default_persona_seed()
    persona = Persona(**persona_payload)
    session.add(persona)
    session.flush()

    accounts: list[Account] = []
    for payload in account_payloads:
        account = Account(persona_id=persona.id, **payload)
        session.add(account)
        accounts.append(account)
    session.flush()

    for route in _default_routes_for_accounts(accounts):
        session.add(route)
    session.flush()

    legacy_database_path = (settings.project_root / "db" / "database.json").resolve()
    legacy_post_cache_path = (settings.project_root / "db" / "post.cache").resolve()
    _copy_legacy_artifact(legacy_database_path)
    _copy_legacy_artifact(legacy_post_cache_path)

    accounts_by_service = {account.service: account for account in accounts}
    if not legacy_database_path.exists():
        return persona

    max_retries = int((persona.retry_settings_json or {}).get("max_retries", 5) or 5)
    for line in legacy_database_path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue

        post = CanonicalPost(
            persona_id=persona.id,
            origin_kind="legacy",
            status="posted",
            body=row.get("text", "") if isinstance(row, dict) else "",
            publish_overrides_json={},
            metadata_json={"imported_from_legacy": True},
            published_at=datetime.now(timezone.utc),
        )
        session.add(post)
        session.flush()

        ids = row.get("ids", {}) if isinstance(row, dict) else {}
        failed = row.get("failed", {}) if isinstance(row, dict) else {}
        service_key_map = {
            "bluesky": "bsky_id",
            "twitter": "twitter_id",
            "mastodon": "mastodon_id",
            "discord": "discord_id",
            "tumblr": "tumblr_id",
        }

        for service, id_key in service_key_map.items():
            account = accounts_by_service.get(service)
            if not account:
                continue
            external_id = ids.get(id_key) or ids.get(id_key.replace("_id", "Id"))
            if not external_id:
                continue

            status = "posted"
            stored_external_id = str(external_id)
            if external_id in {"skipped", "FailedToPost"}:
                status = "skipped"
                stored_external_id = ""

            job = DeliveryJob(
                post_id=post.id,
                target_account_id=account.id,
                status=status,
                external_id=stored_external_id or None,
                external_url=None,
                attempt_count=int(failed.get(service, 0) or 0),
                max_retries=max_retries,
                delivered_at=datetime.now(timezone.utc) if status == "posted" else None,
            )
            session.add(job)

            if status == "posted" and stored_external_id:
                session.add(
                    AccountPostRef(
                        post_id=post.id,
                        account_id=account.id,
                        external_id=stored_external_id,
                        external_url=None,
                        observed_at=datetime.now(timezone.utc),
                    )
                )

    session.flush()
    return persona
