from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _resolve_path(raw_value: str | None, project_root: Path) -> Path | None:
    if raw_value is None or not raw_value.strip():
        return None
    candidate = Path(raw_value.strip()).expanduser()
    if not candidate.is_absolute():
        candidate = (project_root / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return candidate


def _resolve_config_dir(project_root: Path) -> Path:
    explicit = _resolve_path(os.getenv("APP_CONFIG_DIR"), project_root)
    if explicit is not None:
        return explicit
    return (_resolve_app_data_dir(project_root) / "config").resolve()


def _resolve_app_data_dir(project_root: Path) -> Path:
    return (_resolve_path(os.getenv("APP_DATA_DIR"), project_root) or (project_root / "app_data")).resolve()


def _resolve_env_file_path(project_root: Path) -> Path:
    explicit_env_file = _resolve_path(os.getenv("APP_ENV_FILE"), project_root)
    if explicit_env_file is not None:
        return explicit_env_file
    config_dir = _resolve_config_dir(project_root)
    primary = (config_dir / ".env").resolve()
    if primary.exists():
        return primary
    legacy = (project_root / ".env").resolve()
    if legacy.exists():
        primary.parent.mkdir(parents=True, exist_ok=True)
        primary.write_text(legacy.read_text(encoding="utf-8"), encoding="utf-8")
        return primary
    return primary


def _load_dotenv() -> None:
    dotenv_path = _resolve_env_file_path(_project_root())
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


@dataclass(frozen=True)
class Settings:
    project_root: Path
    config_dir: Path
    env_file_path: Path
    static_dir: Path
    app_data_dir: Path
    database_path: Path
    uploads_dir: Path
    imported_media_dir: Path
    logs_dir: Path
    backups_dir: Path
    database_url: str
    instance_name: str
    app_base_url: str
    webhook_logging_enabled: bool
    webhook_logging_endpoint: str
    webhook_logging_bearer_token: str
    webhook_logging_timeout_seconds: int
    webhook_logging_retry_count: int
    webhook_logging_min_severity: str
    discord_notification_enabled: bool
    discord_notification_webhook_url: str
    discord_notification_username: str
    discord_notification_min_severity: str
    auth_oidc_enabled: bool
    auth_oidc_issuer_url: str
    auth_oidc_client_id: str
    auth_oidc_client_secret: str
    auth_oidc_scope: str
    auth_oidc_groups_claim: str
    auth_oidc_username_claim: str
    auth_oidc_admin_groups: str
    auth_oidc_user_groups: str
    auth_session_secret: str
    instagram_webhooks_enabled: bool
    instagram_webhook_verify_token: str
    instagram_app_secret: str
    scheduler_automation_interval_seconds: int
    app_port: int


_load_dotenv()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    project_root = _project_root()
    app_data_dir = _resolve_app_data_dir(project_root)
    config_dir = _resolve_config_dir(project_root)
    env_file_path = _resolve_env_file_path(project_root)
    static_dir = project_root / "app" / "static"
    database_path = app_data_dir / "crossposter.db"
    uploads_dir = app_data_dir / "uploads"
    imported_media_dir = app_data_dir / "imported_media"
    logs_dir = app_data_dir / "logs"
    backups_dir = app_data_dir / "backups"

    return Settings(
        project_root=project_root,
        config_dir=config_dir,
        env_file_path=env_file_path,
        static_dir=static_dir,
        app_data_dir=app_data_dir,
        database_path=database_path,
        uploads_dir=uploads_dir,
        imported_media_dir=imported_media_dir,
        logs_dir=logs_dir,
        backups_dir=backups_dir,
        database_url=f"sqlite:///{database_path}",
        instance_name=_env_str("APP_INSTANCE_NAME", socket.gethostname()),
        app_base_url=_env_str("APP_BASE_URL", ""),
        webhook_logging_enabled=_env_bool("WEBHOOK_LOGGING_ENABLED", _env_bool("ERROR_ALERTS_ENABLED", False)),
        webhook_logging_endpoint=_env_str("WEBHOOK_LOGGING_ENDPOINT", _env_str("ERROR_ALERTS_ENDPOINT", "")),
        webhook_logging_bearer_token=_env_str(
            "WEBHOOK_LOGGING_BEARER_TOKEN",
            _env_str("ERROR_ALERTS_BEARER_TOKEN", ""),
        ),
        webhook_logging_timeout_seconds=_env_int(
            "WEBHOOK_LOGGING_TIMEOUT_SECONDS",
            _env_int("ERROR_ALERTS_TIMEOUT_SECONDS", 10),
        ),
        webhook_logging_retry_count=_env_int(
            "WEBHOOK_LOGGING_RETRY_COUNT",
            _env_int("ERROR_ALERTS_RETRY_COUNT", 2),
        ),
        webhook_logging_min_severity=_env_str("WEBHOOK_LOGGING_MIN_SEVERITY", "warning").lower() or "warning",
        discord_notification_enabled=_env_bool("DISCORD_NOTIFICATION_WEBHOOK_ENABLED", False),
        discord_notification_webhook_url=_env_str("DISCORD_NOTIFICATION_WEBHOOK_URL", ""),
        discord_notification_username=_env_str("DISCORD_NOTIFICATION_WEBHOOK_USERNAME", "LynxPoster"),
        discord_notification_min_severity=_env_str("DISCORD_NOTIFICATION_MIN_SEVERITY", "warning").lower() or "warning",
        auth_oidc_enabled=_env_bool("AUTH_OIDC_ENABLED", False),
        auth_oidc_issuer_url=_env_str("AUTH_OIDC_ISSUER_URL", ""),
        auth_oidc_client_id=_env_str("AUTH_OIDC_CLIENT_ID", ""),
        auth_oidc_client_secret=_env_str("AUTH_OIDC_CLIENT_SECRET", ""),
        auth_oidc_scope=_env_str("AUTH_OIDC_SCOPE", "openid profile email"),
        auth_oidc_groups_claim=_env_str("AUTH_OIDC_GROUPS_CLAIM", "groups"),
        auth_oidc_username_claim=_env_str("AUTH_OIDC_USERNAME_CLAIM", "preferred_username"),
        auth_oidc_admin_groups=_env_str("AUTH_OIDC_ADMIN_GROUPS", ""),
        auth_oidc_user_groups=_env_str("AUTH_OIDC_USER_GROUPS", ""),
        auth_session_secret=_env_str("AUTH_SESSION_SECRET", "lynxposter-dev-session-secret"),
        instagram_webhooks_enabled=_env_bool("INSTAGRAM_WEBHOOKS_ENABLED", False),
        instagram_webhook_verify_token=_env_str("INSTAGRAM_WEBHOOK_VERIFY_TOKEN", ""),
        instagram_app_secret=_env_str("INSTAGRAM_APP_SECRET", ""),
        scheduler_automation_interval_seconds=_env_int("SCHEDULER_AUTORUN_INTERVAL_SECONDS", 300),
        app_port=_env_int("APP_PORT", 8000),
    )


def reload_settings() -> Settings:
    get_settings.cache_clear()
    return get_settings()
