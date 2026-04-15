from __future__ import annotations

import shutil
from datetime import datetime, timezone

from sqlalchemy import inspect

from app.config import get_settings
from app.database import db_session, engine, has_table, init_db
from app.models import Persona
from app.services.importer import import_legacy_install
from app.services.storage import ensure_storage_dirs
from app.services.users import ensure_local_admin_user


settings = get_settings()


def _backup_incompatible_database() -> None:
    if not settings.database_path.exists():
        return
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    target = settings.backups_dir / f"{settings.database_path.stem}-pre-persona-refactor-{timestamp}{settings.database_path.suffix}"
    shutil.copyfile(settings.database_path, target)


def _reset_incompatible_database() -> None:
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    if not tables:
        return
    with engine.begin() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        for table in tables:
            safe_table = table.replace('"', '""')
            connection.exec_driver_sql(f'DROP TABLE IF EXISTS "{safe_table}"')
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")


def _apply_additive_migrations() -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    with engine.begin() as connection:
        if "users" in tables:
            user_columns = {column["name"] for column in inspector.get_columns("users")}
            if "is_enabled" not in user_columns:
                connection.exec_driver_sql('ALTER TABLE "users" ADD COLUMN is_enabled BOOLEAN NOT NULL DEFAULT 1')
            if "ui_theme" not in user_columns:
                connection.exec_driver_sql('ALTER TABLE "users" ADD COLUMN ui_theme VARCHAR(32) NOT NULL DEFAULT "skylight"')
            if "ui_mode" not in user_columns:
                connection.exec_driver_sql('ALTER TABLE "users" ADD COLUMN ui_mode VARCHAR(16) NOT NULL DEFAULT "light"')
            if "preferred_name" not in user_columns:
                connection.exec_driver_sql('ALTER TABLE "users" ADD COLUMN preferred_name VARCHAR(255)')
            connection.exec_driver_sql('CREATE INDEX IF NOT EXISTS "ix_users_is_enabled" ON "users" ("is_enabled")')
        if "personas" in tables:
            persona_columns = {column["name"] for column in inspector.get_columns("personas")}
            if "owner_user_id" not in persona_columns:
                connection.exec_driver_sql('ALTER TABLE "personas" ADD COLUMN owner_user_id VARCHAR(36)')
            connection.exec_driver_sql('CREATE INDEX IF NOT EXISTS "ix_personas_owner_user_id" ON "personas" ("owner_user_id")')
        if "canonical_posts" in tables:
            canonical_post_columns = {column["name"] for column in inspector.get_columns("canonical_posts")}
            if "post_type" not in canonical_post_columns:
                connection.exec_driver_sql('ALTER TABLE "canonical_posts" ADD COLUMN post_type VARCHAR(32) NOT NULL DEFAULT "standard"')
            connection.exec_driver_sql('CREATE INDEX IF NOT EXISTS "ix_canonical_posts_post_type" ON "canonical_posts" ("post_type")')


def bootstrap() -> None:
    ensure_storage_dirs()
    if settings.database_path.exists() and not has_table("personas"):
        _backup_incompatible_database()
        _reset_incompatible_database()
    init_db()
    _apply_additive_migrations()
    with db_session() as session:
        if not session.query(Persona).first():
            import_legacy_install(session)
        if not get_settings().auth_oidc_enabled:
            ensure_local_admin_user(session)
