from __future__ import annotations

from contextlib import contextmanager

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.main import app
from app.schemas import ScheduledPostCreate
from app.services.auth import Principal
from app.services.personas import create_account, create_persona
from app.services.posts import create_scheduled_post


def _create_persona(session: Session, *, name: str, slug: str, owner_user_id: str) -> object:
    return create_persona(
        session,
        {
            "name": name,
            "slug": slug,
            "owner_user_id": owner_user_id,
            "is_enabled": True,
            "timezone": "UTC",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def _create_destination_account(session: Session, persona) -> object:
    return create_account(
        session,
        persona,
        {
            "service": "mastodon",
            "label": "Mastodon",
            "handle_or_identifier": "@me@example.social",
            "is_enabled": True,
            "source_enabled": False,
            "destination_enabled": True,
            "credentials_json": {
                "instance": "https://example.social",
                "token": "secret",
                "handle": "@me@example.social",
            },
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


def test_admin_routes_only_expose_owned_personas_and_posts(monkeypatch, tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'user-isolation.db'}",
        future=True,
        connect_args={"check_same_thread": False},
    )
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, class_=Session)
    Base.metadata.create_all(engine)

    @contextmanager
    def _db_session_override():
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr("app.main.db_session", _db_session_override)
    monkeypatch.setattr("app.main.bootstrap", lambda: None)
    monkeypatch.setattr("app.main.CrossposterScheduler.start", lambda self: None)
    monkeypatch.setattr("app.main.CrossposterScheduler.stop", lambda self: None)
    monkeypatch.setattr(
        "app.main.build_principal_from_request",
        lambda request: Principal(
            user_id="admin-user",
            display_name="Admin",
            role="admin",
            timezone="UTC",
            is_authenticated=True,
        ),
    )

    try:
        with SessionLocal() as session:
            owned_persona = _create_persona(
                session,
                name="Owned Persona",
                slug="owned-persona",
                owner_user_id="admin-user",
            )
            foreign_persona = _create_persona(
                session,
                name="Foreign Persona",
                slug="foreign-persona",
                owner_user_id="other-user",
            )

            owned_account = _create_destination_account(session, owned_persona)
            foreign_account = _create_destination_account(session, foreign_persona)

            create_scheduled_post(
                session,
                ScheduledPostCreate.model_validate(
                    {
                        "persona_id": owned_persona.id,
                        "body": "Owned draft",
                        "status": "draft",
                        "target_account_ids": [owned_account.id],
                        "publish_overrides_json": {},
                        "metadata_json": {},
                        "scheduled_for": None,
                    }
                ),
                [],
            )
            create_scheduled_post(
                session,
                ScheduledPostCreate.model_validate(
                    {
                        "persona_id": foreign_persona.id,
                        "body": "Foreign draft",
                        "status": "draft",
                        "target_account_ids": [foreign_account.id],
                        "publish_overrides_json": {},
                        "metadata_json": {},
                        "scheduled_for": None,
                    }
                ),
                [],
            )
            session.commit()

        with TestClient(app) as client:
            personas_response = client.get("/personas")
            posts_response = client.get("/scheduled-posts")
            page_response = client.get("/personas/page")

        assert personas_response.status_code == 200
        assert personas_response.json() == [
            {
                "id": owned_persona.id,
                "name": "Owned Persona",
                "slug": "owned-persona",
                "owner_user_id": "admin-user",
                "is_enabled": True,
                "timezone": "UTC",
                "settings_json": {},
                "retry_settings_json": {"max_retries": 3},
                "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
                "created_at": personas_response.json()[0]["created_at"],
                "updated_at": personas_response.json()[0]["updated_at"],
            }
        ]

        assert posts_response.status_code == 200
        assert [post["body"] for post in posts_response.json()] == ["Owned draft"]

        assert page_response.status_code == 200
        assert "Owned Persona" in page_response.text
        assert "Foreign Persona" not in page_response.text
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()
