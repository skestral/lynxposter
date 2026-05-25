from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import app.main as main_module
from app.database import Base
from app.main import app
from app.models import CanonicalPost, MediaAttachment, Persona
from app.services.storage import public_instagram_media_filename


def test_instagram_media_route_serves_plain_public_image(monkeypatch, tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'media-route.db'}",
        future=True,
        connect_args={"check_same_thread": False},
    )
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, class_=Session)
    Base.metadata.create_all(engine)

    imported_dir = tmp_path / "imported_media"
    imported_dir.mkdir()
    image_path = imported_dir / "photo.jpg"
    image_path.write_bytes(b"\xff\xd8\xffjpeg")

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
        "app.main.settings",
        replace(main_module.settings, imported_media_dir=imported_dir),
    )

    try:
        with SessionLocal() as session:
            persona = Persona(
                name="Media",
                slug="media-route",
                is_enabled=True,
                timezone="UTC",
                settings_json={},
                retry_settings_json={"max_retries": 3},
                throttle_settings_json={"max_per_hour": 0, "overflow_posts": "retry"},
            )
            session.add(persona)
            session.flush()
            post = CanonicalPost(
                persona_id=persona.id,
                origin_kind="manual",
                body="Image",
                status="draft",
                publish_overrides_json={},
                metadata_json={},
            )
            session.add(post)
            session.flush()
            attachment = MediaAttachment(
                post_id=post.id,
                storage_path=str(image_path),
                mime_type="image/jpeg",
                size_bytes=image_path.stat().st_size,
                checksum="checksum",
                sort_order=0,
            )
            session.add(attachment)
            session.flush()
            attachment_id = attachment.id
            filename = public_instagram_media_filename(attachment.storage_path)
            session.commit()

        with TestClient(app) as client:
            response = client.get(f"/media/instagram/{attachment_id}/{filename}")
            robots = client.get("/robots.txt")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("image/jpeg")
        assert response.headers["cache-control"] == "public, max-age=3600"
        assert "content-disposition" not in response.headers
        assert response.content == b"\xff\xd8\xffjpeg"
        assert robots.status_code == 200
        assert "Allow: /media/instagram/" in robots.text
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()
