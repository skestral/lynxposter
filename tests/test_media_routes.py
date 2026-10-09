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
    uploads_dir = tmp_path / "uploads"

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
        replace(main_module.settings, imported_media_dir=imported_dir, uploads_dir=uploads_dir),
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

        staged_path = uploads_dir / ".instagram-prepared" / "instagram-1234abcd" / attachment_id / "image.jpg"
        staged_path.parent.mkdir(parents=True)
        staged_path.write_bytes(b"\xff\xd8\xffprepared-jpeg")
        staged_url = f"/media/instagram/{attachment_id}/prepared/instagram-1234abcd/image.jpg"
        with TestClient(app) as client:
            response = client.get(f"/media/instagram/{attachment_id}/{filename}")
            head_response = client.head(f"/media/instagram/{attachment_id}/{filename}")
            attachment_head_response = client.head(f"/media/attachments/{attachment_id}")
            robots = client.get("/robots.txt")
            staged_response = client.get(staged_url)
            staged_head = client.head(staged_url)
            wrong_token = client.get(f"/media/instagram/{attachment_id}/prepared/instagram-87654321/image.jpg")
            wrong_filename = client.get(f"/media/instagram/{attachment_id}/prepared/instagram-1234abcd/secret.txt")
            wrong_attachment = client.get("/media/instagram/missing/prepared/instagram-1234abcd/image.jpg")
            renamed_image = image_path.with_name("renamed-photo.jpg")
            image_path.rename(renamed_image)
            stale_original_stage = client.get(staged_url)
            renamed_image.rename(image_path)
            staged_path.unlink()
            removed_stage = client.get(staged_url)

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("image/jpeg")
        assert response.headers["cache-control"] == "public, max-age=3600"
        assert "content-disposition" not in response.headers
        assert response.content == b"\xff\xd8\xffjpeg"
        assert head_response.status_code == 200
        assert head_response.headers["content-type"].startswith("image/jpeg")
        assert head_response.headers["content-length"] == str(image_path.stat().st_size)
        assert head_response.headers["cache-control"] == "public, max-age=3600"
        assert "content-disposition" not in head_response.headers
        assert head_response.content == b""
        assert attachment_head_response.status_code == 200
        assert attachment_head_response.headers["content-type"].startswith("image/jpeg")
        assert attachment_head_response.headers["content-length"] == str(image_path.stat().st_size)
        assert robots.status_code == 200
        assert "Allow: /media/instagram/" in robots.text
        assert staged_response.status_code == 200
        assert staged_response.content == b"\xff\xd8\xffprepared-jpeg"
        assert staged_response.headers["cache-control"] == "no-store"
        assert staged_response.headers["content-type"].startswith("image/jpeg")
        assert staged_head.status_code == 200
        assert staged_head.headers["content-length"] == str(len(staged_response.content))
        assert stale_original_stage.status_code == 200
        assert all(response.status_code == 404 for response in (wrong_token, wrong_filename, wrong_attachment, removed_stage))
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()
