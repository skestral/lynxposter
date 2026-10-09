from __future__ import annotations

import warnings

import pytest
from sqlalchemy.exc import SAWarning

from app.models import CanonicalPost, DeliveryJob, MediaAttachment
from app.schemas import SandboxPreviewRequest
from app.services.personas import create_account, create_persona
from app.services.sandbox import build_sandbox_preview


def _create_persona(session, *, slug: str = "sandbox-persona"):
    return create_persona(
        session,
        {
            "name": "Sandbox Persona",
            "slug": slug,
            "is_enabled": True,
            "timezone": "server",
            "settings_json": {},
            "retry_settings_json": {"max_retries": 3},
            "throttle_settings_json": {"max_per_hour": 0, "overflow_posts": "retry"},
        },
    )


def _create_account(session, persona, *, service: str, label: str, credentials: dict):
    return create_account(
        session,
        persona,
        {
            "service": service,
            "label": label,
            "handle_or_identifier": label,
            "is_enabled": True,
            "source_enabled": False,
            "destination_enabled": True,
            "credentials_json": credentials,
            "source_settings_json": {},
            "publish_settings_json": {},
        },
    )


def test_sandbox_preview_builds_service_specific_payloads(session):
    persona = _create_persona(session)
    mastodon = _create_account(
        session,
        persona,
        service="mastodon",
        label="Mastodon",
        credentials={"instance": "https://example.social", "token": "secret", "handle": "@me@example.social"},
    )
    discord = _create_account(
        session,
        persona,
        service="discord",
        label="Discord",
        credentials={"webhook_url": "https://discord.test/webhook"},
    )

    result = build_sandbox_preview(
        session,
        SandboxPreviewRequest.model_validate(
            {
                "persona_id": persona.id,
                "body": "Hello world",
                "target_account_ids": [mastodon.id, discord.id],
                "publish_overrides_json": {mastodon.id: {"body": "Hello Mastodon"}},
                "metadata_json": {"link": "https://example.com/source", "visibility": "unlisted"},
                "attachment_inputs": [
                    {
                        "filename": "photo.jpg",
                        "mime_type": "image/jpeg",
                        "size_bytes": 1234,
                        "sort_order": 0,
                    }
                ],
                "expectations": {
                    "expected_target_count": 2,
                    "body_must_contain": "Hello",
                    "expected_attachment_count": 1,
                    "require_media": True,
                },
            }
        ),
    )

    assert result.overall_valid is True
    assert result.overall_expectations_passed is True
    assert result.target_count == 2

    mastodon_preview = next(preview for preview in result.previews if preview.service == "mastodon")
    discord_preview = next(preview for preview in result.previews if preview.service == "discord")

    assert mastodon_preview.request_shape["status"] == "Hello Mastodon"
    assert mastodon_preview.request_shape["visibility"] == "unlisted"
    assert mastodon_preview.request_shape["media_ids"] == ["<uploaded-media-1>"]

    assert discord_preview.request_shape["content"] == "Hello world\nSource: https://example.com/source"
    assert discord_preview.publish_ready is True


def test_sandbox_preview_surfaces_validation_and_configuration_issues(session):
    persona = _create_persona(session, slug="sandbox-persona-invalid")
    twitter = _create_account(
        session,
        persona,
        service="twitter",
        label="Twitter",
        credentials={"app_key": "only-one-key"},
    )

    result = build_sandbox_preview(
        session,
        SandboxPreviewRequest.model_validate(
            {
                "persona_id": persona.id,
                "target_account_ids": [twitter.id],
                "body": "x" * 281,
                "expectations": {"max_body_length": 280},
            }
        ),
    )

    preview = result.previews[0]
    messages = [issue.message for issue in preview.validation_issues]

    assert result.overall_valid is False
    assert result.overall_expectations_passed is False
    assert preview.publish_ready is False
    assert any("280 characters" in message for message in messages)
    assert any("credentials are incomplete" in message for message in messages)


def test_sandbox_preview_does_not_emit_persona_relationship_warning_on_commit(session):
    persona = _create_persona(session, slug="sandbox-persona-warning")
    discord = _create_account(
        session,
        persona,
        service="discord",
        label="Discord",
        credentials={"webhook_url": "https://discord.test/webhook"},
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", SAWarning)
        build_sandbox_preview(
            session,
            SandboxPreviewRequest.model_validate(
                {
                    "persona_id": persona.id,
                    "target_account_ids": [discord.id],
                    "body": "warning guard",
                }
            ),
        )
        session.commit()


def test_saved_sandbox_seed_preserves_destination_media_selection_and_expectations(session):
    from app.main import _sandbox_seed_from_post

    persona = _create_persona(session)
    selected = _create_account(
        session, persona, service="discord", label="Selected media",
        credentials={"webhook_url": "https://discord.test/selected"},
    )
    text_only = _create_account(
        session, persona, service="mastodon", label="Text only",
        credentials={"instance": "https://example.social", "token": "preview-only", "handle": "@preview@example.social"},
    )
    post = CanonicalPost(
        persona_id=persona.id, origin_kind="composer", status="draft", body="Saved post",
        publish_overrides_json={
            selected.id: {"body": "Selected destination", "attachment_ids": ["saved-9", "saved-4"]},
            "discord": {"attachment_ids": ["saved-1"]},
            "mastodon": {"attachment_ids": []},
        },
        metadata_json={},
        attachments=[
            MediaAttachment(
                id=f"saved-{index}", storage_path=f"missing/photo-{index}.jpg", mime_type="image/jpeg",
                alt_text=f"Photo {index}", size_bytes=8_000_000, checksum="unused", sort_order=index,
            )
            for index in range(10)
        ],
        delivery_jobs=[DeliveryJob(target_account_id=account.id, status="draft") for account in (selected, text_only)],
    )
    session.add(post)
    session.commit()

    seed = _sandbox_seed_from_post(post)
    assert [attachment["id"] for attachment in seed["attachment_inputs"]] == [f"saved-{index}" for index in range(10)]
    request = SandboxPreviewRequest.model_validate({
        **seed, "expectations": {"expected_attachment_count": 2, "require_media": True},
    })
    result = build_sandbox_preview(session, request)
    previews = {preview.account_id: preview for preview in result.previews}

    assert result.attachment_count == 10
    assert result.overall_valid is True
    assert result.overall_expectations_passed is False
    assert previews[selected.id].rendered_body == "Selected destination"
    assert previews[selected.id].attachment_count == 2
    assert [file["filename"] for file in previews[selected.id].request_shape["files"]] == ["photo-9.jpg", "photo-4.jpg"]
    assert all(check.passed for check in previews[selected.id].expectation_checks)
    assert previews[text_only.id].attachment_count == 0
    assert previews[text_only.id].request_shape["media_ids"] == []
    assert [(check.key, check.actual, check.passed) for check in previews[text_only.id].expectation_checks] == [
        ("expected_attachment_count", 0, False), ("require_media", False, False),
    ]
    assert any("do not read or prepare media" in note for note in previews[selected.id].notes)
    assert len(post.attachments) == 10
    assert post.publish_overrides_json[selected.id]["attachment_ids"] == ["saved-9", "saved-4"]


@pytest.mark.parametrize("image_count", [5, 10])
def test_sandbox_bluesky_gallery_keeps_all_images_and_defers_original_size_preparation(session, image_count):
    persona = _create_persona(session)
    account = _create_account(
        session, persona, service="bluesky", label="Bluesky",
        credentials={"handle": "preview.test", "password": "preview-only"},
    )
    request = SandboxPreviewRequest.model_validate({
        "persona_id": persona.id,
        "body": "Gallery",
        "target_account_ids": [account.id],
        "attachment_inputs": [
            {
                "filename": f"photo-{index}.jpg", "mime_type": "image/jpeg", "size_bytes": 8_000_000,
                "sort_order": index,
            }
            for index in range(image_count)
        ],
        "expectations": {"expected_attachment_count": image_count, "require_media": True},
    })

    result = build_sandbox_preview(session, request)
    preview = result.previews[0]

    assert result.overall_valid is True
    assert result.overall_expectations_passed is True
    assert preview.attachment_count == image_count
    assert preview.action == "send_gallery"
    assert preview.request_shape["embed_type"] == "app.bsky.embed.gallery"
    assert len(preview.request_shape["images"]) == image_count
    assert len(preview.request_shape["embed"]["items"]) == image_count
