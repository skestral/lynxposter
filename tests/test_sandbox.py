from __future__ import annotations

import warnings

from sqlalchemy.exc import SAWarning

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
