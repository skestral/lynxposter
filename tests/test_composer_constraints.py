from __future__ import annotations

import json
import re
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from app.adapters import get_service_definition, service_composer_constraints_context
from app.adapters.bluesky import BLUESKY_IMAGE_LIMITS, BLUESKY_MAX_IMAGES, BLUESKY_VIDEO_LIMITS
from app.main import templates
from app.models import Account, CanonicalPost, MediaAttachment, Persona


def _template_context():
    principal = SimpleNamespace(
        is_authenticated=True, is_user=True, is_admin=False, user_id="test-user",
        display_name="Test user", role="user", timezone="UTC", ui_theme="skylight", ui_mode="light",
    )
    request = Request({"type": "http", "headers": [], "path": "/", "scheme": "http", "server": ("testserver", 80)})
    request.state.principal = principal
    persona = SimpleNamespace(id="persona-1", name="Test persona")
    return {
        "request": request, "current_principal": principal, "auth_enabled": False,
        "personas": [persona], "persona_targets": {persona.id: []}, "accounts": [],
        "persona": persona,
        "post": SimpleNamespace(
            id="post-1", body="", status="draft", display_status="draft", post_type="standard",
            giveaway=None, target_account_ids=[], scheduled_for=None, last_error=None,
            metadata_json={}, publish_overrides_json={}, deliveries={}, attachments=[],
            delivery_breakdown=SimpleNamespace(succeeded=[], failed=[], cancelled=[], pending=[]),
        ),
        "service_post_guidance": service_composer_constraints_context(),
    }


@pytest.mark.parametrize("template_name", ["scheduled_post_create.html", "scheduled_post_detail.html"])
def test_composers_receive_current_bluesky_gallery_and_unicode_guidance(template_name):
    html = templates.env.get_template(template_name).render(**_template_context())
    guidance = json.loads(re.search(r"const servicePostGuidance = (.*);", html).group(1))["bluesky"]

    assert guidance["max_attachments"] == guidance["max_images"] == BLUESKY_MAX_IMAGES == 10
    assert guidance["max_videos"] == 1
    assert guidance["single_video_only"] is True
    assert guidance["prepared_image_max_bytes"] == BLUESKY_IMAGE_LIMITS.max_bytes == 2_000_000
    assert guidance["prepared_video_max_bytes"] == BLUESKY_VIDEO_LIMITS.max_bytes == 300_000_000
    assert guidance["video_max_duration_seconds"] == BLUESKY_VIDEO_LIMITS.max_duration == 600
    assert guidance["body_limit"] == 300
    assert guidance["body_length_unit"] == "graphemes"
    assert guidance["max_body_bytes"] == 3000
    assert any("gallery" in note for note in guidance["notes"])
    assert any(
        f"{BLUESKY_VIDEO_LIMITS.max_bytes // 1_000_000} MB" in note
        and f"{int(BLUESKY_VIDEO_LIMITS.max_duration)} seconds" in note
        for note in guidance["notes"]
    )


def test_ten_image_composer_limit_matches_bluesky_adapter_validation():
    account = Account(id="bluesky-1", service="bluesky", credentials_json={})
    post = CanonicalPost(body="Gallery", publish_overrides_json={}, attachments=[
        MediaAttachment(storage_path=f"image-{index}.png", mime_type="image/png", alt_text="", sort_order=index)
        for index in range(10)
    ])
    adapter = get_service_definition("bluesky").destination_adapter_cls()

    assert adapter.validate(post, Persona(settings_json={}), account) == []
    post.attachments.append(MediaAttachment(storage_path="image-11.png", mime_type="image/png", sort_order=10))
    assert [issue.field for issue in adapter.validate(post, Persona(settings_json={}), account)] == ["media"]


def test_composer_media_gates_allow_prepared_instagram_images_and_telegram_groups():
    guidance = service_composer_constraints_context()

    # PNG/WebP originals can become Instagram JPEGs during preparation.
    assert guidance["instagram"]["allowed_image_mime_types"] == []
    assert guidance["instagram"]["supported_attachment_kinds"] == ["image", "video"]
    # Telegram supports video groups and mixed groups prepared as documents.
    assert guidance["telegram"]["multi_attachment_images_only"] is False
    assert guidance["telegram"]["max_attachments"] == 10


def test_existing_services_keep_default_text_counting_guidance():
    guidance = service_composer_constraints_context()

    assert guidance["mastodon"]["body_length_unit"] == "utf16"
    assert guidance["mastodon"]["max_body_bytes"] is None
