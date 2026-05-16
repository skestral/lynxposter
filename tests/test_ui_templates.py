from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

from starlette.requests import Request

from app.adapters import get_service_definition, iter_service_definitions, service_composer_constraints_context
from app.main import templates
from app.schemas import AccountRead


def _request_with_principal(timezone_name: str = "UTC") -> Request:
    request = Request({"type": "http", "headers": [], "path": "/", "scheme": "http", "server": ("testserver", 80)})
    request.state.principal = SimpleNamespace(
        is_authenticated=True,
        is_user=True,
        is_admin=False,
        user_id="user-1",
        display_name="Lynx",
        role="user",
        timezone=timezone_name,
        ui_theme="skylight",
        ui_mode="light",
    )
    return request


def _account_read(account_id: str, service: str, label: str) -> AccountRead:
    timestamp = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
    return AccountRead(
        id=account_id,
        persona_id="persona-1",
        service=service,
        label=label,
        handle_or_identifier="",
        is_enabled=True,
        source_enabled=False,
        destination_enabled=True,
        credentials_json={},
        source_settings_json={},
        publish_settings_json={},
        last_health_status=None,
        last_error=None,
        source_supported=True,
        destination_supported=True,
        configured=True,
        created_at=timestamp,
        updated_at=timestamp,
    )


def test_telegram_service_definition_exposes_destination_credentials():
    definition = get_service_definition("telegram")
    fields = {field.name: field for field in definition.credential_fields}

    assert definition.destination_supported is True
    assert fields["bot_token"].input_type == "password"
    assert fields["channel_id"].label == "Channel ID"


def test_instagram_service_definition_exposes_destination_credentials():
    definition = get_service_definition("instagram")
    fields = {field.name: field for field in definition.credential_fields}

    assert definition.destination_supported is True
    assert fields["api_key"].input_type == "password"
    assert fields["api_key"].label == "Graph Access Token"
    assert fields["instagrapi_password"].input_type == "password"
    assert fields["instagrapi_sessionid"].label == "Session ID"


def test_mastodon_and_twitter_service_definitions_expose_language_publish_fields():
    mastodon = get_service_definition("mastodon")
    twitter = get_service_definition("twitter")

    mastodon_fields = {field.name: field for field in mastodon.publish_setting_fields}
    twitter_fields = {field.name: field for field in twitter.publish_setting_fields}

    assert mastodon_fields["language"].label == "Default Language"
    assert mastodon_fields["language"].fallback_keys == ("mastodon_lang",)
    assert twitter_fields["language"].label == "Tweet Language"
    assert twitter_fields["language"].fallback_keys == ("twitter_lang",)


def test_persona_detail_template_renders_telegram_controls_and_secret_toggles():
    html = templates.env.get_template("persona_detail.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=False,
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
        ),
        auth_enabled=False,
        persona=SimpleNamespace(
            id="persona-1",
            name="Savannah",
            slug="savannah",
            is_enabled=True,
            timezone="UTC",
            accounts=[],
            settings_json={},
            retry_settings_json={"max_retries": 3},
            throttle_settings_json={"max_per_hour": 0, "overflow_posts": "retry"},
        ),
        accounts=[],
        routes=[],
        routes_map={},
        source_accounts=[],
        destination_accounts=[],
        service_definitions=iter_service_definitions(),
    )

    assert 'option value="telegram"' in html
    assert 'data-field-name="bot_token"' in html
    assert 'data-field-name="channel_id"' in html
    assert "secret-toggle-button" in html
    assert "Channel ID" in html
    assert 'data-field-name="instagrapi_sessionid"' in html
    assert 'data-field-name="instagrapi_username"' in html
    assert 'id="persona-detail-tabs"' in html


def test_persona_detail_template_renders_instagram_token_tracking_controls():
    definition = get_service_definition("instagram")
    html = templates.env.get_template("persona_detail.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=False,
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
        ),
        auth_enabled=False,
        persona=SimpleNamespace(
            id="persona-1",
            name="Savannah",
            slug="savannah",
            is_enabled=True,
            timezone="UTC",
            accounts=[],
            settings_json={},
            retry_settings_json={"max_retries": 3},
            throttle_settings_json={"max_per_hour": 0, "overflow_posts": "retry"},
        ),
        accounts=[
            {
                "account": SimpleNamespace(
                    id="account-1",
                    service="instagram",
                    label="Instagram",
                    handle_or_identifier="@studio",
                    is_enabled=True,
                    source_enabled=True,
                    destination_enabled=True,
                    credentials_json={
                        "api_key": "secret",
                        "instagrapi_username": "larkyn.lynx",
                        "instagrapi_sessionid": "12345%3Aabcdef1234567890abcdef1234567890",
                    },
                    source_settings_json={},
                    publish_settings_json={},
                    last_error="Instagram login failed. Challenge required.",
                ),
                "account_read": SimpleNamespace(source_supported=True, destination_supported=True, configured=True),
                "definition": definition,
                "instagram_token_status": {
                    "token_present": True,
                    "tracking_enabled": True,
                    "badge_class": "text-bg-warning",
                    "badge_label": "5 days left",
                    "summary": "This token is approaching the end of Meta's typical long-lived access-token window.",
                    "recorded_at": None,
                    "estimated_expires_at": None,
                },
            }
        ],
        routes=[],
        routes_map={},
        source_accounts=[],
        destination_accounts=[],
        service_definitions=iter_service_definitions(),
    )

    assert "Token Lifespan" in html
    assert "Record Token Refresh" in html
    assert "instagram-token-refresh-button" in html
    assert "Check Login" in html
    assert "Last connection hiccup" in html
    assert "Instagram login failed. Challenge required." in html


def test_scheduled_post_templates_render_attachment_previews():
    detail_html = templates.env.get_template("scheduled_post_detail.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=False,
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
        ),
        auth_enabled=False,
        persona=SimpleNamespace(id="persona-1", name="Savannah"),
        accounts=[],
        post=SimpleNamespace(
            id="post-1",
            status="draft",
            post_type="standard",
            giveaway=None,
            display_status="partial_failure",
            target_account_ids=[],
            body="Hello",
            scheduled_for=None,
            last_error="Instagram session expired.",
            metadata_json={},
            publish_overrides_json={},
            deliveries={
                "account-1": SimpleNamespace(
                    label="Instagram",
                    status="failed",
                    last_error="Session ID expired.",
                )
            },
            delivery_breakdown=SimpleNamespace(
                succeeded=[
                    SimpleNamespace(
                        label="Mastodon",
                        external_url="https://example.social/@me/1",
                    )
                ],
                failed=[
                    SimpleNamespace(
                        label="Instagram",
                        last_error="Session ID expired.",
                    )
                ],
                cancelled=[],
                pending=[],
            ),
            attachments=[
                SimpleNamespace(
                    id="attachment-1",
                    mime_type="image/jpeg",
                    alt_text="Sample image",
                    size_bytes=1234,
                    storage_path="/tmp/sample.jpg",
                    sort_order=0,
                )
            ],
        ),
        service_post_guidance=service_composer_constraints_context(),
    )

    create_html = templates.env.get_template("scheduled_post_create.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=False,
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
        ),
        auth_enabled=False,
        personas=[SimpleNamespace(id="persona-1", name="Savannah")],
        persona_targets={"persona-1": []},
        service_post_guidance=service_composer_constraints_context(),
    )

    planner_html = templates.env.get_template("scheduled_posts.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=False,
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
        ),
        auth_enabled=False,
        personas=[],
        posts=[
            SimpleNamespace(
                id="post-1",
                status="failed",
                post_type="standard",
                giveaway=None,
                display_status="partial_failure",
                persona_id="persona-1",
                target_account_ids=[],
                attachments=[],
                scheduled_for=None,
                last_error="Instagram session expired.",
                delivery_breakdown=SimpleNamespace(
                    succeeded=[SimpleNamespace(label="Mastodon")],
                    failed=[SimpleNamespace(label="Instagram", last_error="Session ID expired.")],
                    cancelled=[],
                    pending=[],
                ),
            )
        ],
        persona_targets={},
        persona_name_by_id={"persona-1": "Savannah"},
        service_post_guidance=service_composer_constraints_context(),
    )

    assert '/media/attachments/attachment-1' in detail_html
    assert 'upload-preview-list' in create_html
    assert 'name="uploads"' in detail_html
    assert 'id="saved-attachment-list"' in detail_html
    assert 'Saved Media' in detail_html
    assert 'data-action="up"' in detail_html
    assert 'attachment_order' in detail_html
    assert 'deleted_attachment_ids' in detail_html
    assert 'Selected images and videos will travel with this post plan.' in create_html
    assert 'Post Snapshot' in detail_html
    assert 'Post Snapshot' in create_html
    assert 'id="compose-tabs"' in create_html
    assert 'preview-persona-name' in create_html
    assert 'Create Post Plan' in create_html
    assert 'id="scheduled-workspace-tabs"' in planner_html
    assert 'workspace-calendar-pane' in planner_html
    assert 'workspace-kanban-pane' in planner_html
    assert 'calendar-view-month' in planner_html
    assert 'calendar-view-week' in planner_html
    assert 'scheduled-calendar-grid' in planner_html
    assert 'scheduled-kanban-board' in planner_html
    assert 'kanban-lane-drafts' in planner_html
    assert 'kanban-lane-attention' in planner_html
    assert 'planner-move-modal' in planner_html
    assert 'document.body.appendChild(plannerMoveModalElement)' in planner_html
    assert 'function queueMoveModal(post, target)' in planner_html
    assert 'Create Post' in planner_html
    assert 'scheduledPostsPlannerData' in planner_html
    assert 'id="scheduled-detail-tabs"' in detail_html
    assert 'servicePostGuidance' in detail_html
    assert 'servicePostGuidance' in create_html
    assert "let giveawayBuilder = null;" in detail_html
    assert "let giveawayBuilder = null;" in create_html
    assert "if (giveawayBuilder) {" in detail_html
    assert "if (giveawayBuilder) {" in create_html
    assert 'Delete Draft' in detail_html
    assert 'Post Type' in detail_html
    assert 'Post Type' in create_html
    assert 'Last delivery hiccup' in detail_html
    assert 'Delivery Outcome' in detail_html
    assert 'Succeeded' in detail_html
    assert 'Failed' in detail_html
    assert 'Mastodon' in detail_html
    assert 'Instagram session expired.' in detail_html
    assert 'Session ID expired.' in detail_html
    assert 'Partial Failure' in detail_html
    assert 'Instagram session expired.' in planner_html
    assert 'Queue Now' in planner_html
    assert 'Drag cards between editable lanes' in planner_html


def test_scheduled_post_templates_render_generic_giveaway_controls():
    giveaway = {
        "status": "collecting",
        "giveaway_end_at": datetime(2026, 5, 15, 22, 30, tzinfo=timezone.utc),
        "pool_mode": "separate",
        "audit_summary": {"entrants": 2, "eligible": 0, "provisional": 2, "disqualified": 0},
        "channels": [
            {
                "service": "instagram",
                "account_id": "account-ig",
                "rules": {
                    "kind": "all",
                    "children": [
                        {"kind": "atom", "atom": "comment_present", "params": {}, "children": []},
                        {"kind": "atom", "atom": "story_mention_present", "params": {}, "children": []},
                    ],
                },
                "summary": {"entrants": 1, "eligible": 0, "provisional": 1, "disqualified": 0},
                "target_post_url": "https://instagram.test/p/abc123/",
                "entrants": [
                    {
                        "display_label": "entrant.one",
                        "provider_user_id": "user-1",
                        "signal_state": {"comment_count": 1, "friend_mention_count": 1, "story_mention_count": 1},
                        "eligibility_status": "provisional",
                        "inconclusive_reasons": ["Missing final live check."],
                        "disqualification_reasons": [],
                    }
                ],
            },
            {
                "service": "bluesky",
                "account_id": "account-bsky",
                "rules": {
                    "kind": "all",
                    "children": [
                        {"kind": "atom", "atom": "reply_or_quote_present", "params": {}, "children": []},
                        {"kind": "atom", "atom": "like_present", "params": {}, "children": []},
                    ],
                },
                "summary": {"entrants": 1, "eligible": 0, "provisional": 1, "disqualified": 0},
                "target_post_url": "https://bsky.app/profile/savannah.test/post/xyz",
                "entrants": [
                    {
                        "display_label": "bsky.one",
                        "provider_user_id": "did:plc:user-1",
                        "signal_state": {"reply_present": True, "like_present": True},
                        "eligibility_status": "provisional",
                        "inconclusive_reasons": [],
                        "disqualification_reasons": [],
                    }
                ],
            },
        ],
        "pools": [
            {
                "pool_key": "instagram",
                "label": "Instagram",
                "status": "review_required",
                "candidate_count": 1,
                "provisional_winner": {
                    "display_label": "entrant.one",
                    "provider_user_id": "user-1",
                    "inconclusive_reasons": ["Missing final live check."],
                },
                "final_winner": None,
                "selection_log": {
                    "selection_method": "system_random_shuffle",
                    "candidate_source": "provisional fallback",
                    "qualified_member_count": 0,
                    "candidate_count": 1,
                    "note": "No fully verified winner was available, so the first provisional candidate was held for manual review.",
                    "qualified_members": [],
                    "candidates": [
                        {
                            "rank": 1,
                            "selected": True,
                            "note": "Selected as the top provisional candidate pending review.",
                            "entrant": {
                                "display_label": "entrant.one",
                                "provider_user_id": "user-1",
                                "eligibility_status": "provisional",
                            },
                        }
                    ],
                },
            }
        ],
        "last_error": None,
    }
    html = templates.env.get_template("scheduled_post_detail.html").render(
        request=_request_with_principal("America/Los_Angeles"),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=False,
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
        ),
        auth_enabled=False,
        persona=SimpleNamespace(id="persona-1", name="Savannah"),
        accounts=[
            _account_read("account-ig", "instagram", "Instagram"),
            _account_read("account-bsky", "bluesky", "Bluesky"),
        ],
        post=SimpleNamespace(
            id="post-1",
            status="draft",
            post_type="giveaway",
            giveaway=giveaway,
            display_status="draft",
            target_account_ids=["account-ig", "account-bsky"],
            body="Giveaway body",
            scheduled_for=None,
            last_error=None,
            metadata_json={},
            publish_overrides_json={},
            deliveries={},
            delivery_breakdown=SimpleNamespace(succeeded=[], failed=[], cancelled=[], pending=[]),
            attachments=[],
        ),
        service_post_guidance=service_composer_constraints_context(),
    )

    assert "Giveaway Builder" in html
    assert "Instagram Channel" in html
    assert "Bluesky Channel" in html
    assert "Separate" in html
    assert 'id="detail-giveaway-details-tab"' in html
    assert 'id="detail-giveaway-details-pane"' in html
    assert "Activity Dashboard" in html
    assert '"giveaway_end_at": "2026-05-15T15:30"' in html
    assert "Timezone: America/Los_Angeles" in html
    assert "Entrant Audit Log" in html
    assert "Selection Log" in html
    assert "Confirm Winner" in html
    assert "Advance To Next Candidate" in html
    assert "End Giveaway" in html
    assert "/giveaway/end-now" in html
    assert "Open published Instagram post" in html
    assert "Open published Bluesky post" in html


def test_scheduled_posts_planner_renders_generic_giveaway_data():
    giveaway = {
        "status": "scheduled",
        "giveaway_end_at": datetime(2026, 5, 15, 20, 0, tzinfo=timezone.utc),
        "pool_mode": "combined",
        "channels": [
            {
                "service": "instagram",
                "account_id": "account-ig",
                "rules": {
                    "kind": "all",
                    "children": [
                        {"kind": "atom", "atom": "comment_present", "params": {}, "children": []},
                        {"kind": "atom", "atom": "story_mention_present", "params": {}, "children": []},
                    ],
                },
            },
            {
                "service": "bluesky",
                "account_id": "account-bsky",
                "rules": {
                    "kind": "all",
                    "children": [
                        {"kind": "atom", "atom": "reply_or_quote_present", "params": {}, "children": []},
                    ],
                },
            },
        ],
    }
    html = templates.env.get_template("scheduled_posts.html").render(
        request=_request_with_principal("America/Los_Angeles"),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=False,
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
        ),
        auth_enabled=False,
        personas=[],
        posts=[
            SimpleNamespace(
                id="post-1",
                status="draft",
                post_type="giveaway",
                giveaway=giveaway,
                display_status="draft",
                persona_id="persona-1",
                target_account_ids=["account-ig", "account-bsky"],
                attachments=[],
                body="Giveaway body",
                scheduled_for=datetime(2026, 5, 14, 20, 0, tzinfo=timezone.utc),
                last_error=None,
                delivery_breakdown=SimpleNamespace(succeeded=[], failed=[], cancelled=[], pending=[]),
            )
        ],
        persona_targets={},
        persona_name_by_id={"persona-1": "Savannah"},
        service_post_guidance=service_composer_constraints_context(),
    )

    assert "scheduledPostsPlannerData" in html
    assert 'displayStatus: "draft"' in html
    assert 'scheduledFor: "2026-05-14T13:00"' in html
    assert 'giveaway_end_at: "2026-05-15T13:00"' in html
    assert 'const plannerTimezoneName = "America/Los_Angeles";' in html
    assert "Timezone: America/Los_Angeles" in html
    assert "return Boolean(post.scheduledDate);" in html
    assert "including drafts that are still being refined on the board" in html
    assert "reply_or_quote_present" in html
    assert "pool_mode" in html
    assert "normalizeGiveawayConfig" in html


def test_dashboard_template_shows_recent_scheduled_post_errors():
    html = templates.env.get_template("dashboard.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=False,
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
        ),
        auth_enabled=False,
        personas=[],
        account_count=0,
        posts=[
            SimpleNamespace(
                persona_id="persona-1",
                status="failed",
                display_status="failure",
                scheduled_for=None,
                last_error="Instagram session expired.",
                delivery_breakdown=SimpleNamespace(
                    succeeded=[],
                    failed=[SimpleNamespace(label="Instagram", last_error="Instagram session expired.")],
                    cancelled=[],
                    pending=[],
                ),
            )
        ],
        persona_name_by_id={"persona-1": "Savannah"},
        run_groups=[
            {
                "run_id": "run-1",
                "trigger": "autorun",
                "summary_message": "Autorun automation cycle completed.",
                "severity": "error",
                "finished_at": None,
                "counts": {"personas": 2, "accounts": 3, "posts_found": 4, "reposted": 2, "errors": 1},
                "published_posts": [
                    SimpleNamespace(
                        post_id="post-1",
                        persona_name="Savannah",
                        post_preview="Launch update",
                        latest_at=None,
                        deliveries=[
                            SimpleNamespace(account_label="Instagram", service="instagram", external_url="https://instagram.example/p/1", external_id="ig-1"),
                        ],
                    )
                ],
                "system_events": [
                    SimpleNamespace(
                        created_at=None,
                        operation="automation_cycle",
                        severity="info",
                        message="Autorun automation cycle started.",
                    )
                ],
                "persona_summaries": [
                    SimpleNamespace(
                        persona_name="Savannah",
                        counts=SimpleNamespace(accounts=2, posts_found=4, reposted=1, errors=1),
                        events=[
                            SimpleNamespace(
                                created_at=None,
                                operation="poll",
                                severity="info",
                                account_label="Instagram",
                                service="instagram",
                                message="Imported 4 posts from Instagram",
                            )
                        ],
                    )
                ],
            }
        ],
        alert_events=[],
        scheduler_status=SimpleNamespace(
            automation_enabled=False,
            cycle_in_progress=False,
            autorun_interval_seconds=300,
            next_run_at=None,
            last_run_trigger=None,
            last_run_finished_at=None,
        ),
        giveaway_activity_monitor={
            "filters": {"persona_id": "", "service": "", "event_type": ""},
            "available_services": [
                {"value": "instagram", "label": "Instagram"},
                {"value": "bluesky", "label": "Bluesky"},
            ],
            "available_event_types": [
                {"value": "instagram_comment", "label": "Comment"},
                {"value": "bluesky_reply", "label": "Reply"},
            ],
            "metrics": {"campaigns": 1, "channels": 2, "entrants": 2, "activities": 2},
            "rollups": [
                {
                    "label": "Instagram",
                    "activity_count": 1,
                    "entrant_count": 1,
                    "campaign_count": 1,
                    "latest_activity_at": None,
                    "event_breakdown": [{"label": "Comment", "count": 1}],
                },
                {
                    "label": "Bluesky",
                    "activity_count": 1,
                    "entrant_count": 1,
                    "campaign_count": 1,
                    "latest_activity_at": None,
                    "event_breakdown": [{"label": "Reply", "count": 1}],
                },
            ],
            "recent_events": [
                {
                    "created_at": None,
                    "service_label": "Instagram",
                    "event_label": "Comment",
                    "account_label": "Instagram",
                    "campaign_status": "collecting",
                    "actor_label": "entrant.one",
                    "entrant_status": "pending",
                    "campaign_href": "/scheduled-posts/post-1/page",
                    "campaign_label": "Spring giveaway",
                    "persona_name": "Savannah",
                    "detail": "Count me in @friend",
                    "activity_href": "https://instagram.example/p/1",
                    "activity_href_label": "Open Instagram post",
                },
                {
                    "created_at": None,
                    "service_label": "Bluesky",
                    "event_label": "Reply",
                    "account_label": "Bluesky",
                    "campaign_status": "collecting",
                    "actor_label": "bsky.one",
                    "entrant_status": "pending",
                    "campaign_href": "/scheduled-posts/post-1/page",
                    "campaign_label": "Spring giveaway",
                    "persona_name": "Savannah",
                    "detail": "ready to join",
                    "activity_href": "https://bsky.app/profile/savannah.test/post/reply-1",
                    "activity_href_label": "Open reply",
                },
            ],
        },
        admin_mode=False,
        cleared_dashboard_alert_count=0,
        instagram_webhook_observability=None,
    )

    assert "Giveaway Activity Monitor" in html
    assert "Captured Activities" in html
    assert "ready to join" in html
    assert "Recent Post Plans" in html
    assert "Instagram session expired." in html
    assert "Failure" in html
    assert "delivery-summary-pill is-failure" in html
    assert "Recent Automation Runs" in html
    assert "Autorun Run" in html
    assert "Posted This Run" in html
    assert "Launch update" in html
    assert "Imported 4 posts from Instagram" in html


def test_dashboard_template_truncates_long_navbar_identity_text():
    html = templates.env.get_template("dashboard.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=True,
            user_id="user-1",
            display_name="Very Long Display Name For OIDC User Account",
            role="admin",
            timezone="UTC",
        ),
        auth_enabled=True,
        personas=[],
        account_count=0,
        posts=[],
        persona_name_by_id={},
        run_groups=[],
        alert_events=[],
        scheduler_status=SimpleNamespace(
            automation_enabled=False,
            cycle_in_progress=False,
            autorun_interval_seconds=300,
            next_run_at=None,
            last_run_trigger=None,
            last_run_finished_at=None,
        ),
        giveaway_activity_monitor={
            "filters": {"persona_id": "", "service": "", "event_type": ""},
            "available_services": [],
            "available_event_types": [],
            "metrics": {"campaigns": 0, "channels": 0, "entrants": 0, "activities": 0},
            "rollups": [],
            "recent_events": [],
        },
        admin_mode=True,
        cleared_dashboard_alert_count=0,
        instagram_webhook_observability=None,
    )

    assert "navbar-identity" in html
    assert 'title="Very Long Display Name For OIDC User Account | Admin"' in html
    assert 'aria-label="Light mode"' in html
    assert 'aria-label="Dark mode"' in html
    assert "topbar-theme-label" not in html
    assert "dashboard-live-update-status" in html
    assert "registerLiveUpdates" in html


def test_settings_template_renders_instagram_webhook_setup_guidance():
    html = templates.env.get_template("settings.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=True,
            user_id="user-1",
            display_name="Lynx",
            role="admin",
            timezone="UTC",
        ),
        auth_enabled=True,
        app_settings=SimpleNamespace(
            instance_name="LynxPoster",
            app_base_url="https://lynxposter.example.com",
            app_port=8000,
            instagram_webhooks_enabled=True,
            instagram_webhook_verify_token="verify-me",
            instagram_app_secret="secret",
            scheduler_automation_interval_seconds=300,
            webhook_logging_enabled=False,
            webhook_logging_endpoint="",
            webhook_logging_bearer_token="",
            webhook_logging_timeout_seconds=10,
            webhook_logging_retry_count=2,
            webhook_logging_min_severity="warning",
            discord_notification_enabled=False,
            discord_notification_webhook_url="",
            discord_notification_username="LynxPoster",
            discord_notification_min_severity="warning",
            auth_oidc_enabled=False,
            auth_oidc_issuer_url="",
            auth_oidc_client_id="",
            auth_oidc_client_secret="",
            auth_oidc_scope="openid profile email",
            auth_oidc_groups_claim="groups",
            auth_oidc_username_claim="preferred_username",
            auth_oidc_admin_groups="",
            auth_oidc_user_groups="",
            auth_session_secret="session-secret",
        ),
        scheduler_status=SimpleNamespace(
            scheduler_started=True,
            automation_enabled=True,
            next_run_at=None,
        ),
        saved=False,
        tested=False,
        error_message=None,
        oidc_warning_message=None,
        instagram_webhook_callback_url="https://lynxposter.example.com/webhooks/instagram",
        instagram_webhook_latest_received_at=None,
        instagram_webhook_latest_event_type=None,
        instagram_webhook_required_fields=["comments", "mentions", "likes", "shares"],
        instagram_tunnel_local_target="http://127.0.0.1:8000",
        instagram_tunnel_cloudflared_command="cloudflared tunnel --url http://127.0.0.1:8000",
        instagram_tunnel_ngrok_command="ngrok http 8000",
        instagram_tunnel_verify_probe='curl "https://lynxposter.example.com/webhooks/instagram?hub.mode=subscribe&hub.verify_token=verify-token&hub.challenge=test123"',
        instagram_webhook_logs_href="/logs/page#instagram-webhooks",
        instagram_webhook_dashboard_href="/#instagram-webhooks",
    )

    assert "Instagram Webhooks" in html
    assert "Webhook Callback URL" in html
    assert "/webhooks/instagram" in html
    assert "Verify Token" in html
    assert "Recommended Subscriptions" in html
    assert "comments, mentions, likes, shares" in html
    assert "Tunnel Helper" in html
    assert "cloudflared tunnel --url http://127.0.0.1:8000" in html
    assert "ngrok http 8000" in html
    assert "Verification Probe" in html
    assert "Open Raw Webhook Logs" in html
    assert 'id="settings-tabs"' in html


def test_account_settings_template_renders_theme_picker():
    html = templates.env.get_template("account_settings.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=False,
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
            ui_theme="skylight",
            ui_mode="dark",
        ),
        auth_enabled=True,
        user=SimpleNamespace(
            display_name="Lynx",
            preferred_name="Savannah",
            effective_display_name="Savannah",
            role="user",
            timezone="UTC",
            ui_theme="lagoon",
            ui_mode="dark",
            email="lynx@example.com",
            username="lynx",
            groups_json=["users"],
            last_login_at=None,
        ),
    )

    assert "Theme Presets" in html
    assert 'name="ui_theme"' in html
    assert "Lagoon" in html
    assert "Rainbow Pride" in html
    assert "Trans Pride" in html
    assert "app-mode-button" in html
    assert 'data-ui-mode="dark"' in html
    assert "theme-picker-grid" in html
    assert 'name="preferred_name"' in html
    assert 'value="Savannah"' in html


def test_logs_template_renders_grouped_worker_runs():
    html = templates.env.get_template("logs.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=False,
            user_id="user-1",
            display_name="Lynx",
            role="user",
            timezone="UTC",
        ),
        auth_enabled=False,
        filters=SimpleNamespace(persona_id=None, account_id=None, service=None, severity=None, operation=None, since=None),
        personas=[],
        accounts=[],
        run_groups=[
            {
                "run_id": "run-1",
                "trigger": "manual",
                "summary_message": "Manual automation cycle completed.",
                "severity": "warning",
                "finished_at": None,
                "counts": {"personas": 2, "accounts": 2, "posts_found": 3, "reposted": 1, "queued": 1, "errors": 1},
                "published_posts": [
                    SimpleNamespace(
                        post_id="post-44",
                        persona_name="Savannah",
                        post_preview="Crossposted launch note",
                        latest_at=None,
                        deliveries=[
                            SimpleNamespace(account_label="Discord", service="discord", external_url=None, external_id="discord-44"),
                        ],
                    )
                ],
                "system_events": [
                    SimpleNamespace(
                        created_at=None,
                        operation="automation_cycle",
                        severity="info",
                        message="Manual automation cycle started.",
                    )
                ],
                "persona_summaries": [
                    SimpleNamespace(
                        persona_name="Lynx",
                        counts=SimpleNamespace(accounts=1, posts_found=3, reposted=0, queued=0, errors=0),
                        events=[
                            SimpleNamespace(
                                created_at=None,
                                operation="poll",
                                severity="info",
                                account_label="Bluesky",
                                service="bluesky",
                                message="Imported 3 posts from Bluesky",
                            )
                        ],
                    ),
                    SimpleNamespace(
                        persona_name="Savannah",
                        counts=SimpleNamespace(accounts=1, posts_found=0, reposted=1, queued=1, errors=1),
                        events=[
                            SimpleNamespace(
                                created_at=None,
                                operation="publish",
                                severity="error",
                                account_label="Discord",
                                service="discord",
                                message="Webhook rejected the payload",
                            )
                        ],
                    ),
                ],
            }
        ],
        alert_events=[],
        cleared_count=None,
        instagram_webhook_observability=None,
    )

    assert "Automation Runs" in html
    assert "Manual Run" in html
    assert "Lynx" in html
    assert "Savannah" in html
    assert "Posted This Run" in html
    assert "Crossposted launch note" in html
    assert "discord-44" in html
    assert "Imported 3 posts from Bluesky" in html
    assert "Webhook rejected the payload" in html
    assert "logs-live-update-status" in html
    assert "registerLiveUpdates" in html


def test_logs_template_renders_instagram_webhook_observability_for_admin():
    html = templates.env.get_template("logs.html").render(
        request=_request_with_principal(),
        current_principal=SimpleNamespace(
            is_authenticated=True,
            is_user=True,
            is_admin=True,
            user_id="user-1",
            display_name="Lynx",
            role="admin",
            timezone="UTC",
        ),
        auth_enabled=False,
        filters=SimpleNamespace(persona_id=None, account_id=None, service=None, severity=None, operation=None, since=None),
        personas=[],
        accounts=[],
        run_groups=[],
        alert_events=[],
        cleared_count=None,
        instagram_webhook_observability={
            "window_days": 7,
            "total_events": 3,
            "matched_events": 1,
            "unmatched_events": 2,
            "giveaway_relevant_events": 2,
            "unique_fields": 3,
            "unique_actors": 2,
            "latest_received_at": None,
            "field_chart": [
                {"label": "Comments", "count": 1, "width_pct": 100},
                {"label": "Messages", "count": 1, "width_pct": 100},
            ],
            "daily_chart": [
                {"label": "Apr 14", "count": 0, "width_pct": 0},
                {"label": "Apr 15", "count": 3, "width_pct": 100},
            ],
            "recent_events": [
                {
                    "id": "event-1",
                    "created_at": None,
                    "field": "comments",
                    "field_label": "Comments",
                    "event_type": "comment",
                    "event_type_label": "Comment",
                    "matched": True,
                    "matched_giveaway_id": "giveaway-1",
                    "matched_post_id": "post-1",
                    "matched_account_id": "account-1",
                    "matched_state_label": "Matched to giveaway",
                    "provider_object_id": "media-1",
                    "provider_account_id": "account-1",
                    "provider_local_account": {
                        "account_id": "account-1",
                        "label": "Larkyn Lynx",
                        "handle_or_identifier": "larkyn.lynx",
                        "service": "instagram",
                        "persona_name": "Savannah",
                        "display_label": "Larkyn Lynx (larkyn.lynx)",
                        "profile_href": "https://www.instagram.com/larkyn.lynx/",
                    },
                    "matched_local_account": {
                        "account_id": "account-1",
                        "label": "Larkyn Lynx",
                        "handle_or_identifier": "larkyn.lynx",
                        "service": "instagram",
                        "persona_name": "Savannah",
                        "display_label": "Larkyn Lynx (larkyn.lynx)",
                        "profile_href": "https://www.instagram.com/larkyn.lynx/",
                    },
                    "actor_local_account": None,
                    "actor_profile": {
                        "id": "user-1",
                        "username": "entrant.one",
                        "name": "Entrant One",
                        "profile_href": "https://www.instagram.com/entrant.one/",
                        "profile_image_url": "https://cdn.test/entrant.jpg",
                    },
                    "actor_profile_href": "https://www.instagram.com/entrant.one/",
                    "actor_profile_image_url": "https://cdn.test/entrant.jpg",
                    "actor_id": "user-1",
                    "actor_username": "entrant.one",
                    "actor_label": "Entrant One (@entrant.one)",
                    "recipient_id": "account-1",
                    "recipient_username": None,
                    "recipient_local_account": {
                        "account_id": "account-1",
                        "label": "Larkyn Lynx",
                        "handle_or_identifier": "larkyn.lynx",
                        "service": "instagram",
                        "persona_name": "Savannah",
                        "display_label": "Larkyn Lynx (@larkyn.lynx)",
                        "profile_href": "https://www.instagram.com/larkyn.lynx/",
                    },
                    "recipient_profile": {
                        "id": "account-1",
                        "username": "larkyn.lynx",
                        "name": "Larkyn Lynx",
                        "profile_href": "https://www.instagram.com/larkyn.lynx/",
                        "profile_image_url": "https://cdn.test/larkyn.jpg",
                    },
                    "recipient_profile_href": "https://www.instagram.com/larkyn.lynx/",
                    "recipient_profile_image_url": "https://cdn.test/larkyn.jpg",
                    "recipient_label": "Larkyn Lynx (@larkyn.lynx)",
                    "summary_text": "Larkyn Lynx (@larkyn.lynx) received a new comment from Entrant One (@entrant.one) on Giveaway launch post.",
                    "chat_href": None,
                    "chat_href_label": None,
                    "activity_href": "https://instagram.test/p/post-1/",
                    "activity_href_label": "Open Instagram post",
                    "text_preview": "Count me in @friend",
                    "value_keys": ["from", "id", "media_id", "text"],
                    "parent_post": {
                        "post_id": "post-1",
                        "href": "/scheduled-posts/post-1/page",
                        "label": "Giveaway launch post",
                        "persona_name": "Savannah",
                        "instagram_external_url": "https://instagram.test/p/post-1/",
                        "instagram_external_id": "ig-media-1",
                    },
                    "payload_json": {"entry": {"id": "account-1"}, "change": {"field": "comments", "value": {"text": "Count me in @friend"}}},
                }
            ],
        },
    )

    assert "Instagram Webhook Activity" in html
    assert "Field Activity" in html
    assert "Recent Deliveries" in html
    assert "Count me in @friend" in html
    assert "Raw Payload" in html
    assert "Matched to giveaway" in html
    assert "/scheduled-posts/post-1/page" in html
    assert "Giveaway launch post" in html
    assert "Open Instagram post" in html
    assert "Larkyn Lynx (larkyn.lynx)" in html
    assert "received a new comment from" in html
    assert "Open sender profile" in html
    assert "https://www.instagram.com/entrant.one/" in html
    assert "Matched lynxposter account" in html


def test_instagram_webhook_observability_collapses_recent_deliveries_after_three():
    base_event = {
        "id": "event-1",
        "created_at": None,
        "field": "comments",
        "field_label": "Comments",
        "event_type": "comment",
        "event_type_label": "Comment",
        "matched": True,
        "matched_giveaway_id": "giveaway-1",
        "matched_post_id": "post-1",
        "matched_account_id": "account-1",
        "matched_state_label": "Matched to giveaway",
        "provider_object_id": "media-1",
        "provider_account_id": "account-1",
        "provider_local_account": {
            "account_id": "account-1",
            "label": "Larkyn Lynx",
            "handle_or_identifier": "larkyn.lynx",
            "service": "instagram",
            "persona_name": "Savannah",
            "display_label": "Larkyn Lynx (larkyn.lynx)",
            "profile_href": "https://www.instagram.com/larkyn.lynx/",
        },
        "matched_local_account": {
            "account_id": "account-1",
            "label": "Larkyn Lynx",
            "handle_or_identifier": "larkyn.lynx",
            "service": "instagram",
            "persona_name": "Savannah",
            "display_label": "Larkyn Lynx (larkyn.lynx)",
            "profile_href": "https://www.instagram.com/larkyn.lynx/",
        },
        "actor_local_account": None,
        "actor_profile": {
            "id": "user-1",
            "username": "entrant.one",
            "name": "Entrant One",
            "profile_href": "https://www.instagram.com/entrant.one/",
            "profile_image_url": "https://cdn.test/entrant.jpg",
        },
        "actor_profile_href": "https://www.instagram.com/entrant.one/",
        "actor_profile_image_url": "https://cdn.test/entrant.jpg",
        "actor_id": "user-1",
        "actor_username": "entrant.one",
        "actor_label": "Entrant One (@entrant.one)",
        "recipient_id": "account-1",
        "recipient_username": None,
        "recipient_local_account": {
            "account_id": "account-1",
            "label": "Larkyn Lynx",
            "handle_or_identifier": "larkyn.lynx",
            "service": "instagram",
            "persona_name": "Savannah",
            "display_label": "Larkyn Lynx (@larkyn.lynx)",
            "profile_href": "https://www.instagram.com/larkyn.lynx/",
        },
        "recipient_profile": {
            "id": "account-1",
            "username": "larkyn.lynx",
            "name": "Larkyn Lynx",
            "profile_href": "https://www.instagram.com/larkyn.lynx/",
            "profile_image_url": "https://cdn.test/larkyn.jpg",
        },
        "recipient_profile_href": "https://www.instagram.com/larkyn.lynx/",
        "recipient_profile_image_url": "https://cdn.test/larkyn.jpg",
        "recipient_label": "Larkyn Lynx (@larkyn.lynx)",
        "summary_text": "Larkyn Lynx (@larkyn.lynx) received a new comment from Entrant One (@entrant.one) on Giveaway launch post.",
        "chat_href": None,
        "chat_href_label": None,
        "activity_href": "https://instagram.test/p/post-1/",
        "activity_href_label": "Open Instagram post",
        "text_preview": "Count me in @friend",
        "value_keys": ["from", "id", "media_id", "text"],
        "parent_post": {
            "post_id": "post-1",
            "href": "/scheduled-posts/post-1/page",
            "label": "Giveaway launch post",
            "persona_name": "Savannah",
            "instagram_external_url": "https://instagram.test/p/post-1/",
            "instagram_external_id": "ig-media-1",
        },
        "related_media": None,
        "payload_json": {"entry": {"id": "account-1"}, "change": {"field": "comments", "value": {"text": "Count me in @friend"}}},
    }
    recent_events = []
    for index in range(4):
        item = deepcopy(base_event)
        item["id"] = f"event-{index + 1}"
        item["summary_text"] = f"Event {index + 1}"
        item["text_preview"] = f"Message {index + 1}"
        recent_events.append(item)

    html = templates.env.get_template("_instagram_webhook_observability.html").render(
        instagram_webhook_observability={
            "window_days": 7,
            "total_events": 4,
            "matched_events": 4,
            "unmatched_events": 0,
            "giveaway_relevant_events": 4,
            "unique_fields": 1,
            "unique_actors": 1,
            "latest_received_at": None,
            "field_chart": [{"label": "Comments", "count": 4, "width_pct": 100}],
            "daily_chart": [{"label": "Apr 15", "count": 4, "width_pct": 100}],
            "recent_events": recent_events,
        },
    )

    assert "3 of 4 shown" in html
    assert "Show 1 more delivery" in html
    assert "Event 4" in html
