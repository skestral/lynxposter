from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from app.services.oidc import DEFAULT_OIDC_SCOPE, normalize_oidc_scope


class PersonaBase(BaseModel):
    name: str
    slug: str
    is_enabled: bool = True
    timezone: str = "server"
    settings_json: dict[str, Any] = Field(default_factory=dict)
    retry_settings_json: dict[str, Any] = Field(default_factory=dict)
    throttle_settings_json: dict[str, Any] = Field(default_factory=dict)


class PersonaCreate(PersonaBase):
    pass


class PersonaUpdate(BaseModel):
    name: str | None = None
    slug: str | None = None
    is_enabled: bool | None = None
    timezone: str | None = None
    settings_json: dict[str, Any] | None = None
    retry_settings_json: dict[str, Any] | None = None
    throttle_settings_json: dict[str, Any] | None = None


class PersonaRead(PersonaBase):
    id: str
    owner_user_id: str | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class AppSettingsUpdate(BaseModel):
    instance_name: str = ""
    app_base_url: str = ""
    app_port: int = Field(default=8000, ge=1, le=65535)
    scheduler_automation_interval_seconds: int = Field(default=300, ge=30, le=86400)
    webhook_logging_enabled: bool = False
    webhook_logging_endpoint: str = ""
    webhook_logging_bearer_token: str = ""
    webhook_logging_timeout_seconds: int = Field(default=10, ge=1, le=120)
    webhook_logging_retry_count: int = Field(default=2, ge=0, le=10)
    webhook_logging_min_severity: Literal["debug", "info", "warning", "error", "critical"] = "warning"
    discord_notification_enabled: bool = False
    discord_notification_webhook_url: str = ""
    discord_notification_username: str = "LynxPoster"
    discord_notification_min_severity: Literal["debug", "info", "warning", "error", "critical"] = "warning"
    auth_oidc_enabled: bool = False
    auth_oidc_issuer_url: str = ""
    auth_oidc_client_id: str = ""
    auth_oidc_client_secret: str = ""
    auth_oidc_scope: str = DEFAULT_OIDC_SCOPE
    auth_oidc_groups_claim: str = "groups"
    auth_oidc_username_claim: str = "preferred_username"
    auth_oidc_admin_groups: str = ""
    auth_oidc_user_groups: str = ""
    auth_session_secret: str = ""
    instagram_webhooks_enabled: bool = False
    instagram_webhook_verify_token: str = ""
    instagram_app_secret: str = ""

    @field_validator("auth_oidc_scope", mode="before")
    @classmethod
    def _normalize_auth_oidc_scope(cls, value: Any) -> str:
        return normalize_oidc_scope(str(value) if value is not None else None)


class AppSettingsRead(AppSettingsUpdate):
    config_dir: str
    env_file_path: str
    app_data_dir: str
    database_path: str
    uploads_dir: str
    imported_media_dir: str
    logs_dir: str
    backups_dir: str
    updated_at: datetime


class AccountBase(BaseModel):
    service: str
    label: str
    handle_or_identifier: str = ""
    is_enabled: bool = True
    source_enabled: bool = False
    destination_enabled: bool = False
    credentials_json: dict[str, Any] = Field(default_factory=dict)
    source_settings_json: dict[str, Any] = Field(default_factory=dict)
    publish_settings_json: dict[str, Any] = Field(default_factory=dict)


class AccountCreate(AccountBase):
    pass


class AccountUpdate(BaseModel):
    label: str | None = None
    handle_or_identifier: str | None = None
    is_enabled: bool | None = None
    source_enabled: bool | None = None
    destination_enabled: bool | None = None
    credentials_json: dict[str, Any] | None = None
    source_settings_json: dict[str, Any] | None = None
    publish_settings_json: dict[str, Any] | None = None


class AccountRead(AccountBase):
    id: str
    persona_id: str
    last_health_status: str | None = None
    last_error: str | None = None
    source_supported: bool
    destination_supported: bool
    configured: bool
    created_at: datetime
    updated_at: datetime


class AccountRouteWrite(BaseModel):
    source_account_id: str
    destination_account_id: str
    is_enabled: bool = True


class AccountRouteReplaceRequest(BaseModel):
    routes: list[AccountRouteWrite] = Field(default_factory=list)


class AccountRouteRead(AccountRouteWrite):
    id: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class MediaAttachmentRead(BaseModel):
    id: str
    storage_path: str
    mime_type: str
    alt_text: str
    size_bytes: int
    checksum: str
    sort_order: int

    model_config = {"from_attributes": True}


class DeliveryStateRead(BaseModel):
    account_id: str
    label: str
    service: str
    status: str
    external_id: str | None = None
    external_url: str | None = None
    attempt_count: int
    last_error: str | None = None
    delivered_at: datetime | None = None


class DeliveryBreakdownRead(BaseModel):
    succeeded: list[DeliveryStateRead] = Field(default_factory=list)
    failed: list[DeliveryStateRead] = Field(default_factory=list)
    cancelled: list[DeliveryStateRead] = Field(default_factory=list)
    pending: list[DeliveryStateRead] = Field(default_factory=list)


class GiveawayRuleNodeInput(BaseModel):
    kind: Literal["all", "any", "not", "atom"]
    atom: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    children: list["GiveawayRuleNodeInput"] = Field(default_factory=list)

    @field_validator("children")
    @classmethod
    def _validate_children(cls, value: list["GiveawayRuleNodeInput"], info) -> list["GiveawayRuleNodeInput"]:
        kind = info.data.get("kind")
        if kind == "atom" and value:
            raise ValueError("Atom rules cannot have child rules.")
        if kind in {"all", "any"} and not value:
            raise ValueError(f"{kind.upper()} rules require at least one child rule.")
        if kind == "not" and len(value) != 1:
            raise ValueError("NOT rules require exactly one child rule.")
        return value

    @field_validator("atom")
    @classmethod
    def _validate_atom(cls, value: str | None, info) -> str | None:
        kind = info.data.get("kind")
        if kind == "atom" and not str(value or "").strip():
            raise ValueError("Atom rules require an atom name.")
        if kind != "atom" and value is not None:
            raise ValueError("Group rules do not accept atom names.")
        return value


class GiveawayChannelConfigInput(BaseModel):
    service: Literal["instagram", "bluesky"]
    account_id: str
    rules: GiveawayRuleNodeInput


class GiveawayConfigInput(BaseModel):
    giveaway_end_at: datetime | None = None
    pool_mode: Literal["combined", "separate"] = "combined"
    channels: list[GiveawayChannelConfigInput] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_instagram_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if value.get("channels") is not None:
            return value
        legacy_keys = {
            "min_friend_mentions",
            "required_keywords",
            "required_hashtags",
            "require_story_mention",
            "require_like",
            "require_follow",
        }
        if not legacy_keys.intersection(value.keys()):
            return value
        children: list[dict[str, Any]] = [{"kind": "atom", "atom": "comment_present", "params": {}}]
        min_friend_mentions = int(value.get("min_friend_mentions") or 0)
        if min_friend_mentions > 0:
            children.append(
                {
                    "kind": "atom",
                    "atom": "friend_mention_count_gte",
                    "params": {"count": min_friend_mentions},
                }
            )
        keywords = [str(item).strip() for item in value.get("required_keywords") or [] if str(item).strip()]
        if keywords:
            children.append(
                {
                    "kind": "atom",
                    "atom": "comment_keywords_all",
                    "params": {"keywords": keywords},
                }
            )
        hashtags = [str(item).strip() for item in value.get("required_hashtags") or [] if str(item).strip()]
        if hashtags:
            children.append(
                {
                    "kind": "atom",
                    "atom": "comment_hashtags_all",
                    "params": {"hashtags": hashtags},
                }
            )
        if bool(value.get("require_story_mention")):
            children.append({"kind": "atom", "atom": "story_mention_present", "params": {}})
        if bool(value.get("require_like")):
            children.append({"kind": "atom", "atom": "like_present", "params": {}})
        if bool(value.get("require_follow")):
            children.append({"kind": "atom", "atom": "follow_present", "params": {}})
        return {
            "giveaway_end_at": value.get("giveaway_end_at"),
            "pool_mode": value.get("pool_mode") or "combined",
            "channels": [
                {
                    "service": "instagram",
                    "account_id": value.get("account_id") or "",
                    "rules": {"kind": "all", "children": children},
                }
            ],
        }


class GiveawayEntrantRead(BaseModel):
    id: str
    service: str
    provider_user_id: str
    provider_username: str | None = None
    display_label: str | None = None
    signal_state: dict[str, Any] = Field(default_factory=dict)
    rule_match_details: dict[str, Any] = Field(default_factory=dict)
    activity_total: int = 0
    activity_breakdown: dict[str, int] = Field(default_factory=dict)
    checks: list["GiveawayRuleCheckRead"] = Field(default_factory=list)
    eligibility_status: str = "pending"
    inconclusive_reasons: list[str] = Field(default_factory=list)
    disqualification_reasons: list[str] = Field(default_factory=list)


class GiveawayRuleCheckRead(BaseModel):
    atom: str
    label: str
    status: Literal["passed", "failed", "inconclusive"]
    detail: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class GiveawaySelectionCandidateRead(BaseModel):
    rank: int
    selected: bool = False
    note: str | None = None
    entrant: GiveawayEntrantRead


class GiveawaySelectionLogRead(BaseModel):
    selection_method: str
    candidate_source: str
    note: str | None = None
    qualified_member_count: int = 0
    candidate_count: int = 0
    qualified_members: list[GiveawayEntrantRead] = Field(default_factory=list)
    candidates: list[GiveawaySelectionCandidateRead] = Field(default_factory=list)


class GiveawayPoolRead(BaseModel):
    id: str
    pool_key: str
    label: str
    status: str
    frozen_at: datetime | None = None
    last_evaluated_at: datetime | None = None
    last_error: str | None = None
    candidate_count: int = 0
    provisional_winner: GiveawayEntrantRead | None = None
    final_winner: GiveawayEntrantRead | None = None
    selection_log: GiveawaySelectionLogRead | None = None


class GiveawayChannelSummaryRead(BaseModel):
    entrants: int = 0
    eligible: int = 0
    provisional: int = 0
    disqualified: int = 0
    engagement_activities: int = 0
    activity_breakdown: dict[str, int] = Field(default_factory=dict)


class GiveawayChannelRead(BaseModel):
    id: str
    service: str
    account_id: str
    status: str
    rules: GiveawayRuleNodeInput
    target_post_external_id: str | None = None
    target_post_uri: str | None = None
    target_post_cid: str | None = None
    target_post_url: str | None = None
    last_collected_at: datetime | None = None
    last_error: str | None = None
    summary: GiveawayChannelSummaryRead = Field(default_factory=GiveawayChannelSummaryRead)
    entrants: list[GiveawayEntrantRead] = Field(default_factory=list)


class GiveawayAuditSummaryRead(BaseModel):
    entrants: int = 0
    eligible: int = 0
    provisional: int = 0
    disqualified: int = 0
    engagement_activities: int = 0
    per_channel: dict[str, GiveawayChannelSummaryRead] = Field(default_factory=dict)


class GiveawayRead(BaseModel):
    id: str
    post_id: str
    giveaway_end_at: datetime
    pool_mode: Literal["combined", "separate"]
    status: str
    frozen_at: datetime | None = None
    last_evaluated_at: datetime | None = None
    last_error: str | None = None
    audit_summary: GiveawayAuditSummaryRead = Field(default_factory=GiveawayAuditSummaryRead)
    channels: list[GiveawayChannelRead] = Field(default_factory=list)
    pools: list[GiveawayPoolRead] = Field(default_factory=list)


class InstagramGiveawayConfigInput(BaseModel):
    giveaway_end_at: datetime | None = None
    min_friend_mentions: int = 0
    required_keywords: list[str] = Field(default_factory=list)
    required_hashtags: list[str] = Field(default_factory=list)
    require_story_mention: bool = False
    require_like: bool = False
    require_follow: bool = False


class InstagramGiveawayEntryRead(BaseModel):
    id: str
    instagram_user_id: str
    instagram_username: str | None = None
    comment_count: int = 0
    mention_count: int = 0
    keyword_matches: list[str] = Field(default_factory=list)
    liked_status: str = "unknown"
    followed_status: str = "unknown"
    shared_status: str = "unknown"
    eligibility_status: str = "pending"
    inconclusive_reasons: list[str] = Field(default_factory=list)
    disqualification_reasons: list[str] = Field(default_factory=list)
    frozen_rank: int | None = None
    is_provisional_candidate: bool = False
    comments: list[dict[str, Any]] = Field(default_factory=list)
    story_mentions: list[dict[str, Any]] = Field(default_factory=list)


class InstagramGiveawayAuditSummaryRead(BaseModel):
    entrants: int = 0
    eligible: int = 0
    provisional: int = 0
    disqualified: int = 0
    comments_captured: int = 0
    story_mentions_captured: int = 0


class InstagramGiveawayRead(BaseModel):
    id: str
    post_id: str
    instagram_account_id: str
    giveaway_end_at: datetime
    status: str
    rules: InstagramGiveawayConfigInput
    frozen_at: datetime | None = None
    provisional_winner_rank: int | None = None
    final_winner_rank: int | None = None
    last_evaluated_at: datetime | None = None
    last_webhook_received_at: datetime | None = None
    last_error: str | None = None
    instagram_media_id: str | None = None
    instagram_media_url: str | None = None
    audit_summary: InstagramGiveawayAuditSummaryRead = Field(default_factory=InstagramGiveawayAuditSummaryRead)
    provisional_winner: InstagramGiveawayEntryRead | None = None
    final_winner: InstagramGiveawayEntryRead | None = None
    entries: list[InstagramGiveawayEntryRead] = Field(default_factory=list)


class ScheduledPostBase(BaseModel):
    persona_id: str
    body: str = ""
    post_type: Literal["standard", "giveaway", "instagram_giveaway"] = "standard"
    status: str = "draft"
    target_account_ids: list[str] = Field(default_factory=list)
    publish_overrides_json: dict[str, Any] = Field(default_factory=dict)
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    scheduled_for: datetime | None = None
    giveaway: "GiveawayConfigInput | None" = None

    @field_validator("post_type", mode="before")
    @classmethod
    def _normalize_post_type(cls, value: Any) -> str:
        if str(value or "") == "instagram_giveaway":
            return "giveaway"
        return str(value or "standard")


class ScheduledPostCreate(ScheduledPostBase):
    pass


class ScheduledPostUpdate(BaseModel):
    body: str | None = None
    post_type: Literal["standard", "giveaway", "instagram_giveaway"] | None = None
    status: str | None = None
    target_account_ids: list[str] | None = None
    publish_overrides_json: dict[str, Any] | None = None
    metadata_json: dict[str, Any] | None = None
    scheduled_for: datetime | None = None
    giveaway: "GiveawayConfigInput | None" = None
    attachment_order: list[str] | None = None
    deleted_attachment_ids: list[str] | None = None

    @field_validator("post_type", mode="before")
    @classmethod
    def _normalize_post_type(cls, value: Any) -> str | None:
        if value is None:
            return None
        if str(value or "") == "instagram_giveaway":
            return "giveaway"
        return str(value)


class ScheduledPostRead(ScheduledPostBase):
    id: str
    origin_kind: str
    origin_account_id: str | None = None
    published_at: datetime | None = None
    last_error: str | None = None
    display_status: str = "draft"
    can_delete: bool = False
    created_at: datetime
    updated_at: datetime
    deliveries: dict[str, DeliveryStateRead] = Field(default_factory=dict)
    delivery_breakdown: DeliveryBreakdownRead = Field(default_factory=DeliveryBreakdownRead)
    attachments: list[MediaAttachmentRead] = Field(default_factory=list)
    giveaway: GiveawayRead | None = None


class SandboxAttachmentInput(BaseModel):
    filename: str
    mime_type: str
    alt_text: str = ""
    size_bytes: int = 0
    sort_order: int = 0
    storage_path: str | None = None


class SandboxExpectationInput(BaseModel):
    expected_target_count: int | None = None
    body_must_contain: str | None = None
    body_must_not_contain: str | None = None
    max_body_length: int | None = None
    expected_attachment_count: int | None = None
    expected_visibility: str | None = None
    require_media: bool | None = None
    require_source_link_in_payload: bool | None = None


class SandboxPreviewRequest(BaseModel):
    persona_id: str
    body: str = ""
    target_account_ids: list[str] = Field(default_factory=list)
    publish_overrides_json: dict[str, Any] = Field(default_factory=dict)
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    attachment_inputs: list[SandboxAttachmentInput] = Field(default_factory=list)
    expectations: SandboxExpectationInput = Field(default_factory=SandboxExpectationInput)


class ValidationIssueRead(BaseModel):
    service: str
    message: str
    field: str | None = None
    level: str = "error"


class SandboxExpectationCheckRead(BaseModel):
    key: str
    label: str
    passed: bool
    expected: Any | None = None
    actual: Any | None = None
    message: str


class SandboxAccountPreviewRead(BaseModel):
    account_id: str
    account_label: str
    service: str
    configured: bool
    publish_ready: bool
    action: str
    endpoint_label: str | None = None
    rendered_body: str
    body_length: int
    attachment_count: int
    request_shape: dict[str, Any] = Field(default_factory=dict)
    validation_issues: list[ValidationIssueRead] = Field(default_factory=list)
    expectation_checks: list[SandboxExpectationCheckRead] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class SandboxPreviewRead(BaseModel):
    persona_id: str
    persona_name: str
    generated_at: datetime
    target_count: int
    attachment_count: int
    overall_valid: bool
    overall_expectations_passed: bool
    global_checks: list[SandboxExpectationCheckRead] = Field(default_factory=list)
    previews: list[SandboxAccountPreviewRead] = Field(default_factory=list)
    global_errors: list[str] = Field(default_factory=list)


class RunEventRead(BaseModel):
    id: str
    run_id: str
    persona_id: str | None
    persona_name: str | None = None
    account_id: str | None
    account_label: str | None = None
    service: str | None
    operation: str
    severity: str
    message: str
    post_id: str | None
    delivery_job_id: str | None
    metadata_json: dict[str, Any]
    created_at: datetime

    model_config = {"from_attributes": True}


class AlertEventRead(BaseModel):
    id: str
    run_id: str
    fingerprint: str
    event_type: str
    severity: str
    persona_id: str | None
    persona_name: str | None = None
    account_id: str | None
    account_label: str | None = None
    service: str | None
    operation: str
    post_id: str | None
    delivery_job_id: str | None
    message: str
    error_class: str | None
    retry_count: int
    payload_json: dict[str, Any]
    created_at: datetime

    model_config = {"from_attributes": True}


class UserRead(BaseModel):
    id: str
    oidc_sub: str
    email: str | None = None
    username: str | None = None
    display_name: str
    preferred_name: str | None = None
    effective_display_name: str
    role: Literal["user", "admin"]
    is_enabled: bool = True
    timezone: str = "UTC"
    ui_theme: str = "skylight"
    ui_mode: str = "light"
    groups_json: list[str] = Field(default_factory=list)
    last_login_at: datetime | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class UserSettingsUpdate(BaseModel):
    timezone: str
    ui_theme: str | None = None
    ui_mode: str | None = None
    preferred_name: str | None = None


class AdminUserUpdate(BaseModel):
    is_enabled: bool
    timezone: str


GiveawayRuleNodeInput.model_rebuild()
GiveawayEntrantRead.model_rebuild()
GiveawaySelectionCandidateRead.model_rebuild()
GiveawaySelectionLogRead.model_rebuild()
