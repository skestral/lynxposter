from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import Boolean, CheckConstraint, DateTime, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return str(uuid4())


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    oidc_sub: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True, unique=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    preferred_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    role: Mapped[str] = mapped_column(String(32), default="user", nullable=False, index=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)
    timezone: Mapped[str] = mapped_column(String(64), default="UTC", nullable=False)
    ui_theme: Mapped[str] = mapped_column(String(32), default="skylight", nullable=False)
    ui_mode: Mapped[str] = mapped_column(String(16), default="light", nullable=False)
    groups_json: Mapped[list[str]] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=False)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    personas: Mapped[list["Persona"]] = relationship(back_populates="owner_user")

    @property
    def effective_display_name(self) -> str:
        preferred = str(self.preferred_name or "").strip()
        if preferred:
            return preferred
        return str(self.display_name or "").strip() or "User"


class Persona(Base):
    __tablename__ = "personas"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    owner_user_id: Mapped[str | None] = mapped_column(ForeignKey("users.id"), nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    slug: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), default="server", nullable=False)
    settings_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    retry_settings_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    throttle_settings_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    accounts: Mapped[list["Account"]] = relationship(back_populates="persona", cascade="all, delete-orphan")
    posts: Mapped[list["CanonicalPost"]] = relationship(back_populates="persona", cascade="all, delete-orphan")
    run_events: Mapped[list["RunEvent"]] = relationship(back_populates="persona")
    alert_events: Mapped[list["AlertEvent"]] = relationship(back_populates="persona")
    owner_user: Mapped["User | None"] = relationship(back_populates="personas")

    @property
    def source_accounts(self) -> list["Account"]:
        return sorted(
            [account for account in self.accounts if account.source_enabled],
            key=lambda item: (item.service, item.label or item.handle_or_identifier or ""),
        )

    @property
    def destination_accounts(self) -> list["Account"]:
        return sorted(
            [account for account in self.accounts if account.destination_enabled],
            key=lambda item: (item.service, item.label or item.handle_or_identifier or ""),
        )


class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = (UniqueConstraint("persona_id", "service", name="uq_account_persona_service"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    persona_id: Mapped[str] = mapped_column(ForeignKey("personas.id"), nullable=False, index=True)
    service: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    label: Mapped[str] = mapped_column(String(120), nullable=False)
    handle_or_identifier: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    source_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    destination_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    credentials_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    source_settings_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    publish_settings_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    last_health_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    persona: Mapped["Persona"] = relationship(back_populates="accounts")
    outgoing_routes: Mapped[list["AccountRoute"]] = relationship(
        back_populates="source_account",
        cascade="all, delete-orphan",
        foreign_keys="AccountRoute.source_account_id",
    )
    incoming_routes: Mapped[list["AccountRoute"]] = relationship(
        back_populates="destination_account",
        cascade="all, delete-orphan",
        foreign_keys="AccountRoute.destination_account_id",
    )
    sync_state: Mapped["AccountSyncState | None"] = relationship(back_populates="source_account", cascade="all, delete-orphan")
    post_refs: Mapped[list["AccountPostRef"]] = relationship(back_populates="account", cascade="all, delete-orphan")
    delivery_jobs: Mapped[list["DeliveryJob"]] = relationship(back_populates="target_account")
    run_events: Mapped[list["RunEvent"]] = relationship(back_populates="account")
    alert_events: Mapped[list["AlertEvent"]] = relationship(back_populates="account")


class AccountRoute(Base):
    __tablename__ = "account_routes"
    __table_args__ = (
        UniqueConstraint("source_account_id", "destination_account_id", name="uq_account_route_source_destination"),
        CheckConstraint("source_account_id != destination_account_id", name="ck_account_route_not_self"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    source_account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False, index=True)
    destination_account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False, index=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    source_account: Mapped["Account"] = relationship(
        back_populates="outgoing_routes",
        foreign_keys=[source_account_id],
    )
    destination_account: Mapped["Account"] = relationship(
        back_populates="incoming_routes",
        foreign_keys=[destination_account_id],
    )


class CanonicalPost(Base):
    __tablename__ = "canonical_posts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    persona_id: Mapped[str] = mapped_column(ForeignKey("personas.id"), nullable=False, index=True)
    origin_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    post_type: Mapped[str] = mapped_column(String(32), nullable=False, default="standard", index=True)
    origin_account_id: Mapped[str | None] = mapped_column(ForeignKey("accounts.id"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="draft")
    body: Mapped[str] = mapped_column(Text, default="", nullable=False)
    publish_overrides_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    reply_to_post_id: Mapped[str | None] = mapped_column(ForeignKey("canonical_posts.id"), nullable=True, index=True)
    quote_of_post_id: Mapped[str | None] = mapped_column(ForeignKey("canonical_posts.id"), nullable=True, index=True)
    scheduled_for: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    persona: Mapped["Persona"] = relationship(back_populates="posts")
    origin_account: Mapped["Account | None"] = relationship(foreign_keys=[origin_account_id])
    attachments: Mapped[list["MediaAttachment"]] = relationship(back_populates="post", cascade="all, delete-orphan")
    account_post_refs: Mapped[list["AccountPostRef"]] = relationship(back_populates="post", cascade="all, delete-orphan")
    delivery_jobs: Mapped[list["DeliveryJob"]] = relationship(back_populates="post", cascade="all, delete-orphan")
    instagram_giveaway: Mapped["InstagramGiveaway | None"] = relationship(
        back_populates="post",
        cascade="all, delete-orphan",
        uselist=False,
    )
    giveaway_campaign: Mapped["GiveawayCampaign | None"] = relationship(
        back_populates="post",
        cascade="all, delete-orphan",
        uselist=False,
    )


class MediaAttachment(Base):
    __tablename__ = "media_attachments"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    post_id: Mapped[str] = mapped_column(ForeignKey("canonical_posts.id"), nullable=False, index=True)
    storage_path: Mapped[str] = mapped_column(Text, nullable=False)
    mime_type: Mapped[str] = mapped_column(String(128), nullable=False)
    alt_text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    checksum: Mapped[str] = mapped_column(String(128), nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    post: Mapped["CanonicalPost"] = relationship(back_populates="attachments")


class AccountPostRef(Base):
    __tablename__ = "account_post_refs"
    __table_args__ = (UniqueConstraint("account_id", "external_id", name="uq_account_post_ref_account_external"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    post_id: Mapped[str] = mapped_column(ForeignKey("canonical_posts.id"), nullable=False, index=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False, index=True)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    external_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    post: Mapped["CanonicalPost"] = relationship(back_populates="account_post_refs")
    account: Mapped["Account"] = relationship(back_populates="post_refs")


class DeliveryJob(Base):
    __tablename__ = "delivery_jobs"
    __table_args__ = (UniqueConstraint("post_id", "target_account_id", name="uq_delivery_job_post_target_account"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    post_id: Mapped[str] = mapped_column(ForeignKey("canonical_posts.id"), nullable=False, index=True)
    target_account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), default="draft", nullable=False, index=True)
    external_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    external_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error_class: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    post: Mapped["CanonicalPost"] = relationship(back_populates="delivery_jobs")
    target_account: Mapped["Account"] = relationship(back_populates="delivery_jobs")
    attempts: Mapped[list["DeliveryAttempt"]] = relationship(back_populates="delivery_job", cascade="all, delete-orphan")


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    delivery_job_id: Mapped[str] = mapped_column(ForeignKey("delivery_jobs.id"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_class: Mapped[str | None] = mapped_column(String(255), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    response_payload: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)

    delivery_job: Mapped["DeliveryJob"] = relationship(back_populates="attempts")


class InstagramGiveaway(Base):
    __tablename__ = "instagram_giveaways"
    __table_args__ = (UniqueConstraint("post_id", name="uq_instagram_giveaway_post"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    post_id: Mapped[str] = mapped_column(ForeignKey("canonical_posts.id"), nullable=False, index=True)
    instagram_account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False, index=True)
    giveaway_end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="scheduled", index=True)
    rules_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    selection_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="single_winner")
    evaluation_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="webhook_hybrid")
    review_behavior: Mapped[str] = mapped_column(String(32), nullable=False, default="provisional_winner")
    frozen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    provisional_winner_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    final_winner_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_webhook_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    post: Mapped["CanonicalPost"] = relationship(back_populates="instagram_giveaway")
    instagram_account: Mapped["Account"] = relationship()
    entries: Mapped[list["InstagramGiveawayEntry"]] = relationship(back_populates="giveaway", cascade="all, delete-orphan")


class InstagramGiveawayEntry(Base):
    __tablename__ = "instagram_giveaway_entries"
    __table_args__ = (UniqueConstraint("giveaway_id", "instagram_user_id", name="uq_instagram_giveaway_entry_user"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    giveaway_id: Mapped[str] = mapped_column(ForeignKey("instagram_giveaways.id"), nullable=False, index=True)
    instagram_user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    instagram_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    comments_json: Mapped[list[dict[str, Any]]] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=False)
    comment_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mention_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    keyword_matches_json: Mapped[list[str]] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=False)
    story_mentions_json: Mapped[list[dict[str, Any]]] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=False)
    liked_status: Mapped[str] = mapped_column(String(32), nullable=False, default="unknown")
    followed_status: Mapped[str] = mapped_column(String(32), nullable=False, default="unknown")
    shared_status: Mapped[str] = mapped_column(String(32), nullable=False, default="unknown")
    eligibility_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)
    inconclusive_reasons_json: Mapped[list[str]] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=False)
    disqualification_reasons_json: Mapped[list[str]] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=False)
    frozen_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_provisional_candidate: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    giveaway: Mapped["InstagramGiveaway"] = relationship(back_populates="entries")


class InstagramGiveawayWebhookEvent(Base):
    __tablename__ = "instagram_giveaway_webhook_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    matched_giveaway_id: Mapped[str | None] = mapped_column(ForeignKey("giveaway_campaigns.id"), nullable=True, index=True)
    matched_post_id: Mapped[str | None] = mapped_column(ForeignKey("canonical_posts.id"), nullable=True, index=True)
    matched_account_id: Mapped[str | None] = mapped_column(ForeignKey("accounts.id"), nullable=True, index=True)
    provider_object_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    provider_event_field: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    signature_valid: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    processed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    giveaway: Mapped["GiveawayCampaign | None"] = relationship(back_populates="webhook_events")


class GiveawayCampaign(Base):
    __tablename__ = "giveaway_campaigns"
    __table_args__ = (UniqueConstraint("post_id", name="uq_giveaway_campaign_post"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    post_id: Mapped[str] = mapped_column(ForeignKey("canonical_posts.id"), nullable=False, index=True)
    giveaway_end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    pool_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="combined")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="scheduled", index=True)
    frozen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    post: Mapped["CanonicalPost"] = relationship(back_populates="giveaway_campaign")
    channels: Mapped[list["GiveawayChannel"]] = relationship(back_populates="campaign", cascade="all, delete-orphan")
    pools: Mapped[list["GiveawayPoolResult"]] = relationship(back_populates="campaign", cascade="all, delete-orphan")
    evidence_events: Mapped[list["GiveawayEvidenceEvent"]] = relationship(back_populates="campaign", cascade="all, delete-orphan")
    webhook_events: Mapped[list["InstagramGiveawayWebhookEvent"]] = relationship(back_populates="giveaway")


class GiveawayChannel(Base):
    __tablename__ = "giveaway_channels"
    __table_args__ = (UniqueConstraint("campaign_id", "service", name="uq_giveaway_channel_campaign_service"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    campaign_id: Mapped[str] = mapped_column(ForeignKey("giveaway_campaigns.id"), nullable=False, index=True)
    service: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False, index=True)
    rules_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="scheduled", index=True)
    target_post_external_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    target_post_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    target_post_cid: Mapped[str | None] = mapped_column(String(255), nullable=True)
    target_post_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_collected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    campaign: Mapped["GiveawayCampaign"] = relationship(back_populates="channels")
    account: Mapped["Account"] = relationship()
    entrants: Mapped[list["GiveawayEntrant"]] = relationship(back_populates="channel", cascade="all, delete-orphan")
    evidence_events: Mapped[list["GiveawayEvidenceEvent"]] = relationship(back_populates="channel", cascade="all, delete-orphan")


class GiveawayEntrant(Base):
    __tablename__ = "giveaway_entrants"
    __table_args__ = (UniqueConstraint("channel_id", "provider_user_id", name="uq_giveaway_entrant_channel_user"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    channel_id: Mapped[str] = mapped_column(ForeignKey("giveaway_channels.id"), nullable=False, index=True)
    provider_user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    provider_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    display_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    signal_state_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    rule_match_details_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    eligibility_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)
    inconclusive_reasons_json: Mapped[list[str]] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=False)
    disqualification_reasons_json: Mapped[list[str]] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    channel: Mapped["GiveawayChannel"] = relationship(back_populates="entrants")
    evidence_events: Mapped[list["GiveawayEvidenceEvent"]] = relationship(back_populates="entrant", cascade="all, delete-orphan")


class GiveawayEvidenceEvent(Base):
    __tablename__ = "giveaway_evidence_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    campaign_id: Mapped[str] = mapped_column(ForeignKey("giveaway_campaigns.id"), nullable=False, index=True)
    channel_id: Mapped[str] = mapped_column(ForeignKey("giveaway_channels.id"), nullable=False, index=True)
    entrant_id: Mapped[str | None] = mapped_column(ForeignKey("giveaway_entrants.id"), nullable=True, index=True)
    provider_event_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False, default="collector")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    campaign: Mapped["GiveawayCampaign"] = relationship(back_populates="evidence_events")
    channel: Mapped["GiveawayChannel"] = relationship(back_populates="evidence_events")
    entrant: Mapped["GiveawayEntrant | None"] = relationship(back_populates="evidence_events")


class GiveawayPoolResult(Base):
    __tablename__ = "giveaway_pool_results"
    __table_args__ = (UniqueConstraint("campaign_id", "pool_key", name="uq_giveaway_pool_campaign_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    campaign_id: Mapped[str] = mapped_column(ForeignKey("giveaway_campaigns.id"), nullable=False, index=True)
    pool_key: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="scheduled", index=True)
    label: Mapped[str] = mapped_column(String(120), nullable=False, default="Combined")
    candidate_entry_ids_json: Mapped[list[str]] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=False)
    provisional_winner_entry_id: Mapped[str | None] = mapped_column(ForeignKey("giveaway_entrants.id"), nullable=True)
    final_winner_entry_id: Mapped[str | None] = mapped_column(ForeignKey("giveaway_entrants.id"), nullable=True)
    frozen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    campaign: Mapped["GiveawayCampaign"] = relationship(back_populates="pools")
    provisional_winner_entry: Mapped["GiveawayEntrant | None"] = relationship(foreign_keys=[provisional_winner_entry_id])
    final_winner_entry: Mapped["GiveawayEntrant | None"] = relationship(foreign_keys=[final_winner_entry_id])


class AccountSyncState(Base):
    __tablename__ = "account_sync_states"
    __table_args__ = (UniqueConstraint("source_account_id", name="uq_account_sync_state_source_account"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    source_account_id: Mapped[str] = mapped_column(ForeignKey("accounts.id"), nullable=False, index=True)
    cursor: Mapped[str | None] = mapped_column(Text, nullable=True)
    state_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    source_account: Mapped["Account"] = relationship(back_populates="sync_state")


class RunEvent(Base):
    __tablename__ = "run_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    persona_id: Mapped[str | None] = mapped_column(ForeignKey("personas.id"), nullable=True, index=True)
    account_id: Mapped[str | None] = mapped_column(ForeignKey("accounts.id"), nullable=True, index=True)
    service: Mapped[str | None] = mapped_column(String(32), nullable=True)
    operation: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), default="info", nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    post_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    delivery_job_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    persona: Mapped["Persona | None"] = relationship(back_populates="run_events")
    account: Mapped["Account | None"] = relationship(back_populates="run_events")


class AlertEvent(Base):
    __tablename__ = "alert_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    fingerprint: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    persona_id: Mapped[str | None] = mapped_column(ForeignKey("personas.id"), nullable=True, index=True)
    account_id: Mapped[str | None] = mapped_column(ForeignKey("accounts.id"), nullable=True, index=True)
    service: Mapped[str | None] = mapped_column(String(32), nullable=True)
    operation: Mapped[str] = mapped_column(String(64), nullable=False)
    post_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    delivery_job_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    error_class: Mapped[str | None] = mapped_column(String(255), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    persona: Mapped["Persona | None"] = relationship(back_populates="alert_events")
    account: Mapped["Account | None"] = relationship(back_populates="alert_events")
