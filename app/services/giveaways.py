from __future__ import annotations

from collections import Counter
import hashlib
import hmac
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

import requests
from sqlalchemy import Select, select
from sqlalchemy.orm import Session, selectinload

from app.adapters.instagram import _authenticated_publish_client, _instagram_destination_dependency_issue
from app.models import (
    Account,
    CanonicalPost,
    DeliveryJob,
    GiveawayCampaign,
    GiveawayChannel,
    GiveawayEntrant,
    InstagramGiveaway,
    InstagramGiveawayEntry,
    InstagramGiveawayWebhookEvent,
    Persona,
)
from app.schemas import (
    InstagramGiveawayAuditSummaryRead,
    InstagramGiveawayConfigInput,
    InstagramGiveawayEntryRead,
    InstagramGiveawayRead,
)
from app.services.alerts import AlertDispatcher
from app.services.events import log_run_event
from app.services.giveaway_engine import (
    ENTRY_STATUS_ELIGIBLE as GENERIC_ENTRY_STATUS_ELIGIBLE,
    ENTRY_STATUS_PROVISIONAL as GENERIC_ENTRY_STATUS_PROVISIONAL,
    GIVEAWAY_STATUS_COLLECTING as GENERIC_GIVEAWAY_STATUS_COLLECTING,
    GIVEAWAY_STATUS_FAILED as GENERIC_GIVEAWAY_STATUS_FAILED,
    GIVEAWAY_STATUS_REVIEW_REQUIRED as GENERIC_GIVEAWAY_STATUS_REVIEW_REQUIRED,
    GIVEAWAY_STATUS_SCHEDULED as GENERIC_GIVEAWAY_STATUS_SCHEDULED,
    GIVEAWAY_STATUS_WINNER_CONFIRMED as GENERIC_GIVEAWAY_STATUS_WINNER_CONFIRMED,
    GIVEAWAY_STATUS_WINNER_SELECTED as GENERIC_GIVEAWAY_STATUS_WINNER_SELECTED,
    POST_TYPE_GIVEAWAY,
    advance_giveaway_winner as advance_generic_giveaway_winner,
    confirm_giveaway_winner as confirm_generic_giveaway_winner,
    get_giveaway_by_post_id as get_generic_giveaway_by_post_id,
    get_or_create_channel_entrant,
    hydrate_channel_targets,
    process_giveaway_lifecycle,
    sync_instagram_webhook_event_to_channel,
)
from app.services.live_updates import (
    LIVE_UPDATE_TOPIC_DASHBOARD,
    LIVE_UPDATE_TOPIC_INSTAGRAM_WEBHOOKS,
    LIVE_UPDATE_TOPIC_LOGS,
    publish_live_update,
)

POST_TYPE_STANDARD = "standard"
POST_TYPE_INSTAGRAM_GIVEAWAY = POST_TYPE_GIVEAWAY

GIVEAWAY_STATUS_SCHEDULED = GENERIC_GIVEAWAY_STATUS_SCHEDULED
GIVEAWAY_STATUS_COLLECTING = GENERIC_GIVEAWAY_STATUS_COLLECTING
GIVEAWAY_STATUS_REVIEW_REQUIRED = GENERIC_GIVEAWAY_STATUS_REVIEW_REQUIRED
GIVEAWAY_STATUS_WINNER_SELECTED = GENERIC_GIVEAWAY_STATUS_WINNER_SELECTED
GIVEAWAY_STATUS_WINNER_CONFIRMED = GENERIC_GIVEAWAY_STATUS_WINNER_CONFIRMED
GIVEAWAY_STATUS_FAILED = GENERIC_GIVEAWAY_STATUS_FAILED

ENTRY_STATUS_PENDING = "pending"
ENTRY_STATUS_ELIGIBLE = GENERIC_ENTRY_STATUS_ELIGIBLE
ENTRY_STATUS_PROVISIONAL = GENERIC_ENTRY_STATUS_PROVISIONAL
ENTRY_STATUS_DISQUALIFIED = "disqualified"

RULE_STATUS_UNKNOWN = "unknown"
RULE_STATUS_NOT_REQUIRED = "not_required"
RULE_STATUS_VERIFIED = "verified"
RULE_STATUS_INCONCLUSIVE = "inconclusive"
RULE_STATUS_MISSING = "missing"

MENTION_PATTERN = re.compile(r"(?<!\w)@([A-Za-z0-9._]+)")
COMMENT_EVIDENCE_SOURCE_WEBHOOK = "webhook_capture"
COMMENT_EVIDENCE_SOURCE_LIVE = "close_time_live"
STORY_EVIDENCE_SOURCE_WEBHOOK = "webhook_capture"
STORY_EVIDENCE_SOURCE_MESSAGE_SHARE = "message_share_capture"
INSTAGRAM_GRAPH_API_BASE_URL = "https://graph.instagram.com/v25.0"
INSTAGRAM_GRAPH_CACHE_TTL_SECONDS = 300

_GRAPH_PROFILE_CACHE: dict[tuple[str, str], tuple[float, dict[str, Any] | None]] = {}
_GRAPH_CONVERSATION_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_GRAPH_MEDIA_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def giveaway_selectinloads() -> tuple[Any, ...]:
    return (
        selectinload(CanonicalPost.instagram_giveaway).selectinload(InstagramGiveaway.entries),
        selectinload(CanonicalPost.instagram_giveaway).selectinload(InstagramGiveaway.instagram_account),
        selectinload(CanonicalPost.instagram_giveaway).selectinload(InstagramGiveaway.webhook_events),
    )


def _normalized_terms(values: list[str] | None, *, prefix: str = "") -> list[str]:
    normalized: list[str] = []
    for raw in values or []:
        value = str(raw or "").strip().lower()
        if not value:
            continue
        if prefix and not value.startswith(prefix):
            value = f"{prefix}{value.lstrip(prefix)}"
        if value not in normalized:
            normalized.append(value)
    return normalized


def normalize_giveaway_rules(config: InstagramGiveawayConfigInput | dict[str, Any] | None) -> dict[str, Any] | None:
    if config is None:
        return None
    if isinstance(config, dict):
        parsed = InstagramGiveawayConfigInput.model_validate(config)
    else:
        parsed = config
    return {
        "giveaway_end_at": normalize_datetime(parsed.giveaway_end_at).isoformat() if parsed.giveaway_end_at else None,
        "min_friend_mentions": int(parsed.min_friend_mentions or 0),
        "required_keywords": _normalized_terms(parsed.required_keywords),
        "required_hashtags": _normalized_terms(parsed.required_hashtags, prefix="#"),
        "require_story_mention": bool(parsed.require_story_mention),
        "require_like": bool(parsed.require_like),
        "require_follow": bool(parsed.require_follow),
    }


def giveaway_rules_input_from_json(rules_json: dict[str, Any] | None) -> InstagramGiveawayConfigInput:
    payload = dict(rules_json or {})
    payload["giveaway_end_at"] = normalize_datetime(
        datetime.fromisoformat(payload["giveaway_end_at"]) if payload.get("giveaway_end_at") else None
    )
    return InstagramGiveawayConfigInput.model_validate(payload)


def _instagram_destination_targets(target_accounts: list[Account]) -> list[Account]:
    return [account for account in target_accounts if account.service == "instagram"]


def _scheduled_publish_anchor(post: CanonicalPost) -> datetime | None:
    if post.scheduled_for is not None:
        return normalize_datetime(post.scheduled_for)
    if post.status in {"queued", "posting", "scheduled"}:
        return utcnow()
    return None


def validate_instagram_giveaway_post(post: CanonicalPost, target_accounts: list[Account], giveaway: InstagramGiveawayConfigInput | None) -> None:
    if post.post_type != POST_TYPE_INSTAGRAM_GIVEAWAY:
        return
    if giveaway is None or giveaway.giveaway_end_at is None:
        raise ValueError("Instagram giveaway posts require a giveaway end time.")
    instagram_targets = _instagram_destination_targets(target_accounts)
    if len(target_accounts) != 1 or len(instagram_targets) != 1:
        raise ValueError("Instagram giveaway posts must target exactly one Instagram destination account.")
    publish_anchor = _scheduled_publish_anchor(post)
    giveaway_end_at = normalize_datetime(giveaway.giveaway_end_at)
    if publish_anchor is not None and giveaway_end_at is not None and giveaway_end_at <= publish_anchor:
        raise ValueError("Instagram giveaway end time must be after the scheduled publish time.")


def sync_instagram_giveaway(
    session: Session,
    post: CanonicalPost,
    target_accounts: list[Account],
    giveaway_config: InstagramGiveawayConfigInput | None,
) -> InstagramGiveaway | None:
    if post.post_type != POST_TYPE_INSTAGRAM_GIVEAWAY:
        if post.instagram_giveaway is not None:
            session.delete(post.instagram_giveaway)
            session.flush()
        return None

    validate_instagram_giveaway_post(post, target_accounts, giveaway_config)
    if giveaway_config is None:
        raise ValueError("Instagram giveaway configuration is required.")
    instagram_account = _instagram_destination_targets(target_accounts)[0]
    rules_json = normalize_giveaway_rules(giveaway_config)
    giveaway = post.instagram_giveaway
    if giveaway is None:
        giveaway = InstagramGiveaway(
            post_id=post.id,
            instagram_account_id=instagram_account.id,
            giveaway_end_at=normalize_datetime(giveaway_config.giveaway_end_at),
            status=GIVEAWAY_STATUS_SCHEDULED,
            rules_json=rules_json or {},
        )
        post.instagram_giveaway = giveaway
        session.add(giveaway)
    else:
        giveaway.instagram_account_id = instagram_account.id
        giveaway.giveaway_end_at = normalize_datetime(giveaway_config.giveaway_end_at)
        giveaway.rules_json = rules_json or {}
        if giveaway.status == GIVEAWAY_STATUS_FAILED and not giveaway.frozen_at:
            giveaway.status = GIVEAWAY_STATUS_SCHEDULED
            giveaway.last_error = None
    session.flush()
    return giveaway


def _winner_entry_by_rank(giveaway: InstagramGiveaway, rank: int | None) -> InstagramGiveawayEntry | None:
    if rank is None:
        return None
    for entry in giveaway.entries:
        if entry.frozen_rank == rank:
            return entry
    return None


def giveaway_audit_summary(giveaway: InstagramGiveaway | None) -> InstagramGiveawayAuditSummaryRead:
    if giveaway is None:
        return InstagramGiveawayAuditSummaryRead()
    entries = list(giveaway.entries or [])
    return InstagramGiveawayAuditSummaryRead(
        entrants=len(entries),
        eligible=sum(1 for entry in entries if entry.eligibility_status == ENTRY_STATUS_ELIGIBLE),
        provisional=sum(1 for entry in entries if entry.eligibility_status == ENTRY_STATUS_PROVISIONAL),
        disqualified=sum(1 for entry in entries if entry.eligibility_status == ENTRY_STATUS_DISQUALIFIED),
        comments_captured=sum(int(entry.comment_count or 0) for entry in entries),
        story_mentions_captured=sum(len(entry.story_mentions_json or []) for entry in entries),
    )


def serialize_giveaway_entry(entry: InstagramGiveawayEntry) -> InstagramGiveawayEntryRead:
    return InstagramGiveawayEntryRead(
        id=entry.id,
        instagram_user_id=entry.instagram_user_id,
        instagram_username=entry.instagram_username,
        comment_count=entry.comment_count,
        mention_count=entry.mention_count,
        keyword_matches=list(entry.keyword_matches_json or []),
        liked_status=entry.liked_status,
        followed_status=entry.followed_status,
        shared_status=entry.shared_status,
        eligibility_status=entry.eligibility_status,
        inconclusive_reasons=list(entry.inconclusive_reasons_json or []),
        disqualification_reasons=list(entry.disqualification_reasons_json or []),
        frozen_rank=entry.frozen_rank,
        is_provisional_candidate=bool(entry.is_provisional_candidate),
        comments=list(entry.comments_json or []),
        story_mentions=list(entry.story_mentions_json or []),
    )


def _instagram_media_job(giveaway: InstagramGiveaway) -> DeliveryJob | None:
    for job in giveaway.post.delivery_jobs:
        if job.target_account_id == giveaway.instagram_account_id and job.status == "posted":
            return job
    return None


def serialize_giveaway(giveaway: InstagramGiveaway | None) -> InstagramGiveawayRead | None:
    if giveaway is None:
        return None
    media_job = _instagram_media_job(giveaway)
    rules = giveaway_rules_input_from_json(giveaway.rules_json or {})
    provisional_winner = _winner_entry_by_rank(giveaway, giveaway.provisional_winner_rank)
    final_winner = _winner_entry_by_rank(giveaway, giveaway.final_winner_rank)
    sorted_entries = sorted(
        giveaway.entries,
        key=lambda item: (item.frozen_rank is None, item.frozen_rank or 999999, item.instagram_username or item.instagram_user_id),
    )
    return InstagramGiveawayRead(
        id=giveaway.id,
        post_id=giveaway.post_id,
        instagram_account_id=giveaway.instagram_account_id,
        giveaway_end_at=giveaway.giveaway_end_at,
        status=giveaway.status,
        rules=rules,
        frozen_at=giveaway.frozen_at,
        provisional_winner_rank=giveaway.provisional_winner_rank,
        final_winner_rank=giveaway.final_winner_rank,
        last_evaluated_at=giveaway.last_evaluated_at,
        last_webhook_received_at=giveaway.last_webhook_received_at,
        last_error=giveaway.last_error,
        instagram_media_id=media_job.external_id if media_job else None,
        instagram_media_url=media_job.external_url if media_job else None,
        audit_summary=giveaway_audit_summary(giveaway),
        provisional_winner=serialize_giveaway_entry(provisional_winner) if provisional_winner else None,
        final_winner=serialize_giveaway_entry(final_winner) if final_winner else None,
        entries=[serialize_giveaway_entry(entry) for entry in sorted_entries],
    )


def list_instagram_giveaways_stmt() -> Select:
    return (
        select(InstagramGiveaway)
        .options(
            selectinload(InstagramGiveaway.entries),
            selectinload(InstagramGiveaway.webhook_events),
            selectinload(InstagramGiveaway.instagram_account),
            selectinload(InstagramGiveaway.post)
            .selectinload(CanonicalPost.delivery_jobs)
            .selectinload(DeliveryJob.target_account),
            selectinload(InstagramGiveaway.post).selectinload(CanonicalPost.persona),
        )
    )


def get_giveaway_for_post(session: Session, post_id: str, *, owner_user_id: str | None = None) -> InstagramGiveaway | None:
    stmt = list_instagram_giveaways_stmt().join(InstagramGiveaway.post).where(InstagramGiveaway.post_id == post_id)
    if owner_user_id is not None:
        stmt = stmt.join(CanonicalPost.persona).where(Persona.owner_user_id == owner_user_id)
    return session.scalar(stmt)


def get_giveaway_by_post_id(session: Session, post_id: str) -> InstagramGiveaway | None:
    return session.scalar(list_instagram_giveaways_stmt().where(InstagramGiveaway.post_id == post_id))


def latest_instagram_webhook_event(session: Session) -> InstagramGiveawayWebhookEvent | None:
    return session.scalar(select(InstagramGiveawayWebhookEvent).order_by(InstagramGiveawayWebhookEvent.created_at.desc()).limit(1))


def list_instagram_webhook_events(session: Session, *, limit: int = 50) -> list[InstagramGiveawayWebhookEvent]:
    stmt = (
        select(InstagramGiveawayWebhookEvent)
        .order_by(InstagramGiveawayWebhookEvent.created_at.desc())
        .limit(limit)
    )
    return list(session.scalars(stmt))


def instagram_webhook_callback_url(app_base_url: str) -> str | None:
    base_url = str(app_base_url or "").strip().rstrip("/")
    if not base_url:
        return None
    return f"{base_url}/webhooks/instagram"


def verify_instagram_webhook_signature(raw_body: bytes, provided_signature: str | None, app_secret: str) -> bool:
    if not app_secret:
        return False
    if not provided_signature or "=" not in provided_signature:
        return False
    prefix, signature = provided_signature.split("=", 1)
    if prefix == "sha256":
        expected = hmac.new(app_secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    elif prefix == "sha1":
        expected = hmac.new(app_secret.encode("utf-8"), raw_body, hashlib.sha1).hexdigest()
    else:
        return False
    return hmac.compare_digest(expected, signature)


def _extract_actor(value: dict[str, Any]) -> tuple[str | None, str | None]:
    for candidate in (value.get("from"), value.get("user"), value.get("sender"), value.get("author")):
        if isinstance(candidate, dict):
            user_id = str(candidate.get("id") or candidate.get("user_id") or "").strip() or None
            username = str(candidate.get("username") or candidate.get("name") or "").strip() or None
            if user_id or username:
                return user_id, username
    user_id = str(value.get("from_id") or value.get("user_id") or "").strip() or None
    username = str(value.get("username") or value.get("user_name") or "").strip() or None
    return user_id, username


def _extract_recipient(value: dict[str, Any]) -> tuple[str | None, str | None]:
    recipient = value.get("recipient")
    if isinstance(recipient, dict):
        user_id = str(recipient.get("id") or recipient.get("user_id") or "").strip() or None
        username = str(recipient.get("username") or recipient.get("name") or "").strip() or None
        if user_id or username:
            return user_id, username
    user_id = str(value.get("recipient_id") or "").strip() or None
    username = str(value.get("recipient_username") or "").strip() or None
    return user_id, username


def _webhook_event_type(field: str, value: dict[str, Any]) -> str:
    lowered = field.lower()
    if "comment" in lowered or value.get("item") == "comment":
        if lowered == "live_comments":
            return "live_comment"
        return "comment"
    if "mention" in lowered or str(value.get("mention_type") or "").lower() == "story":
        return "story_mention"
    if "like" in lowered or str(value.get("item") or "").lower() == "like":
        return "like"
    if "share" in lowered or "repost" in lowered or str(value.get("item") or "").lower() in {"share", "repost"}:
        return "share"
    known_field_types = {
        "messages": "message",
        "message_edit": "message_edit",
        "message_reactions": "message_reaction",
        "messaging_postbacks": "message_postback",
        "messaging_referral": "message_referral",
        "messaging_seen": "message_seen",
        "messaging_handover": "message_handover",
        "messaging_optins": "message_optin",
        "standby": "standby",
        "agentic_message": "agentic_message",
    }
    if lowered in known_field_types:
        return known_field_types[lowered]
    return lowered or "unknown"


def _provider_object_id(value: dict[str, Any]) -> str | None:
    for key in ("comment_id", "story_id", "id", "media_id", "post_id", "object_id", "creation_id"):
        candidate = str(value.get(key) or "").strip()
        if candidate:
            return candidate
    for nested_key in ("message", "reaction", "postback", "agentic_message"):
        nested_value = value.get(nested_key)
        if isinstance(nested_value, dict):
            for key in ("mid", "id"):
                candidate = str(nested_value.get(key) or "").strip()
                if candidate:
                    return candidate
    return None


def _message_attachments(value: dict[str, Any]) -> list[dict[str, Any]]:
    message = value.get("message")
    if not isinstance(message, dict):
        return []
    attachments = message.get("attachments")
    if not isinstance(attachments, list):
        return []
    return [dict(item) for item in attachments if isinstance(item, dict)]


def _message_attachment_types(value: dict[str, Any]) -> list[str]:
    attachment_types: list[str] = []
    for attachment in _message_attachments(value):
        attachment_type = str(attachment.get("type") or "").strip().lower()
        if attachment_type and attachment_type not in attachment_types:
            attachment_types.append(attachment_type)
    return attachment_types


def _shared_instagram_media_id(value: dict[str, Any]) -> str | None:
    for attachment in _message_attachments(value):
        payload = attachment.get("payload")
        if not isinstance(payload, dict):
            continue
        for key in ("ig_post_media_id", "media_id", "post_id"):
            candidate = str(payload.get(key) or "").strip()
            if candidate:
                return candidate
    return None


def _is_shared_post_message(value: dict[str, Any]) -> bool:
    attachment_types = set(_message_attachment_types(value))
    return bool(_shared_instagram_media_id(value)) and bool(attachment_types.intersection({"share", "ig_post"}))


def _is_shared_post_event(event_type: str, value: dict[str, Any]) -> bool:
    return event_type == "message" and _is_shared_post_message(value)


def _giveaway_window_accepts_event(giveaway: GiveawayCampaign, *, occurred_at: datetime | None) -> bool:
    if occurred_at is None:
        return True
    published_at = normalize_datetime(giveaway.post.published_at)
    giveaway_end_at = normalize_datetime(giveaway.giveaway_end_at)
    if published_at and occurred_at < published_at:
        return False
    return giveaway_end_at is None or occurred_at <= giveaway_end_at


def _instagram_permalink_key(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlsplit(raw)
    if parsed.netloc:
        return f"{parsed.netloc.lower()}{parsed.path.rstrip('/')}"
    return raw.rstrip("/").lower()


def _instagram_account_provider_id_candidates(account: Account) -> set[str]:
    credentials = dict(account.credentials_json or {})
    candidates = {
        str(account.id or "").strip(),
        str(credentials.get("instagram_user_id") or "").strip(),
        str(credentials.get("provider_account_id") or "").strip(),
        str(credentials.get("professional_account_id") or "").strip(),
        str(credentials.get("ig_user_id") or "").strip(),
    }
    return {candidate for candidate in candidates if candidate}


def _instagram_accounts_for_webhook_account(session: Session, account_id: str | None) -> list[Account]:
    normalized = str(account_id or "").strip()
    if not normalized:
        return []
    accounts = list(session.scalars(select(Account).where(Account.service == "instagram")))
    return [
        account
        for account in accounts
        if normalized in _instagram_account_provider_id_candidates(account)
    ]


def _active_instagram_giveaway_channels(
    session: Session,
    *,
    account_ids: list[str] | None = None,
) -> list[GiveawayChannel]:
    stmt = (
        select(GiveawayChannel)
        .join(GiveawayChannel.campaign)
        .where(
            GiveawayChannel.service == "instagram",
            GiveawayCampaign.status.in_([GIVEAWAY_STATUS_SCHEDULED, GIVEAWAY_STATUS_COLLECTING, GIVEAWAY_STATUS_REVIEW_REQUIRED]),
        )
        .order_by(GiveawayCampaign.giveaway_end_at.asc())
    )
    if account_ids:
        stmt = stmt.where(GiveawayChannel.account_id.in_(account_ids))
    return list(session.scalars(stmt))


def _instagram_channel_post_url_candidates(channel: GiveawayChannel) -> list[str]:
    urls = [str(channel.target_post_url or "").strip()]
    post = channel.campaign.post if channel.campaign else None
    if post:
        for job in post.delivery_jobs:
            if job.target_account_id == channel.account_id and job.status == "posted":
                urls.append(str(job.external_url or "").strip())
    return [url for url in urls if url]


def _find_instagram_channel_by_permalink(
    session: Session,
    permalink: str | None,
    *,
    account_ids: list[str] | None = None,
) -> GiveawayChannel | None:
    target_key = _instagram_permalink_key(permalink)
    if not target_key:
        return None
    for channel in _active_instagram_giveaway_channels(session, account_ids=account_ids):
        for candidate in _instagram_channel_post_url_candidates(channel):
            if _instagram_permalink_key(candidate) == target_key:
                return channel
    return None


def _find_instagram_channel_for_event(
    session: Session,
    account_id: str | None,
    provider_object_id: str | None,
) -> GiveawayChannel | None:
    account_candidates = _instagram_accounts_for_webhook_account(session, account_id)
    account_ids = [account.id for account in account_candidates]
    normalized_provider_object_id = str(provider_object_id or "").strip()

    if normalized_provider_object_id:
        stmt = (
            select(GiveawayChannel)
            .join(GiveawayChannel.campaign)
            .join(GiveawayCampaign.post)
            .join(CanonicalPost.delivery_jobs)
            .where(
                GiveawayChannel.service == "instagram",
                GiveawayCampaign.status.in_([GIVEAWAY_STATUS_SCHEDULED, GIVEAWAY_STATUS_COLLECTING, GIVEAWAY_STATUS_REVIEW_REQUIRED]),
                DeliveryJob.external_id == normalized_provider_object_id,
                DeliveryJob.status == "posted",
                DeliveryJob.target_account_id == GiveawayChannel.account_id,
            )
        )
        if account_ids:
            stmt = stmt.where(GiveawayChannel.account_id.in_(account_ids))
        channel = session.scalar(stmt)
        if channel:
            return channel

        stmt = (
            select(GiveawayChannel)
            .join(GiveawayChannel.campaign)
            .where(
                GiveawayChannel.service == "instagram",
                GiveawayCampaign.status.in_([GIVEAWAY_STATUS_SCHEDULED, GIVEAWAY_STATUS_COLLECTING, GIVEAWAY_STATUS_REVIEW_REQUIRED]),
                GiveawayChannel.target_post_external_id == normalized_provider_object_id,
            )
        )
        if account_ids:
            stmt = stmt.where(GiveawayChannel.account_id.in_(account_ids))
        channel = session.scalar(stmt)
        if channel:
            return channel

        for account in account_candidates:
            media_match = _instagram_graph_media_match(account, media_id=normalized_provider_object_id)
            if not media_match:
                continue
            channel = _find_instagram_channel_by_permalink(
                session,
                str(media_match.get("href") or "").strip() or None,
                account_ids=[account.id],
            )
            if channel:
                return channel

    if account_ids:
        channels = _active_instagram_giveaway_channels(session, account_ids=account_ids)
        if len(channels) == 1:
            return channels[0]
    elif account_id:
        channels = _active_instagram_giveaway_channels(session, account_ids=[account_id])
        if len(channels) == 1:
            return channels[0]
    return None


def _entry_for_user(channel: GiveawayChannel, instagram_user_id: str, instagram_username: str | None) -> GiveawayEntrant:
    for entry in channel.entrants:
        if entry.provider_user_id == instagram_user_id:
            if instagram_username:
                entry.provider_username = instagram_username
                entry.display_label = instagram_username
            return entry
    entry = GiveawayEntrant(
        channel=channel,
        provider_user_id=instagram_user_id,
        provider_username=instagram_username,
        display_label=instagram_username or instagram_user_id,
    )
    channel.entrants.append(entry)
    return entry


def _comment_payload_summary(
    value: dict[str, Any],
    *,
    source: str = COMMENT_EVIDENCE_SOURCE_WEBHOOK,
) -> dict[str, Any]:
    return {
        "comment_id": str(value.get("id") or value.get("comment_id") or "").strip() or None,
        "text": str(value.get("text") or "").strip(),
        "created_time": str(value.get("created_time") or value.get("timestamp") or "").strip() or None,
        "source": source,
    }


def _webhook_text_value(value: dict[str, Any]) -> str | None:
    for key in ("text", "caption", "title"):
        candidate = str(value.get(key) or "").strip()
        if candidate:
            return candidate
    message_value = value.get("message")
    if isinstance(message_value, dict):
        candidate = str(message_value.get("text") or "").strip()
        if candidate:
            return candidate
        for attachment in _message_attachments(value):
            payload = attachment.get("payload")
            if not isinstance(payload, dict):
                continue
            for key in ("title", "caption"):
                candidate = str(payload.get(key) or "").strip()
                if candidate:
                    return candidate
    agentic_value = value.get("agentic_message")
    if isinstance(agentic_value, dict):
        candidate = str(agentic_value.get("text") or "").strip()
        if candidate:
            return candidate
    scalar_message = value.get("message")
    if isinstance(scalar_message, str) and scalar_message.strip():
        return scalar_message.strip()
    return None


def _story_mention_payload_summary(
    value: dict[str, Any],
    *,
    source: str = STORY_EVIDENCE_SOURCE_WEBHOOK,
) -> dict[str, Any]:
    summary = {
        "story_id": str(value.get("story_id") or value.get("id") or _provider_object_id(value) or "").strip() or None,
        "media_id": str(value.get("media_id") or value.get("post_id") or _shared_instagram_media_id(value) or "").strip() or None,
        "text": str(value.get("text") or value.get("caption") or "").strip() or None,
        "created_time": str(value.get("created_time") or value.get("timestamp") or "").strip() or None,
        "source": source,
    }
    return summary


def _normalize_evidence_items(items: list[dict[str, Any]] | None, *, default_source: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        payload = dict(item)
        payload["source"] = str(payload.get("source") or "").strip() or default_source
        normalized.append(payload)
    return normalized


def _recompute_entry_comment_metrics(entry: InstagramGiveawayEntry) -> None:
    entry.comments_json = _normalize_evidence_items(entry.comments_json, default_source=COMMENT_EVIDENCE_SOURCE_WEBHOOK)
    combined_text = " ".join(str(item.get("text") or "") for item in entry.comments_json if isinstance(item, dict))
    entry.comment_count = len(entry.comments_json or [])
    entry.mention_count = len({match.lower() for match in MENTION_PATTERN.findall(combined_text)})


def _normalize_story_mention_evidence(entry: InstagramGiveawayEntry) -> None:
    entry.story_mentions_json = _normalize_evidence_items(
        entry.story_mentions_json,
        default_source=STORY_EVIDENCE_SOURCE_WEBHOOK,
    )


def _comment_summary_from_live_comment(comment: Any) -> dict[str, Any]:
    created_at = getattr(comment, "created_at_utc", None)
    created_time = None
    if isinstance(created_at, datetime):
        created_time = normalize_datetime(created_at).isoformat()
    return {
        "comment_id": str(getattr(comment, "pk", "") or "").strip() or None,
        "text": str(getattr(comment, "text", "") or "").strip(),
        "created_time": created_time,
        "source": COMMENT_EVIDENCE_SOURCE_LIVE,
    }


def _refresh_giveaway_comment_evidence(giveaway: InstagramGiveaway) -> tuple[str, list[str]]:
    for entry in giveaway.entries:
        _recompute_entry_comment_metrics(entry)
        _normalize_story_mention_evidence(entry)

    media_job = _instagram_media_job(giveaway)
    media_id = str(media_job.external_id or "").strip() if media_job else ""
    if not media_id:
        return "captured_comment_fallback", [
            "Live comment revalidation was skipped because the Instagram media ID was unavailable.",
        ]

    dependency_issue = _instagram_destination_dependency_issue()
    if dependency_issue:
        return "captured_comment_fallback", [dependency_issue]

    try:
        client = _authenticated_publish_client(giveaway.instagram_account.credentials_json or {})
    except Exception as exc:
        return "captured_comment_fallback", [f"Live comment revalidation login failed: {exc}"]

    try:
        live_comments = client.media_comments(media_id, amount=0)
    except Exception as exc:
        return "captured_comment_fallback", [f"Live comment revalidation failed: {exc}"]

    preserved_story_mentions = {
        entry.instagram_user_id: list(entry.story_mentions_json or [])
        for entry in giveaway.entries
    }
    preserved_usernames = {
        entry.instagram_user_id: str(entry.instagram_username or "").strip() or None
        for entry in giveaway.entries
    }

    for entry in giveaway.entries:
        entry.comments_json = []
        entry.comment_count = 0
        entry.mention_count = 0

    for comment in live_comments or []:
        user = getattr(comment, "user", None)
        instagram_user_id = str(getattr(user, "pk", "") or "").strip()
        instagram_username = str(getattr(user, "username", "") or "").strip() or None
        if not instagram_user_id:
            continue
        entry = _entry_for_user(
            giveaway,
            instagram_user_id,
            instagram_username or preserved_usernames.get(instagram_user_id),
        )
        comments = list(entry.comments_json or [])
        summary = _comment_summary_from_live_comment(comment)
        comment_id = summary.get("comment_id")
        if comment_id and any(existing.get("comment_id") == comment_id for existing in comments if isinstance(existing, dict)):
            continue
        comments.append(summary)
        entry.comments_json = comments

    for entry in giveaway.entries:
        if entry.instagram_user_id in preserved_story_mentions:
            entry.story_mentions_json = _normalize_evidence_items(
                preserved_story_mentions.get(entry.instagram_user_id),
                default_source=STORY_EVIDENCE_SOURCE_WEBHOOK,
            )
        _recompute_entry_comment_metrics(entry)

    return "live_comment_revalidation", []


def _story_mention_evidence_mode(giveaway: InstagramGiveaway) -> tuple[str, list[str]]:
    for entry in giveaway.entries:
        _normalize_story_mention_evidence(entry)
    if any(entry.story_mentions_json for entry in giveaway.entries):
        return "captured_story_mentions", [
            "Story-share verification relied on captured webhook evidence because a reliable close-time recheck is not yet available.",
        ]
    return "no_story_evidence", [
        "No captured story mention evidence was available at giveaway close.",
    ]


def _messaging_event_field(container: str, item: dict[str, Any]) -> str:
    if container == "standby":
        return "standby"
    if "agentic_message" in item:
        return "agentic_message"
    if "reaction" in item:
        return "message_reactions"
    if "postback" in item:
        return "messaging_postbacks"
    if "referral" in item:
        return "messaging_referral"
    if "optin" in item:
        return "messaging_optins"
    if any(key in item for key in ("take_thread_control", "pass_thread_control", "request_thread_control", "app_roles")):
        return "messaging_handover"
    if any(key in item for key in ("read", "delivery", "seen")):
        return "messaging_seen"
    if "message" in item:
        return "messages"
    return container or "messages"


def _iter_instagram_webhook_events(entry_payload: dict[str, Any]) -> list[dict[str, Any]]:
    account_id = str(entry_payload.get("id") or "").strip() or None
    parsed: list[dict[str, Any]] = []

    for change in entry_payload.get("changes") or []:
        if not isinstance(change, dict):
            continue
        field = str(change.get("field") or "").strip()
        raw_value = change.get("value") or {}
        value_items = raw_value if isinstance(raw_value, list) else [raw_value]
        for value_item in value_items:
            value = dict(value_item) if isinstance(value_item, dict) else {"value": value_item}
            normalized_change = dict(change)
            normalized_change["value"] = value
            parsed.append(
                {
                    "account_id": account_id,
                    "field": field,
                    "value": value,
                    "payload_json": {"entry": entry_payload, "change": normalized_change},
                }
            )

    for container in ("messaging", "standby"):
        for item in entry_payload.get(container) or []:
            if not isinstance(item, dict):
                continue
            field = _messaging_event_field(container, item)
            value = dict(item)
            parsed.append(
                {
                    "account_id": account_id,
                    "field": field,
                    "value": value,
                    "payload_json": {
                        "entry": entry_payload,
                        "container": container,
                        "event": item,
                        "change": {
                            "field": field,
                            "value": value,
                        },
                    },
                }
            )

    return parsed


def _instagram_signal_state(entrant: GiveawayEntrant) -> dict[str, Any]:
    state = dict(entrant.signal_state_json or {})
    comments = _normalize_evidence_items(state.get("comments"), default_source=COMMENT_EVIDENCE_SOURCE_WEBHOOK)
    story_mentions = _normalize_evidence_items(state.get("story_mentions"), default_source=STORY_EVIDENCE_SOURCE_WEBHOOK)
    combined_text = " ".join(str(item.get("text") or "") for item in comments if isinstance(item, dict))
    return {
        "comments": comments,
        "comment_count": len(comments),
        "friend_mention_count": len({match.lower() for match in MENTION_PATTERN.findall(combined_text)}),
        "story_mentions": story_mentions,
        "story_mention_count": len(story_mentions),
    }


def ingest_instagram_webhook_payload(
    session: Session,
    payload: dict[str, Any],
    *,
    signature_valid: bool,
    run_id: str | None = None,
) -> list[InstagramGiveawayWebhookEvent]:
    stored_events: list[InstagramGiveawayWebhookEvent] = []
    if not signature_valid:
        return stored_events
    for entry_payload in payload.get("entry") or []:
        if not isinstance(entry_payload, dict):
            continue
        for event_payload in _iter_instagram_webhook_events(entry_payload):
            account_id = event_payload["account_id"]
            field = event_payload["field"]
            value = event_payload["value"]
            event_type = _webhook_event_type(field, value)
            provider_object_id = _provider_object_id(value)
            related_media_id = _webhook_media_id(value)
            channel = _find_instagram_channel_for_event(session, account_id, related_media_id or provider_object_id)
            campaign = channel.campaign if channel else None
            if campaign:
                hydrate_channel_targets(campaign)
            counts_as_story_share = event_type == "message" and _is_shared_post_message(value)
            webhook_event = InstagramGiveawayWebhookEvent(
                matched_giveaway_id=campaign.id if campaign else None,
                matched_post_id=campaign.post_id if campaign else None,
                matched_account_id=channel.account_id if channel else None,
                provider_object_id=provider_object_id,
                provider_event_field=field or None,
                event_type=event_type,
                payload_json=event_payload["payload_json"],
                signature_valid=True,
                processed=False,
            )
            session.add(webhook_event)
            session.flush()
            stored_events.append(webhook_event)

            captured_activities: list[str] = []
            if channel is not None and campaign is not None:
                captured_activities = sync_instagram_webhook_event_to_channel(session, channel, webhook_event)
            if not captured_activities:
                webhook_event.processed = True
                webhook_event.processed_at = utcnow()
                continue

            if run_id:
                captured_event_type = ", ".join(activity.replace("_", " ") for activity in captured_activities)
                instagram_user_id, _ = _extract_actor(value)
                log_run_event(
                    session,
                    run_id=run_id,
                    persona_id=campaign.post.persona_id,
                    persona_name=campaign.post.persona.name if campaign.post.persona else None,
                    account_id=channel.account_id,
                    service="instagram",
                    operation="giveaway_webhook",
                    message=f"Captured Instagram {captured_event_type} evidence for giveaway post {campaign.post_id}.",
                    post_id=campaign.post_id,
                    metadata={
                        "event_type": event_type,
                        "instagram_user_id": instagram_user_id,
                        "captured_activities": captured_activities,
                        "story_share_inferred_from_message": counts_as_story_share and event_type != "story_mention",
                    },
                )
    session.flush()
    if stored_events:
        publish_live_update(
            LIVE_UPDATE_TOPIC_INSTAGRAM_WEBHOOKS,
            LIVE_UPDATE_TOPIC_DASHBOARD,
            LIVE_UPDATE_TOPIC_LOGS,
        )
    return stored_events


def _webhook_label(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Unknown"
    return raw.replace("_", " ").title()


def _webhook_display_field_label(field: str | None, event_type: str, value: dict[str, Any]) -> str:
    if _is_shared_post_event(event_type, value):
        return "Shared Post"
    if event_type == "story_mention":
        return "Story Mention"
    return _webhook_label(field)


def _webhook_display_event_type_label(event_type: str, value: dict[str, Any]) -> str:
    if _is_shared_post_event(event_type, value):
        return "Shared Post"
    if event_type == "story_mention":
        return "Story Mention"
    return _webhook_label(event_type)


def _webhook_account_context_label(event_type: str, value: dict[str, Any]) -> str:
    if _is_shared_post_event(event_type, value):
        return "Account"
    if event_type.startswith("message") or event_type in {"agentic_message", "standby"}:
        return "Recipient"
    return "Account"


def _webhook_actor_context_label(event_type: str, value: dict[str, Any]) -> str:
    if _is_shared_post_event(event_type, value):
        return "Shared By"
    if event_type.startswith("message") or event_type in {"agentic_message", "standby"}:
        return "Sender"
    return "Actor"


def _webhook_provider_object_label(event_type: str, value: dict[str, Any]) -> str:
    if _is_shared_post_event(event_type, value):
        return "Provider Share Message ID"
    if event_type.startswith("message") or event_type in {"agentic_message", "standby"}:
        return "Provider Message ID"
    return "Provider Event ID"


def _webhook_value_payload(event: InstagramGiveawayWebhookEvent) -> dict[str, Any]:
    payload = dict(event.payload_json or {})
    change = payload.get("change")
    if not isinstance(change, dict):
        return {}
    value = change.get("value")
    return dict(value) if isinstance(value, dict) else {}


def _webhook_text_preview(value: dict[str, Any], *, max_chars: int = 120) -> str | None:
    candidate = _webhook_text_value(value)
    if candidate:
        if len(candidate) <= max_chars:
            return candidate
        return candidate[: max_chars - 1].rstrip() + "…"
    return None


def _account_display_label(account: Account) -> str:
    credentials = dict(account.credentials_json or {})
    persona_name = account.persona.name if account.persona else None
    instagram_username = (
        str(credentials.get("instagram_username") or "").strip()
        or str(credentials.get("instagrapi_username") or "").strip()
        or str(credentials.get("login_username") or "").strip()
        or str(credentials.get("username") or "").strip()
    )
    handle = str(account.handle_or_identifier or "").strip()
    generic_instagram_handles = {"instagram graph api", "instagram"}

    if account.service == "instagram":
        if persona_name and instagram_username:
            return f"{persona_name} (@{instagram_username})"
        if instagram_username:
            return f"@{instagram_username}"
        if persona_name and (account.label.lower() == "instagram" or handle.lower() in generic_instagram_handles):
            return persona_name

    if handle and handle != account.label and handle.lower() not in generic_instagram_handles:
        return f"{account.label} ({handle})"
    return account.label


def _account_instagram_username(account: Account) -> str | None:
    credentials = dict(account.credentials_json or {})
    username = (
        str(credentials.get("instagram_username") or "").strip()
        or str(credentials.get("instagrapi_username") or "").strip()
        or str(credentials.get("login_username") or "").strip()
        or str(credentials.get("username") or "").strip()
    )
    return username or None


def _account_instagram_profile_href(account: Account) -> str | None:
    username = _account_instagram_username(account)
    if username:
        return f"https://www.instagram.com/{username}/"
    return None


def _account_graph_token(account: Account | None) -> str | None:
    if account is None:
        return None
    credentials = dict(account.credentials_json or {})
    token = str(credentials.get("api_key") or "").strip()
    return token or None


def _account_graph_user_id(account: Account | None) -> str | None:
    if account is None:
        return None
    credentials = dict(account.credentials_json or {})
    for key in ("instagram_user_id", "provider_account_id", "professional_account_id", "ig_user_id"):
        candidate = str(credentials.get(key) or "").strip()
        if candidate:
            return candidate
    return None


def _account_username_candidates(account: Account) -> list[str]:
    candidates: list[str] = []
    for raw in (
        _account_instagram_username(account),
        str(account.handle_or_identifier or "").strip(),
        str(account.label or "").strip(),
        str(account.persona.name if account.persona else "").strip(),
    ):
        value = str(raw or "").strip().lstrip("@")
        if not value:
            continue
        lowered = value.lower()
        if lowered not in candidates:
            candidates.append(lowered)
    return candidates


def _looks_like_instagram_graph_id(value: str | None) -> bool:
    normalized = str(value or "").strip()
    return bool(normalized) and normalized.isdigit()


def _cache_get(cache: dict[Any, tuple[float, Any]], key: Any) -> Any | None:
    cached = cache.get(key)
    if not cached:
        return None
    expires_at, value = cached
    if expires_at <= time.time():
        cache.pop(key, None)
        return None
    return value


def _cache_put(cache: dict[Any, tuple[float, Any]], key: Any, value: Any, *, ttl_seconds: int = INSTAGRAM_GRAPH_CACHE_TTL_SECONDS) -> Any:
    cache[key] = (time.time() + ttl_seconds, value)
    return value


def _instagram_graph_get_json(
    account: Account,
    path_or_url: str,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    token = _account_graph_token(account)
    if not token:
        return None
    request_params = dict(params or {})
    if path_or_url.startswith("https://"):
        request_url = path_or_url
    else:
        request_url = f"{INSTAGRAM_GRAPH_API_BASE_URL}/{path_or_url.lstrip('/')}"
    request_params.setdefault("access_token", token)
    try:
        response = requests.get(request_url, params=request_params, timeout=20)
    except requests.RequestException:
        return None
    if response.status_code != 200:
        return None
    try:
        data = response.json()
    except ValueError:
        return None
    if not isinstance(data, dict) or data.get("error"):
        return None
    return data


def _instagram_graph_profile(
    account: Account,
    *,
    user_id: str,
) -> dict[str, Any] | None:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return None
    cache_key = (account.id, normalized_user_id)
    cached = _cache_get(_GRAPH_PROFILE_CACHE, cache_key)
    if cached is not None:
        return cached
    data = None
    for fields in ("id,username,name,profile_pic", "id,username,name,profile_picture_url", "id,username,name"):
        data = _instagram_graph_get_json(
            account,
            normalized_user_id,
            params={"fields": fields},
        )
        if data:
            break
    if not data:
        return _cache_put(_GRAPH_PROFILE_CACHE, cache_key, None)
    username = str(data.get("username") or "").strip() or None
    profile = {
        "id": str(data.get("id") or normalized_user_id).strip() or normalized_user_id,
        "username": username,
        "name": str(data.get("name") or "").strip() or None,
        "profile_image_url": str(data.get("profile_pic") or data.get("profile_picture_url") or "").strip() or None,
        "profile_href": f"https://www.instagram.com/{username}/" if username else None,
    }
    return _cache_put(_GRAPH_PROFILE_CACHE, cache_key, profile)


def _instagram_graph_conversations(account: Account) -> list[dict[str, Any]]:
    cached = _cache_get(_GRAPH_CONVERSATION_CACHE, account.id)
    if cached is not None:
        return cached
    account_user_id = _account_graph_user_id(account)
    if not account_user_id:
        return _cache_put(_GRAPH_CONVERSATION_CACHE, account.id, [])
    conversations: list[dict[str, Any]] = []
    next_url: str | None = f"{INSTAGRAM_GRAPH_API_BASE_URL}/{account_user_id}/conversations"
    params: dict[str, Any] | None = {
        "fields": "id,updated_time,participants,messages.limit(25){id,message,from,created_time,to}",
        "limit": 50,
    }
    for _ in range(4):
        data = _instagram_graph_get_json(account, next_url, params=params)
        if not data:
            break
        for item in data.get("data") or []:
            if isinstance(item, dict):
                conversations.append(item)
        paging = data.get("paging") or {}
        next_candidate = str(paging.get("next") or "").strip() or None
        if not next_candidate:
            break
        next_url = next_candidate
        params = None
    return _cache_put(_GRAPH_CONVERSATION_CACHE, account.id, conversations)


def _instagram_graph_conversation_match(
    account: Account,
    *,
    sender_id: str | None,
    message_id: str | None,
) -> dict[str, Any] | None:
    normalized_sender_id = str(sender_id or "").strip() or None
    normalized_message_id = str(message_id or "").strip() or None
    if not normalized_sender_id and not normalized_message_id:
        return None
    for conversation in _instagram_graph_conversations(account):
        participants = ((conversation.get("participants") or {}).get("data") or [])
        participant_ids = {
            str(item.get("id") or "").strip()
            for item in participants
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        }
        messages = ((conversation.get("messages") or {}).get("data") or [])
        message_ids = {
            str(item.get("id") or "").strip()
            for item in messages
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        }
        if normalized_message_id and normalized_message_id in message_ids:
            return conversation
        if normalized_sender_id and normalized_sender_id in participant_ids:
            return conversation
    return None


def _instagram_graph_recent_media(account: Account) -> list[dict[str, Any]]:
    cached = _cache_get(_GRAPH_MEDIA_CACHE, account.id)
    if cached is not None:
        return cached
    account_user_id = _account_graph_user_id(account)
    if not account_user_id:
        return _cache_put(_GRAPH_MEDIA_CACHE, account.id, [])
    media_items: list[dict[str, Any]] = []
    next_url: str | None = f"{INSTAGRAM_GRAPH_API_BASE_URL}/{account_user_id}/media"
    params: dict[str, Any] | None = {
        "fields": "id,caption,permalink,timestamp,media_type",
        "limit": 50,
    }
    for _ in range(4):
        data = _instagram_graph_get_json(account, next_url, params=params)
        if not data:
            break
        for item in data.get("data") or []:
            if isinstance(item, dict):
                media_items.append(item)
        paging = data.get("paging") or {}
        next_candidate = str(paging.get("next") or "").strip() or None
        if not next_candidate:
            break
        next_url = next_candidate
        params = None
    return _cache_put(_GRAPH_MEDIA_CACHE, account.id, media_items)


def _webhook_media_id(value: dict[str, Any]) -> str | None:
    for key in ("media_id", "post_id"):
        candidate = str(value.get(key) or "").strip()
        if candidate:
            return candidate
    candidate = _shared_instagram_media_id(value)
    if candidate:
        return candidate
    media = value.get("media")
    if isinstance(media, dict):
        candidate = str(media.get("id") or "").strip()
        if candidate:
            return candidate
    candidate = str(value.get("parent_id") or "").strip()
    if candidate:
        return candidate
    return None


def _instagram_graph_media_match(account: Account, *, media_id: str | None) -> dict[str, Any] | None:
    normalized_media_id = str(media_id or "").strip() or None
    if not normalized_media_id:
        return None
    for media in _instagram_graph_recent_media(account):
        if str(media.get("id") or "").strip() == normalized_media_id:
            return {
                "id": normalized_media_id,
                "href": str(media.get("permalink") or "").strip() or None,
                "label": _post_excerpt(str(media.get("caption") or "").strip() or "Instagram post"),
                "timestamp": media.get("timestamp"),
                "media_type": media.get("media_type"),
            }
    return None


def _instagram_account_username_context(session: Session) -> dict[str, dict[str, Any]]:
    stmt = (
        select(Account)
        .options(selectinload(Account.persona))
        .where(Account.service == "instagram")
    )
    context: dict[str, dict[str, Any]] = {}
    for account in session.scalars(stmt):
        base = {
            "account_id": account.id,
            "label": account.label,
            "handle_or_identifier": account.handle_or_identifier,
            "service": account.service,
            "persona_name": account.persona.name if account.persona else None,
            "display_label": _account_display_label(account),
            "profile_href": _account_instagram_profile_href(account),
        }
        for username in _account_username_candidates(account):
            context.setdefault(username, base)
    return context


def _profile_party_label(profile: dict[str, Any] | None) -> str | None:
    if not profile:
        return None
    username = str(profile.get("username") or "").strip()
    name = str(profile.get("name") or "").strip()
    if username and name and name.lower() != username.lower():
        return f"{name} (@{username})"
    if username:
        return f"@{username}"
    if name:
        return name
    return None


def _local_profile_party_label(local_account: dict[str, Any] | None, profile: dict[str, Any] | None) -> str | None:
    if not local_account:
        return _profile_party_label(profile)
    display_label = str(local_account.get("display_label") or "").strip()
    username = str((profile or {}).get("username") or "").strip()
    persona_name = str(local_account.get("persona_name") or "").strip()
    if username and f"@{username.lower()}" not in display_label.lower():
        if persona_name:
            return f"{persona_name} (@{username})"
        label = str(local_account.get("label") or "").strip() or display_label
        return f"{label} (@{username})"
    return display_label or _profile_party_label(profile)


def _profile_username(profile: dict[str, Any] | None) -> str | None:
    username = str((profile or {}).get("username") or "").strip().lower()
    return username or None


def _local_account_from_profile(
    profile: dict[str, Any] | None,
    *,
    username_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    username = _profile_username(profile)
    if not username:
        return None
    account = dict(username_lookup.get(username, {}))
    return account or None


def _event_graph_enrichment(
    session: Session,
    event: InstagramGiveawayWebhookEvent,
    *,
    account_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    value = _webhook_value_payload(event)
    actor_id, actor_username = _extract_actor(value)
    recipient_id, recipient_username = _extract_recipient(value)
    payload = dict(event.payload_json or {})
    entry = payload.get("entry")
    provider_account_id = str(entry.get("id") or "").strip() if isinstance(entry, dict) else ""
    is_message_event = event.event_type.startswith("message") or event.event_type in {"agentic_message", "standby"}
    is_comment_event = event.event_type in {"comment", "live_comment", "story_mention"}
    recipient_account_dict = (
        dict(account_lookup.get(recipient_id or "", {}))
        or dict(account_lookup.get(provider_account_id or "", {}))
        or dict(account_lookup.get(event.matched_account_id or "", {}))
    )
    recipient_account = session.get(Account, recipient_account_dict.get("account_id")) if recipient_account_dict.get("account_id") else None
    if recipient_account is None or recipient_account.service != "instagram" or not _account_graph_token(recipient_account):
        return {}

    enrichment: dict[str, Any] = {}
    actor_graph_id = actor_id if _looks_like_instagram_graph_id(actor_id) else None
    should_lookup_actor_profile = bool(actor_graph_id) and (
        is_message_event
        or is_comment_event
        or not actor_username
        or not event.matched_post_id
    )
    actor_profile = (
        _instagram_graph_profile(recipient_account, user_id=actor_graph_id or "")
        if should_lookup_actor_profile and actor_graph_id
        else None
    )
    if is_message_event:
        recipient_graph_id = (
            recipient_id
            if _looks_like_instagram_graph_id(recipient_id)
            else provider_account_id
            if _looks_like_instagram_graph_id(provider_account_id)
            else _account_graph_user_id(recipient_account)
        )
    else:
        recipient_graph_id = recipient_id if _looks_like_instagram_graph_id(recipient_id) else None
    should_lookup_recipient_profile = bool(recipient_graph_id) and (
        is_message_event or (not recipient_username and bool(recipient_id))
    )
    recipient_profile = (
        _instagram_graph_profile(recipient_account, user_id=recipient_graph_id or "")
        if should_lookup_recipient_profile and recipient_graph_id
        else None
    )
    if actor_profile:
        enrichment["actor_profile"] = actor_profile
    if recipient_profile:
        enrichment["recipient_profile"] = recipient_profile

    if event.event_type.startswith("message") or event.event_type in {"agentic_message", "standby"}:
        conversation = _instagram_graph_conversation_match(
            recipient_account,
            sender_id=actor_id,
            message_id=event.provider_object_id,
        )
        if conversation:
            conversation_id = str(conversation.get("id") or "").strip() or None
            if conversation_id:
                enrichment["conversation"] = {
                    "id": conversation_id,
                    "href": f"https://www.instagram.com/direct/t/{conversation_id}/",
                    "href_label": "Open Instagram conversation",
                    "updated_time": conversation.get("updated_time"),
                }
    elif (is_comment_event or (event.event_type == "message" and _is_shared_post_message(value))) and not event.matched_post_id:
        media_match = _instagram_graph_media_match(
            recipient_account,
            media_id=_webhook_media_id(value),
        )
        if media_match:
            enrichment["related_media"] = media_match
    return enrichment

def _local_account_party_label(account: dict[str, Any] | None) -> str | None:
    if not account:
        return None
    display_label = str(account.get("display_label") or "").strip()
    if display_label:
        return display_label
    label = str(account.get("label") or "").strip()
    if label:
        return label
    return None


def _actor_party_label(*, actor_local_account: dict[str, Any] | None, actor_username: str | None, actor_id: str | None) -> str:
    if actor_local_account:
        return _local_account_party_label(actor_local_account) or "a linked Instagram account"
    if actor_username:
        return f"@{actor_username}"
    if actor_id:
        return f"Instagram user {actor_id}"
    return "another Instagram user"


def _recipient_party_label(*, recipient_local_account: dict[str, Any] | None, recipient_username: str | None, recipient_id: str | None) -> str:
    if recipient_local_account:
        return _local_account_party_label(recipient_local_account) or "a linked Instagram account"
    if recipient_username:
        return f"@{recipient_username}"
    if recipient_id == "0":
        return "Meta's webhook test account"
    if recipient_id:
        return f"Instagram account {recipient_id}"
    return "an Instagram account"


def _webhook_thread_id(value: dict[str, Any]) -> str | None:
    for key in ("thread_id", "conversation_id"):
        candidate = str(value.get(key) or "").strip()
        if candidate:
            return candidate
    for key in ("thread", "conversation"):
        nested = value.get(key)
        if isinstance(nested, dict):
            candidate = str(nested.get("id") or nested.get("thread_id") or "").strip()
            if candidate:
                return candidate
    return None


def _instagram_message_link(event_type: str, value: dict[str, Any]) -> tuple[str | None, str | None]:
    if not event_type.startswith("message") and event_type not in {"agentic_message", "standby"}:
        return None, None
    thread_id = _webhook_thread_id(value)
    if thread_id:
        return f"https://www.instagram.com/direct/t/{thread_id}/", "Open Instagram conversation"
    return "https://www.instagram.com/direct/inbox/", "Open Instagram inbox"


def _instagram_activity_link(event_type: str, parent_post: dict[str, Any] | None, *, value: dict[str, Any] | None = None) -> tuple[str | None, str | None]:
    if not parent_post:
        return None, None
    instagram_external_url = str(parent_post.get("instagram_external_url") or "").strip()
    if not instagram_external_url:
        return None, None
    if event_type == "message" and value and _is_shared_post_message(value):
        return instagram_external_url, "Open shared Instagram post"
    if event_type == "comment":
        return instagram_external_url, "Open Instagram post"
    if event_type == "like":
        return instagram_external_url, "Open Instagram post"
    if event_type == "share":
        return instagram_external_url, "Open shared Instagram post"
    if event_type == "live_comment":
        return instagram_external_url, "Open live post"
    if event_type == "story_mention":
        return instagram_external_url, "Open mentioned post"
    return None, None


def _related_media_context(media: dict[str, Any] | None) -> dict[str, Any] | None:
    if not media:
        return None
    href = str(media.get("href") or "").strip() or None
    label = str(media.get("label") or "").strip() or "Instagram post"
    if not href and not label:
        return None
    return {
        "href": href,
        "label": label,
        "media_id": str(media.get("id") or "").strip() or None,
        "timestamp": media.get("timestamp"),
        "media_type": media.get("media_type"),
    }


def _webhook_summary_text(
    *,
    event_type: str,
    field_label: str,
    actor_label: str,
    recipient_label: str,
    parent_post: dict[str, Any] | None,
    related_post_label: str | None = None,
    value: dict[str, Any] | None = None,
) -> str:
    if event_type == "message":
        post_label = (
            str((parent_post or {}).get("label") or "").strip()
            or str(related_post_label or "").strip()
            or None
        )
        if value and _is_shared_post_message(value):
            if post_label:
                return f"{recipient_label} received a shared Instagram post from {actor_label} for {post_label}."
            return f"{recipient_label} received a shared Instagram post from {actor_label}."
        return f"{recipient_label} received a direct message from {actor_label}."
    if event_type == "message_edit":
        return f"{recipient_label} received an edited direct message from {actor_label}."
    if event_type == "message_reaction":
        return f"{recipient_label} received a direct message reaction from {actor_label}."
    if event_type == "message_postback":
        return f"{recipient_label} received an Instagram postback from {actor_label}."
    if event_type == "message_referral":
        return f"{recipient_label} received an Instagram referral event from {actor_label}."
    if event_type == "message_seen":
        return f"{recipient_label}'s conversation was marked as seen by {actor_label}."
    if event_type == "message_handover":
        return f"{recipient_label} received an Instagram handover event."
    if event_type == "message_optin":
        return f"{recipient_label} received an Instagram opt-in event from {actor_label}."
    if event_type == "comment":
        post_label = (
            str((parent_post or {}).get("label") or "").strip()
            or str(related_post_label or "").strip()
            or None
        )
        if post_label:
            return (
                f"{recipient_label} received a new comment from {actor_label} on "
                f"{post_label}."
            )
        return f"{recipient_label} received a new comment from {actor_label}."
    if event_type == "live_comment":
        return f"{recipient_label} received a new live comment from {actor_label}."
    if event_type == "story_mention":
        return f"{recipient_label} was mentioned in an Instagram story by {actor_label}."
    if event_type == "agentic_message":
        return f"{recipient_label} received an Instagram AI message event."
    if event_type == "standby":
        return f"{recipient_label} received a standby webhook event."
    return f"{recipient_label} received {field_label.lower()} activity from {actor_label}."


def _post_excerpt(body: str, *, max_chars: int = 80) -> str:
    text = " ".join(str(body or "").split())
    if not text:
        return "Untitled post"
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def _webhook_account_context(session: Session, events: list[InstagramGiveawayWebhookEvent]) -> dict[str, dict[str, Any]]:
    account_ids: set[str] = set()
    provider_ids: set[str] = set()
    for event in events:
        if event.matched_account_id:
            account_ids.add(event.matched_account_id)
        payload = dict(event.payload_json or {})
        entry = payload.get("entry")
        if isinstance(entry, dict):
            provider_account_id = str(entry.get("id") or "").strip()
            if provider_account_id:
                provider_ids.add(provider_account_id)
        value = _webhook_value_payload(event)
        for provider_id in (_extract_actor(value)[0], _extract_recipient(value)[0]):
            normalized = str(provider_id or "").strip()
            if normalized:
                provider_ids.add(normalized)
    if not account_ids and not provider_ids:
        return {}
    stmt = (
        select(Account)
        .options(selectinload(Account.persona))
    )
    if account_ids:
        stmt = stmt.where(Account.id.in_(sorted(account_ids)))
    context: dict[str, dict[str, Any]] = {}
    for account in session.scalars(stmt):
        context[account.id] = {
            "account_id": account.id,
            "label": account.label,
            "handle_or_identifier": account.handle_or_identifier,
            "service": account.service,
            "persona_name": account.persona.name if account.persona else None,
            "display_label": _account_display_label(account),
            "profile_href": _account_instagram_profile_href(account),
        }
    if provider_ids:
        provider_stmt = (
            select(Account)
            .options(selectinload(Account.persona))
            .where(Account.service == "instagram")
        )
        for account in session.scalars(provider_stmt):
            credentials = dict(account.credentials_json or {})
            provider_candidates = {
                str(credentials.get("instagram_user_id") or "").strip(),
                str(credentials.get("provider_account_id") or "").strip(),
                str(credentials.get("professional_account_id") or "").strip(),
                str(credentials.get("ig_user_id") or "").strip(),
            }
            for provider_id in provider_candidates:
                if not provider_id or provider_id not in provider_ids or provider_id in context:
                    continue
                context[provider_id] = {
                    "account_id": account.id,
                    "provider_account_id": provider_id,
                    "label": account.label,
                    "handle_or_identifier": account.handle_or_identifier,
                    "service": account.service,
                    "persona_name": account.persona.name if account.persona else None,
                    "display_label": _account_display_label(account),
                    "profile_href": _account_instagram_profile_href(account),
                }
    return context


def _webhook_parent_post_context(session: Session, events: list[InstagramGiveawayWebhookEvent]) -> dict[str, dict[str, Any]]:
    post_ids = sorted({event.matched_post_id for event in events if event.matched_post_id})
    if not post_ids:
        return {}
    stmt = (
        select(CanonicalPost)
        .options(
            selectinload(CanonicalPost.persona),
            selectinload(CanonicalPost.delivery_jobs).selectinload(DeliveryJob.target_account),
        )
        .where(CanonicalPost.id.in_(post_ids))
    )
    context: dict[str, dict[str, Any]] = {}
    for post in session.scalars(stmt):
        instagram_job = next(
            (
                job
                for job in post.delivery_jobs
                if job.target_account and job.target_account.service == "instagram"
            ),
            None,
        )
        context[post.id] = {
            "post_id": post.id,
            "href": f"/scheduled-posts/{post.id}/page" if post.origin_kind == "composer" else None,
            "label": _post_excerpt(post.body),
            "persona_name": post.persona.name if post.persona else None,
            "instagram_external_url": instagram_job.external_url if instagram_job else None,
            "instagram_external_id": instagram_job.external_id if instagram_job else None,
        }
    return context


def serialize_instagram_webhook_event(
    event: InstagramGiveawayWebhookEvent,
    *,
    post_lookup: dict[str, dict[str, Any]] | None = None,
    account_lookup: dict[str, dict[str, Any]] | None = None,
    username_lookup: dict[str, dict[str, Any]] | None = None,
    graph_enrichment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(event.payload_json or {})
    entry = payload.get("entry")
    entry_payload = dict(entry) if isinstance(entry, dict) else {}
    value = _webhook_value_payload(event)
    actor_id, actor_username = _extract_actor(value)
    recipient_id, recipient_username = _extract_recipient(value)
    field = str(event.provider_event_field or "").strip() or None
    parent_post = dict((post_lookup or {}).get(event.matched_post_id or "", {}))
    provider_account_id = str(entry_payload.get("id") or "").strip() or None
    provider_local_account = dict((account_lookup or {}).get(provider_account_id or "", {}))
    matched_local_account = dict((account_lookup or {}).get(event.matched_account_id or "", {}))
    actor_profile = dict((graph_enrichment or {}).get("actor_profile") or {})
    recipient_profile = dict((graph_enrichment or {}).get("recipient_profile") or {})
    related_media = _related_media_context((graph_enrichment or {}).get("related_media") or None)
    actor_local_account = (
        dict((account_lookup or {}).get(actor_id or "", {}))
        or dict(_local_account_from_profile(actor_profile or None, username_lookup=username_lookup or {}) or {})
    )
    recipient_local_account = (
        dict((account_lookup or {}).get(recipient_id or provider_account_id or "", {}))
        or dict(_local_account_from_profile(recipient_profile or None, username_lookup=username_lookup or {}) or {})
    )
    actor_profile_label = _profile_party_label(actor_profile or None)
    recipient_profile_label = _profile_party_label(recipient_profile or None)
    if actor_local_account and actor_profile and not actor_local_account.get("profile_href"):
        actor_local_account["profile_href"] = str(actor_profile.get("profile_href") or "").strip() or None
    if recipient_local_account and recipient_profile and not recipient_local_account.get("profile_href"):
        recipient_local_account["profile_href"] = str(recipient_profile.get("profile_href") or "").strip() or None
    actor_label = _actor_party_label(
        actor_local_account=actor_local_account or None,
        actor_username=(str(actor_profile.get("username") or "").strip() or actor_username),
        actor_id=actor_id,
    )
    if actor_local_account:
        actor_label = _local_profile_party_label(actor_local_account or None, actor_profile or None) or actor_label
    if not actor_local_account and actor_profile_label:
        actor_label = actor_profile_label
    recipient_label = _recipient_party_label(
        recipient_local_account=recipient_local_account or provider_local_account or matched_local_account or None,
        recipient_username=(str(recipient_profile.get("username") or "").strip() or recipient_username),
        recipient_id=recipient_id or provider_account_id,
    )
    if recipient_local_account:
        recipient_label = _local_profile_party_label(recipient_local_account or None, recipient_profile or None) or recipient_label
    if not (recipient_local_account or provider_local_account or matched_local_account) and recipient_profile_label:
        recipient_label = recipient_profile_label
    conversation = dict((graph_enrichment or {}).get("conversation") or {})
    chat_href, chat_href_label = (
        (str(conversation.get("href") or "").strip() or None, str(conversation.get("href_label") or "").strip() or None)
        if conversation
        else _instagram_message_link(event.event_type, value)
    )
    display_field_label = _webhook_display_field_label(field, event.event_type, value)
    display_event_type_label = _webhook_display_event_type_label(event.event_type, value)
    account_context_label = _webhook_account_context_label(event.event_type, value)
    actor_context_label = _webhook_actor_context_label(event.event_type, value)
    provider_object_label = _webhook_provider_object_label(event.event_type, value)
    activity_href, activity_href_label = _instagram_activity_link(event.event_type, parent_post or None, value=value)
    if not activity_href and related_media:
        activity_href = str(related_media.get("href") or "").strip() or None
        if activity_href:
            if event.event_type == "comment":
                activity_href_label = "Open Instagram post"
            elif event.event_type == "live_comment":
                activity_href_label = "Open live post"
            elif event.event_type == "story_mention":
                activity_href_label = "Open mentioned post"
    summary_text = _webhook_summary_text(
        event_type=event.event_type,
        field_label=display_field_label,
        actor_label=actor_label,
        recipient_label=recipient_label,
        parent_post=parent_post or None,
        related_post_label=str((related_media or {}).get("label") or "").strip() or None,
        value=value,
    )
    actor_profile_href = str(actor_profile.get("profile_href") or "").strip() or (
        f"https://www.instagram.com/{actor_username}/" if actor_username else None
    )
    recipient_profile_href = str(recipient_profile.get("profile_href") or "").strip() or (
        f"https://www.instagram.com/{recipient_username}/" if recipient_username else None
    )
    return {
        "id": event.id,
        "created_at": event.created_at,
        "field": field,
        "field_label": display_field_label,
        "event_type": event.event_type,
        "event_type_label": display_event_type_label,
        "matched": bool(event.matched_giveaway_id),
        "matched_giveaway_id": event.matched_giveaway_id,
        "matched_post_id": event.matched_post_id,
        "matched_account_id": event.matched_account_id,
        "matched_state_label": "Matched to giveaway" if event.matched_giveaway_id else "Stored for diagnostics",
        "provider_object_id": event.provider_object_id,
        "provider_account_id": provider_account_id,
        "provider_local_account": provider_local_account or None,
        "matched_local_account": matched_local_account or None,
        "actor_local_account": actor_local_account or None,
        "actor_profile": actor_profile or None,
        "actor_profile_href": actor_profile_href,
        "actor_profile_image_url": str(actor_profile.get("profile_image_url") or "").strip() or None,
        "actor_id": actor_id,
        "actor_username": str(actor_profile.get("username") or "").strip() or actor_username,
        "actor_label": actor_label,
        "recipient_id": recipient_id or provider_account_id,
        "recipient_username": str(recipient_profile.get("username") or "").strip() or recipient_username,
        "recipient_local_account": recipient_local_account or provider_local_account or matched_local_account or None,
        "recipient_profile": recipient_profile or None,
        "recipient_profile_href": recipient_profile_href,
        "recipient_profile_image_url": str(recipient_profile.get("profile_image_url") or "").strip() or None,
        "recipient_label": recipient_label,
        "account_context_label": account_context_label,
        "actor_context_label": actor_context_label,
        "provider_object_label": provider_object_label,
        "summary_text": summary_text,
        "chat_href": chat_href,
        "chat_href_label": chat_href_label,
        "activity_href": activity_href,
        "activity_href_label": activity_href_label,
        "text_preview": _webhook_text_preview(value),
        "value_keys": sorted(str(key) for key in value.keys()),
        "parent_post": parent_post or None,
        "related_media": related_media,
        "payload_json": payload,
    }


def instagram_webhook_observability(
    session: Session,
    *,
    window_days: int = 7,
    recent_limit: int = 10,
    field_limit: int = 8,
) -> dict[str, Any]:
    now = utcnow()
    window_start = now - timedelta(days=max(window_days, 1))
    window_stmt = (
        select(InstagramGiveawayWebhookEvent)
        .where(InstagramGiveawayWebhookEvent.created_at >= window_start)
        .order_by(InstagramGiveawayWebhookEvent.created_at.desc())
    )
    window_events = list(session.scalars(window_stmt))
    recent_events = list_instagram_webhook_events(session, limit=recent_limit)

    field_counts: Counter[str] = Counter()
    day_counts: Counter[str] = Counter()
    matched_events = 0
    giveaway_relevant_events = 0
    account_ids: set[str] = set()

    for event in window_events:
        value = _webhook_value_payload(event)
        field_key = (
            "shared_post"
            if _is_shared_post_event(event.event_type, value)
            else str(event.provider_event_field or event.event_type or "unknown").strip() or "unknown"
        )
        field_counts[field_key] += 1
        day_counts[event.created_at.strftime("%Y-%m-%d")] += 1
        if event.matched_giveaway_id:
            matched_events += 1
        if event.event_type in {"comment", "story_mention", "live_comment", "like", "share"} or (
            event.event_type == "message" and _is_shared_post_message(value)
        ):
            giveaway_relevant_events += 1
        actor_id, _actor_username = _extract_actor(value)
        if actor_id:
            account_ids.add(actor_id)

    chart_start_date = (now - timedelta(days=max(window_days, 1) - 1)).date()
    window_dates = [
        chart_start_date + timedelta(days=offset)
        for offset in range(max(window_days, 1))
    ]
    max_day_count = max(day_counts.values(), default=0)
    daily_chart = [
        {
            "date_key": day.isoformat(),
            "label": day.strftime("%b %d"),
            "count": day_counts.get(day.isoformat(), 0),
            "width_pct": (
                max(8, round(day_counts.get(day.isoformat(), 0) / max_day_count * 100))
                if max_day_count
                else 0
            ),
        }
        for day in window_dates
    ]

    top_fields = sorted(field_counts.items(), key=lambda item: (-item[1], item[0]))[: max(field_limit, 1)]
    max_field_count = max((count for _label, count in top_fields), default=0)
    field_chart = [
        {
            "key": key,
            "label": "Shared Post" if key == "shared_post" else _webhook_label(key),
            "count": count,
            "width_pct": max(10, round(count / max_field_count * 100)) if max_field_count else 0,
        }
        for key, count in top_fields
    ]

    post_lookup = _webhook_parent_post_context(session, recent_events)
    account_lookup = _webhook_account_context(session, recent_events)
    username_lookup = _instagram_account_username_context(session)
    graph_enrichments = {
        event.id: _event_graph_enrichment(session, event, account_lookup=account_lookup)
        for event in recent_events
    }

    return {
        "window_days": max(window_days, 1),
        "total_events": len(window_events),
        "matched_events": matched_events,
        "unmatched_events": max(0, len(window_events) - matched_events),
        "giveaway_relevant_events": giveaway_relevant_events,
        "unique_fields": len(field_counts),
        "unique_actors": len(account_ids),
        "latest_received_at": recent_events[0].created_at if recent_events else None,
        "field_chart": field_chart,
        "daily_chart": daily_chart,
        "recent_events": [
            serialize_instagram_webhook_event(
                event,
                post_lookup=post_lookup,
                account_lookup=account_lookup,
                username_lookup=username_lookup,
                graph_enrichment=graph_enrichments.get(event.id) or None,
            )
            for event in recent_events
        ],
    }


def _required_term_matches(comments: list[dict[str, Any]], *, keywords: list[str], hashtags: list[str]) -> list[str]:
    combined = " ".join(str(item.get("text") or "") for item in comments if isinstance(item, dict)).lower()
    matches: list[str] = []
    for keyword in keywords:
        if keyword in combined:
            matches.append(keyword)
    for hashtag in hashtags:
        if hashtag in combined:
            matches.append(hashtag)
    return matches


def _giveaway_candidate_pool(giveaway: InstagramGiveaway) -> list[InstagramGiveawayEntry]:
    return [
        entry
        for entry in giveaway.entries
        if entry.eligibility_status in {ENTRY_STATUS_ELIGIBLE, ENTRY_STATUS_PROVISIONAL}
    ]


def _randomize_entries(entries: list[InstagramGiveawayEntry]) -> list[InstagramGiveawayEntry]:
    ranked = list(entries)
    secrets.SystemRandom().shuffle(ranked)
    return ranked


def _entry_text(entry: InstagramGiveawayEntry) -> str:
    return " ".join(str(item.get("text") or "") for item in entry.comments_json if isinstance(item, dict))


def _reset_entry_evaluation(entry: InstagramGiveawayEntry) -> None:
    entry.keyword_matches_json = []
    entry.inconclusive_reasons_json = []
    entry.disqualification_reasons_json = []
    entry.liked_status = RULE_STATUS_UNKNOWN
    entry.followed_status = RULE_STATUS_UNKNOWN
    entry.shared_status = RULE_STATUS_UNKNOWN
    entry.eligibility_status = ENTRY_STATUS_PENDING
    entry.frozen_rank = None
    entry.is_provisional_candidate = False


def _verify_like_and_follow(giveaway: InstagramGiveaway, entry: InstagramGiveawayEntry) -> tuple[str, str, list[str], list[str]]:
    rules = giveaway.rules_json or {}
    inconclusive: list[str] = []
    disqualified: list[str] = []
    like_status = RULE_STATUS_NOT_REQUIRED
    follow_status = RULE_STATUS_NOT_REQUIRED
    if not rules.get("require_like") and not rules.get("require_follow"):
        return like_status, follow_status, inconclusive, disqualified

    dependency_issue = _instagram_destination_dependency_issue()
    if dependency_issue:
        if rules.get("require_like"):
            like_status = RULE_STATUS_INCONCLUSIVE
            inconclusive.append(dependency_issue)
        if rules.get("require_follow"):
            follow_status = RULE_STATUS_INCONCLUSIVE
            inconclusive.append(dependency_issue)
        return like_status, follow_status, inconclusive, disqualified

    try:
        client = _authenticated_publish_client(giveaway.instagram_account.credentials_json or {})
    except Exception as exc:
        reason = f"Instagram giveaway verification login failed: {exc}"
        if rules.get("require_like"):
            like_status = RULE_STATUS_INCONCLUSIVE
        if rules.get("require_follow"):
            follow_status = RULE_STATUS_INCONCLUSIVE
        inconclusive.append(reason)
        return like_status, follow_status, inconclusive, disqualified

    media_job = _instagram_media_job(giveaway)
    media_id = media_job.external_id if media_job else None

    if rules.get("require_like"):
        try:
            if not media_id:
                raise RuntimeError("Instagram media ID is not available for like verification.")
            likers = client.media_likers(media_id)
            liker_ids = {str(getattr(user, "pk", "") or "").strip() for user in likers}
            if entry.instagram_user_id in liker_ids:
                like_status = RULE_STATUS_VERIFIED
            else:
                like_status = RULE_STATUS_MISSING
                disqualified.append("Account did not like the giveaway post.")
        except Exception as exc:
            like_status = RULE_STATUS_INCONCLUSIVE
            inconclusive.append(f"Like verification could not be completed: {exc}")

    if rules.get("require_follow"):
        try:
            relationship = client.user_friendship_v1(entry.instagram_user_id)
            if bool(getattr(relationship, "followed_by", False)):
                follow_status = RULE_STATUS_VERIFIED
            else:
                follow_status = RULE_STATUS_MISSING
                disqualified.append("Account does not follow the giveaway account.")
        except Exception as exc:
            follow_status = RULE_STATUS_INCONCLUSIVE
            inconclusive.append(f"Follow verification could not be completed: {exc}")

    return like_status, follow_status, inconclusive, disqualified


def finalize_instagram_giveaway(
    session: Session,
    giveaway: InstagramGiveaway,
    alerts: AlertDispatcher,
    *,
    run_id: str,
) -> InstagramGiveaway:
    rules = giveaway.rules_json or {}
    required_keywords = list(rules.get("required_keywords") or [])
    required_hashtags = list(rules.get("required_hashtags") or [])
    comment_evidence_mode, comment_evidence_notes = _refresh_giveaway_comment_evidence(giveaway)
    story_evidence_mode, story_evidence_notes = _story_mention_evidence_mode(giveaway)

    evidence_summary: list[str] = []
    if comment_evidence_mode == "live_comment_revalidation":
        evidence_summary.append("live comment revalidation")
    else:
        evidence_summary.append("captured comment evidence fallback")
    if rules.get("require_story_mention"):
        if story_evidence_mode == "captured_story_mentions":
            evidence_summary.append("captured story mention evidence")
        else:
            evidence_summary.append("no story mention evidence")
    if rules.get("require_like") or rules.get("require_follow"):
        evidence_summary.append("live like/follow checks")

    notes = [note for note in [*comment_evidence_notes, *story_evidence_notes] if note]
    log_run_event(
        session,
        run_id=run_id,
        persona_id=giveaway.post.persona_id,
        persona_name=giveaway.post.persona.name if giveaway.post.persona else None,
        account_id=giveaway.instagram_account_id,
        service="instagram",
        operation="giveaway",
        message=(
            f"Instagram giveaway for post {giveaway.post_id} evaluated using "
            + ", ".join(evidence_summary)
            + "."
        ),
        post_id=giveaway.post_id,
        metadata={
            "comment_evidence_mode": comment_evidence_mode,
            "story_evidence_mode": story_evidence_mode,
            "notes": notes,
        },
    )

    for entry in giveaway.entries:
        _reset_entry_evaluation(entry)
        entry.shared_status = RULE_STATUS_NOT_REQUIRED if not rules.get("require_story_mention") else RULE_STATUS_UNKNOWN
        if entry.comment_count <= 0:
            if comment_evidence_mode == "live_comment_revalidation":
                entry.disqualification_reasons_json = ["No current giveaway comment was found at close time."]
            else:
                entry.disqualification_reasons_json = ["No giveaway comment evidence was available at close time."]
            entry.eligibility_status = ENTRY_STATUS_DISQUALIFIED
            continue

        if entry.mention_count < int(rules.get("min_friend_mentions") or 0):
            entry.disqualification_reasons_json.append(
                f"Needs at least {int(rules.get('min_friend_mentions') or 0)} friend mentions."
            )

        matches = _required_term_matches(entry.comments_json, keywords=required_keywords, hashtags=required_hashtags)
        entry.keyword_matches_json = matches
        if required_keywords and any(keyword not in matches for keyword in required_keywords):
            entry.disqualification_reasons_json.append("Required giveaway keywords were not all present.")
        if required_hashtags and any(hashtag not in matches for hashtag in required_hashtags):
            entry.disqualification_reasons_json.append("Required giveaway hashtags were not all present.")

        if rules.get("require_story_mention"):
            if entry.story_mentions_json:
                entry.shared_status = RULE_STATUS_VERIFIED
            else:
                entry.shared_status = RULE_STATUS_MISSING
                entry.disqualification_reasons_json.append(
                    "No story mention evidence was available at giveaway close."
                )

        like_status, follow_status, inconclusive, disqualified = _verify_like_and_follow(giveaway, entry)
        entry.liked_status = like_status
        entry.followed_status = follow_status
        entry.inconclusive_reasons_json.extend(reason for reason in inconclusive if reason not in entry.inconclusive_reasons_json)
        entry.disqualification_reasons_json.extend(reason for reason in disqualified if reason not in entry.disqualification_reasons_json)

        if entry.disqualification_reasons_json:
            entry.eligibility_status = ENTRY_STATUS_DISQUALIFIED
        elif entry.inconclusive_reasons_json:
            entry.eligibility_status = ENTRY_STATUS_PROVISIONAL
            entry.is_provisional_candidate = True
        else:
            entry.eligibility_status = ENTRY_STATUS_ELIGIBLE

    eligible = [entry for entry in giveaway.entries if entry.eligibility_status == ENTRY_STATUS_ELIGIBLE]
    provisional = [entry for entry in giveaway.entries if entry.eligibility_status == ENTRY_STATUS_PROVISIONAL]
    candidate_pool = eligible if eligible else provisional
    ranked_entries = _randomize_entries(candidate_pool)
    for rank, entry in enumerate(ranked_entries, start=1):
        entry.frozen_rank = rank

    giveaway.frozen_at = utcnow()
    giveaway.last_evaluated_at = utcnow()
    giveaway.last_error = None
    giveaway.provisional_winner_rank = None
    giveaway.final_winner_rank = None

    if not ranked_entries:
        giveaway.status = GIVEAWAY_STATUS_FAILED
        giveaway.last_error = "No qualifying giveaway entrants were found."
        log_run_event(
            session,
            run_id=run_id,
            persona_id=giveaway.post.persona_id,
            persona_name=giveaway.post.persona.name if giveaway.post.persona else None,
            account_id=giveaway.instagram_account_id,
            service="instagram",
            operation="giveaway",
            severity="error",
            message=f"Instagram giveaway for post {giveaway.post_id} closed without any qualifying entrants.",
            post_id=giveaway.post_id,
            metadata={"status": giveaway.status},
        )
        alerts.emit_hard_failure(
            session,
            run_id=run_id,
            persona=giveaway.post.persona,
            account=giveaway.instagram_account,
            service="instagram",
            post=giveaway.post,
            operation="giveaway",
            message=giveaway.last_error,
            error_class="NoQualifyingEntrants",
            event_type="instagram_giveaway_failed",
        )
        session.flush()
        return giveaway

    winner = ranked_entries[0]
    if winner.eligibility_status == ENTRY_STATUS_PROVISIONAL:
        giveaway.status = GIVEAWAY_STATUS_REVIEW_REQUIRED
        giveaway.provisional_winner_rank = winner.frozen_rank
        log_run_event(
            session,
            run_id=run_id,
            persona_id=giveaway.post.persona_id,
            persona_name=giveaway.post.persona.name if giveaway.post.persona else None,
            account_id=giveaway.instagram_account_id,
            service="instagram",
            operation="giveaway",
            severity="warning",
            message=(
                f"Instagram giveaway for post {giveaway.post_id} selected provisional winner "
                f"{winner.instagram_username or winner.instagram_user_id} and now requires review."
            ),
            post_id=giveaway.post_id,
            metadata={"status": giveaway.status, "winner_rank": winner.frozen_rank},
        )
        alerts.emit_hard_failure(
            session,
            run_id=run_id,
            persona=giveaway.post.persona,
            account=giveaway.instagram_account,
            service="instagram",
            post=giveaway.post,
            operation="giveaway",
            message="Instagram giveaway winner requires manual review.",
            error_class="ReviewRequired",
            severity="warning",
            event_type="instagram_giveaway_review_required",
            payload={"winner_rank": winner.frozen_rank, "winner_username": winner.instagram_username},
        )
    else:
        giveaway.status = GIVEAWAY_STATUS_WINNER_SELECTED
        giveaway.final_winner_rank = winner.frozen_rank
        log_run_event(
            session,
            run_id=run_id,
            persona_id=giveaway.post.persona_id,
            persona_name=giveaway.post.persona.name if giveaway.post.persona else None,
            account_id=giveaway.instagram_account_id,
            service="instagram",
            operation="giveaway",
            message=(
                f"Instagram giveaway for post {giveaway.post_id} selected winner "
                f"{winner.instagram_username or winner.instagram_user_id}."
            ),
            post_id=giveaway.post_id,
            metadata={"status": giveaway.status, "winner_rank": winner.frozen_rank},
        )
    session.flush()
    return giveaway


def process_instagram_giveaway_lifecycle(
    session: Session,
    alerts: AlertDispatcher,
    *,
    run_id: str,
) -> str:
    return process_giveaway_lifecycle(session, alerts, run_id=run_id)


def confirm_giveaway_winner(
    session: Session,
    giveaway: GiveawayCampaign | InstagramGiveaway,
    *,
    run_id: str,
) -> GiveawayCampaign:
    if isinstance(giveaway, InstagramGiveaway):
        campaign = get_generic_giveaway_by_post_id(session, giveaway.post_id)
        if campaign is None:
            raise ValueError("Giveaway not found.")
    else:
        campaign = giveaway
    return confirm_generic_giveaway_winner(session, campaign, run_id=run_id)


def advance_giveaway_winner(
    session: Session,
    giveaway: GiveawayCampaign | InstagramGiveaway,
    *,
    run_id: str,
) -> GiveawayCampaign:
    if isinstance(giveaway, InstagramGiveaway):
        campaign = get_generic_giveaway_by_post_id(session, giveaway.post_id)
        if campaign is None:
            raise ValueError("Giveaway not found.")
    else:
        campaign = giveaway
    return advance_generic_giveaway_winner(session, campaign, run_id=run_id)
