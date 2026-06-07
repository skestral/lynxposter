from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import re
import secrets
import time
from typing import Any, Callable
from urllib.parse import quote

import requests
from sqlalchemy import Select, select
from sqlalchemy.orm import Session, object_session, selectinload

from app.adapters.bluesky import _get_client as _get_bluesky_client
from app.adapters.bluesky import _post_id_from_uri as _bluesky_post_id_from_uri
from app.adapters.instagram import _authenticated_publish_client, _instagram_destination_dependency_issue
from app.config import get_settings
from app.models import (
    Account,
    CanonicalPost,
    DeliveryAttempt,
    DeliveryJob,
    GiveawayCampaign,
    GiveawayChannel,
    GiveawayEntrant,
    GiveawayEvidenceEvent,
    GiveawayPoolResult,
    InstagramGiveaway,
    InstagramGiveawayEntry,
    InstagramGiveawayWebhookEvent,
    Persona,
)
from app.schemas import (
    GiveawayAuditSummaryRead,
    GiveawayChannelConfigInput,
    GiveawayChannelRead,
    GiveawayChannelSummaryRead,
    GiveawayConfigInput,
    GiveawayEntrantRead,
    GiveawayPoolRead,
    GiveawayRead,
    GiveawayRuleNodeInput,
    GiveawayRuleCheckRead,
    GiveawaySelectionCandidateRead,
    GiveawaySelectionLogRead,
)
from app.services.alerts import AlertDispatcher
from app.services.events import log_run_event
from app.services.instagram_private_policy import (
    INSTAGRAM_PRIVATE_REASON_END_OF_GIVEAWAY,
    INSTAGRAM_PRIVATE_REASON_MANUAL,
    INSTAGRAM_PRIVATE_REASON_WEEKLY_DUE,
    INSTAGRAM_PRIVATE_SCAN_MODE_WEEKLY,
    InstagramPrivateAccessDecision,
    instagram_private_access_decision,
    instagram_private_scan_mode,
    instagram_private_scan_mode_label,
)
from app.services.live_updates import LIVE_UPDATE_TOPIC_DASHBOARD, LIVE_UPDATE_TOPIC_LOGS, publish_live_update

POST_TYPE_STANDARD = "standard"
POST_TYPE_GIVEAWAY = "giveaway"
POST_TYPE_INSTAGRAM_GIVEAWAY = POST_TYPE_GIVEAWAY

GIVEAWAY_STATUS_SCHEDULED = "scheduled"
GIVEAWAY_STATUS_COLLECTING = "collecting"
GIVEAWAY_STATUS_REVIEW_REQUIRED = "review_required"
GIVEAWAY_STATUS_WINNER_SELECTED = "winner_selected"
GIVEAWAY_STATUS_WINNER_CONFIRMED = "winner_confirmed"
GIVEAWAY_STATUS_FAILED = "failed"

ENTRY_STATUS_PENDING = "pending"
ENTRY_STATUS_ELIGIBLE = "eligible"
ENTRY_STATUS_PROVISIONAL = "provisional"
ENTRY_STATUS_DISQUALIFIED = "disqualified"

MANUAL_REVIEW_STATUS_APPROVED = "approved"
MANUAL_REVIEW_SIGNAL_KEY = "manual_review"

RULE_STATUS_UNKNOWN = "unknown"
RULE_STATUS_VERIFIED = "verified"
RULE_STATUS_MISSING = "missing"
RULE_STATUS_INCONCLUSIVE = "inconclusive"

INSTAGRAM_MENTION_PATTERN = re.compile(r"(?<!\w)@([A-Za-z0-9._]+)")
BLUESKY_MENTION_PATTERN = re.compile(r"(?<!\w)@([A-Za-z0-9][A-Za-z0-9-]*(?:\.[A-Za-z0-9][A-Za-z0-9-]*)+)")
BLUESKY_ACTIVITY_EVENT_TYPES = (
    "bluesky_reply",
    "bluesky_quote",
    "bluesky_like",
    "bluesky_repost",
    "bluesky_follow",
)
COMMENT_EVIDENCE_SOURCE_LIVE = "close_time_live"
INSTAGRAM_WEBHOOK_CAPTURE_SOURCE = "webhook_capture"
INSTAGRAM_MESSAGE_SHARE_CAPTURE_SOURCE = "message_share_capture"
INSTAGRAM_LIVE_COLLECTION_SOURCE = "live_collection"
INSTAGRAM_PRIVATE_MAX_ATTEMPTS = 3
INSTAGRAM_COMMENT_RULE_ATOMS = {
    "comment_present",
    "friend_mention_count_gte",
    "comment_keywords_all",
    "comment_hashtags_all",
}
BLUESKY_COLLECTION_MAX_ATTEMPTS = 3
INSTAGRAM_ACTIVITY_EVENT_TYPES = (
    "instagram_comment",
    "instagram_story_mention",
    "instagram_like",
    "instagram_repost",
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def instagram_private_scan_interval_hours() -> int:
    return max(0, int(get_settings().instagram_private_scan_interval_hours or 0))


def instagram_private_scan_due_at(channel: GiveawayChannel, *, now: datetime | None = None) -> datetime | None:
    if channel.service != "instagram":
        return None
    if instagram_private_scan_mode() != INSTAGRAM_PRIVATE_SCAN_MODE_WEEKLY:
        return None
    interval_hours = instagram_private_scan_interval_hours()
    if interval_hours <= 0:
        return None
    last_private_scan = normalize_datetime(channel.last_private_collected_at)
    if last_private_scan is None:
        return now or utcnow()
    return last_private_scan + timedelta(hours=interval_hours)


def instagram_private_scan_is_due(channel: GiveawayChannel, *, now: datetime | None = None) -> bool:
    if channel.service != "instagram":
        return False
    due_at = instagram_private_scan_due_at(channel, now=now)
    if due_at is None:
        return False
    return normalize_datetime(now or utcnow()) >= normalize_datetime(due_at)


def giveaway_selectinloads() -> tuple[Any, ...]:
    return (
        selectinload(CanonicalPost.giveaway_campaign)
        .selectinload(GiveawayCampaign.channels)
        .selectinload(GiveawayChannel.entrants),
        selectinload(CanonicalPost.giveaway_campaign)
        .selectinload(GiveawayCampaign.channels)
        .selectinload(GiveawayChannel.account),
        selectinload(CanonicalPost.giveaway_campaign)
        .selectinload(GiveawayCampaign.pools)
        .selectinload(GiveawayPoolResult.provisional_winner_entry),
        selectinload(CanonicalPost.giveaway_campaign)
        .selectinload(GiveawayCampaign.pools)
        .selectinload(GiveawayPoolResult.final_winner_entry),
        selectinload(CanonicalPost.giveaway_campaign).selectinload(GiveawayCampaign.evidence_events),
        selectinload(CanonicalPost.instagram_giveaway).selectinload(InstagramGiveaway.entries),
    )


def list_giveaway_campaigns_stmt() -> Select:
    return (
        select(GiveawayCampaign)
        .options(
            selectinload(GiveawayCampaign.channels).selectinload(GiveawayChannel.entrants),
            selectinload(GiveawayCampaign.channels).selectinload(GiveawayChannel.account),
            selectinload(GiveawayCampaign.pools).selectinload(GiveawayPoolResult.provisional_winner_entry),
            selectinload(GiveawayCampaign.pools).selectinload(GiveawayPoolResult.final_winner_entry),
            selectinload(GiveawayCampaign.post).selectinload(CanonicalPost.delivery_jobs).selectinload(DeliveryJob.target_account),
            selectinload(GiveawayCampaign.post).selectinload(CanonicalPost.persona),
        )
    )


def get_giveaway_for_post(session: Session, post_id: str, *, owner_user_id: str | None = None) -> GiveawayCampaign | None:
    stmt = list_giveaway_campaigns_stmt().join(GiveawayCampaign.post).where(GiveawayCampaign.post_id == post_id)
    if owner_user_id is not None:
        stmt = stmt.join(CanonicalPost.persona).where(Persona.owner_user_id == owner_user_id)
    return session.scalar(stmt)


def get_giveaway_by_post_id(session: Session, post_id: str) -> GiveawayCampaign | None:
    return session.scalar(list_giveaway_campaigns_stmt().where(GiveawayCampaign.post_id == post_id))


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


def _rule_tree_atoms(rule: dict[str, Any] | None) -> set[str]:
    payload = dict(rule or {})
    atoms: set[str] = set()
    if str(payload.get("kind") or "").strip().lower() == "atom":
        atom = str(payload.get("atom") or "").strip()
        if atom:
            atoms.add(atom)
    for child in payload.get("children") or []:
        if isinstance(child, dict):
            atoms.update(_rule_tree_atoms(child))
    return atoms


def instagram_rule_tree_from_legacy(raw_config: dict[str, Any] | None) -> dict[str, Any]:
    config = dict(raw_config or {})
    children: list[dict[str, Any]] = [{"kind": "atom", "atom": "comment_present", "params": {}}]
    min_mentions = int(config.get("min_friend_mentions") or 0)
    if min_mentions > 0:
        children.append(
            {
                "kind": "atom",
                "atom": "friend_mention_count_gte",
                "params": {"count": min_mentions},
            }
        )
    keywords = _normalized_terms(config.get("required_keywords"))
    if keywords:
        children.append(
            {
                "kind": "atom",
                "atom": "comment_keywords_all",
                "params": {"keywords": keywords},
            }
        )
    hashtags = _normalized_terms(config.get("required_hashtags"), prefix="#")
    if hashtags:
        children.append(
            {
                "kind": "atom",
                "atom": "comment_hashtags_all",
                "params": {"hashtags": hashtags},
            }
        )
    if bool(config.get("require_story_mention")):
        children.append({"kind": "atom", "atom": "story_mention_present", "params": {}})
    if bool(config.get("require_like")):
        children.append({"kind": "atom", "atom": "like_present", "params": {}})
    if bool(config.get("require_follow")):
        children.append({"kind": "atom", "atom": "follow_present", "params": {}})
    if bool(config.get("require_repost")):
        children.append({"kind": "atom", "atom": "repost_present", "params": {}})
    return {"kind": "all", "children": children}


def giveaway_config_input_from_json(config_json: dict[str, Any] | None) -> GiveawayConfigInput:
    payload = dict(config_json or {})
    if payload.get("channels") is None and any(
        key in payload
        for key in (
            "min_friend_mentions",
            "required_keywords",
            "required_hashtags",
            "require_story_mention",
            "require_like",
            "require_follow",
            "require_repost",
        )
    ):
        payload = {
            "giveaway_end_at": payload.get("giveaway_end_at"),
            "pool_mode": "combined",
            "winner_count": payload.get("winner_count") or 1,
            "channels": [
                {
                    "service": "instagram",
                    "account_id": payload.get("account_id") or "",
                    "rules": instagram_rule_tree_from_legacy(payload),
                }
            ],
        }
    payload["giveaway_end_at"] = normalize_datetime(
        datetime.fromisoformat(payload["giveaway_end_at"]) if payload.get("giveaway_end_at") else None
    )
    return GiveawayConfigInput.model_validate(payload)


def normalize_giveaway_config(config: GiveawayConfigInput | dict[str, Any] | None) -> dict[str, Any] | None:
    if config is None:
        return None
    if isinstance(config, dict):
        parsed = giveaway_config_input_from_json(config)
    else:
        parsed = config
    return {
        "giveaway_end_at": normalize_datetime(parsed.giveaway_end_at).isoformat() if parsed.giveaway_end_at else None,
        "pool_mode": parsed.pool_mode,
        "winner_count": parsed.winner_count,
        "channels": [
            {
                "service": channel.service,
                "account_id": channel.account_id,
                "rules": channel.rules.model_dump(mode="json"),
            }
            for channel in parsed.channels
        ],
    }


def _campaign_target_accounts(target_accounts: list[Account], config: GiveawayConfigInput) -> dict[str, Account]:
    accounts_by_service: dict[str, list[Account]] = defaultdict(list)
    for account in target_accounts:
        accounts_by_service[account.service].append(account)
    for channel in config.channels:
        if str(channel.account_id or "").strip():
            continue
        matching_accounts = accounts_by_service.get(channel.service, [])
        if len(matching_accounts) == 1:
            channel.account_id = matching_accounts[0].id
    target_map = {account.id: account for account in target_accounts}
    resolved: dict[str, Account] = {}
    for channel in config.channels:
        account = target_map.get(channel.account_id)
        if account is None:
            raise ValueError("Giveaway channels must target selected destination accounts.")
        if account.service != channel.service:
            raise ValueError("Giveaway channel service must match the selected destination account.")
        resolved[channel.account_id] = account
    if len(resolved) != len(target_accounts):
        raise ValueError("Giveaway posts must only target accounts that are configured as giveaway channels.")
    return resolved


def validate_giveaway_post(post: CanonicalPost, target_accounts: list[Account], giveaway: GiveawayConfigInput | None) -> None:
    if post.post_type != POST_TYPE_GIVEAWAY:
        return
    if giveaway is None or giveaway.giveaway_end_at is None:
        raise ValueError("Giveaway posts require a giveaway end time.")
    if not giveaway.channels:
        raise ValueError("Giveaway posts require at least one channel.")
    if len(giveaway.channels) == 1 and giveaway.channels[0].service == "instagram" and not str(giveaway.channels[0].account_id or "").strip():
        instagram_targets = [account for account in target_accounts if account.service == "instagram"]
        if len(target_accounts) != 1 or len(instagram_targets) != 1:
            raise ValueError("Instagram giveaway posts must target exactly one Instagram destination account.")
    _campaign_target_accounts(target_accounts, giveaway)
    services = [channel.service for channel in giveaway.channels]
    if len(set(services)) != len(services):
        raise ValueError("Giveaway posts support at most one channel per service.")
    publish_anchor = normalize_datetime(post.scheduled_for) or (utcnow() if post.status in {"queued", "posting", "scheduled"} else None)
    giveaway_end_at = normalize_datetime(giveaway.giveaway_end_at)
    if publish_anchor is not None and giveaway_end_at is not None and giveaway_end_at <= publish_anchor:
        raise ValueError("Giveaway end time must be after the scheduled publish time.")


def migrate_legacy_instagram_giveaway(session: Session, post: CanonicalPost) -> GiveawayCampaign | None:
    legacy = post.instagram_giveaway
    if legacy is None:
        return None
    if post.giveaway_campaign is not None:
        return post.giveaway_campaign
    channel_rules = instagram_rule_tree_from_legacy(legacy.rules_json or {})
    campaign = GiveawayCampaign(
        post_id=post.id,
        giveaway_end_at=normalize_datetime(legacy.giveaway_end_at) or utcnow(),
        pool_mode="combined",
        winner_count=1,
        status=legacy.status,
        frozen_at=legacy.frozen_at,
        last_evaluated_at=legacy.last_evaluated_at,
        last_error=legacy.last_error,
    )
    post.giveaway_campaign = campaign
    session.add(campaign)
    channel = GiveawayChannel(
        campaign=campaign,
        service="instagram",
        account_id=legacy.instagram_account_id,
        rules_json=channel_rules,
        status=legacy.status,
    )
    campaign.channels.append(channel)
    job = _channel_delivery_job(channel)
    if job:
        channel.target_post_external_id = job.external_id
        channel.target_post_url = job.external_url
    for legacy_entry in legacy.entries:
        entrant = GiveawayEntrant(
            channel=channel,
            provider_user_id=legacy_entry.instagram_user_id,
            provider_username=legacy_entry.instagram_username,
            display_label=legacy_entry.instagram_username or legacy_entry.instagram_user_id,
            signal_state_json={
                "comments": list(legacy_entry.comments_json or []),
                "comment_count": int(legacy_entry.comment_count or 0),
                "friend_mention_count": int(legacy_entry.mention_count or 0),
                "story_mentions": list(legacy_entry.story_mentions_json or []),
                "story_mention_count": len(legacy_entry.story_mentions_json or []),
            },
            rule_match_details_json={"legacy_keyword_matches": list(legacy_entry.keyword_matches_json or [])},
            eligibility_status=legacy_entry.eligibility_status,
            inconclusive_reasons_json=list(legacy_entry.inconclusive_reasons_json or []),
            disqualification_reasons_json=list(legacy_entry.disqualification_reasons_json or []),
        )
        channel.entrants.append(entrant)
    pool = GiveawayPoolResult(
        campaign=campaign,
        pool_key="combined",
        label="Combined",
        status=legacy.status,
        frozen_at=legacy.frozen_at,
        last_evaluated_at=legacy.last_evaluated_at,
        last_error=legacy.last_error,
    )
    if legacy.provisional_winner_rank:
        winner = _legacy_entry_by_rank(legacy, legacy.provisional_winner_rank)
        if winner:
            matching = next((entrant for entrant in channel.entrants if entrant.provider_user_id == winner.instagram_user_id), None)
            pool.provisional_winner_entry = matching
            pool.provisional_winner_entry_ids_json = [matching.id] if matching and matching.id else []
    if legacy.final_winner_rank:
        winner = _legacy_entry_by_rank(legacy, legacy.final_winner_rank)
        if winner:
            matching = next((entrant for entrant in channel.entrants if entrant.provider_user_id == winner.instagram_user_id), None)
            pool.final_winner_entry = matching
            pool.final_winner_entry_ids_json = [matching.id] if matching and matching.id else []
    campaign.pools.append(pool)
    session.flush()
    return campaign


def sync_giveaway_campaign(
    session: Session,
    post: CanonicalPost,
    target_accounts: list[Account],
    giveaway_config: GiveawayConfigInput | None,
) -> GiveawayCampaign | None:
    if post.post_type != POST_TYPE_GIVEAWAY:
        if post.giveaway_campaign is not None:
            session.delete(post.giveaway_campaign)
            session.flush()
        return None
    if post.giveaway_campaign is None and post.instagram_giveaway is not None:
        migrate_legacy_instagram_giveaway(session, post)
    validate_giveaway_post(post, target_accounts, giveaway_config)
    if giveaway_config is None:
        raise ValueError("Giveaway configuration is required.")

    campaign = post.giveaway_campaign
    if campaign is None:
        campaign = GiveawayCampaign(
            post_id=post.id,
            giveaway_end_at=normalize_datetime(giveaway_config.giveaway_end_at) or utcnow(),
            pool_mode=giveaway_config.pool_mode,
            winner_count=giveaway_config.winner_count,
            status=GIVEAWAY_STATUS_SCHEDULED,
        )
        post.giveaway_campaign = campaign
        session.add(campaign)
    else:
        campaign.giveaway_end_at = normalize_datetime(giveaway_config.giveaway_end_at) or utcnow()
        campaign.pool_mode = giveaway_config.pool_mode
        campaign.winner_count = giveaway_config.winner_count
        if campaign.status == GIVEAWAY_STATUS_FAILED and not campaign.frozen_at:
            campaign.status = GIVEAWAY_STATUS_SCHEDULED
            campaign.last_error = None

    existing_channels = {channel.service: channel for channel in campaign.channels}
    desired_services = {channel.service for channel in giveaway_config.channels}
    for service, channel in list(existing_channels.items()):
        if service not in desired_services:
            session.delete(channel)
    for channel_input in giveaway_config.channels:
        channel = existing_channels.get(channel_input.service)
        if channel is None:
            channel = GiveawayChannel(
                campaign=campaign,
                service=channel_input.service,
                account_id=channel_input.account_id,
                rules_json=channel_input.rules.model_dump(mode="json"),
                status=campaign.status,
            )
            campaign.channels.append(channel)
        else:
            channel.account_id = channel_input.account_id
            channel.rules_json = channel_input.rules.model_dump(mode="json")
    session.flush()
    _sync_campaign_pools(campaign)
    return campaign


def _sync_campaign_pools(campaign: GiveawayCampaign) -> None:
    desired: dict[str, str] = {}
    if campaign.pool_mode == "combined":
        desired["combined"] = "Combined"
    else:
        for channel in campaign.channels:
            desired[channel.service] = channel.service.title()
    existing = {pool.pool_key: pool for pool in campaign.pools}
    for key, label in desired.items():
        pool = existing.get(key)
        if pool is None:
            pool = GiveawayPoolResult(campaign=campaign, pool_key=key, label=label, status=campaign.status)
            campaign.pools.append(pool)
        else:
            pool.label = label
    for key, pool in list(existing.items()):
        if key not in desired:
            campaign.pools.remove(pool)


def _legacy_entry_by_rank(giveaway: InstagramGiveaway, rank: int | None) -> InstagramGiveawayEntry | None:
    if rank is None:
        return None
    for entry in giveaway.entries:
        if entry.frozen_rank == rank:
            return entry
    return None


def _channel_delivery_job(channel: GiveawayChannel) -> DeliveryJob | None:
    for job in channel.campaign.post.delivery_jobs:
        if job.target_account_id == channel.account_id and job.status == "posted":
            return job
    return None


def _account_credentials(account: Account | None) -> dict[str, Any]:
    return dict(account.credentials_json or {}) if account else {}


def _log_instagram_private_scan_event(
    session: Session,
    channel: GiveawayChannel,
    *,
    run_id: str | None,
    decision: InstagramPrivateAccessDecision,
    status: str,
    message: str,
    severity: str = "info",
    error: str | None = None,
) -> None:
    if not run_id:
        return
    post = channel.campaign.post if channel.campaign else None
    persona = post.persona if post else None
    metadata: dict[str, Any] = {
        "campaign_id": channel.campaign_id,
        "channel_id": channel.id,
        "private_scan_mode": decision.mode,
        "private_scan_mode_label": instagram_private_scan_mode_label(decision.mode),
        "private_scan_reason": decision.reason,
        "private_scan_status": status,
        "private_scan_interval_hours": instagram_private_scan_interval_hours(),
    }
    if error:
        metadata["error"] = error
    log_run_event(
        session,
        run_id=run_id,
        persona_id=persona.id if persona else None,
        persona_name=persona.name if persona else None,
        account_id=channel.account_id,
        service="instagram",
        operation="giveaway_private_scan",
        severity=severity,
        message=message,
        post_id=post.id if post else None,
        metadata=metadata,
    )


def _log_blocked_instagram_end_scan(session: Session, campaign: GiveawayCampaign, *, run_id: str, decision: InstagramPrivateAccessDecision) -> None:
    for channel in campaign.channels:
        if channel.service != "instagram" or not _channel_target_ready(channel):
            continue
        _log_instagram_private_scan_event(
            session,
            channel,
            run_id=run_id,
            decision=decision,
            status="blocked",
            severity="warning",
            message=decision.message,
        )


def _is_instagram_private_transient_error(exc: Exception) -> bool:
    text = str(exc).lower()
    class_name = exc.__class__.__name__.lower()
    return (
        isinstance(exc, TimeoutError)
        or "timeout" in class_name
        or "timeout" in text
        or "connectionpool" in text
        or "too many 500" in text
        or "500 error" in text
        or "temporarily unavailable" in text
    )


def _is_instagram_media_unavailable_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "media not found" in text or "media unavailable" in text or "not found or unavailable" in text


def _call_instagram_private(fetch: Callable[[], Any]) -> Any:
    last_error: Exception | None = None
    for attempt in range(INSTAGRAM_PRIVATE_MAX_ATTEMPTS):
        try:
            return fetch()
        except Exception as exc:
            last_error = exc
            if not _is_instagram_private_transient_error(exc) or attempt == INSTAGRAM_PRIVATE_MAX_ATTEMPTS - 1:
                raise
            time.sleep(1 + attempt)
    if last_error is not None:
        raise last_error
    return None


def _instagram_media_identifier_candidates(client: Any, channel: GiveawayChannel) -> list[str]:
    candidates: list[str] = []
    media_id = str(channel.target_post_external_id or "").strip()
    if media_id:
        candidates.append(media_id)
    media_url = str(channel.target_post_url or "").strip()
    if media_url and hasattr(client, "media_pk_from_url"):
        try:
            media_pk = _call_instagram_private(lambda: client.media_pk_from_url(media_url))
        except Exception:
            media_pk = None
        normalized_media_pk = str(media_pk or "").strip()
        if normalized_media_pk and normalized_media_pk not in candidates:
            candidates.append(normalized_media_pk)
    return candidates


def _instagram_media_likers(client: Any, channel: GiveawayChannel) -> tuple[list[Any], str]:
    media_ids = _instagram_media_identifier_candidates(client, channel)
    if not media_ids:
        raise RuntimeError("Instagram media ID is not available for like verification.")
    last_error: Exception | None = None
    for media_id in media_ids:
        try:
            return list(_call_instagram_private(lambda media_id=media_id: client.media_likers(media_id)) or []), media_id
        except Exception as exc:
            last_error = exc
            if _is_instagram_media_unavailable_error(exc):
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError("Instagram media ID is not available for like verification.")


def _instagram_media_comments(client: Any, channel: GiveawayChannel) -> tuple[list[Any], str]:
    media_ids = _instagram_media_identifier_candidates(client, channel)
    if not media_ids:
        raise RuntimeError("Instagram media ID is not available for giveaway verification.")
    last_error: Exception | None = None
    for media_id in media_ids:
        try:
            return list(_call_instagram_private(lambda media_id=media_id: client.media_comments(media_id, amount=0)) or []), media_id
        except Exception as exc:
            last_error = exc
            if _is_instagram_media_unavailable_error(exc):
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError("Instagram media ID is not available for giveaway verification.")


def _object_value(item: Any, *keys: str) -> Any:
    for key in keys:
        if isinstance(item, dict):
            value = item.get(key)
        else:
            value = getattr(item, key, None)
        if value not in (None, ""):
            return value
    return None


def _relationship_followed_by(relationship: Any) -> bool:
    return bool(_object_value(relationship, "followed_by"))


def _instagram_authenticated_user_id(client: Any) -> str | None:
    for key in ("user_id", "uid", "ds_user_id"):
        value = getattr(client, key, None)
        if callable(value):
            try:
                value = value()
            except TypeError:
                value = None
        normalized = str(value or "").strip()
        if normalized:
            return normalized

    for source_key in ("authorization_data", "settings", "last_json"):
        source = getattr(client, source_key, None)
        if callable(source):
            try:
                source = source()
            except TypeError:
                source = None
        if not isinstance(source, dict):
            continue
        for key in ("user_id", "ds_user_id", "pk", "id"):
            normalized = str(source.get(key) or "").strip()
            if normalized:
                return normalized

    cookie_dict = getattr(client, "cookie_dict", None)
    if callable(cookie_dict):
        try:
            cookie_dict = cookie_dict()
        except TypeError:
            cookie_dict = None
    if isinstance(cookie_dict, dict):
        for key in ("ds_user_id", "user_id"):
            normalized = str(cookie_dict.get(key) or "").strip()
            if normalized:
                return normalized
    return None


def _instagram_user_id_set(users: Any) -> set[str]:
    user_ids: set[str] = set()
    if isinstance(users, dict):
        iterable = users.items()
        for raw_user_id, user in iterable:
            for value in (raw_user_id, _object_value(user, "pk", "id", "user_id")):
                normalized = str(value or "").strip()
                if normalized:
                    user_ids.add(normalized)
    else:
        for user in list(users or []):
            normalized = str(_object_value(user, "pk", "id", "user_id") or "").strip()
            if normalized:
                user_ids.add(normalized)
    return user_ids


def _instagram_account_followers(client: Any) -> set[str]:
    account_user_id = _instagram_authenticated_user_id(client)
    if not account_user_id:
        raise RuntimeError("Instagram follower verification requires the authenticated account user ID.")

    if hasattr(client, "user_followers"):
        try:
            followers = _call_instagram_private(
                lambda: client.user_followers(account_user_id, use_cache=False, amount=0)
            )
        except TypeError:
            followers = _call_instagram_private(lambda: client.user_followers(account_user_id, amount=0))
        return _instagram_user_id_set(followers)

    if hasattr(client, "user_followers_v1"):
        followers = _call_instagram_private(lambda: client.user_followers_v1(account_user_id, amount=0))
        return _instagram_user_id_set(followers)

    raise RuntimeError("Instagram follower list verification is unavailable for this private client.")


def _instagram_user_friendships(client: Any, user_ids: list[str]) -> dict[str, bool]:
    requested_ids = [str(user_id or "").strip() for user_id in user_ids if str(user_id or "").strip()]
    requested_ids = list(dict.fromkeys(requested_ids))
    if not requested_ids:
        return {}

    if hasattr(client, "user_followers") or hasattr(client, "user_followers_v1"):
        follower_ids = _instagram_account_followers(client)
        return {user_id: user_id in follower_ids for user_id in requested_ids}

    if len(requested_ids) == 1 and hasattr(client, "user_friendship_v1"):
        user_id = requested_ids[0]
        relationship = _call_instagram_private(lambda: client.user_friendship_v1(user_id))
        return {user_id: _relationship_followed_by(relationship)}

    raise RuntimeError("Instagram follower list verification is unavailable for this private client.")


def _instagram_target_media_codes(channel: GiveawayChannel) -> set[str]:
    codes: set[str] = set()
    for value in (channel.target_post_url, channel.target_post_external_id):
        text = str(value or "").strip()
        if not text:
            continue
        match = re.search(r"/(?:p|reel|tv)/([^/?#]+)/?", text)
        if match:
            codes.add(match.group(1))
    return codes


def _instagram_story_share_summary(
    story: Any,
    *,
    target_media_ids: set[str],
    target_media_codes: set[str],
    provider_user_id: str,
    provider_username: str | None,
) -> dict[str, Any] | None:
    medias = list(_object_value(story, "medias") or [])
    for media in medias:
        media_pk = str(_object_value(media, "media_pk", "pk", "id") or "").strip()
        media_code = str(_object_value(media, "media_code", "code") or "").strip()
        if (media_pk and media_pk in target_media_ids) or (media_code and media_code in target_media_codes):
            story_id = str(_object_value(story, "pk", "id") or "").strip()
            return {
                "repost_id": f"story:{provider_user_id}:{story_id or media_pk or media_code}",
                "story_id": story_id or None,
                "media_id": media_pk or None,
                "media_code": media_code or None,
                "actor_id": provider_user_id,
                "actor_username": provider_username,
                "source": INSTAGRAM_LIVE_COLLECTION_SOURCE,
            }
    return None


def _instagram_user_stories(client: Any, provider_user_id: str) -> list[Any]:
    if not hasattr(client, "user_stories"):
        return []
    return list(_call_instagram_private(lambda: client.user_stories(provider_user_id)) or [])


def _resolve_bluesky_uri(handle: str, rkey: str) -> tuple[str | None, str | None]:
    normalized_handle = str(handle or "").strip()
    normalized_rkey = str(rkey or "").strip()
    if not normalized_handle or not normalized_rkey:
        return None, None
    try:
        response = requests.get(
            "https://bsky.social/xrpc/com.atproto.identity.resolveHandle",
            params={"handle": normalized_handle},
            timeout=10,
        )
        response.raise_for_status()
        did = str(response.json().get("did") or "").strip()
        if not did:
            return None, None
        return f"at://{did}/app.bsky.feed.post/{normalized_rkey}", did
    except Exception:
        return None, None


def hydrate_channel_targets(campaign: GiveawayCampaign) -> None:
    for channel in campaign.channels:
        job = _channel_delivery_job(channel)
        if not job:
            continue
        channel.target_post_external_id = job.external_id
        channel.target_post_url = job.external_url
        if channel.service == "bluesky" and not channel.target_post_uri and job.external_id:
            handle = str(_account_credentials(channel.account).get("handle") or "").strip()
            uri, _ = _resolve_bluesky_uri(handle, job.external_id)
            channel.target_post_uri = uri
        if channel.service == "instagram" and not channel.target_post_url:
            channel.target_post_url = job.external_url


def _channel_target_ready(channel: GiveawayChannel) -> bool:
    return bool(str(channel.target_post_external_id or channel.target_post_uri or "").strip())


def _entry_display_label(entrant: GiveawayEntrant) -> str:
    return entrant.display_label or entrant.provider_username or entrant.provider_user_id


def _entry_profile_url(channel: GiveawayChannel, entrant: GiveawayEntrant) -> str | None:
    username = str(entrant.provider_username or "").strip().lstrip("@")
    if channel.service == "instagram":
        if not username:
            return None
        return f"https://www.instagram.com/{quote(username, safe='._')}/"
    if channel.service == "bluesky":
        actor = username or str(entrant.provider_user_id or "").strip()
        if not actor:
            return None
        return f"https://bsky.app/profile/{quote(actor, safe='._:-')}"
    return None


def _parse_manual_reviewed_at(value: Any) -> datetime | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _entrant_manual_review(entrant: GiveawayEntrant) -> dict[str, Any] | None:
    state = dict(entrant.signal_state_json or {})
    review = state.get(MANUAL_REVIEW_SIGNAL_KEY)
    if not isinstance(review, dict):
        return None
    if str(review.get("status") or "").strip().lower() != MANUAL_REVIEW_STATUS_APPROVED:
        return None
    return dict(review)


def _manual_review_detail(review: dict[str, Any] | None) -> str:
    note = str((review or {}).get("note") or "").strip()
    if note:
        return note
    return "Entrant was manually approved for this giveaway."


def _apply_manual_review_override(entrant: GiveawayEntrant) -> bool:
    review = _entrant_manual_review(entrant)
    if review is None:
        return False
    detail = dict(entrant.rule_match_details_json or {})
    detail["manual_review"] = {
        "kind": "manual_review",
        "status": MANUAL_REVIEW_STATUS_APPROVED,
        "note": _manual_review_detail(review),
        "reviewed_at": review.get("reviewed_at"),
        "reviewed_by": review.get("reviewed_by"),
    }
    entrant.rule_match_details_json = detail
    entrant.eligibility_status = ENTRY_STATUS_ELIGIBLE
    entrant.inconclusive_reasons_json = []
    entrant.disqualification_reasons_json = []
    return True


def _normalize_evidence_items(items: list[dict[str, Any]] | None, *, default_source: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        payload = dict(item)
        payload["source"] = str(payload.get("source") or "").strip() or default_source
        normalized.append(payload)
    return normalized


def _instagram_comment_parent_id(value: dict[str, Any]) -> str | None:
    for key in ("parent_id", "parent_comment_id", "reply_to_id", "replied_to_comment_id"):
        candidate = str(value.get(key) or "").strip()
        if candidate:
            return candidate
    parent = value.get("parent")
    if isinstance(parent, dict):
        candidate = str(parent.get("id") or parent.get("comment_id") or "").strip()
        if candidate:
            return candidate
    return None


def _normalize_instagram_comment_items(raw_state: dict[str, Any]) -> list[dict[str, Any]]:
    comments = _normalize_evidence_items(
        raw_state.get("comments"),
        default_source=INSTAGRAM_WEBHOOK_CAPTURE_SOURCE,
    )
    for key in ("comment_replies", "replies"):
        replies = _normalize_evidence_items(
            raw_state.get(key),
            default_source=INSTAGRAM_WEBHOOK_CAPTURE_SOURCE,
        )
        for reply in replies:
            payload = dict(reply)
            if not payload.get("comment_id") and payload.get("reply_id"):
                payload["comment_id"] = payload.get("reply_id")
            payload["is_reply"] = True
            comments = _append_unique_evidence_item(
                comments,
                payload,
                key_fields=("comment_id", "parent_id", "text"),
            )
    return comments


def _normalized_instagram_signal_state(state: dict[str, Any] | None) -> dict[str, Any]:
    raw_state = dict(state or {})
    comments = _normalize_instagram_comment_items(raw_state)
    story_mentions = _normalize_evidence_items(
        raw_state.get("story_mentions"),
        default_source=INSTAGRAM_WEBHOOK_CAPTURE_SOURCE,
    )
    likes = _normalize_evidence_items(
        raw_state.get("likes"),
        default_source=INSTAGRAM_WEBHOOK_CAPTURE_SOURCE,
    )
    reposts = _normalize_evidence_items(
        raw_state.get("reposts"),
        default_source=INSTAGRAM_WEBHOOK_CAPTURE_SOURCE,
    )
    combined_text = " ".join(str(item.get("text") or "") for item in comments if isinstance(item, dict))
    observed_friend_mentions = len({match.lower() for match in INSTAGRAM_MENTION_PATTERN.findall(combined_text)})
    stored_friend_mentions = int(raw_state.get("friend_mention_count") or 0)
    friend_mention_items = len(raw_state.get("friend_mentions") or [])
    normalized: dict[str, Any] = {
        "comments": comments,
        "comment_count": max(len(comments), int(raw_state.get("comment_count") or 0)),
        "friend_mention_count": max(observed_friend_mentions, stored_friend_mentions, friend_mention_items),
        "story_mentions": story_mentions,
        "story_mention_count": len(story_mentions),
        "likes": likes,
        "like_present": bool(raw_state.get("like_present") or likes),
        "reposts": reposts,
        "repost_present": bool(raw_state.get("repost_present") or reposts),
    }
    if "like_collection_checked" in raw_state:
        normalized["like_collection_checked"] = bool(raw_state.get("like_collection_checked"))
    if "follow_present" in raw_state:
        normalized["follow_present"] = raw_state.get("follow_present")
    if "follow_collection_checked" in raw_state:
        normalized["follow_collection_checked"] = bool(raw_state.get("follow_collection_checked"))
    aliases = [
        str(value).strip()
        for value in raw_state.get("provider_user_id_aliases", [])
        if str(value).strip()
    ]
    if aliases:
        normalized["provider_user_id_aliases"] = sorted(dict.fromkeys(aliases))
    manual_review = raw_state.get(MANUAL_REVIEW_SIGNAL_KEY)
    if isinstance(manual_review, dict):
        normalized[MANUAL_REVIEW_SIGNAL_KEY] = dict(manual_review)
    return normalized


def _append_unique_evidence_item(items: list[dict[str, Any]], item: dict[str, Any], *, key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    candidate = dict(item)
    for existing in items:
        if not isinstance(existing, dict):
            continue
        if all(str(existing.get(key) or "").strip() == str(candidate.get(key) or "").strip() for key in key_fields):
            return items
    return [*items, candidate]


def _merge_instagram_signal_states(primary: dict[str, Any] | None, secondary: dict[str, Any] | None) -> dict[str, Any]:
    merged = _normalized_instagram_signal_state(dict(primary or {}))
    other = _normalized_instagram_signal_state(dict(secondary or {}))
    for key, fields in {
        "comments": ("comment_id", "text"),
        "story_mentions": ("story_id", "media_id", "text"),
        "likes": ("like_id", "actor_id", "media_id"),
        "reposts": ("repost_id", "actor_id", "media_id", "story_id"),
    }.items():
        items = list(merged.get(key) or [])
        for item in other.get(key) or []:
            items = _append_unique_evidence_item(items, item, key_fields=fields)
        merged[key] = items
    for key in ("like_present", "repost_present"):
        merged[key] = bool(merged.get(key) or other.get(key))
    if other.get("follow_present") is True or merged.get("follow_present") is True:
        merged["follow_present"] = True
    elif "follow_present" in other and "follow_present" not in merged:
        merged["follow_present"] = other.get("follow_present")
    if merged.get("like_collection_checked") or other.get("like_collection_checked"):
        merged["like_collection_checked"] = True
    if merged.get("follow_collection_checked") or other.get("follow_collection_checked"):
        merged["follow_collection_checked"] = True
    aliases = {
        str(value).strip()
        for state in (primary or {}, secondary or {})
        for value in state.get("provider_user_id_aliases", [])
        if str(value).strip()
    }
    normalized = _normalized_instagram_signal_state(merged)
    if aliases:
        normalized["provider_user_id_aliases"] = sorted(aliases)
    if merged.get("follow_collection_checked") or other.get("follow_collection_checked"):
        normalized["follow_collection_checked"] = True
    return normalized


def _merge_duplicate_instagram_entrants(channel: GiveawayChannel) -> None:
    if channel.service != "instagram":
        return
    session = object_session(channel)
    by_username: dict[str, GiveawayEntrant] = {}
    for entrant in list(channel.entrants):
        username_key = str(entrant.provider_username or "").strip().lower()
        if not username_key:
            continue
        existing = by_username.get(username_key)
        if existing is None:
            by_username[username_key] = entrant
            continue
        if existing is entrant:
            continue
        keep, duplicate = existing, entrant
        keep_state = dict(keep.signal_state_json or {})
        duplicate_state = dict(duplicate.signal_state_json or {})
        if duplicate_state.get("like_present") and not keep_state.get("like_present"):
            keep, duplicate = duplicate, keep
            by_username[username_key] = keep
        merged_state = _merge_instagram_signal_states(keep.signal_state_json, duplicate.signal_state_json)
        aliases = set(merged_state.get("provider_user_id_aliases") or [])
        aliases.add(str(keep.provider_user_id or "").strip())
        aliases.add(str(duplicate.provider_user_id or "").strip())
        merged_state["provider_user_id_aliases"] = sorted(value for value in aliases if value)
        keep.signal_state_json = merged_state
        keep.provider_username = keep.provider_username or duplicate.provider_username
        keep.display_label = keep.display_label or duplicate.display_label or keep.provider_username
        if session is not None:
            for event in list(duplicate.evidence_events):
                event.entrant = keep
            for event in session.scalars(select(GiveawayEvidenceEvent).where(GiveawayEvidenceEvent.entrant_id == duplicate.id)):
                event.entrant = keep
            duplicate.evidence_events = []
            session.flush()
            session.delete(duplicate)
        elif duplicate in channel.entrants:
            channel.entrants.remove(duplicate)


def _rule_check_label(atom: str, params: dict[str, Any]) -> str:
    if atom == "comment_present":
        return "Comment present"
    if atom == "story_mention_present":
        return "Story mention present"
    if atom == "like_present":
        return "Like present"
    if atom == "follow_present":
        return "Follow present"
    if atom == "friend_mention_count_gte":
        count = int(params.get("count") or 0)
        return f"Comment has at least {count} @mention{'s' if count != 1 else ''}"
    if atom == "comment_keywords_all":
        keywords = ", ".join(_normalized_terms(params.get("keywords")))
        return f"Comment includes keywords: {keywords}" if keywords else "Comment includes all configured keywords"
    if atom == "comment_hashtags_all":
        hashtags = ", ".join(_normalized_terms(params.get("hashtags"), prefix="#"))
        return f"Comment includes hashtags: {hashtags}" if hashtags else "Comment includes all configured hashtags"
    if atom == "reply_present":
        return "Reply present"
    if atom == "quote_present":
        return "Quote post present"
    if atom == "reply_or_quote_present":
        return "Reply or quote present"
    if atom == "reply_or_quote_mention_count_gte":
        count = int(params.get("count") or 0)
        return f"Reply or quote has at least {count} @mention{'s' if count != 1 else ''}"
    if atom == "repost_present":
        return "Repost present"
    return atom.replace("_", " ").title()


def _check_status(result: bool | None) -> str:
    if result is True:
        return "passed"
    if result is False:
        return "failed"
    return "inconclusive"


def _check_detail(result: bool | None, reason: str | None) -> str | None:
    if reason:
        return reason
    if result is True:
        return "Requirement satisfied at the last evaluation."
    if result is False:
        return "Requirement was not satisfied at the last evaluation."
    return "Requirement could not be conclusively verified at the last evaluation."


def _flatten_rule_checks(detail: dict[str, Any]) -> list[GiveawayRuleCheckRead]:
    checks: list[GiveawayRuleCheckRead] = []

    def visit(node: dict[str, Any]) -> None:
        kind = str(node.get("kind") or "").strip().lower()
        if kind == "atom":
            atom = str(node.get("atom") or "").strip()
            params = dict(node.get("params") or {})
            result = node.get("result")
            reason = node.get("reason")
            checks.append(
                GiveawayRuleCheckRead(
                    atom=atom,
                    label=_rule_check_label(atom, params),
                    status=_check_status(result),
                    detail=_check_detail(result, reason),
                    params=params,
                )
            )
            return
        for child in node.get("children") or []:
            if isinstance(child, dict):
                visit(child)

    visit(detail or {})
    return checks


def _entrant_activity_breakdown(channel: GiveawayChannel, entrant: GiveawayEntrant) -> tuple[dict[str, int], int]:
    state = dict(entrant.signal_state_json or {})
    breakdown: dict[str, int] = {}
    if channel.service == "instagram":
        breakdown["comments"] = int(state.get("comment_count") or len(state.get("comments") or []))
        breakdown["story_mentions"] = int(state.get("story_mention_count") or len(state.get("story_mentions") or []))
        likes = list(state.get("likes") or [])
        reposts = list(state.get("reposts") or [])
        if state.get("like_present"):
            breakdown["likes"] = len(likes) if likes else 1
        if state.get("repost_present"):
            breakdown["reposts"] = len(reposts) if reposts else 1
        if state.get("follow_present"):
            breakdown["follows"] = 1
    else:
        breakdown["replies"] = len(state.get("reply_posts") or []) if state.get("reply_posts") is not None else int(bool(state.get("reply_present")))
        breakdown["quotes"] = len(state.get("quote_posts") or []) if state.get("quote_posts") is not None else int(bool(state.get("quote_present")))
        breakdown["likes"] = int(bool(state.get("like_present")))
        breakdown["reposts"] = int(bool(state.get("repost_present")))
        breakdown["follows"] = int(bool(state.get("follow_present")))
    normalized = {key: value for key, value in breakdown.items() if int(value or 0) > 0}
    return normalized, sum(normalized.values())


def _serialize_entrant(channel: GiveawayChannel, entrant: GiveawayEntrant) -> GiveawayEntrantRead:
    activity_breakdown, activity_total = _entrant_activity_breakdown(channel, entrant)
    checks = _flatten_rule_checks(dict(entrant.rule_match_details_json or {}))
    manual_review = _entrant_manual_review(entrant)
    manual_review_note = str(manual_review.get("note") or "").strip() if manual_review else ""
    if manual_review is not None:
        checks.append(
            GiveawayRuleCheckRead(
                atom="manual_review",
                label="Manual approval",
                status="passed",
                detail=_manual_review_detail(manual_review),
                params={},
            )
        )
    return GiveawayEntrantRead(
        id=entrant.id,
        service=channel.service,
        provider_user_id=entrant.provider_user_id,
        provider_username=entrant.provider_username,
        display_label=_entry_display_label(entrant),
        profile_url=_entry_profile_url(channel, entrant),
        signal_state=dict(entrant.signal_state_json or {}),
        rule_match_details=dict(entrant.rule_match_details_json or {}),
        activity_total=activity_total,
        activity_breakdown=activity_breakdown,
        checks=checks,
        eligibility_status=entrant.eligibility_status,
        inconclusive_reasons=list(entrant.inconclusive_reasons_json or []),
        disqualification_reasons=list(entrant.disqualification_reasons_json or []),
        manual_review_status=manual_review.get("status") if manual_review else None,
        manual_review_note=manual_review_note or None,
        manual_reviewed_at=_parse_manual_reviewed_at(manual_review.get("reviewed_at")) if manual_review else None,
    )


def _channel_summary(channel: GiveawayChannel) -> GiveawayChannelSummaryRead:
    entrants = list(channel.entrants or [])
    activity_breakdown: dict[str, int] = defaultdict(int)
    activity_total = 0
    for entrant in entrants:
        entrant_breakdown, entrant_total = _entrant_activity_breakdown(channel, entrant)
        activity_total += entrant_total
        for key, value in entrant_breakdown.items():
            activity_breakdown[key] += value
    return GiveawayChannelSummaryRead(
        entrants=len(entrants),
        eligible=sum(1 for entrant in entrants if entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE),
        provisional=sum(1 for entrant in entrants if entrant.eligibility_status == ENTRY_STATUS_PROVISIONAL),
        disqualified=sum(1 for entrant in entrants if entrant.eligibility_status == ENTRY_STATUS_DISQUALIFIED),
        engagement_activities=activity_total,
        activity_breakdown=dict(activity_breakdown),
    )


def _selection_log(
    campaign: GiveawayCampaign,
    pool: GiveawayPoolResult,
    serialized_entrant_map: dict[str, GiveawayEntrantRead],
) -> GiveawaySelectionLogRead | None:
    entries = _pool_entries(campaign, pool)
    if not entries and not (pool.candidate_entry_ids_json or []):
        return None
    eligible_members = [
        serialized_entrant_map[entrant.id]
        for entrant in entries
        if entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE and entrant.id in serialized_entrant_map
    ]
    provisional_members = [
        entrant
        for entrant in entries
        if entrant.eligibility_status == ENTRY_STATUS_PROVISIONAL
    ]
    selected_ids = set(_pool_final_winner_ids(pool) or _pool_provisional_winner_ids(pool))
    candidate_source = "eligible entrants" if eligible_members else "provisional fallback" if provisional_members else "no qualifying entrants"
    candidates: list[GiveawaySelectionCandidateRead] = []
    for index, entrant_id in enumerate(pool.candidate_entry_ids_json or [], start=1):
        entrant = serialized_entrant_map.get(entrant_id)
        if entrant is None:
            continue
        note = None
        if entrant_id in selected_ids:
            note = "Selected after the randomized draw."
            if pool.status == GIVEAWAY_STATUS_REVIEW_REQUIRED:
                note = "Selected as a provisional winner pending review."
        candidates.append(
            GiveawaySelectionCandidateRead(
                rank=index,
                selected=entrant_id in selected_ids,
                note=note,
                entrant=entrant,
            )
        )
    winner_count = _campaign_winner_count(campaign)
    note = (
        f"Candidates were shuffled with SystemRandom and up to {winner_count} "
        f"winner{'s' if winner_count != 1 else ''} were selected for this pool."
    )
    if pool.status == GIVEAWAY_STATUS_REVIEW_REQUIRED:
        note = (
            f"No fully verified winner was available, so up to {winner_count} provisional "
            f"candidate{'s' if winner_count != 1 else ''} were held for manual review."
        )
    if pool.status == GIVEAWAY_STATUS_FAILED:
        note = pool.last_error or "No eligible or provisional entrants were available for this pool."
    return GiveawaySelectionLogRead(
        selection_method="system_random_shuffle",
        candidate_source=candidate_source,
        note=note,
        qualified_member_count=len(eligible_members),
        candidate_count=len(candidates),
        qualified_members=eligible_members,
        candidates=candidates,
    )


def serialize_giveaway(campaign: GiveawayCampaign | None) -> GiveawayRead | None:
    if campaign is None:
        return None
    channels = sorted(campaign.channels, key=lambda item: item.service)
    pools = sorted(campaign.pools, key=lambda item: item.pool_key)
    per_channel = {channel.service: _channel_summary(channel) for channel in channels}
    all_entrants = [entrant for channel in channels for entrant in channel.entrants]
    entrant_channel_map = {entrant.id: channel for channel in channels for entrant in channel.entrants}
    serialized_entrant_map = {
        entrant.id: _serialize_entrant(channel, entrant)
        for channel in channels
        for entrant in channel.entrants
    }

    def serialize_channel(channel: GiveawayChannel) -> GiveawayChannelRead:
        delivery_job = _channel_delivery_job(channel)
        target_post_external_id = channel.target_post_external_id or (delivery_job.external_id if delivery_job else None)
        target_post_url = channel.target_post_url or (delivery_job.external_url if delivery_job else None)
        weekly_private_scans_allowed = channel.service == "instagram" and instagram_private_scan_mode() == INSTAGRAM_PRIVATE_SCAN_MODE_WEEKLY
        return GiveawayChannelRead(
            id=channel.id,
            service=channel.service,
            account_id=channel.account_id,
            status=channel.status,
            rules=GiveawayRuleNodeInput.model_validate(channel.rules_json or {"kind": "all", "children": []}),
            target_post_external_id=target_post_external_id,
            target_post_uri=channel.target_post_uri,
            target_post_cid=channel.target_post_cid,
            target_post_url=target_post_url,
            last_collected_at=channel.last_collected_at,
            last_private_collected_at=channel.last_private_collected_at,
            private_scan_due_at=instagram_private_scan_due_at(channel),
            private_scan_interval_hours=instagram_private_scan_interval_hours() if weekly_private_scans_allowed else 0 if channel.service == "instagram" else None,
            private_scan_available=channel.service == "instagram" and bool(target_post_external_id or channel.target_post_uri),
            last_error=channel.last_error,
            summary=per_channel[channel.service],
            entrants=[
                serialized_entrant_map[entrant.id]
                for entrant in sorted(channel.entrants, key=lambda item: (item.provider_username or item.provider_user_id))
            ],
        )

    def serialize_winner_entries(ids: list[str]) -> list[GiveawayEntrantRead]:
        winners: list[GiveawayEntrantRead] = []
        for entrant_id in ids:
            entrant = serialized_entrant_map.get(entrant_id)
            if entrant is not None:
                winners.append(entrant)
        return winners

    return GiveawayRead(
        id=campaign.id,
        post_id=campaign.post_id,
        giveaway_end_at=campaign.giveaway_end_at,
        pool_mode=campaign.pool_mode,
        winner_count=_campaign_winner_count(campaign),
        status=campaign.status,
        frozen_at=campaign.frozen_at,
        last_evaluated_at=campaign.last_evaluated_at,
        last_error=campaign.last_error,
        audit_summary=GiveawayAuditSummaryRead(
            entrants=len(all_entrants),
            eligible=sum(1 for entrant in all_entrants if entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE),
            provisional=sum(1 for entrant in all_entrants if entrant.eligibility_status == ENTRY_STATUS_PROVISIONAL),
            disqualified=sum(1 for entrant in all_entrants if entrant.eligibility_status == ENTRY_STATUS_DISQUALIFIED),
            engagement_activities=sum(summary.engagement_activities for summary in per_channel.values()),
            per_channel=per_channel,
        ),
        channels=[serialize_channel(channel) for channel in channels],
        pools=[
            GiveawayPoolRead(
                id=pool.id,
                pool_key=pool.pool_key,
                label=pool.label,
                status=pool.status,
                frozen_at=pool.frozen_at,
                last_evaluated_at=pool.last_evaluated_at,
                last_error=pool.last_error,
                candidate_count=len(pool.candidate_entry_ids_json or []),
                provisional_winner=(provisional_winners[0] if provisional_winners else None),
                final_winner=(final_winners[0] if final_winners else None),
                provisional_winners=provisional_winners,
                final_winners=final_winners,
                selection_log=_selection_log(campaign, pool, serialized_entrant_map),
            )
            for pool in pools
            for provisional_winners in [serialize_winner_entries(_pool_provisional_winner_ids(pool))]
            for final_winners in [serialize_winner_entries(_pool_final_winner_ids(pool))]
        ],
    )


def _record_evidence_event(
    session: Session,
    campaign: GiveawayCampaign,
    channel: GiveawayChannel,
    *,
    entrant: GiveawayEntrant | None,
    provider_event_id: str | None,
    event_type: str,
    source: str,
    payload: dict[str, Any],
    active: bool = True,
) -> GiveawayEvidenceEvent:
    event = GiveawayEvidenceEvent(
        campaign_id=campaign.id,
        channel_id=channel.id,
        entrant_id=entrant.id if entrant else None,
        provider_event_id=provider_event_id,
        event_type=event_type,
        source=source,
        active=active,
        payload_json=payload,
    )
    session.add(event)
    return event


def get_or_create_channel_entrant(
    channel: GiveawayChannel,
    *,
    provider_user_id: str,
    provider_username: str | None = None,
    display_label: str | None = None,
    prefer_provider_user_id: bool = False,
) -> GiveawayEntrant:
    for entrant in channel.entrants:
        if entrant.provider_user_id == provider_user_id:
            if provider_username:
                entrant.provider_username = provider_username
            if display_label:
                entrant.display_label = display_label
            elif provider_username:
                entrant.display_label = provider_username
            return entrant
    normalized_username = str(provider_username or "").strip().lower()
    if normalized_username:
        for entrant in channel.entrants:
            if str(entrant.provider_username or "").strip().lower() != normalized_username:
                continue
            if prefer_provider_user_id:
                state = dict(entrant.signal_state_json or {})
                aliases = {
                    str(value).strip()
                    for value in state.get("provider_user_id_aliases", [])
                    if str(value).strip()
                }
                aliases.add(str(entrant.provider_user_id or "").strip())
                aliases.add(str(provider_user_id or "").strip())
                entrant.provider_user_id = provider_user_id
                state["provider_user_id_aliases"] = sorted(aliases)
                entrant.signal_state_json = state
            if provider_username:
                entrant.provider_username = provider_username
            if display_label:
                entrant.display_label = display_label
            elif provider_username:
                entrant.display_label = provider_username
            return entrant
    entrant = GiveawayEntrant(
        provider_user_id=provider_user_id,
        provider_username=provider_username,
        display_label=display_label or provider_username or provider_user_id,
    )
    channel.entrants.append(entrant)
    return entrant


def _combined_comment_text(items: list[dict[str, Any]] | None) -> str:
    return " ".join(str(item.get("text") or "") for item in items or [] if isinstance(item, dict)).lower()


def _instagram_verify_like(channel: GiveawayChannel, entrant: GiveawayEntrant) -> tuple[bool | None, str | None]:
    dependency_issue = _instagram_destination_dependency_issue()
    if dependency_issue:
        return None, dependency_issue
    try:
        client = _authenticated_publish_client(_account_credentials(channel.account))
        likers, _media_id = _instagram_media_likers(client, channel)
        liker_ids = {str(getattr(user, "pk", "") or "").strip() for user in likers}
        return entrant.provider_user_id in liker_ids, None
    except Exception as exc:
        return None, f"Like verification could not be completed: {exc}"


def _instagram_verify_follow(channel: GiveawayChannel, entrant: GiveawayEntrant) -> tuple[bool | None, str | None]:
    dependency_issue = _instagram_destination_dependency_issue()
    if dependency_issue:
        return None, dependency_issue
    try:
        client = _authenticated_publish_client(_account_credentials(channel.account))
        relationship = _call_instagram_private(lambda: client.user_friendship_v1(entrant.provider_user_id))
        follows_account = bool(getattr(relationship, "followed_by", False))
        state = _normalized_instagram_signal_state(dict(entrant.signal_state_json or {}))
        state["follow_present"] = follows_account
        state["follow_collection_checked"] = True
        entrant.signal_state_json = state
        return follows_account, None
    except Exception as exc:
        return None, f"Follow verification could not be completed: {exc}"


def _evaluate_instagram_atom(
    channel: GiveawayChannel,
    entrant: GiveawayEntrant,
    atom: str,
    params: dict[str, Any],
    *,
    allow_private_verification: bool = False,
) -> tuple[bool | None, str | None]:
    state = dict(entrant.signal_state_json or {})
    comments = list(state.get("comments") or [])
    comment_text = _combined_comment_text(comments)
    mention_count = int(state.get("friend_mention_count") or 0)
    if atom == "comment_present":
        return bool(int(state.get("comment_count") or 0) > 0), None
    if atom == "story_mention_present":
        return bool(int(state.get("story_mention_count") or 0) > 0), None
    if atom == "friend_mention_count_gte":
        return mention_count >= int(params.get("count") or 0), None
    if atom == "comment_keywords_all":
        keywords = _normalized_terms(params.get("keywords"))
        return all(keyword in comment_text for keyword in keywords), None
    if atom == "comment_hashtags_all":
        hashtags = _normalized_terms(params.get("hashtags"), prefix="#")
        return all(hashtag in comment_text for hashtag in hashtags), None
    if atom == "like_present":
        if state.get("like_present") is True:
            return True, None
        if state.get("like_collection_checked") is True:
            return False, None
        if not allow_private_verification:
            return None, "Instagram like verification is waiting for a manual, due, or end-of-giveaway private check."
        return _instagram_verify_like(channel, entrant)
    if atom == "repost_present":
        if state.get("repost_present") is True:
            return True, None
        if not allow_private_verification:
            return None, (
                "Instagram repost verification is waiting for manual review or an explicit private scan. "
                "Official Instagram APIs do not expose public profile repost checks."
            )
        return False, "No Instagram repost or share evidence was captured during the latest private/manual check."
    if atom == "follow_present":
        if state.get("follow_present") is True:
            return True, None
        if state.get("follow_collection_checked") is True:
            return False, None
        if allow_private_verification:
            return _instagram_verify_follow(channel, entrant)
        return None, "Instagram follow verification is waiting for a manual, due, or end-of-giveaway private check."
    return False, f"Unsupported Instagram atom: {atom}"


def _evaluate_bluesky_atom(channel: GiveawayChannel, entrant: GiveawayEntrant, atom: str, params: dict[str, Any]) -> tuple[bool | None, str | None]:
    state = dict(entrant.signal_state_json or {})
    if atom == "reply_present":
        return bool(state.get("reply_present")), None
    if atom == "quote_present":
        return bool(state.get("quote_present")), None
    if atom == "reply_or_quote_present":
        return bool(state.get("reply_present") or state.get("quote_present")), None
    if atom == "reply_or_quote_mention_count_gte":
        return int(state.get("reply_or_quote_mention_count") or 0) >= int(params.get("count") or 0), None
    if atom == "like_present":
        value = state.get("like_present")
        return (None if value is None else bool(value)), None
    if atom == "follow_present":
        value = state.get("follow_present")
        return (None if value is None else bool(value)), None
    if atom == "repost_present":
        value = state.get("repost_present")
        return (None if value is None else bool(value)), None
    return False, f"Unsupported Bluesky atom: {atom}"


def _sync_instagram_live_comment_events(
    session: Session,
    channel: GiveawayChannel,
    observed_comments: list[tuple[GiveawayEntrant, dict[str, Any], dict[str, Any]]],
) -> None:
    existing_events = list(
        session.scalars(
            select(GiveawayEvidenceEvent).where(
                GiveawayEvidenceEvent.channel_id == channel.id,
                GiveawayEvidenceEvent.event_type == "instagram_comment",
                GiveawayEvidenceEvent.source == COMMENT_EVIDENCE_SOURCE_LIVE,
            )
        )
    )
    existing_by_key = {str(event.provider_event_id or ""): event for event in existing_events}
    observed_keys: set[str] = set()
    seen_at = utcnow().isoformat()

    for entrant, summary, raw_comment in observed_comments:
        provider_event_id = str(summary.get("comment_id") or "").strip()
        if not provider_event_id:
            continue
        observed_keys.add(provider_event_id)
        payload = {
            "change": {
                "field": "comments",
                "value": {
                    "media_id": channel.target_post_external_id,
                    "id": provider_event_id,
                    "parent_id": summary.get("parent_id"),
                    "text": summary.get("text") or "",
                    "created_time": raw_comment.get("created_time"),
                    "from": {
                        "id": entrant.provider_user_id,
                        "username": entrant.provider_username,
                    },
                },
            },
            "source": COMMENT_EVIDENCE_SOURCE_LIVE,
            "last_seen_at": seen_at,
        }
        existing = existing_by_key.get(provider_event_id)
        if existing is None:
            payload["first_seen_at"] = seen_at
            _record_evidence_event(
                session,
                channel.campaign,
                channel,
                entrant=entrant,
                provider_event_id=provider_event_id,
                event_type="instagram_comment",
                source=COMMENT_EVIDENCE_SOURCE_LIVE,
                payload=payload,
            )
            continue
        existing.entrant_id = entrant.id
        existing_payload = dict(existing.payload_json or {})
        payload["first_seen_at"] = existing_payload.get("first_seen_at") or existing.created_at.isoformat()
        existing.payload_json = payload
        existing.active = True

    for key, existing in existing_by_key.items():
        if key in observed_keys:
            continue
        payload = dict(existing.payload_json or {})
        payload["last_seen_at"] = seen_at
        existing.payload_json = payload
        existing.active = False


def _instagram_webhook_value_payload(event: InstagramGiveawayWebhookEvent) -> dict[str, Any]:
    payload = dict(event.payload_json or {})
    change = payload.get("change")
    if not isinstance(change, dict):
        return {}
    value = change.get("value")
    return dict(value) if isinstance(value, dict) else {}


def _instagram_webhook_change_field(event: InstagramGiveawayWebhookEvent) -> str:
    payload = dict(event.payload_json or {})
    change = payload.get("change")
    if isinstance(change, dict):
        return str(change.get("field") or event.provider_event_field or "").strip().lower()
    return str(event.provider_event_field or "").strip().lower()


def _instagram_webhook_actor(value: dict[str, Any]) -> tuple[str | None, str | None]:
    if _instagram_webhook_is_echo_message(value) and _instagram_webhook_is_story_mention_message(value):
        recipient = value.get("recipient")
        if isinstance(recipient, dict):
            user_id = str(recipient.get("id") or recipient.get("user_id") or "").strip() or None
            username = str(recipient.get("username") or recipient.get("name") or "").strip() or None
            if user_id or username:
                return user_id, username
    for candidate in (value.get("from"), value.get("user"), value.get("sender"), value.get("author")):
        if isinstance(candidate, dict):
            user_id = str(candidate.get("id") or candidate.get("user_id") or "").strip() or None
            username = str(candidate.get("username") or candidate.get("name") or "").strip() or None
            if user_id or username:
                return user_id, username
    user_id = str(value.get("from_id") or value.get("user_id") or "").strip() or None
    username = str(value.get("username") or value.get("user_name") or "").strip() or None
    return user_id, username


def _instagram_webhook_message_attachments(value: dict[str, Any]) -> list[dict[str, Any]]:
    message = value.get("message")
    if not isinstance(message, dict):
        return []
    attachments = message.get("attachments")
    if not isinstance(attachments, list):
        return []
    return [dict(item) for item in attachments if isinstance(item, dict)]


def _instagram_webhook_message_attachment_types(value: dict[str, Any]) -> set[str]:
    return {
        str(attachment.get("type") or "").strip().lower()
        for attachment in _instagram_webhook_message_attachments(value)
        if str(attachment.get("type") or "").strip()
    }


def _instagram_webhook_is_echo_message(value: dict[str, Any]) -> bool:
    message = value.get("message")
    return isinstance(message, dict) and bool(message.get("is_echo"))


def _instagram_webhook_shared_media_id(value: dict[str, Any]) -> str | None:
    for attachment in _instagram_webhook_message_attachments(value):
        payload = attachment.get("payload")
        if not isinstance(payload, dict):
            continue
        for key in ("ig_post_media_id", "media_id", "post_id"):
            candidate = str(payload.get(key) or "").strip()
            if candidate:
                return candidate
    return None


def _instagram_webhook_is_shared_post_message(value: dict[str, Any]) -> bool:
    return bool(_instagram_webhook_shared_media_id(value)) and bool(
        _instagram_webhook_message_attachment_types(value).intersection({"share", "ig_post"})
    )


def _instagram_webhook_is_story_mention_message(value: dict[str, Any]) -> bool:
    return "story_mention" in _instagram_webhook_message_attachment_types(value)


def _instagram_webhook_is_indirect_share_message(value: dict[str, Any]) -> bool:
    return _instagram_webhook_is_shared_post_message(value) or _instagram_webhook_is_story_mention_message(value)


def _instagram_webhook_media_ids(value: dict[str, Any]) -> set[str]:
    media_ids: set[str] = set()
    for key in ("media_id", "post_id", "parent_id"):
        candidate = str(value.get(key) or "").strip()
        if candidate:
            media_ids.add(candidate)
    media = value.get("media")
    if isinstance(media, dict):
        candidate = str(media.get("id") or "").strip()
        if candidate:
            media_ids.add(candidate)
    shared_media_id = _instagram_webhook_shared_media_id(value)
    if shared_media_id:
        media_ids.add(shared_media_id)
    return media_ids


def _instagram_channel_target_ids(channel: GiveawayChannel) -> set[str]:
    target_ids = {
        str(channel.target_post_external_id or "").strip(),
    }
    job = _channel_delivery_job(channel)
    if job:
        target_ids.add(str(job.external_id or "").strip())
    return {target_id for target_id in target_ids if target_id}


def _instagram_account_provider_id_candidates(account: Account | None) -> set[str]:
    if account is None:
        return set()
    credentials = _account_credentials(account)
    candidates = {
        str(account.id or "").strip(),
        str(credentials.get("instagram_user_id") or "").strip(),
        str(credentials.get("provider_account_id") or "").strip(),
        str(credentials.get("professional_account_id") or "").strip(),
        str(credentials.get("ig_user_id") or "").strip(),
    }
    return {candidate for candidate in candidates if candidate}


def _instagram_webhook_matches_channel(event: InstagramGiveawayWebhookEvent, channel: GiveawayChannel, value: dict[str, Any]) -> bool:
    if event.matched_giveaway_id and event.matched_giveaway_id == channel.campaign_id:
        return True
    if event.matched_post_id and event.matched_post_id == channel.campaign.post_id:
        return True
    if event.matched_account_id and event.matched_account_id != channel.account_id:
        return False

    payload = dict(event.payload_json or {})
    entry = payload.get("entry")
    entry_account_id = str(entry.get("id") or "").strip() if isinstance(entry, dict) else ""
    if entry_account_id and entry_account_id not in _instagram_account_provider_id_candidates(channel.account):
        return False

    media_ids = _instagram_webhook_media_ids(value)
    if not media_ids:
        provider_object_id = str(event.provider_object_id or "").strip()
        if provider_object_id:
            media_ids.add(provider_object_id)
    target_ids = _instagram_channel_target_ids(channel)
    return bool(media_ids and target_ids and media_ids.intersection(target_ids))


def _instagram_webhook_entry_account_id(event: InstagramGiveawayWebhookEvent) -> str | None:
    payload = dict(event.payload_json or {})
    entry = payload.get("entry")
    if not isinstance(entry, dict):
        return None
    return str(entry.get("id") or "").strip() or None


def _instagram_webhook_entry_matches_channel_account(event: InstagramGiveawayWebhookEvent, channel: GiveawayChannel) -> bool:
    if event.matched_account_id:
        return event.matched_account_id == channel.account_id
    entry_account_id = _instagram_webhook_entry_account_id(event)
    if not entry_account_id:
        return True
    return entry_account_id in _instagram_account_provider_id_candidates(channel.account)


def _match_indirect_instagram_share_event_to_single_channel(
    session: Session,
    channel: GiveawayChannel,
    event: InstagramGiveawayWebhookEvent,
) -> None:
    if event.matched_giveaway_id or event.matched_post_id:
        return
    value = _instagram_webhook_value_payload(event)
    if not value or not _instagram_webhook_is_indirect_share_message(value):
        return
    if _instagram_webhook_media_ids(value):
        return
    if not _instagram_webhook_entry_matches_channel_account(event, channel):
        return

    channels = list(
        session.scalars(
            select(GiveawayChannel)
            .join(GiveawayChannel.campaign)
            .where(
                GiveawayChannel.service == "instagram",
                GiveawayChannel.account_id == channel.account_id,
                GiveawayCampaign.status.in_([GIVEAWAY_STATUS_SCHEDULED, GIVEAWAY_STATUS_COLLECTING, GIVEAWAY_STATUS_REVIEW_REQUIRED]),
            )
        )
    )
    if len(channels) != 1 or channels[0].id != channel.id:
        return

    event.matched_giveaway_id = channel.campaign_id
    event.matched_post_id = channel.campaign.post_id
    event.matched_account_id = channel.account_id


def _instagram_webhook_occurred_at(value: dict[str, Any]) -> datetime | None:
    timestamp = value.get("created_time") or value.get("timestamp")
    if isinstance(timestamp, str) and timestamp.strip():
        try:
            return normalize_datetime(datetime.fromisoformat(timestamp.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def _instagram_campaign_window_accepts_event(campaign: GiveawayCampaign, *, occurred_at: datetime | None) -> bool:
    if occurred_at is None:
        return True
    published_at = normalize_datetime(campaign.post.published_at)
    giveaway_end_at = normalize_datetime(campaign.giveaway_end_at)
    if published_at and occurred_at < published_at:
        return False
    return giveaway_end_at is None or occurred_at <= giveaway_end_at


def _instagram_webhook_activity_types(event: InstagramGiveawayWebhookEvent, value: dict[str, Any]) -> list[str]:
    event_type = str(event.event_type or "").strip().lower()
    field = _instagram_webhook_change_field(event)
    item = str(value.get("item") or value.get("type") or "").strip().lower()
    activities: list[str] = []

    if event_type in {"comment", "live_comment"} or "comment" in field or item == "comment":
        activities.append("comment")
    if event_type == "story_mention" or "mention" in field or str(value.get("mention_type") or "").strip().lower() == "story":
        activities.append("story_mention")
    if event_type in {"like", "likes"} or "like" in field or item == "like":
        activities.append("like")
    if (
        event_type in {"share", "shares", "repost", "reposts", "shared_post"}
        or "share" in field
        or "repost" in field
        or item in {"share", "repost"}
    ):
        activities.append("repost")
    if _instagram_webhook_is_shared_post_message(value):
        activities.extend(["story_mention", "repost"])
    if _instagram_webhook_is_story_mention_message(value):
        activities.extend(["story_mention", "repost"])

    deduped: list[str] = []
    for activity in activities:
        if activity not in deduped:
            deduped.append(activity)
    return deduped


def _instagram_webhook_text_value(value: dict[str, Any]) -> str | None:
    for key in ("text", "caption", "title"):
        candidate = str(value.get(key) or "").strip()
        if candidate:
            return candidate
    message = value.get("message")
    if isinstance(message, dict):
        candidate = str(message.get("text") or "").strip()
        if candidate:
            return candidate
        for attachment in _instagram_webhook_message_attachments(value):
            payload = attachment.get("payload")
            if not isinstance(payload, dict):
                continue
            for key in ("title", "caption"):
                candidate = str(payload.get(key) or "").strip()
                if candidate:
                    return candidate
    if isinstance(message, str) and message.strip():
        return message.strip()
    return None


def _instagram_provider_event_id(
    event: InstagramGiveawayWebhookEvent,
    value: dict[str, Any],
    *,
    activity: str,
    actor_id: str,
    channel: GiveawayChannel,
) -> str:
    activity_keys = {
        "comment": ("comment_id", "id"),
        "story_mention": ("story_id", "id"),
        "like": ("like_id", "id", "creation_id"),
        "repost": ("share_id", "repost_id", "id", "creation_id"),
    }
    for key in activity_keys.get(activity, ("id",)):
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
    provider_object_id = str(event.provider_object_id or "").strip()
    media_ids = sorted(_instagram_webhook_media_ids(value) or _instagram_channel_target_ids(channel))
    if provider_object_id and provider_object_id not in media_ids:
        return provider_object_id
    return f"{activity}:{actor_id}:{':'.join(media_ids) if media_ids else channel.id}"


def _record_or_update_instagram_evidence_event(
    session: Session,
    channel: GiveawayChannel,
    entrant: GiveawayEntrant,
    *,
    provider_event_id: str,
    event_type: str,
    source: str,
    payload: dict[str, Any],
) -> None:
    existing = session.scalar(
        select(GiveawayEvidenceEvent).where(
            GiveawayEvidenceEvent.channel_id == channel.id,
            GiveawayEvidenceEvent.event_type == event_type,
            GiveawayEvidenceEvent.source == source,
            GiveawayEvidenceEvent.provider_event_id == provider_event_id,
        )
    )
    if existing is None:
        _record_evidence_event(
            session,
            channel.campaign,
            channel,
            entrant=entrant,
            provider_event_id=provider_event_id,
            event_type=event_type,
            source=source,
            payload=payload,
        )
        return
    existing.entrant_id = entrant.id
    existing.payload_json = payload
    existing.active = True


def _instagram_activity_summary(
    value: dict[str, Any],
    *,
    activity: str,
    provider_event_id: str,
    actor_id: str,
    actor_username: str | None,
    source: str,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "source": source,
        "created_time": str(value.get("created_time") or value.get("timestamp") or "").strip() or None,
        "actor_id": actor_id,
        "actor_username": actor_username,
    }
    if activity == "comment":
        parent_id = _instagram_comment_parent_id(value)
        summary.update(
            {
                "comment_id": provider_event_id,
                "parent_id": parent_id,
                "is_reply": bool(parent_id),
                "text": _instagram_webhook_text_value(value) or "",
            }
        )
    elif activity == "story_mention":
        summary.update(
            {
                "story_id": provider_event_id,
                "media_id": next(iter(_instagram_webhook_media_ids(value)), None),
                "text": _instagram_webhook_text_value(value),
            }
        )
    elif activity == "like":
        summary["like_id"] = provider_event_id
    elif activity == "repost":
        summary.update(
            {
                "repost_id": provider_event_id,
                "media_id": next(iter(_instagram_webhook_media_ids(value)), None),
                "text": _instagram_webhook_text_value(value),
            }
        )
    return summary


def sync_instagram_webhook_event_to_channel(
    session: Session,
    channel: GiveawayChannel,
    event: InstagramGiveawayWebhookEvent,
) -> list[str]:
    value = _instagram_webhook_value_payload(event)
    if not value or not _instagram_webhook_matches_channel(event, channel, value):
        return []
    occurred_at = _instagram_webhook_occurred_at(value)
    if not _instagram_campaign_window_accepts_event(channel.campaign, occurred_at=occurred_at):
        return []
    activities = _instagram_webhook_activity_types(event, value)
    if not activities:
        return []
    actor_id, actor_username = _instagram_webhook_actor(value)
    if not actor_id:
        return []

    entrant = get_or_create_channel_entrant(
        channel,
        provider_user_id=actor_id,
        provider_username=actor_username,
        display_label=actor_username or actor_id,
    )
    state = _normalized_instagram_signal_state(dict(entrant.signal_state_json or {}))
    session.flush()

    captured: list[str] = []
    for activity in activities:
        provider_event_id = _instagram_provider_event_id(
            event,
            value,
            activity=activity,
            actor_id=actor_id,
            channel=channel,
        )
        source = (
            INSTAGRAM_MESSAGE_SHARE_CAPTURE_SOURCE
            if _instagram_webhook_is_indirect_share_message(value) and activity in {"story_mention", "repost"}
            else INSTAGRAM_WEBHOOK_CAPTURE_SOURCE
        )
        summary = _instagram_activity_summary(
            value,
            activity=activity,
            provider_event_id=provider_event_id,
            actor_id=actor_id,
            actor_username=actor_username,
            source=source,
        )
        if activity == "comment":
            state["comments"] = _append_unique_evidence_item(
                list(state.get("comments") or []),
                summary,
                key_fields=("comment_id",),
            )
            evidence_type = "instagram_comment"
        elif activity == "story_mention":
            state["story_mentions"] = _append_unique_evidence_item(
                list(state.get("story_mentions") or []),
                summary,
                key_fields=("story_id",),
            )
            evidence_type = "instagram_story_mention"
        elif activity == "like":
            state["likes"] = _append_unique_evidence_item(
                list(state.get("likes") or []),
                summary,
                key_fields=("actor_id", "like_id"),
            )
            state["like_present"] = True
            evidence_type = "instagram_like"
        elif activity == "repost":
            state["reposts"] = _append_unique_evidence_item(
                list(state.get("reposts") or []),
                summary,
                key_fields=("actor_id", "repost_id"),
            )
            state["repost_present"] = True
            evidence_type = "instagram_repost"
        else:
            continue

        _record_or_update_instagram_evidence_event(
            session,
            channel,
            entrant,
            provider_event_id=provider_event_id,
            event_type=evidence_type,
            source=source,
            payload=dict(event.payload_json or {}),
        )
        captured.append(activity)

    if not captured:
        return []
    entrant.signal_state_json = _normalized_instagram_signal_state(state)
    channel.last_collected_at = utcnow()
    channel.last_error = None
    event.matched_giveaway_id = channel.campaign_id
    event.matched_post_id = channel.campaign.post_id
    event.matched_account_id = channel.account_id
    event.processed = True
    event.processed_at = utcnow()
    session.flush()
    return captured


def _match_instagram_comment_reply_event_to_channel(
    session: Session,
    channel: GiveawayChannel,
    event: InstagramGiveawayWebhookEvent,
) -> None:
    if event.matched_giveaway_id or event.matched_post_id:
        return
    value = _instagram_webhook_value_payload(event)
    if not value:
        return
    parent_id = _instagram_comment_parent_id(value)
    if not parent_id:
        return
    if not _instagram_webhook_entry_matches_channel_account(event, channel):
        return
    parent_event = session.scalar(
        select(GiveawayEvidenceEvent).where(
            GiveawayEvidenceEvent.channel_id == channel.id,
            GiveawayEvidenceEvent.event_type == "instagram_comment",
            GiveawayEvidenceEvent.provider_event_id == parent_id,
        )
    )
    if parent_event is None:
        return
    event.matched_giveaway_id = channel.campaign_id
    event.matched_post_id = channel.campaign.post_id
    event.matched_account_id = channel.account_id


def sync_instagram_webhook_events_for_channel(session: Session, channel: GiveawayChannel) -> int:
    events = list(
        session.scalars(
            select(InstagramGiveawayWebhookEvent)
            .where(InstagramGiveawayWebhookEvent.signature_valid.is_(True))
            .order_by(InstagramGiveawayWebhookEvent.created_at.asc())
        )
    )
    captured = 0
    for event in events:
        _match_indirect_instagram_share_event_to_single_channel(session, channel, event)
        _match_instagram_comment_reply_event_to_channel(session, channel, event)
        captured += len(sync_instagram_webhook_event_to_channel(session, channel, event))
    return captured


def _evaluate_rule_node(
    rule: dict[str, Any],
    resolve_atom: Callable[[str, dict[str, Any]], tuple[bool | None, str | None]],
) -> tuple[bool | None, list[str], dict[str, Any]]:
    kind = str(rule.get("kind") or "").strip().lower()
    if kind == "atom":
        atom = str(rule.get("atom") or "").strip()
        result, reason = resolve_atom(atom, dict(rule.get("params") or {}))
        detail = {"kind": "atom", "atom": atom, "result": result, "reason": reason, "params": dict(rule.get("params") or {})}
        reasons = [reason] if reason else []
        return result, reasons, detail

    children = [dict(child) for child in rule.get("children") or []]
    child_results = [_evaluate_rule_node(child, resolve_atom) for child in children]
    child_values = [item[0] for item in child_results]
    child_reasons = [reason for _, reasons, _ in child_results for reason in reasons]
    detail = {"kind": kind, "children": [item[2] for item in child_results]}

    if kind == "all":
        if any(value is False for value in child_values):
            return False, child_reasons, detail
        if any(value is None for value in child_values):
            return None, child_reasons, detail
        return True, child_reasons, detail
    if kind == "any":
        if any(value is True for value in child_values):
            return True, child_reasons, detail
        if any(value is None for value in child_values):
            return None, child_reasons, detail
        return False, child_reasons, detail
    if kind == "not":
        value = child_values[0] if child_values else None
        if value is None:
            return None, child_reasons, detail
        return (not value), child_reasons, detail
    return False, [f"Unsupported giveaway rule kind: {kind}"], detail


def evaluate_channel_entrants(channel: GiveawayChannel, *, allow_instagram_private_verification: bool = False) -> None:
    _merge_duplicate_instagram_entrants(channel)
    rule = dict(channel.rules_json or {})
    for entrant in channel.entrants:
        if channel.service == "instagram":
            entrant.signal_state_json = _normalized_instagram_signal_state(dict(entrant.signal_state_json or {}))
        entrant.rule_match_details_json = {}
        entrant.inconclusive_reasons_json = []
        entrant.disqualification_reasons_json = []
        entrant.eligibility_status = ENTRY_STATUS_PENDING

        def resolve_atom(atom: str, params: dict[str, Any]) -> tuple[bool | None, str | None]:
            if channel.service == "instagram":
                return _evaluate_instagram_atom(
                    channel,
                    entrant,
                    atom,
                    params,
                    allow_private_verification=allow_instagram_private_verification,
                )
            return _evaluate_bluesky_atom(channel, entrant, atom, params)

        result, reasons, detail = _evaluate_rule_node(rule, resolve_atom)
        entrant.rule_match_details_json = detail
        if result is True:
            entrant.eligibility_status = ENTRY_STATUS_ELIGIBLE
        elif result is None:
            entrant.eligibility_status = ENTRY_STATUS_PROVISIONAL
            entrant.inconclusive_reasons_json = list(dict.fromkeys(reason for reason in reasons if reason))
        else:
            entrant.eligibility_status = ENTRY_STATUS_DISQUALIFIED
            entrant.disqualification_reasons_json = list(dict.fromkeys(reason for reason in reasons if reason)) or ["Entrant did not satisfy the giveaway rules."]
        _apply_manual_review_override(entrant)


def _randomize_entries(entries: list[GiveawayEntrant]) -> list[GiveawayEntrant]:
    ranked = list(entries)
    secrets.SystemRandom().shuffle(ranked)
    return ranked


def _campaign_winner_count(campaign: GiveawayCampaign) -> int:
    return max(1, int(getattr(campaign, "winner_count", 1) or 1))


def _normalized_entry_ids(values: list[str] | None) -> list[str]:
    entry_ids: list[str] = []
    for value in values or []:
        entry_id = str(value or "").strip()
        if entry_id and entry_id not in entry_ids:
            entry_ids.append(entry_id)
    return entry_ids


def _pool_provisional_winner_ids(pool: GiveawayPoolResult) -> list[str]:
    entry_ids = _normalized_entry_ids(pool.provisional_winner_entry_ids_json)
    if not entry_ids and pool.provisional_winner_entry_id:
        entry_ids = [pool.provisional_winner_entry_id]
    return entry_ids


def _pool_final_winner_ids(pool: GiveawayPoolResult) -> list[str]:
    entry_ids = _normalized_entry_ids(pool.final_winner_entry_ids_json)
    if not entry_ids and pool.final_winner_entry_id:
        entry_ids = [pool.final_winner_entry_id]
    return entry_ids


def _set_pool_provisional_winners(pool: GiveawayPoolResult, winners: list[GiveawayEntrant]) -> None:
    selected = winners[:]
    pool.provisional_winner_entry_ids_json = [entrant.id for entrant in selected if entrant.id]
    pool.provisional_winner_entry = selected[0] if selected else None


def _set_pool_final_winners(pool: GiveawayPoolResult, winners: list[GiveawayEntrant]) -> None:
    selected = winners[:]
    pool.final_winner_entry_ids_json = [entrant.id for entrant in selected if entrant.id]
    pool.final_winner_entry = selected[0] if selected else None


def _clear_pool_winners(pool: GiveawayPoolResult) -> None:
    pool.provisional_winner_entry_ids_json = []
    pool.final_winner_entry_ids_json = []
    pool.provisional_winner_entry = None
    pool.final_winner_entry = None


def _pool_entries(campaign: GiveawayCampaign, pool: GiveawayPoolResult) -> list[GiveawayEntrant]:
    if pool.pool_key == "combined":
        return [entrant for channel in campaign.channels for entrant in channel.entrants]
    return [entrant for channel in campaign.channels if channel.service == pool.pool_key for entrant in channel.entrants]


def _campaign_status_from_pools(campaign: GiveawayCampaign) -> str:
    statuses = {pool.status for pool in campaign.pools}
    if not statuses:
        return campaign.status
    if GIVEAWAY_STATUS_REVIEW_REQUIRED in statuses:
        return GIVEAWAY_STATUS_REVIEW_REQUIRED
    if statuses <= {GIVEAWAY_STATUS_WINNER_CONFIRMED}:
        return GIVEAWAY_STATUS_WINNER_CONFIRMED
    if statuses <= {GIVEAWAY_STATUS_WINNER_SELECTED, GIVEAWAY_STATUS_WINNER_CONFIRMED}:
        return GIVEAWAY_STATUS_WINNER_SELECTED
    if statuses == {GIVEAWAY_STATUS_FAILED}:
        return GIVEAWAY_STATUS_FAILED
    return GIVEAWAY_STATUS_COLLECTING


def _select_giveaway_pool_winners(campaign: GiveawayCampaign) -> None:
    _sync_campaign_pools(campaign)
    winner_count = _campaign_winner_count(campaign)
    for pool in campaign.pools:
        entries = _pool_entries(campaign, pool)
        eligible = [entrant for entrant in entries if entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE]
        provisional = [entrant for entrant in entries if entrant.eligibility_status == ENTRY_STATUS_PROVISIONAL]
        candidate_pool = eligible if eligible else provisional
        ranked_entries = _randomize_entries(candidate_pool)
        pool.candidate_entry_ids_json = [entrant.id for entrant in ranked_entries]
        _clear_pool_winners(pool)
        pool.frozen_at = utcnow()
        pool.last_evaluated_at = utcnow()
        pool.last_error = None
        if not ranked_entries:
            pool.status = GIVEAWAY_STATUS_FAILED
            pool.last_error = "No qualifying giveaway entrants were found."
            continue
        winners = ranked_entries[:winner_count]
        if winners[0].eligibility_status == ENTRY_STATUS_PROVISIONAL:
            pool.status = GIVEAWAY_STATUS_REVIEW_REQUIRED
            _set_pool_provisional_winners(pool, winners)
        else:
            pool.status = GIVEAWAY_STATUS_WINNER_SELECTED
            _set_pool_final_winners(pool, winners)

    campaign.status = _campaign_status_from_pools(campaign)
    campaign.last_error = "No qualifying giveaway entrants were found." if campaign.status == GIVEAWAY_STATUS_FAILED else None


def recalculate_giveaway_entries(
    session: Session,
    campaign: GiveawayCampaign,
    *,
    run_id: str,
) -> GiveawayCampaign:
    hydrate_channel_targets(campaign)
    for channel in campaign.channels:
        if channel.service == "instagram":
            sync_instagram_webhook_events_for_channel(session, channel)
        evaluate_channel_entrants(channel, allow_instagram_private_verification=False)

    channel_errors = [str(channel.last_error) for channel in campaign.channels if channel.last_error]
    campaign.last_error = "; ".join(channel_errors) if channel_errors else None
    campaign.last_evaluated_at = utcnow()
    for pool in campaign.pools:
        pool.last_evaluated_at = utcnow()

    log_run_event(
        session,
        run_id=run_id,
        persona_id=campaign.post.persona_id,
        persona_name=campaign.post.persona.name if campaign.post.persona else None,
        service="giveaway",
        operation="giveaway",
        message=f"Recalculated giveaway entries for post {campaign.post_id}.",
        post_id=campaign.post_id,
        metadata={"campaign_id": campaign.id},
    )
    session.flush()
    return campaign


def _find_campaign_entrant(campaign: GiveawayCampaign, entrant_id: str) -> tuple[GiveawayChannel, GiveawayEntrant]:
    for channel in campaign.channels:
        for entrant in channel.entrants:
            if entrant.id == entrant_id:
                return channel, entrant
    raise ValueError("Giveaway entrant not found.")


def approve_giveaway_entrant(
    session: Session,
    campaign: GiveawayCampaign,
    *,
    entrant_id: str,
    run_id: str,
    note: str | None = None,
    reviewed_by: str | None = None,
) -> GiveawayCampaign:
    channel, entrant = _find_campaign_entrant(campaign, entrant_id)
    state = dict(entrant.signal_state_json or {})
    clean_note = str(note or "").strip()
    state[MANUAL_REVIEW_SIGNAL_KEY] = {
        "status": MANUAL_REVIEW_STATUS_APPROVED,
        "note": clean_note or "Manually approved for this giveaway.",
        "reviewed_at": utcnow().isoformat(),
        "reviewed_by": str(reviewed_by or "").strip() or None,
        "run_id": run_id,
    }
    entrant.signal_state_json = state
    evaluate_channel_entrants(channel, allow_instagram_private_verification=False)
    campaign.last_evaluated_at = utcnow()
    log_run_event(
        session,
        run_id=run_id,
        persona_id=campaign.post.persona_id,
        persona_name=campaign.post.persona.name if campaign.post.persona else None,
        service="giveaway",
        operation="giveaway_manual_review",
        message=f"Manually approved giveaway entrant {_entry_display_label(entrant)} for post {campaign.post_id}.",
        post_id=campaign.post_id,
        metadata={
            "campaign_id": campaign.id,
            "entrant_id": entrant.id,
            "channel_id": channel.id,
            "action": "approve",
            "reviewed_by": str(reviewed_by or "").strip() or None,
        },
    )
    session.flush()
    return campaign


def clear_giveaway_entrant_approval(
    session: Session,
    campaign: GiveawayCampaign,
    *,
    entrant_id: str,
    run_id: str,
    reviewed_by: str | None = None,
) -> GiveawayCampaign:
    channel, entrant = _find_campaign_entrant(campaign, entrant_id)
    state = dict(entrant.signal_state_json or {})
    state.pop(MANUAL_REVIEW_SIGNAL_KEY, None)
    entrant.signal_state_json = state
    evaluate_channel_entrants(channel, allow_instagram_private_verification=False)
    campaign.last_evaluated_at = utcnow()
    log_run_event(
        session,
        run_id=run_id,
        persona_id=campaign.post.persona_id,
        persona_name=campaign.post.persona.name if campaign.post.persona else None,
        service="giveaway",
        operation="giveaway_manual_review",
        message=f"Cleared manual approval for giveaway entrant {_entry_display_label(entrant)} on post {campaign.post_id}.",
        post_id=campaign.post_id,
        metadata={
            "campaign_id": campaign.id,
            "entrant_id": entrant.id,
            "channel_id": channel.id,
            "action": "clear",
            "reviewed_by": str(reviewed_by or "").strip() or None,
        },
    )
    session.flush()
    return campaign


def rerun_giveaway_raffle(
    session: Session,
    campaign: GiveawayCampaign,
    alerts: AlertDispatcher,
    *,
    run_id: str,
) -> GiveawayCampaign:
    recalculate_giveaway_entries(session, campaign, run_id=run_id)
    session.flush()
    campaign.frozen_at = utcnow()
    campaign.last_evaluated_at = utcnow()
    _select_giveaway_pool_winners(campaign)
    if campaign.status == GIVEAWAY_STATUS_FAILED and campaign.last_error:
        alerts.emit_hard_failure(
            session,
            run_id=run_id,
            persona=campaign.post.persona,
            service="giveaway",
            post=campaign.post,
            operation="giveaway",
            message=campaign.last_error,
            error_class="NoQualifyingEntrants",
            event_type="giveaway_failed",
        )
    log_run_event(
        session,
        run_id=run_id,
        persona_id=campaign.post.persona_id,
        persona_name=campaign.post.persona.name if campaign.post.persona else None,
        service="giveaway",
        operation="giveaway",
        message=f"Reran giveaway raffle for post {campaign.post_id}.",
        post_id=campaign.post_id,
        metadata={"campaign_id": campaign.id, "status": campaign.status},
    )
    session.flush()
    return campaign


def reopen_giveaway_campaign(
    session: Session,
    campaign: GiveawayCampaign,
    *,
    giveaway_end_at: datetime,
    run_id: str,
) -> GiveawayCampaign:
    new_end_at = normalize_datetime(giveaway_end_at)
    if new_end_at is None:
        raise ValueError("Choose a new giveaway end date and time.")
    if new_end_at <= utcnow():
        raise ValueError("Choose a giveaway end date and time in the future.")
    if campaign.status in {GIVEAWAY_STATUS_SCHEDULED, GIVEAWAY_STATUS_COLLECTING}:
        raise ValueError("This giveaway is already active. Update the end date and save the post plan instead.")

    hydrate_channel_targets(campaign)
    ready_channels = [channel for channel in campaign.channels if _channel_target_ready(channel)]
    if not ready_channels:
        raise ValueError("This giveaway cannot be reopened until at least one published target post is available.")

    campaign.giveaway_end_at = new_end_at
    campaign.status = GIVEAWAY_STATUS_COLLECTING
    campaign.frozen_at = None
    campaign.last_evaluated_at = None
    campaign.last_error = None

    for channel in campaign.channels:
        channel.status = GIVEAWAY_STATUS_COLLECTING if _channel_target_ready(channel) else GIVEAWAY_STATUS_SCHEDULED
        channel.last_error = None

    _sync_campaign_pools(campaign)
    for pool in campaign.pools:
        pool.status = GIVEAWAY_STATUS_COLLECTING
        pool.candidate_entry_ids_json = []
        _clear_pool_winners(pool)
        pool.frozen_at = None
        pool.last_evaluated_at = None
        pool.last_error = None

    log_run_event(
        session,
        run_id=run_id,
        persona_id=campaign.post.persona_id,
        persona_name=campaign.post.persona.name if campaign.post.persona else None,
        service="giveaway",
        operation="giveaway",
        message=f"Reopened giveaway post {campaign.post_id} until {new_end_at.isoformat()}.",
        post_id=campaign.post_id,
        metadata={"campaign_id": campaign.id, "giveaway_end_at": new_end_at.isoformat()},
    )
    session.flush()
    return campaign


def finalize_giveaway_campaign(
    session: Session,
    campaign: GiveawayCampaign,
    alerts: AlertDispatcher,
    *,
    run_id: str,
    force_instagram_private_scan: bool = False,
    allow_instagram_private_scan: bool = False,
) -> GiveawayCampaign:
    hydrate_channel_targets(campaign)
    for channel in campaign.channels:
        private_scan_ran = False
        if channel.service == "bluesky":
            collect_bluesky_channel_state(session, channel, run_id=run_id)
        elif channel.service == "instagram":
            if force_instagram_private_scan:
                private_scan_ran = refresh_instagram_channel_state(
                    session,
                    channel,
                    force_private_scan=True,
                    private_scan_reason=INSTAGRAM_PRIVATE_REASON_END_OF_GIVEAWAY,
                    run_id=run_id,
                )
            else:
                private_scan_ran = refresh_instagram_channel_state(
                    session,
                    channel,
                    allow_due_private_scan=allow_instagram_private_scan,
                    private_scan_reason=INSTAGRAM_PRIVATE_REASON_WEEKLY_DUE,
                    run_id=run_id,
                )
        evaluate_channel_entrants(channel, allow_instagram_private_verification=force_instagram_private_scan or private_scan_ran)

    # Ensure entrant primary keys exist before we freeze candidate ordering.
    session.flush()
    campaign.frozen_at = utcnow()
    campaign.last_evaluated_at = utcnow()
    _select_giveaway_pool_winners(campaign)
    if campaign.status == GIVEAWAY_STATUS_FAILED:
        campaign.last_error = "No qualifying giveaway entrants were found."
        alerts.emit_hard_failure(
            session,
            run_id=run_id,
            persona=campaign.post.persona,
            service="giveaway",
            post=campaign.post,
            operation="giveaway",
            message=campaign.last_error,
            error_class="NoQualifyingEntrants",
            event_type="giveaway_failed",
        )
    session.flush()
    return campaign


def end_giveaway_campaign(
    session: Session,
    campaign: GiveawayCampaign,
    alerts: AlertDispatcher,
    *,
    run_id: str,
) -> GiveawayCampaign:
    if campaign.status not in {GIVEAWAY_STATUS_SCHEDULED, GIVEAWAY_STATUS_COLLECTING}:
        raise ValueError("This giveaway has already been ended.")

    hydrate_channel_targets(campaign)
    ready_channels = [channel for channel in campaign.channels if _channel_target_ready(channel)]
    if not ready_channels:
        raise ValueError("This giveaway cannot be ended until at least one published target post is available.")

    ended_at = utcnow()
    campaign.giveaway_end_at = ended_at
    campaign.status = GIVEAWAY_STATUS_COLLECTING
    for channel in ready_channels:
        if channel.status == GIVEAWAY_STATUS_SCHEDULED:
            channel.status = GIVEAWAY_STATUS_COLLECTING

    log_run_event(
        session,
        run_id=run_id,
        persona_id=campaign.post.persona_id,
        persona_name=campaign.post.persona.name if campaign.post.persona else None,
        service="giveaway",
        operation="giveaway",
        message=f"Ended giveaway post {campaign.post_id} and started final collection.",
        post_id=campaign.post_id,
        metadata={"campaign_id": campaign.id, "ended_at": ended_at.isoformat()},
    )

    try:
        end_scan_decision = instagram_private_access_decision(INSTAGRAM_PRIVATE_REASON_END_OF_GIVEAWAY)
        if not end_scan_decision.allowed:
            _log_blocked_instagram_end_scan(session, campaign, run_id=run_id, decision=end_scan_decision)
        return finalize_giveaway_campaign(
            session,
            campaign,
            alerts,
            run_id=run_id,
            force_instagram_private_scan=end_scan_decision.allowed,
            allow_instagram_private_scan=False,
        )
    except Exception as exc:
        campaign.status = GIVEAWAY_STATUS_FAILED
        campaign.last_error = str(exc)
        alerts.emit_hard_failure(
            session,
            run_id=run_id,
            persona=campaign.post.persona,
            service="giveaway",
            post=campaign.post,
            operation="giveaway",
            message=str(exc),
            error_class=exc.__class__.__name__,
            event_type="giveaway_failed",
        )
        session.flush()
        return campaign


def scan_instagram_giveaway_channels(
    session: Session,
    campaign: GiveawayCampaign,
    *,
    run_id: str,
) -> GiveawayCampaign:
    hydrate_channel_targets(campaign)
    ready_channels = [
        channel
        for channel in campaign.channels
        if channel.service == "instagram" and _channel_target_ready(channel)
    ]
    if not ready_channels:
        raise ValueError("No published Instagram giveaway post is ready to scan.")

    if campaign.status == GIVEAWAY_STATUS_SCHEDULED:
        campaign.status = GIVEAWAY_STATUS_COLLECTING

    for channel in ready_channels:
        if channel.status == GIVEAWAY_STATUS_SCHEDULED:
            channel.status = GIVEAWAY_STATUS_COLLECTING
        private_scan_ran = refresh_instagram_channel_state(
            session,
            channel,
            force_private_scan=True,
            private_scan_reason=INSTAGRAM_PRIVATE_REASON_MANUAL,
            run_id=run_id,
        )
        evaluate_channel_entrants(channel, allow_instagram_private_verification=private_scan_ran)

    campaign.last_evaluated_at = utcnow()
    channel_errors = [str(channel.last_error) for channel in ready_channels if channel.last_error]
    campaign.last_error = "; ".join(channel_errors) if channel_errors else None
    log_run_event(
        session,
        run_id=run_id,
        persona_id=campaign.post.persona_id,
        persona_name=campaign.post.persona.name if campaign.post.persona else None,
        service="instagram",
        operation="giveaway_private_scan",
        message=f"Ran manual Instagram activity scan for giveaway post {campaign.post_id}.",
        post_id=campaign.post_id,
        metadata={
            "campaign_id": campaign.id,
            "channel_ids": [channel.id for channel in ready_channels],
            "private_scan_mode": instagram_private_scan_mode(),
            "private_scan_reason": INSTAGRAM_PRIVATE_REASON_MANUAL,
            "private_scan_interval_hours": instagram_private_scan_interval_hours(),
        },
    )
    session.flush()
    return campaign


def process_giveaway_lifecycle(
    session: Session,
    alerts: AlertDispatcher,
    *,
    run_id: str,
    post_id: str | None = None,
    allow_instagram_private_scan: bool = False,
) -> str:
    now = utcnow()
    stmt = list_giveaway_campaigns_stmt().where(
        GiveawayCampaign.status.in_([GIVEAWAY_STATUS_SCHEDULED, GIVEAWAY_STATUS_COLLECTING])
    )
    if post_id is not None:
        stmt = stmt.where(GiveawayCampaign.post_id == post_id)
    for campaign in session.scalars(stmt):
        hydrate_channel_targets(campaign)
        ready_channels = [channel for channel in campaign.channels if _channel_target_ready(channel)]
        if ready_channels and campaign.status == GIVEAWAY_STATUS_SCHEDULED:
            campaign.status = GIVEAWAY_STATUS_COLLECTING
        for channel in ready_channels:
            if channel.status == GIVEAWAY_STATUS_SCHEDULED:
                channel.status = GIVEAWAY_STATUS_COLLECTING
        if campaign.status == GIVEAWAY_STATUS_COLLECTING:
            for channel in ready_channels:
                private_scan_ran = False
                if channel.service == "bluesky":
                    try:
                        collect_bluesky_channel_state(session, channel, run_id=run_id)
                    except Exception as exc:
                        message = f"Bluesky giveaway collection failed: {str(exc) or exc.__class__.__name__}"
                        transient_timeout = _is_bluesky_collection_timeout(exc)
                        channel.last_error = message
                        campaign.last_error = message
                        log_run_event(
                            session,
                            run_id=run_id,
                            persona_id=campaign.post.persona_id,
                            persona_name=campaign.post.persona.name if campaign.post.persona else None,
                            account_id=channel.account_id,
                            service=channel.service,
                            operation="giveaway",
                            severity="warning" if transient_timeout else "error",
                            message=message,
                            post_id=campaign.post_id,
                            metadata={"channel_id": channel.id, "transient_timeout": transient_timeout},
                        )
                        if not transient_timeout:
                            alerts.emit_hard_failure(
                                session,
                                run_id=run_id,
                                persona=campaign.post.persona,
                                account=channel.account,
                                service=channel.service,
                                post=campaign.post,
                                operation="giveaway_collection",
                                message=message,
                                error_class=exc.__class__.__name__,
                                event_type="giveaway_collection_failed",
                            )
                elif channel.service == "instagram":
                    private_scan_ran = refresh_instagram_channel_state(
                        session,
                        channel,
                        allow_due_private_scan=allow_instagram_private_scan,
                        private_scan_reason=INSTAGRAM_PRIVATE_REASON_WEEKLY_DUE,
                        run_id=run_id,
                    )
                evaluate_channel_entrants(channel, allow_instagram_private_verification=private_scan_ran)
            channel_errors = [str(channel.last_error) for channel in ready_channels if channel.last_error]
            campaign.last_error = "; ".join(channel_errors) if channel_errors else None
        if normalize_datetime(campaign.giveaway_end_at) and normalize_datetime(campaign.giveaway_end_at) <= now and campaign.status in {GIVEAWAY_STATUS_COLLECTING, GIVEAWAY_STATUS_SCHEDULED}:
            try:
                end_scan_decision = instagram_private_access_decision(INSTAGRAM_PRIVATE_REASON_END_OF_GIVEAWAY)
                if not end_scan_decision.allowed:
                    _log_blocked_instagram_end_scan(session, campaign, run_id=run_id, decision=end_scan_decision)
                finalize_giveaway_campaign(
                    session,
                    campaign,
                    alerts,
                    run_id=run_id,
                    force_instagram_private_scan=end_scan_decision.allowed,
                    allow_instagram_private_scan=False,
                )
            except Exception as exc:
                campaign.status = GIVEAWAY_STATUS_FAILED
                campaign.last_error = str(exc)
                alerts.emit_hard_failure(
                    session,
                    run_id=run_id,
                    persona=campaign.post.persona,
                    service="giveaway",
                    post=campaign.post,
                    operation="giveaway",
                    message=str(exc),
                    error_class=exc.__class__.__name__,
                    event_type="giveaway_failed",
                )
    session.flush()
    return run_id


def _resolve_pool(campaign: GiveawayCampaign, pool_key: str | None) -> GiveawayPoolResult:
    pools = sorted(campaign.pools, key=lambda item: item.pool_key)
    if pool_key:
        pool = next((item for item in pools if item.pool_key == pool_key), None)
        if pool is None:
            raise ValueError("Giveaway pool not found.")
        return pool
    if len(pools) != 1:
        raise ValueError("This giveaway has multiple pools. Specify which pool to review.")
    return pools[0]


def confirm_giveaway_winner(session: Session, campaign: GiveawayCampaign, *, run_id: str, pool_key: str | None = None) -> GiveawayCampaign:
    pool = _resolve_pool(campaign, pool_key)
    provisional_ids = _pool_provisional_winner_ids(pool)
    if pool.status != GIVEAWAY_STATUS_REVIEW_REQUIRED or not provisional_ids:
        raise ValueError("This giveaway pool does not have a provisional winner to confirm.")
    entrant_map = {entrant.id: entrant for channel in campaign.channels for entrant in channel.entrants}
    confirmed = [entrant_map[entrant_id] for entrant_id in provisional_ids if entrant_id in entrant_map]
    if not confirmed:
        raise ValueError("Could not resolve the provisional giveaway winners.")
    _set_pool_final_winners(pool, confirmed)
    _set_pool_provisional_winners(pool, [])
    pool.status = GIVEAWAY_STATUS_WINNER_CONFIRMED
    campaign.status = _campaign_status_from_pools(campaign)
    session.flush()
    return campaign


def advance_giveaway_winner(session: Session, campaign: GiveawayCampaign, *, run_id: str, pool_key: str | None = None) -> GiveawayCampaign:
    pool = _resolve_pool(campaign, pool_key)
    provisional_ids = _pool_provisional_winner_ids(pool)
    if pool.status != GIVEAWAY_STATUS_REVIEW_REQUIRED or not provisional_ids:
        raise ValueError("This giveaway pool does not have a provisional winner to advance.")
    candidate_ids = list(pool.candidate_entry_ids_json or [])
    current_id = provisional_ids[0]
    try:
        current_index = candidate_ids.index(current_id)
    except ValueError as exc:
        raise ValueError("The provisional winner is not part of the current candidate pool.") from exc
    if current_index + 1 >= len(candidate_ids):
        raise ValueError("There are no remaining giveaway candidates to advance to.")
    next_ids = candidate_ids[current_index + 1 : current_index + 1 + _campaign_winner_count(campaign)]
    channel_entrant_map = {entrant.id: entrant for channel in campaign.channels for entrant in channel.entrants}
    next_entries = [channel_entrant_map[entrant_id] for entrant_id in next_ids if entrant_id in channel_entrant_map]
    if not next_entries:
        raise ValueError("Could not resolve the next giveaway candidate.")
    _set_pool_provisional_winners(pool, next_entries)
    session.flush()
    return campaign


def refresh_instagram_channel_state(
    session: Session,
    channel: GiveawayChannel,
    *,
    force_private_scan: bool = False,
    allow_due_private_scan: bool = False,
    private_scan_reason: str | None = None,
    run_id: str | None = None,
) -> bool:
    sync_instagram_webhook_events_for_channel(session, channel)
    state_by_user: dict[str, dict[str, Any]] = {}
    for entrant in channel.entrants:
        state_by_user[entrant.provider_user_id] = _normalized_instagram_signal_state(dict(entrant.signal_state_json or {}))

    reason = private_scan_reason or (INSTAGRAM_PRIVATE_REASON_MANUAL if force_private_scan else INSTAGRAM_PRIVATE_REASON_WEEKLY_DUE)
    should_run_private_scan = force_private_scan or (allow_due_private_scan and instagram_private_scan_is_due(channel))
    if should_run_private_scan:
        decision = instagram_private_access_decision(reason)
        if not decision.allowed:
            _log_instagram_private_scan_event(
                session,
                channel,
                run_id=run_id,
                decision=decision,
                status="blocked",
                severity="warning",
                message=decision.message,
            )
            should_run_private_scan = False
        else:
            private_scan_started_at = utcnow()
            channel.last_private_collected_at = private_scan_started_at
            channel.last_collected_at = private_scan_started_at
            dependency_issue = _instagram_destination_dependency_issue()
            if dependency_issue:
                channel.last_error = dependency_issue
            else:
                try:
                    rule_atoms = _rule_tree_atoms(channel.rules_json)
                    needs_comment_scan = bool(rule_atoms.intersection(INSTAGRAM_COMMENT_RULE_ATOMS))
                    needs_like_scan = "like_present" in rule_atoms
                    needs_follow_scan = "follow_present" in rule_atoms
                    needs_repost_scan = "repost_present" in rule_atoms
                    needs_private_client = (
                        needs_comment_scan
                        or needs_like_scan
                        or (needs_repost_scan and bool(state_by_user))
                        or (needs_follow_scan and bool(state_by_user))
                    )
                    client = _authenticated_publish_client(_account_credentials(channel.account)) if needs_private_client else None
                    live_media_id: str | None = None

                    if client is not None and needs_comment_scan:
                        live_comments, live_media_id = _instagram_media_comments(client, channel)
                        observed_comments: list[tuple[GiveawayEntrant, dict[str, Any], dict[str, Any]]] = []
                        if force_private_scan:
                            for state in state_by_user.values():
                                state["comments"] = []
                                state["comment_replies"] = []
                                state["comment_count"] = 0
                                state["friend_mention_count"] = 0
                                state["friend_mentions"] = []
                        for comment in live_comments or []:
                            comment_created_at = normalize_datetime(getattr(comment, "created_at_utc", None))
                            if not _instagram_campaign_window_accepts_event(channel.campaign, occurred_at=comment_created_at):
                                continue
                            user = getattr(comment, "user", None)
                            provider_user_id = str(getattr(user, "pk", "") or "").strip()
                            provider_username = str(getattr(user, "username", "") or "").strip() or None
                            if not provider_user_id:
                                continue
                            existing = state_by_user.setdefault(provider_user_id, _normalized_instagram_signal_state({}))
                            parent_id = str(
                                getattr(comment, "parent_pk", "")
                                or getattr(comment, "parent_id", "")
                                or getattr(comment, "parent_comment_id", "")
                                or getattr(comment, "replied_to_comment_id", "")
                                or ""
                            ).strip() or None
                            existing["comments"].append(
                                {
                                    "comment_id": str(getattr(comment, "pk", "") or "").strip() or None,
                                    "parent_id": parent_id,
                                    "is_reply": bool(parent_id),
                                    "text": str(getattr(comment, "text", "") or "").strip(),
                                    "source": "close_time_live",
                                    "created_time": comment_created_at.isoformat() if comment_created_at else None,
                                }
                            )
                            entrant = get_or_create_channel_entrant(
                                channel,
                                provider_user_id=provider_user_id,
                                provider_username=provider_username,
                                display_label=provider_username or provider_user_id,
                                prefer_provider_user_id=True,
                            )
                            entrant.signal_state_json = dict(entrant.signal_state_json or {})
                            observed_comments.append(
                                (
                                    entrant,
                                    existing["comments"][-1],
                                    {"created_time": existing["comments"][-1].get("created_time")},
                                )
                            )
                        session.flush()
                        _sync_instagram_live_comment_events(session, channel, observed_comments)

                    if client is not None and needs_like_scan:
                        live_likers, live_media_id = _instagram_media_likers(client, channel)
                        observed_likes: list[tuple[GiveawayEntrant, dict[str, Any]]] = []
                        for state in state_by_user.values():
                            if force_private_scan:
                                state["likes"] = []
                                state["like_present"] = False
                                state["like_collection_checked"] = True
                            elif not state.get("like_present"):
                                state["like_present"] = False
                                state["like_collection_checked"] = True
                        for liker in live_likers or []:
                            provider_user_id, provider_username = _instagram_user_identity(liker)
                            if not provider_user_id:
                                continue
                            existing = state_by_user.setdefault(provider_user_id, _normalized_instagram_signal_state({}))
                            existing["like_collection_checked"] = True
                            entrant = get_or_create_channel_entrant(
                                channel,
                                provider_user_id=provider_user_id,
                                provider_username=provider_username,
                                display_label=provider_username or provider_user_id,
                                prefer_provider_user_id=True,
                            )
                            like_summary = {
                                "like_id": f"like:{provider_user_id}:{live_media_id}",
                                "media_id": live_media_id,
                                "actor_id": provider_user_id,
                                "actor_username": provider_username,
                                "source": INSTAGRAM_LIVE_COLLECTION_SOURCE,
                            }
                            existing["likes"] = _append_unique_evidence_item(
                                list(existing.get("likes") or []),
                                like_summary,
                                key_fields=("like_id",),
                            )
                            existing["like_present"] = True
                            entrant.signal_state_json = dict(entrant.signal_state_json or {})
                            observed_likes.append((entrant, like_summary))
                        session.flush()
                        _sync_instagram_live_like_events(session, channel, observed_likes)

                    if client is not None and needs_follow_scan and state_by_user:
                        requested_user_ids = sorted(state_by_user)
                        for provider_user_id in requested_user_ids:
                            state = state_by_user.setdefault(provider_user_id, _normalized_instagram_signal_state({}))
                            state["follow_present"] = False
                            state["follow_collection_checked"] = True
                        friendship_statuses = _instagram_user_friendships(client, requested_user_ids)
                        for provider_user_id, follows_account in friendship_statuses.items():
                            state = state_by_user.setdefault(provider_user_id, _normalized_instagram_signal_state({}))
                            state["follow_present"] = bool(follows_account)
                            state["follow_collection_checked"] = True

                    if client is not None and needs_repost_scan:
                        target_media_ids = set(_instagram_media_identifier_candidates(client, channel))
                        if live_media_id:
                            target_media_ids.add(str(live_media_id))
                        target_media_codes = _instagram_target_media_codes(channel)
                        observed_reposts: list[tuple[GiveawayEntrant, dict[str, Any]]] = []
                        for provider_user_id, state in list(state_by_user.items()):
                            provider_username = str(state.get("provider_username") or "").strip() or None
                            entrant = next(
                                (
                                    item
                                    for item in channel.entrants
                                    if item.provider_user_id == provider_user_id
                                ),
                                None,
                            )
                            if entrant is not None:
                                provider_username = entrant.provider_username or provider_username
                            try:
                                stories = _instagram_user_stories(client, provider_user_id)
                            except Exception:
                                continue
                            for story in stories:
                                repost_summary = _instagram_story_share_summary(
                                    story,
                                    target_media_ids=target_media_ids,
                                    target_media_codes=target_media_codes,
                                    provider_user_id=provider_user_id,
                                    provider_username=provider_username,
                                )
                                if repost_summary is None:
                                    continue
                                entrant = entrant or get_or_create_channel_entrant(
                                    channel,
                                    provider_user_id=provider_user_id,
                                    provider_username=provider_username,
                                    display_label=provider_username or provider_user_id,
                                    prefer_provider_user_id=True,
                                )
                                state["reposts"] = _append_unique_evidence_item(
                                    list(state.get("reposts") or []),
                                    repost_summary,
                                    key_fields=("repost_id",),
                                )
                                state["repost_present"] = True
                                entrant.signal_state_json = dict(entrant.signal_state_json or {})
                                observed_reposts.append((entrant, repost_summary))
                                break
                        session.flush()
                        _sync_instagram_live_repost_events(session, channel, observed_reposts)
                    channel.last_error = None
                except Exception as exc:
                    channel.last_error = f"Instagram live activity collection failed: {exc}"
            _log_instagram_private_scan_event(
                session,
                channel,
                run_id=run_id,
                decision=decision,
                status="failed" if channel.last_error else "completed",
                severity="warning" if channel.last_error else "info",
                message=(
                    f"Instagram private {decision.reason.replace('_', ' ')} scan "
                    f"{'failed' if channel.last_error else 'completed'} for giveaway post {channel.campaign.post_id}."
                ),
                error=channel.last_error,
            )

    for entrant in channel.entrants:
        state = state_by_user.setdefault(entrant.provider_user_id, _normalized_instagram_signal_state(dict(entrant.signal_state_json or {})))
        entrant.signal_state_json = _normalized_instagram_signal_state(state)
    session.flush()
    publish_live_update(LIVE_UPDATE_TOPIC_DASHBOARD, LIVE_UPDATE_TOPIC_LOGS)
    return should_run_private_scan


def _is_bluesky_collection_timeout(exc: Exception) -> bool:
    class_name = exc.__class__.__name__.lower()
    return isinstance(exc, TimeoutError) or "timeout" in class_name


def _call_bluesky_collection(fetch: Callable[[dict[str, Any]], Any], params: dict[str, Any]) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(BLUESKY_COLLECTION_MAX_ATTEMPTS):
        try:
            response = fetch(params)
            try:
                payload = response.model_dump(mode="json", by_alias=True)
            except TypeError:
                payload = response.model_dump()
            return payload if isinstance(payload, dict) else {}
        except Exception as exc:
            last_error = exc
            if not _is_bluesky_collection_timeout(exc) or attempt == BLUESKY_COLLECTION_MAX_ATTEMPTS - 1:
                raise
            time.sleep(1 + attempt)
    if last_error is not None:
        raise last_error
    return {}


def _snake_case_key(name: str) -> str:
    result: list[str] = []
    for character in name:
        if character.isupper():
            result.append("_")
            result.append(character.lower())
        else:
            result.append(character)
    return "".join(result).lstrip("_")


def _bluesky_payload_value(payload: dict[str, Any], key: str, default: Any = None) -> Any:
    if key in payload:
        return payload.get(key)
    return payload.get(_snake_case_key(key), default)


def _bluesky_actor_identity(payload: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    for key in ("actor", "author", "subject", "profile"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            did = str(nested.get("did") or "").strip() or None
            handle = str(nested.get("handle") or "").strip() or None
            display_name = str(nested.get("displayName") or nested.get("display_name") or "").strip() or None
            if did:
                return did, handle, display_name
    did = str(payload.get("did") or "").strip() or None
    handle = str(payload.get("handle") or "").strip() or None
    display_name = str(payload.get("displayName") or payload.get("display_name") or "").strip() or None
    return did, handle, display_name


def _collect_all_pages(fetch_page: Callable[..., Any], *, key: str, uri: str, cid: str | None = None) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    cursor: str | None = None
    for _ in range(10):
        params: dict[str, Any] = {"uri": uri, "limit": 100}
        if cid:
            params["cid"] = cid
        if cursor:
            params["cursor"] = cursor
        payload = _call_bluesky_collection(fetch_page, params)
        items.extend(list(_bluesky_payload_value(payload, key, []) or []))
        cursor = payload.get("cursor")
        if not cursor:
            break
    return items


def _bluesky_author_feed_has_repost(client: Any, *, actor: str, target_uri: str, target_cid: str | None = None) -> bool:
    cursor: str | None = None
    for _ in range(3):
        params: dict[str, Any] = {
            "actor": actor,
            "limit": 100,
            "includePins": False,
            "filter": "posts_with_replies",
        }
        if cursor:
            params["cursor"] = cursor
        payload = _call_bluesky_collection(client.app.bsky.feed.get_author_feed, params)
        for item in list(_bluesky_payload_value(payload, "feed", []) or []):
            if not isinstance(item, dict):
                continue
            post = dict(item.get("post") or {})
            if str(post.get("uri") or "").strip() != target_uri:
                continue
            if target_cid and str(post.get("cid") or "").strip() not in {"", target_cid}:
                continue
            reason = dict(item.get("reason") or {})
            reason_type = str(reason.get("$type") or reason.get("py_type") or "").strip()
            if reason_type.endswith("#reasonRepost") or "reasonRepost" in reason_type:
                return True
        cursor = payload.get("cursor")
        if not cursor:
            break
    return False


def _walk_thread_replies(thread: dict[str, Any], *, target_uri: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    def visit(node: dict[str, Any]) -> None:
        post = node.get("post")
        if isinstance(post, dict):
            record = dict(post.get("record") or {})
            reply = dict(record.get("reply") or {})
            parent = dict(reply.get("parent") or {})
            if str(parent.get("uri") or "").strip() == target_uri:
                results.append(post)
        for reply_node in node.get("replies") or []:
            if isinstance(reply_node, dict):
                visit(reply_node)

    if isinstance(thread, dict):
        visit(thread)
    return results


def _sync_bluesky_activity_events(
    session: Session,
    channel: GiveawayChannel,
    *,
    entrants_by_user_id: dict[str, GiveawayEntrant],
    replies: list[dict[str, Any]],
    quotes: list[dict[str, Any]],
    likes: list[dict[str, Any]],
    reposts: list[dict[str, Any]],
) -> None:
    observed: dict[tuple[str, str], dict[str, Any]] = {}
    seen_at = utcnow().isoformat()

    def remember(
        event_type: str,
        provider_event_id: str,
        entrant: GiveawayEntrant | None,
        payload: dict[str, Any],
    ) -> None:
        if not provider_event_id:
            return
        observed[(event_type, provider_event_id)] = {
            "entrant": entrant,
            "payload": dict(payload),
        }

    for post in replies:
        author = dict(post.get("author") or {})
        did = str(author.get("did") or "").strip()
        uri = str(post.get("uri") or "").strip()
        if not did or not uri:
            continue
        remember(
            "bluesky_reply",
            uri,
            entrants_by_user_id.get(did),
            {
                "actor_did": did,
                "actor_handle": str(author.get("handle") or "").strip() or None,
                "actor_display_label": str(author.get("handle") or "").strip() or did,
                "text": str((post.get("record") or {}).get("text") or "").strip(),
                "uri": uri,
            },
        )

    for post in quotes:
        author = dict(post.get("author") or {})
        did = str(author.get("did") or "").strip()
        uri = str(post.get("uri") or "").strip()
        if not did or not uri:
            continue
        remember(
            "bluesky_quote",
            uri,
            entrants_by_user_id.get(did),
            {
                "actor_did": did,
                "actor_handle": str(author.get("handle") or "").strip() or None,
                "actor_display_label": str(author.get("handle") or "").strip() or did,
                "text": str((post.get("record") or {}).get("text") or "").strip(),
                "uri": uri,
            },
        )

    for like in likes:
        did, handle, display_name = _bluesky_actor_identity(like)
        if not did:
            continue
        remember(
            "bluesky_like",
            f"like:{did}",
            entrants_by_user_id.get(did),
            {
                "actor_did": did,
                "actor_handle": handle,
                "actor_display_label": display_name or handle or did,
            },
        )

    for repost in reposts:
        did, handle, display_name = _bluesky_actor_identity(repost)
        if not did:
            continue
        remember(
            "bluesky_repost",
            f"repost:{did}",
            entrants_by_user_id.get(did),
            {
                "actor_did": did,
                "actor_handle": handle,
                "actor_display_label": display_name or handle or did,
            },
        )

    for did, entrant in entrants_by_user_id.items():
        state = dict(entrant.signal_state_json or {})
        if not state.get("follow_present"):
            continue
        remember(
            "bluesky_follow",
            f"follow:{did}",
            entrant,
            {
                "actor_did": did,
                "actor_handle": entrant.provider_username,
                "actor_display_label": entrant.display_label or entrant.provider_username or did,
            },
        )

    existing_events = list(
        session.scalars(
            select(GiveawayEvidenceEvent).where(
                GiveawayEvidenceEvent.channel_id == channel.id,
                GiveawayEvidenceEvent.source == "collector",
                GiveawayEvidenceEvent.event_type.in_(BLUESKY_ACTIVITY_EVENT_TYPES),
            )
        )
    )
    existing_by_key = {
        (event.event_type, str(event.provider_event_id or "")): event
        for event in existing_events
    }

    for key, item in observed.items():
        event_type, provider_event_id = key
        payload = dict(item["payload"] or {})
        payload["last_seen_at"] = seen_at
        existing = existing_by_key.get(key)
        if existing is None:
            payload["first_seen_at"] = seen_at
            _record_evidence_event(
                session,
                channel.campaign,
                channel,
                entrant=item["entrant"],
                provider_event_id=provider_event_id,
                event_type=event_type,
                source="collector",
                payload=payload,
            )
            continue
        existing_payload = dict(existing.payload_json or {})
        payload["first_seen_at"] = existing_payload.get("first_seen_at") or existing.created_at.isoformat()
        existing.entrant_id = item["entrant"].id if item["entrant"] else None
        existing.payload_json = payload
        existing.active = True

    observed_keys = set(observed)
    for key, existing in existing_by_key.items():
        if key in observed_keys:
            continue
        payload = dict(existing.payload_json or {})
        payload["last_seen_at"] = seen_at
        existing.payload_json = payload
        existing.active = False


def _instagram_user_identity(user: Any) -> tuple[str | None, str | None]:
    if isinstance(user, dict):
        provider_user_id = str(user.get("pk") or user.get("id") or user.get("user_id") or "").strip() or None
        provider_username = str(user.get("username") or user.get("name") or "").strip() or None
        return provider_user_id, provider_username
    provider_user_id = str(
        getattr(user, "pk", "")
        or getattr(user, "id", "")
        or getattr(user, "user_id", "")
        or ""
    ).strip() or None
    provider_username = str(getattr(user, "username", "") or getattr(user, "name", "") or "").strip() or None
    return provider_user_id, provider_username


def _sync_instagram_live_like_events(
    session: Session,
    channel: GiveawayChannel,
    observed_likes: list[tuple[GiveawayEntrant, dict[str, Any]]],
) -> None:
    existing_events = list(
        session.scalars(
            select(GiveawayEvidenceEvent).where(
                GiveawayEvidenceEvent.channel_id == channel.id,
                GiveawayEvidenceEvent.event_type == "instagram_like",
                GiveawayEvidenceEvent.source == INSTAGRAM_LIVE_COLLECTION_SOURCE,
            )
        )
    )
    existing_by_key = {str(event.provider_event_id or ""): event for event in existing_events}
    observed_keys: set[str] = set()
    seen_at = utcnow().isoformat()

    for entrant, summary in observed_likes:
        provider_event_id = str(summary.get("like_id") or "").strip()
        if not provider_event_id:
            continue
        observed_keys.add(provider_event_id)
        payload = {
            "change": {
                "field": "likes",
                "value": {
                    "media_id": channel.target_post_external_id,
                    "id": provider_event_id,
                    "from": {
                        "id": entrant.provider_user_id,
                        "username": entrant.provider_username,
                    },
                },
            },
            "source": INSTAGRAM_LIVE_COLLECTION_SOURCE,
            "last_seen_at": seen_at,
        }
        existing = existing_by_key.get(provider_event_id)
        if existing is None:
            payload["first_seen_at"] = seen_at
            _record_evidence_event(
                session,
                channel.campaign,
                channel,
                entrant=entrant,
                provider_event_id=provider_event_id,
                event_type="instagram_like",
                source=INSTAGRAM_LIVE_COLLECTION_SOURCE,
                payload=payload,
            )
            continue
        existing.entrant_id = entrant.id
        existing_payload = dict(existing.payload_json or {})
        payload["first_seen_at"] = existing_payload.get("first_seen_at") or existing.created_at.isoformat()
        existing.payload_json = payload
        existing.active = True

    for key, existing in existing_by_key.items():
        if key in observed_keys:
            continue
        payload = dict(existing.payload_json or {})
        payload["last_seen_at"] = seen_at
        existing.payload_json = payload
        existing.active = False


def _sync_instagram_live_repost_events(
    session: Session,
    channel: GiveawayChannel,
    observed_reposts: list[tuple[GiveawayEntrant, dict[str, Any]]],
) -> None:
    existing_events = list(
        session.scalars(
            select(GiveawayEvidenceEvent).where(
                GiveawayEvidenceEvent.channel_id == channel.id,
                GiveawayEvidenceEvent.event_type == "instagram_repost",
                GiveawayEvidenceEvent.source == INSTAGRAM_LIVE_COLLECTION_SOURCE,
            )
        )
    )
    existing_by_key = {str(event.provider_event_id or ""): event for event in existing_events}
    observed_keys: set[str] = set()
    seen_at = utcnow().isoformat()

    for entrant, summary in observed_reposts:
        provider_event_id = str(summary.get("repost_id") or "").strip()
        if not provider_event_id:
            continue
        observed_keys.add(provider_event_id)
        payload = {
            "change": {
                "field": "shares",
                "value": {
                    "media_id": summary.get("media_id") or channel.target_post_external_id,
                    "media_code": summary.get("media_code"),
                    "id": provider_event_id,
                    "story_id": summary.get("story_id"),
                    "from": {
                        "id": entrant.provider_user_id,
                        "username": entrant.provider_username,
                    },
                },
            },
            "source": INSTAGRAM_LIVE_COLLECTION_SOURCE,
            "last_seen_at": seen_at,
        }
        existing = existing_by_key.get(provider_event_id)
        if existing is None:
            payload["first_seen_at"] = seen_at
            _record_evidence_event(
                session,
                channel.campaign,
                channel,
                entrant=entrant,
                provider_event_id=provider_event_id,
                event_type="instagram_repost",
                source=INSTAGRAM_LIVE_COLLECTION_SOURCE,
                payload=payload,
            )
            continue
        existing.entrant_id = entrant.id
        existing_payload = dict(existing.payload_json or {})
        payload["first_seen_at"] = existing_payload.get("first_seen_at") or existing.created_at.isoformat()
        existing.payload_json = payload
        existing.active = True

    for key, existing in existing_by_key.items():
        if key in observed_keys:
            continue
        payload = dict(existing.payload_json or {})
        payload["last_seen_at"] = seen_at
        existing.payload_json = payload
        existing.active = False


def collect_bluesky_channel_state(session: Session, channel: GiveawayChannel, *, run_id: str) -> None:
    handle = str(_account_credentials(channel.account).get("handle") or "").strip()
    if not handle:
        channel.last_error = "Bluesky handle is missing for giveaway collection."
        return
    client = _get_bluesky_client(_account_credentials(channel.account))
    if not channel.target_post_uri and channel.target_post_external_id:
        channel.target_post_uri, owner_did = _resolve_bluesky_uri(handle, channel.target_post_external_id)
    else:
        owner_did = channel.target_post_uri.split("/")[2] if channel.target_post_uri else None
    if not channel.target_post_uri:
        channel.last_error = "Bluesky target post URI is not available for giveaway collection."
        return

    likes = _collect_all_pages(client.app.bsky.feed.get_likes, key="likes", uri=channel.target_post_uri, cid=channel.target_post_cid)
    reposts = _collect_all_pages(client.app.bsky.feed.get_reposted_by, key="repostedBy", uri=channel.target_post_uri, cid=channel.target_post_cid)
    quotes = _collect_all_pages(client.app.bsky.feed.get_quotes, key="posts", uri=channel.target_post_uri, cid=channel.target_post_cid)
    thread = _call_bluesky_collection(
        client.app.bsky.feed.get_post_thread,
        {"uri": channel.target_post_uri, "depth": 10},
    )
    if not channel.target_post_cid:
        channel.target_post_cid = str(thread.get("thread", {}).get("post", {}).get("cid") or "").strip() or None
    replies = _walk_thread_replies(dict(thread.get("thread") or {}), target_uri=channel.target_post_uri)

    entrants: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "provider_username": None,
        "display_label": None,
        "reply_present": False,
        "quote_present": False,
        "like_present": False,
        "repost_present": False,
        "follow_present": None,
        "reply_posts": [],
        "quote_posts": [],
        "reply_or_quote_mention_count": 0,
    })

    for post in replies:
        author = dict(post.get("author") or {})
        did = str(author.get("did") or "").strip()
        if not did:
            continue
        text = str((post.get("record") or {}).get("text") or "").strip()
        mention_count = len({match.lower() for match in BLUESKY_MENTION_PATTERN.findall(text)})
        entry = entrants[did]
        entry["provider_username"] = str(author.get("handle") or "").strip() or entry["provider_username"]
        entry["display_label"] = entry["provider_username"] or did
        entry["reply_present"] = True
        entry["reply_posts"].append({"uri": post.get("uri"), "text": text})
        entry["reply_or_quote_mention_count"] = max(int(entry["reply_or_quote_mention_count"] or 0), mention_count)

    for post in quotes:
        author = dict(post.get("author") or {})
        did = str(author.get("did") or "").strip()
        if not did:
            continue
        text = str((post.get("record") or {}).get("text") or "").strip()
        mention_count = len({match.lower() for match in BLUESKY_MENTION_PATTERN.findall(text)})
        entry = entrants[did]
        entry["provider_username"] = str(author.get("handle") or "").strip() or entry["provider_username"]
        entry["display_label"] = entry["provider_username"] or did
        entry["quote_present"] = True
        entry["quote_posts"].append({"uri": post.get("uri"), "text": text})
        entry["reply_or_quote_mention_count"] = max(int(entry["reply_or_quote_mention_count"] or 0), mention_count)

    for like in likes:
        did, handle, display_name = _bluesky_actor_identity(like)
        if not did:
            continue
        entry = entrants[did]
        entry["provider_username"] = handle or entry["provider_username"]
        entry["display_label"] = display_name or entry["provider_username"] or did
        entry["like_present"] = True

    for repost in reposts:
        did, handle, display_name = _bluesky_actor_identity(repost)
        if not did:
            continue
        entry = entrants[did]
        entry["provider_username"] = handle or entry["provider_username"]
        entry["display_label"] = display_name or entry["provider_username"] or did
        entry["repost_present"] = True

    for did, entry in list(entrants.items()):
        if entry.get("repost_present"):
            continue
        try:
            has_repost = _bluesky_author_feed_has_repost(
                client,
                actor=did,
                target_uri=channel.target_post_uri,
                target_cid=channel.target_post_cid,
            )
        except Exception:
            continue
        if not has_repost:
            continue
        entry["repost_present"] = True
        reposts.append(
            {
                "did": did,
                "handle": entry.get("provider_username"),
                "displayName": entry.get("display_label"),
                "source": "author_feed",
            }
        )

    other_dids = list(entrants.keys())
    for index in range(0, len(other_dids), 30):
        batch = other_dids[index : index + 30]
        relationships = _call_bluesky_collection(
            client.app.bsky.graph.get_relationships,
            {"actor": owner_did or handle, "others": batch},
        )
        for item in relationships.get("relationships") or []:
            did = str(item.get("did") or "").strip()
            if did in entrants:
                entrants[did]["follow_present"] = bool(_bluesky_payload_value(item, "followedBy"))

    for provider_user_id, state in entrants.items():
        entrant = get_or_create_channel_entrant(
            channel,
            provider_user_id=provider_user_id,
            provider_username=state["provider_username"],
            display_label=state["display_label"],
        )
        entrant.signal_state_json = dict(state)

    for entrant in channel.entrants:
        if entrant.provider_user_id in entrants:
            continue
        entrant.signal_state_json = {
            "reply_present": False,
            "quote_present": False,
            "like_present": False,
            "repost_present": False,
            "follow_present": None,
            "reply_posts": [],
            "quote_posts": [],
            "reply_or_quote_mention_count": 0,
        }

    session.flush()
    entrants_by_user_id = {
        entrant.provider_user_id: entrant
        for entrant in channel.entrants
    }
    _sync_bluesky_activity_events(
        session,
        channel,
        entrants_by_user_id=entrants_by_user_id,
        replies=replies,
        quotes=quotes,
        likes=likes,
        reposts=reposts,
    )
    channel.last_collected_at = utcnow()
    channel.last_error = None
    _record_evidence_event(
        session,
        channel.campaign,
        channel,
        entrant=None,
        provider_event_id=None,
        event_type="bluesky_collection_snapshot",
        source="collector",
        payload={
            "reply_count": len(replies),
            "quote_count": len(quotes),
            "like_count": len(likes),
            "repost_count": len(reposts),
            "entrant_count": len(entrants),
        },
    )
    session.flush()
    publish_live_update(LIVE_UPDATE_TOPIC_DASHBOARD, LIVE_UPDATE_TOPIC_LOGS)
