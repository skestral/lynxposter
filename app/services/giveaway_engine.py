from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import re
import secrets
from typing import Any, Callable

import requests
from sqlalchemy import Select, select
from sqlalchemy.orm import Session, selectinload

from app.adapters.bluesky import _get_client as _get_bluesky_client
from app.adapters.bluesky import _post_id_from_uri as _bluesky_post_id_from_uri
from app.adapters.instagram import _authenticated_publish_client, _instagram_destination_dependency_issue
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
    return {"kind": "all", "children": children}


def giveaway_config_input_from_json(config_json: dict[str, Any] | None) -> GiveawayConfigInput:
    payload = dict(config_json or {})
    if payload.get("channels") is None and any(
        key in payload for key in ("min_friend_mentions", "required_keywords", "required_hashtags", "require_story_mention", "require_like", "require_follow")
    ):
        payload = {
            "giveaway_end_at": payload.get("giveaway_end_at"),
            "pool_mode": "combined",
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
    if legacy.final_winner_rank:
        winner = _legacy_entry_by_rank(legacy, legacy.final_winner_rank)
        if winner:
            matching = next((entrant for entrant in channel.entrants if entrant.provider_user_id == winner.instagram_user_id), None)
            pool.final_winner_entry = matching
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
            status=GIVEAWAY_STATUS_SCHEDULED,
        )
        post.giveaway_campaign = campaign
        session.add(campaign)
    else:
        campaign.giveaway_end_at = normalize_datetime(giveaway_config.giveaway_end_at) or utcnow()
        campaign.pool_mode = giveaway_config.pool_mode
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


def _normalize_evidence_items(items: list[dict[str, Any]] | None, *, default_source: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        payload = dict(item)
        payload["source"] = str(payload.get("source") or "").strip() or default_source
        normalized.append(payload)
    return normalized


def _normalized_instagram_signal_state(state: dict[str, Any] | None) -> dict[str, Any]:
    raw_state = dict(state or {})
    comments = _normalize_evidence_items(
        raw_state.get("comments"),
        default_source=INSTAGRAM_WEBHOOK_CAPTURE_SOURCE,
    )
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
    normalized: dict[str, Any] = {
        "comments": comments,
        "comment_count": len(comments),
        "friend_mention_count": len({match.lower() for match in INSTAGRAM_MENTION_PATTERN.findall(combined_text)}),
        "story_mentions": story_mentions,
        "story_mention_count": len(story_mentions),
        "likes": likes,
        "like_present": bool(raw_state.get("like_present") or likes),
        "reposts": reposts,
        "repost_present": bool(raw_state.get("repost_present") or reposts),
    }
    if "follow_present" in raw_state:
        normalized["follow_present"] = raw_state.get("follow_present")
    return normalized


def _append_unique_evidence_item(items: list[dict[str, Any]], item: dict[str, Any], *, key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    candidate = dict(item)
    for existing in items:
        if not isinstance(existing, dict):
            continue
        if all(str(existing.get(key) or "").strip() == str(candidate.get(key) or "").strip() for key in key_fields):
            return items
    return [*items, candidate]


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
    return GiveawayEntrantRead(
        id=entrant.id,
        service=channel.service,
        provider_user_id=entrant.provider_user_id,
        provider_username=entrant.provider_username,
        display_label=_entry_display_label(entrant),
        signal_state=dict(entrant.signal_state_json or {}),
        rule_match_details=dict(entrant.rule_match_details_json or {}),
        activity_total=activity_total,
        activity_breakdown=activity_breakdown,
        checks=checks,
        eligibility_status=entrant.eligibility_status,
        inconclusive_reasons=list(entrant.inconclusive_reasons_json or []),
        disqualification_reasons=list(entrant.disqualification_reasons_json or []),
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
    candidate_source = "eligible entrants" if eligible_members else "provisional fallback" if provisional_members else "no qualifying entrants"
    selected_id = None
    if pool.final_winner_entry is not None:
        selected_id = pool.final_winner_entry.id
    elif pool.provisional_winner_entry is not None:
        selected_id = pool.provisional_winner_entry.id
    candidates: list[GiveawaySelectionCandidateRead] = []
    for index, entrant_id in enumerate(pool.candidate_entry_ids_json or [], start=1):
        entrant = serialized_entrant_map.get(entrant_id)
        if entrant is None:
            continue
        note = None
        if entrant_id == selected_id:
            note = "Selected as the top candidate after the randomized draw."
            if pool.status == GIVEAWAY_STATUS_REVIEW_REQUIRED:
                note = "Selected as the top provisional candidate pending review."
        candidates.append(
            GiveawaySelectionCandidateRead(
                rank=index,
                selected=entrant_id == selected_id,
                note=note,
                entrant=entrant,
            )
        )
    note = "Candidates were shuffled with SystemRandom and the first candidate became the selected result for this pool."
    if pool.status == GIVEAWAY_STATUS_REVIEW_REQUIRED:
        note = "No fully verified winner was available, so the first provisional candidate was held for manual review."
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
    return GiveawayRead(
        id=campaign.id,
        post_id=campaign.post_id,
        giveaway_end_at=campaign.giveaway_end_at,
        pool_mode=campaign.pool_mode,
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
        channels=[
            GiveawayChannelRead(
                id=channel.id,
                service=channel.service,
                account_id=channel.account_id,
                status=channel.status,
                rules=GiveawayRuleNodeInput.model_validate(channel.rules_json or {"kind": "all", "children": []}),
                target_post_external_id=channel.target_post_external_id,
                target_post_uri=channel.target_post_uri,
                target_post_cid=channel.target_post_cid,
                target_post_url=channel.target_post_url,
                last_collected_at=channel.last_collected_at,
                last_error=channel.last_error,
                summary=per_channel[channel.service],
                entrants=[
                    serialized_entrant_map[entrant.id]
                    for entrant in sorted(channel.entrants, key=lambda item: (item.provider_username or item.provider_user_id))
                ],
            )
            for channel in channels
        ],
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
                provisional_winner=(
                    serialized_entrant_map[pool.provisional_winner_entry.id]
                    if pool.provisional_winner_entry and pool.provisional_winner_entry.id in entrant_channel_map
                    else None
                ),
                final_winner=(
                    serialized_entrant_map[pool.final_winner_entry.id]
                    if pool.final_winner_entry and pool.final_winner_entry.id in entrant_channel_map
                    else None
                ),
                selection_log=_selection_log(campaign, pool, serialized_entrant_map),
            )
            for pool in pools
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
        media_id = str(channel.target_post_external_id or "").strip()
        if not media_id:
            return None, "Instagram media ID is not available for like verification."
        liker_ids = {str(getattr(user, "pk", "") or "").strip() for user in client.media_likers(media_id)}
        return entrant.provider_user_id in liker_ids, None
    except Exception as exc:
        return None, f"Like verification could not be completed: {exc}"


def _instagram_verify_follow(channel: GiveawayChannel, entrant: GiveawayEntrant) -> tuple[bool | None, str | None]:
    dependency_issue = _instagram_destination_dependency_issue()
    if dependency_issue:
        return None, dependency_issue
    try:
        client = _authenticated_publish_client(_account_credentials(channel.account))
        relationship = client.user_friendship_v1(entrant.provider_user_id)
        return bool(getattr(relationship, "followed_by", False)), None
    except Exception as exc:
        return None, f"Follow verification could not be completed: {exc}"


def _evaluate_instagram_atom(channel: GiveawayChannel, entrant: GiveawayEntrant, atom: str, params: dict[str, Any]) -> tuple[bool | None, str | None]:
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
        return _instagram_verify_like(channel, entrant)
    if atom == "repost_present":
        if state.get("repost_present") is True:
            return True, None
        return False, "No Instagram repost or share evidence was captured."
    if atom == "follow_present":
        return _instagram_verify_follow(channel, entrant)
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
    for attachment in _instagram_webhook_message_attachments(value):
        attachment_type = str(attachment.get("type") or "").strip().lower()
        if attachment_type in {"share", "ig_post"} and _instagram_webhook_shared_media_id(value):
            return True
    return False


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
        summary.update(
            {
                "comment_id": provider_event_id,
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
            if _instagram_webhook_is_shared_post_message(value) and activity in {"story_mention", "repost"}
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


def evaluate_channel_entrants(channel: GiveawayChannel) -> None:
    rule = dict(channel.rules_json or {})
    for entrant in channel.entrants:
        entrant.rule_match_details_json = {}
        entrant.inconclusive_reasons_json = []
        entrant.disqualification_reasons_json = []
        entrant.eligibility_status = ENTRY_STATUS_PENDING

        def resolve_atom(atom: str, params: dict[str, Any]) -> tuple[bool | None, str | None]:
            if channel.service == "instagram":
                return _evaluate_instagram_atom(channel, entrant, atom, params)
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


def _randomize_entries(entries: list[GiveawayEntrant]) -> list[GiveawayEntrant]:
    ranked = list(entries)
    secrets.SystemRandom().shuffle(ranked)
    return ranked


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


def finalize_giveaway_campaign(
    session: Session,
    campaign: GiveawayCampaign,
    alerts: AlertDispatcher,
    *,
    run_id: str,
) -> GiveawayCampaign:
    hydrate_channel_targets(campaign)
    for channel in campaign.channels:
        if channel.service == "bluesky":
            collect_bluesky_channel_state(session, channel, run_id=run_id)
        elif channel.service == "instagram":
            refresh_instagram_channel_state(session, channel)
        evaluate_channel_entrants(channel)

    # Ensure entrant primary keys exist before we freeze candidate ordering.
    session.flush()
    campaign.frozen_at = utcnow()
    campaign.last_evaluated_at = utcnow()
    campaign.last_error = None
    _sync_campaign_pools(campaign)

    for pool in campaign.pools:
        entries = _pool_entries(campaign, pool)
        eligible = [entrant for entrant in entries if entrant.eligibility_status == ENTRY_STATUS_ELIGIBLE]
        provisional = [entrant for entrant in entries if entrant.eligibility_status == ENTRY_STATUS_PROVISIONAL]
        candidate_pool = eligible if eligible else provisional
        ranked_entries = _randomize_entries(candidate_pool)
        pool.candidate_entry_ids_json = [entrant.id for entrant in ranked_entries]
        pool.provisional_winner_entry = None
        pool.final_winner_entry = None
        pool.frozen_at = utcnow()
        pool.last_evaluated_at = utcnow()
        pool.last_error = None
        if not ranked_entries:
            pool.status = GIVEAWAY_STATUS_FAILED
            pool.last_error = "No qualifying giveaway entrants were found."
            continue
        winner = ranked_entries[0]
        if winner.eligibility_status == ENTRY_STATUS_PROVISIONAL:
            pool.status = GIVEAWAY_STATUS_REVIEW_REQUIRED
            pool.provisional_winner_entry = winner
        else:
            pool.status = GIVEAWAY_STATUS_WINNER_SELECTED
            pool.final_winner_entry = winner

    campaign.status = _campaign_status_from_pools(campaign)
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
        return finalize_giveaway_campaign(session, campaign, alerts, run_id=run_id)
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


def process_giveaway_lifecycle(
    session: Session,
    alerts: AlertDispatcher,
    *,
    run_id: str,
    post_id: str | None = None,
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
                if channel.service == "bluesky":
                    try:
                        collect_bluesky_channel_state(session, channel, run_id=run_id)
                    except Exception as exc:
                        message = f"Bluesky giveaway collection failed: {str(exc) or exc.__class__.__name__}"
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
                            severity="error",
                            message=message,
                            post_id=campaign.post_id,
                            metadata={"channel_id": channel.id},
                        )
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
                    refresh_instagram_channel_state(session, channel)
        if normalize_datetime(campaign.giveaway_end_at) and normalize_datetime(campaign.giveaway_end_at) <= now and campaign.status in {GIVEAWAY_STATUS_COLLECTING, GIVEAWAY_STATUS_SCHEDULED}:
            try:
                finalize_giveaway_campaign(session, campaign, alerts, run_id=run_id)
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
    if pool.status != GIVEAWAY_STATUS_REVIEW_REQUIRED or pool.provisional_winner_entry is None:
        raise ValueError("This giveaway pool does not have a provisional winner to confirm.")
    pool.final_winner_entry = pool.provisional_winner_entry
    pool.status = GIVEAWAY_STATUS_WINNER_CONFIRMED
    campaign.status = _campaign_status_from_pools(campaign)
    session.flush()
    return campaign


def advance_giveaway_winner(session: Session, campaign: GiveawayCampaign, *, run_id: str, pool_key: str | None = None) -> GiveawayCampaign:
    pool = _resolve_pool(campaign, pool_key)
    if pool.status != GIVEAWAY_STATUS_REVIEW_REQUIRED or pool.provisional_winner_entry is None:
        raise ValueError("This giveaway pool does not have a provisional winner to advance.")
    candidate_ids = list(pool.candidate_entry_ids_json or [])
    current_id = pool.provisional_winner_entry.id
    try:
        current_index = candidate_ids.index(current_id)
    except ValueError as exc:
        raise ValueError("The provisional winner is not part of the current candidate pool.") from exc
    if current_index + 1 >= len(candidate_ids):
        raise ValueError("There are no remaining giveaway candidates to advance to.")
    next_id = candidate_ids[current_index + 1]
    channel_entrant_map = {entrant.id: entrant for channel in campaign.channels for entrant in channel.entrants}
    next_entry = channel_entrant_map.get(next_id)
    if next_entry is None:
        raise ValueError("Could not resolve the next giveaway candidate.")
    pool.provisional_winner_entry = next_entry
    session.flush()
    return campaign


def refresh_instagram_channel_state(session: Session, channel: GiveawayChannel) -> None:
    sync_instagram_webhook_events_for_channel(session, channel)
    state_by_user: dict[str, dict[str, Any]] = {}
    for entrant in channel.entrants:
        state_by_user[entrant.provider_user_id] = _normalized_instagram_signal_state(dict(entrant.signal_state_json or {}))

    dependency_issue = _instagram_destination_dependency_issue()
    if dependency_issue or not str(channel.target_post_external_id or "").strip():
        channel.last_error = dependency_issue or "Instagram media ID is not available for giveaway verification."
    else:
        try:
            client = _authenticated_publish_client(_account_credentials(channel.account))
            live_comments = client.media_comments(channel.target_post_external_id, amount=0)
            observed_comments: list[tuple[GiveawayEntrant, dict[str, Any], dict[str, Any]]] = []
            for state in state_by_user.values():
                state["comments"] = []
            for comment in live_comments or []:
                user = getattr(comment, "user", None)
                provider_user_id = str(getattr(user, "pk", "") or "").strip()
                provider_username = str(getattr(user, "username", "") or "").strip() or None
                if not provider_user_id:
                    continue
                existing = state_by_user.setdefault(provider_user_id, _normalized_instagram_signal_state({}))
                existing["comments"].append(
                    {
                        "comment_id": str(getattr(comment, "pk", "") or "").strip() or None,
                        "text": str(getattr(comment, "text", "") or "").strip(),
                        "source": "close_time_live",
                    }
                )
                entrant = get_or_create_channel_entrant(
                    channel,
                    provider_user_id=provider_user_id,
                    provider_username=provider_username,
                    display_label=provider_username or provider_user_id,
                )
                entrant.signal_state_json = dict(entrant.signal_state_json or {})
                observed_comments.append(
                    (
                        entrant,
                        existing["comments"][-1],
                        {
                            "created_time": (
                                normalize_datetime(getattr(comment, "created_at_utc", None)).isoformat()
                                if getattr(comment, "created_at_utc", None)
                                else None
                            ),
                        },
                    )
                )
            session.flush()
            _sync_instagram_live_comment_events(session, channel, observed_comments)

            live_likers = client.media_likers(channel.target_post_external_id)
            observed_likes: list[tuple[GiveawayEntrant, dict[str, Any]]] = []
            for state in state_by_user.values():
                state["likes"] = []
                state["like_present"] = False
            for liker in live_likers or []:
                provider_user_id, provider_username = _instagram_user_identity(liker)
                if not provider_user_id:
                    continue
                existing = state_by_user.setdefault(provider_user_id, _normalized_instagram_signal_state({}))
                entrant = get_or_create_channel_entrant(
                    channel,
                    provider_user_id=provider_user_id,
                    provider_username=provider_username,
                    display_label=provider_username or provider_user_id,
                )
                like_summary = {
                    "like_id": f"like:{provider_user_id}:{channel.target_post_external_id}",
                    "media_id": channel.target_post_external_id,
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
            channel.last_error = None
        except Exception as exc:
            channel.last_error = f"Instagram live activity collection failed: {exc}"

    for entrant in channel.entrants:
        state = state_by_user.setdefault(entrant.provider_user_id, _normalized_instagram_signal_state(dict(entrant.signal_state_json or {})))
        entrant.signal_state_json = _normalized_instagram_signal_state(state)
    channel.last_collected_at = utcnow()
    session.flush()
    publish_live_update(LIVE_UPDATE_TOPIC_DASHBOARD, LIVE_UPDATE_TOPIC_LOGS)


def _collect_all_pages(fetch_page: Callable[..., Any], *, key: str, uri: str, cid: str | None = None) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    cursor: str | None = None
    for _ in range(10):
        params: dict[str, Any] = {"uri": uri, "limit": 100}
        if cid:
            params["cid"] = cid
        if cursor:
            params["cursor"] = cursor
        response = fetch_page(params)
        payload = response.model_dump()
        items.extend(list(payload.get(key) or []))
        cursor = payload.get("cursor")
        if not cursor:
            break
    return items


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
        actor = dict(like.get("actor") or {})
        did = str(actor.get("did") or "").strip()
        if not did:
            continue
        remember(
            "bluesky_like",
            f"like:{did}",
            entrants_by_user_id.get(did),
            {
                "actor_did": did,
                "actor_handle": str(actor.get("handle") or "").strip() or None,
                "actor_display_label": str(actor.get("handle") or "").strip() or did,
            },
        )

    for repost in reposts:
        did = str(repost.get("did") or "").strip()
        if not did:
            continue
        remember(
            "bluesky_repost",
            f"repost:{did}",
            entrants_by_user_id.get(did),
            {
                "actor_did": did,
                "actor_handle": str(repost.get("handle") or "").strip() or None,
                "actor_display_label": str(repost.get("handle") or "").strip() or did,
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
    thread = client.app.bsky.feed.get_post_thread({"uri": channel.target_post_uri, "depth": 10}).model_dump()
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
        actor = dict(like.get("actor") or {})
        did = str(actor.get("did") or "").strip()
        if not did:
            continue
        entry = entrants[did]
        entry["provider_username"] = str(actor.get("handle") or "").strip() or entry["provider_username"]
        entry["display_label"] = entry["provider_username"] or did
        entry["like_present"] = True

    for repost in reposts:
        did = str(repost.get("did") or "").strip()
        if not did:
            continue
        entry = entrants[did]
        entry["provider_username"] = str(repost.get("handle") or "").strip() or entry["provider_username"]
        entry["display_label"] = entry["provider_username"] or did
        entry["repost_present"] = True

    other_dids = list(entrants.keys())
    for index in range(0, len(other_dids), 30):
        batch = other_dids[index : index + 30]
        relationships = client.app.bsky.graph.get_relationships({"actor": owner_did or handle, "others": batch}).model_dump()
        for item in relationships.get("relationships") or []:
            did = str(item.get("did") or "").strip()
            if did in entrants:
                entrants[did]["follow_present"] = bool(item.get("followedBy"))

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
