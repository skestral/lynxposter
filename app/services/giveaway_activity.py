from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.models import CanonicalPost, GiveawayCampaign, GiveawayChannel, GiveawayEntrant, GiveawayEvidenceEvent, Persona
from app.services.giveaway_engine import GIVEAWAY_STATUS_COLLECTING, GIVEAWAY_STATUS_REVIEW_REQUIRED, GIVEAWAY_STATUS_SCHEDULED

OPEN_GIVEAWAY_STATUSES = (
    GIVEAWAY_STATUS_SCHEDULED,
    GIVEAWAY_STATUS_COLLECTING,
    GIVEAWAY_STATUS_REVIEW_REQUIRED,
)

SERVICE_LABELS = {
    "instagram": "Instagram",
    "bluesky": "Bluesky",
}

EVENT_LABELS = {
    "instagram_comment": "Comment",
    "instagram_story_mention": "Story Mention",
    "instagram_like": "Like",
    "instagram_repost": "Share",
    "bluesky_reply": "Reply",
    "bluesky_quote": "Quote",
    "bluesky_like": "Like",
    "bluesky_repost": "Repost",
    "bluesky_follow": "Follow",
    "bluesky_collection_snapshot": "Collection Snapshot",
}

EXCLUDED_RECENT_EVENT_TYPES = {"bluesky_collection_snapshot"}


def _parse_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _activity_timestamp(event: GiveawayEvidenceEvent) -> datetime:
    payload = dict(event.payload_json or {})
    for key in ("last_seen_at", "seen_at", "occurred_at", "created_at"):
        parsed = _parse_datetime(payload.get(key))
        if parsed is not None:
            return parsed
    created_at = event.created_at
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _event_label(event_type: str) -> str:
    return EVENT_LABELS.get(event_type, event_type.replace("_", " ").title())


def _service_label(service: str) -> str:
    return SERVICE_LABELS.get(service, service.title())


def _bluesky_profile_href(handle_or_did: str | None) -> str | None:
    value = str(handle_or_did or "").strip()
    if not value:
        return None
    return f"https://bsky.app/profile/{value}"


def _bluesky_post_href(handle_or_did: str | None, uri: str | None) -> str | None:
    handle = str(handle_or_did or "").strip()
    post_uri = str(uri or "").strip()
    if not handle or not post_uri:
        return None
    rkey = post_uri.split("/")[-1]
    if not rkey:
        return None
    return f"https://bsky.app/profile/{handle}/post/{rkey}"


def _instagram_value_payload(payload: dict[str, Any]) -> dict[str, Any]:
    change = payload.get("change")
    if not isinstance(change, dict):
        return {}
    value = change.get("value")
    if not isinstance(value, dict):
        return {}
    return value


def _event_detail(event_type: str, payload: dict[str, Any]) -> str | None:
    if event_type.startswith("instagram_"):
        value = _instagram_value_payload(payload)
        text = str(value.get("text") or "").strip()
        if text:
            return text
        if event_type == "instagram_story_mention":
            mention_type = str(value.get("mention_type") or "story").strip().replace("_", " ")
            return f"{mention_type.title()} mention captured."
        actor = value.get("from")
        actor_label = None
        if isinstance(actor, dict):
            actor_label = str(actor.get("username") or actor.get("id") or "").strip()
        if event_type == "instagram_like":
            return f"{actor_label or 'Entrant'} liked the giveaway post."
        if event_type == "instagram_repost":
            return f"{actor_label or 'Entrant'} shared the giveaway post."
        return None
    text = str(payload.get("text") or "").strip()
    if text:
        return text
    actor = str(payload.get("actor_handle") or payload.get("actor_display_label") or payload.get("actor_did") or "").strip()
    if event_type == "bluesky_like":
        return f"{actor or 'Entrant'} liked the giveaway post."
    if event_type == "bluesky_repost":
        return f"{actor or 'Entrant'} reposted the giveaway post."
    if event_type == "bluesky_follow":
        return f"{actor or 'Entrant'} is following the giveaway account."
    return None


def _event_activity_href(channel: GiveawayChannel, event_type: str, payload: dict[str, Any]) -> tuple[str | None, str | None]:
    if event_type.startswith("bluesky_"):
        post_href = _bluesky_post_href(payload.get("actor_handle") or payload.get("actor_did"), payload.get("uri"))
        if post_href:
            return post_href, f"Open {_event_label(event_type).lower()}"
        profile_href = _bluesky_profile_href(payload.get("actor_handle") or payload.get("actor_did"))
        if profile_href and event_type in {"bluesky_like", "bluesky_repost", "bluesky_follow"}:
            return profile_href, "Open entrant profile"
    if channel.target_post_url:
        return channel.target_post_url, f"Open {_service_label(channel.service)} post"
    return None, None


def _event_actor_label(event: GiveawayEvidenceEvent, entrant: GiveawayEntrant | None) -> str:
    if entrant is not None:
        return entrant.display_label or entrant.provider_username or entrant.provider_user_id
    payload = dict(event.payload_json or {})
    if event.event_type.startswith("instagram_"):
        value = _instagram_value_payload(payload)
        actor = value.get("from")
        if isinstance(actor, dict):
            return str(actor.get("username") or actor.get("id") or "Instagram entrant")
        return str(value.get("from_id") or "Instagram entrant")
    return str(payload.get("actor_display_label") or payload.get("actor_handle") or payload.get("actor_did") or "Bluesky entrant")


def _campaign_label(campaign: GiveawayCampaign) -> str:
    body = str(campaign.post.body or "").strip()
    if body:
        normalized = " ".join(body.split())
        return normalized[:72] + ("..." if len(normalized) > 72 else "")
    return f"Giveaway post {campaign.post_id}"


def _list_open_campaigns(session: Session, *, owner_user_id: str | None = None, persona_id: str | None = None) -> list[GiveawayCampaign]:
    stmt = (
        select(GiveawayCampaign)
        .join(GiveawayCampaign.post)
        .join(CanonicalPost.persona)
        .options(
            selectinload(GiveawayCampaign.post).selectinload(CanonicalPost.persona),
            selectinload(GiveawayCampaign.channels).selectinload(GiveawayChannel.account),
            selectinload(GiveawayCampaign.channels).selectinload(GiveawayChannel.entrants),
            selectinload(GiveawayCampaign.evidence_events),
        )
        .where(GiveawayCampaign.status.in_(OPEN_GIVEAWAY_STATUSES))
        .order_by(GiveawayCampaign.giveaway_end_at.asc())
    )
    if owner_user_id is not None:
        stmt = stmt.where(Persona.owner_user_id == owner_user_id)
    if persona_id:
        stmt = stmt.where(Persona.id == persona_id)
    return list(session.scalars(stmt))


def build_dashboard_giveaway_activity_monitor(
    session: Session,
    *,
    owner_user_id: str | None = None,
    filters: dict[str, str | None] | None = None,
    limit: int = 18,
) -> dict[str, Any]:
    selected_filters = {
        "persona_id": str((filters or {}).get("persona_id") or "").strip() or None,
        "service": str((filters or {}).get("service") or "").strip() or None,
        "event_type": str((filters or {}).get("event_type") or "").strip() or None,
    }
    campaigns = _list_open_campaigns(
        session,
        owner_user_id=owner_user_id,
        persona_id=selected_filters["persona_id"],
    )

    available_services: set[str] = set()
    available_event_types: set[str] = set()
    matched_campaigns: list[tuple[GiveawayCampaign, list[GiveawayChannel]]] = []
    matched_channels: list[GiveawayChannel] = []
    raw_events: list[tuple[GiveawayCampaign, GiveawayChannel, GiveawayEntrant | None, GiveawayEvidenceEvent]] = []

    for campaign in campaigns:
        channel_map = {channel.id: channel for channel in campaign.channels}
        entrant_map = {entrant.id: entrant for channel in campaign.channels for entrant in channel.entrants}
        for channel in campaign.channels:
            available_services.add(channel.service)
        service_filtered_channels = [
            channel
            for channel in campaign.channels
            if not selected_filters["service"] or channel.service == selected_filters["service"]
        ]
        if not service_filtered_channels:
            continue
        matched_campaigns.append((campaign, service_filtered_channels))
        matched_channels.extend(service_filtered_channels)
        matched_channel_ids = {channel.id for channel in service_filtered_channels}
        for event in campaign.evidence_events:
            if event.channel_id not in matched_channel_ids:
                continue
            if not event.active:
                continue
            available_event_types.add(event.event_type)
            if event.event_type in EXCLUDED_RECENT_EVENT_TYPES:
                continue
            channel = channel_map.get(event.channel_id)
            if channel is None:
                continue
            raw_events.append((campaign, channel, entrant_map.get(event.entrant_id or ""), event))

    filtered_events = [
        item
        for item in raw_events
        if not selected_filters["event_type"] or item[3].event_type == selected_filters["event_type"]
    ]
    filtered_events.sort(key=lambda item: _activity_timestamp(item[3]), reverse=True)

    matched_service_channels: dict[str, list[GiveawayChannel]] = defaultdict(list)
    for channel in matched_channels:
        matched_service_channels[channel.service].append(channel)

    event_rollup_counters: dict[str, Counter[str]] = defaultdict(Counter)
    service_recent_times: dict[str, datetime | None] = {}
    for _, channel, _, event in filtered_events:
        event_rollup_counters[channel.service][event.event_type] += 1
        event_time = _activity_timestamp(event)
        current_latest = service_recent_times.get(channel.service)
        if current_latest is None or event_time > current_latest:
            service_recent_times[channel.service] = event_time

    rollups: list[dict[str, Any]] = []
    for service in sorted(matched_service_channels):
        service_channels = matched_service_channels[service]
        service_campaign_ids = {channel.campaign_id for channel in service_channels}
        service_entrant_ids = {
            entrant.id
            for channel in service_channels
            for entrant in channel.entrants
            if entrant.id
        }
        event_breakdown = [
            {"event_type": event_type, "label": _event_label(event_type), "count": count}
            for event_type, count in event_rollup_counters.get(service, Counter()).most_common()
        ]
        rollups.append(
            {
                "service": service,
                "label": _service_label(service),
                "activity_count": sum(event_rollup_counters.get(service, Counter()).values()),
                "entrant_count": len(service_entrant_ids),
                "campaign_count": len(service_campaign_ids),
                "latest_activity_at": service_recent_times.get(service),
                "event_breakdown": event_breakdown,
            }
        )

    recent_events = []
    for campaign, channel, entrant, event in filtered_events[:limit]:
        payload = dict(event.payload_json or {})
        activity_href, activity_href_label = _event_activity_href(channel, event.event_type, payload)
        recent_events.append(
            {
                "id": event.id,
                "created_at": _activity_timestamp(event),
                "service": channel.service,
                "service_label": _service_label(channel.service),
                "event_type": event.event_type,
                "event_label": _event_label(event.event_type),
                "actor_label": _event_actor_label(event, entrant),
                "entrant_status": entrant.eligibility_status if entrant else None,
                "persona_name": campaign.post.persona.name if campaign.post.persona else "No persona",
                "account_label": channel.account.label if channel.account else channel.account_id,
                "campaign_status": campaign.status,
                "campaign_label": _campaign_label(campaign),
                "campaign_href": f"/scheduled-posts/{campaign.post_id}/page",
                "activity_href": activity_href,
                "activity_href_label": activity_href_label,
                "detail": _event_detail(event.event_type, payload),
                "source": event.source,
            }
        )

    unique_entrant_ids = {
        entrant.id
        for channel in matched_channels
        for entrant in channel.entrants
        if entrant.id
    }
    available_event_type_options = [
        {"value": event_type, "label": _event_label(event_type)}
        for event_type in sorted(available_event_types, key=_event_label)
        if event_type not in EXCLUDED_RECENT_EVENT_TYPES
    ]

    return {
        "filters": selected_filters,
        "available_services": [
            {"value": service, "label": _service_label(service)}
            for service in sorted(available_services, key=_service_label)
        ],
        "available_event_types": available_event_type_options,
        "metrics": {
            "campaigns": len(matched_campaigns),
            "channels": len(matched_channels),
            "entrants": len(unique_entrant_ids),
            "activities": len(filtered_events),
        },
        "rollups": rollups,
        "recent_events": recent_events,
    }
