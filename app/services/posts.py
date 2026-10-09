from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import and_, desc, event, or_, select
from sqlalchemy.orm import Session, selectinload

from app.adapters import get_destination_adapter_for_account
from app.adapters.common import delivery_summary, service_attachments, service_body
from app.domain import CanonicalPostPayload, ExternalPostRefPayload, MediaItem
from app.models import Account, AccountPostRef, AccountSyncState, CanonicalPost, DeliveryJob, MediaAttachment, Persona
from app.schemas import GiveawayConfigInput, ScheduledPostCreate, ScheduledPostUpdate
from app.services.giveaway_engine import (
    POST_TYPE_GIVEAWAY,
    POST_TYPE_STANDARD,
    giveaway_config_input_from_json,
    giveaway_selectinloads,
    migrate_legacy_instagram_giveaway,
    sync_giveaway_campaign,
)
from app.services.personas import _persona_visibility_clause, get_persona, persona_destination_accounts, routed_destination_accounts
from app.services.storage import delete_managed_media_file

NEEDS_ATTENTION_DISPLAY_STATUSES = {"partial_failure", "failure", "cancelled"}
DELETABLE_DISPLAY_STATUSES = {"draft", *NEEDS_ATTENTION_DISPLAY_STATUSES}
IMPORT_DELIVERY_SUPPRESSED_REASON_KEY = "delivery_suppressed_reason"
AUTORUN_IMPORT_DELIVERY_DISABLED_REASON = "autorun_import_delivery_disabled"
MANUAL_RECOVERY_KEY = "manual_recovery"
_DEFERRED_MEDIA_DELETIONS = "scheduled_post_media_deletions"
logger = logging.getLogger(__name__)


@event.listens_for(Session, "after_commit")
def _delete_replaced_media_after_commit(session: Session) -> None:
    for path in session.info.pop(_DEFERRED_MEDIA_DELETIONS, set()):
        try:
            delete_managed_media_file(path)
        except OSError:
            logger.warning("Could not remove replaced post media after commit.", exc_info=True)


@event.listens_for(Session, "after_rollback")
def _keep_replaced_media_after_rollback(session: Session) -> None:
    session.info.pop(_DEFERRED_MEDIA_DELETIONS, None)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo or timezone.utc
        value = value.replace(tzinfo=local_tz)
    return value.astimezone(timezone.utc)


def persona_max_retries(persona: Persona) -> int:
    return int((persona.retry_settings_json or {}).get("max_retries", 5) or 5)


def get_post(
    session: Session,
    post_id: str,
    *,
    owner_user_id: str | None = None,
    access_user_id: str | None = None,
    min_permission: str = "view",
) -> CanonicalPost | None:
    stmt = (
        select(CanonicalPost)
        .options(
            selectinload(CanonicalPost.attachments),
            selectinload(CanonicalPost.delivery_jobs).selectinload(DeliveryJob.target_account),
            selectinload(CanonicalPost.persona).selectinload(Persona.accounts),
            selectinload(CanonicalPost.origin_account),
            *giveaway_selectinloads(),
        )
        .where(CanonicalPost.id == post_id)
        .execution_options(populate_existing=True)
    )
    visibility_clause = _persona_visibility_clause(
        owner_user_id=owner_user_id,
        access_user_id=access_user_id,
        min_permission=min_permission,
    )
    if visibility_clause is not None:
        stmt = stmt.join(Persona, Persona.id == CanonicalPost.persona_id).where(visibility_clause)
    return session.scalar(stmt)


def list_scheduled_posts(
    session: Session,
    *,
    owner_user_id: str | None = None,
    access_user_id: str | None = None,
    min_permission: str = "view",
) -> list[CanonicalPost]:
    stmt = (
        select(CanonicalPost)
        .options(
            selectinload(CanonicalPost.attachments),
            selectinload(CanonicalPost.delivery_jobs).selectinload(DeliveryJob.target_account),
            selectinload(CanonicalPost.persona),
            selectinload(CanonicalPost.origin_account),
            *giveaway_selectinloads(),
        )
        .where(or_(
            CanonicalPost.origin_kind == "composer",
            and_(
                CanonicalPost.origin_kind == "account_import",
                CanonicalPost.post_type == POST_TYPE_STANDARD,
                or_(
                    CanonicalPost.delivery_jobs.any(DeliveryJob.status == "failed"),
                    CanonicalPost.metadata_json[MANUAL_RECOVERY_KEY].as_boolean().is_(True),
                ),
            ),
        ))
        .order_by(desc(CanonicalPost.created_at))
    )
    visibility_clause = _persona_visibility_clause(
        owner_user_id=owner_user_id,
        access_user_id=access_user_id,
        min_permission=min_permission,
    )
    if visibility_clause is not None:
        stmt = stmt.join(Persona, Persona.id == CanonicalPost.persona_id).where(visibility_clause)
    return list(session.scalars(stmt))


def _create_attachment(post: CanonicalPost, item: MediaItem) -> MediaAttachment:
    return MediaAttachment(
        post=post,
        post_id=post.id,
        storage_path=str(item.storage_path),
        mime_type=item.mime_type,
        alt_text=item.alt_text,
        size_bytes=item.size_bytes,
        checksum=item.checksum,
        sort_order=item.sort_order,
    )


def _sorted_attachments(post: CanonicalPost) -> list[MediaAttachment]:
    return sorted(post.attachments, key=lambda item: (item.sort_order, item.id))


def _delete_attachment_file_if_unshared(session: Session, attachment: MediaAttachment, *, after_commit: bool = False) -> None:
    other_attachment = session.scalar(
        select(MediaAttachment.id)
        .where(
            MediaAttachment.storage_path == attachment.storage_path,
            MediaAttachment.id != attachment.id,
        )
        .limit(1)
    )
    if other_attachment is None:
        if after_commit:
            session.info.setdefault(_DEFERRED_MEDIA_DELETIONS, set()).add(attachment.storage_path)
        else:
            delete_managed_media_file(attachment.storage_path)


def remove_post_media_files(session: Session, post: CanonicalPost) -> int:
    removed = 0
    for attachment in list(post.attachments or []):
        _delete_attachment_file_if_unshared(session, attachment)
        session.delete(attachment)
        removed += 1
    if removed:
        session.flush()
    return removed


def can_remove_post_media_after_delivery(post: CanonicalPost) -> bool:
    return post.post_type != POST_TYPE_GIVEAWAY


def _sync_post_attachments(
    session: Session,
    post: CanonicalPost,
    *,
    attachment_order: list[str] | None,
    deleted_attachment_ids: list[str] | None,
    attachment_alt_texts: dict[str, str] | None,
) -> None:
    attachments_by_id = {attachment.id: attachment for attachment in post.attachments}
    alt_text_updates = {
        str(attachment_id or "").strip(): str(alt_text or "").strip()
        for attachment_id, alt_text in dict(attachment_alt_texts or {}).items()
        if str(attachment_id or "").strip()
    }
    unknown_alt_text_ids = [attachment_id for attachment_id in alt_text_updates if attachment_id not in attachments_by_id]
    if unknown_alt_text_ids:
        raise ValueError("Alt text can only be edited for media already attached to this post.")
    for attachment_id, alt_text in alt_text_updates.items():
        attachments_by_id[attachment_id].alt_text = alt_text

    deleted_ids = [str(item or "").strip() for item in deleted_attachment_ids or [] if str(item or "").strip()]
    unknown_deleted = [attachment_id for attachment_id in deleted_ids if attachment_id not in attachments_by_id]
    if unknown_deleted:
        raise ValueError("One or more selected media items no longer exist on this post.")

    for attachment_id in dict.fromkeys(deleted_ids):
        attachment = attachments_by_id.pop(attachment_id)
        _delete_attachment_file_if_unshared(session, attachment, after_commit=True)
        post.attachments.remove(attachment)
        session.delete(attachment)

    remaining = _sorted_attachments(post)
    if attachment_order is not None:
        requested_order = [str(item or "").strip() for item in attachment_order if str(item or "").strip()]
        if len(requested_order) != len(set(requested_order)):
            raise ValueError("Media order contains a duplicate attachment.")
        unknown_ordered = [
            attachment_id
            for attachment_id in requested_order
            if attachment_id not in attachments_by_id and attachment_id not in deleted_ids
        ]
        if unknown_ordered:
            raise ValueError("One or more ordered media items no longer exist on this post.")
        requested_remaining = [attachment_id for attachment_id in requested_order if attachment_id in attachments_by_id]
        ordered_ids = requested_remaining + [attachment.id for attachment in remaining if attachment.id not in requested_remaining]
        remaining = [attachments_by_id[attachment_id] for attachment_id in ordered_ids]

    for index, attachment in enumerate(remaining):
        attachment.sort_order = index


def _active_target_account_ids(post: CanonicalPost) -> list[str]:
    return [
        job.target_account_id
        for job in _sorted_delivery_jobs(post)
        if job.status != "cancelled"
    ]


def validate_publish_override_attachment_ids(post: CanonicalPost, overrides: dict[str, Any] | None) -> None:
    if not overrides:
        return
    attachment_ids = {attachment.id for attachment in post.attachments or []}
    for override in dict(overrides).values():
        if not isinstance(override, dict) or "attachment_ids" not in override:
            continue
        requested_ids = [str(item or "").strip() for item in override.get("attachment_ids") or [] if str(item or "").strip()]
        if len(requested_ids) != len(set(requested_ids)):
            raise ValueError("Destination media override contains a duplicate attachment.")
        unknown_ids = [attachment_id for attachment_id in requested_ids if attachment_id not in attachment_ids]
        if unknown_ids:
            raise ValueError("Destination media overrides can only use media already attached to this post.")


def _sorted_delivery_jobs(post: CanonicalPost) -> list[DeliveryJob]:
    return sorted(
        post.delivery_jobs,
        key=lambda item: (
            item.target_account.label if item.target_account else "",
            item.target_account.service if item.target_account else "",
            item.target_account_id,
        ),
    )


def ordered_delivery_summaries(post: CanonicalPost) -> list[dict[str, Any]]:
    return [delivery_summary(job) for job in _sorted_delivery_jobs(post)]


def build_delivery_states(post: CanonicalPost) -> dict[str, dict[str, Any]]:
    return {summary["account_id"]: summary for summary in ordered_delivery_summaries(post)}


def scheduled_post_delivery_breakdown(post: CanonicalPost) -> dict[str, list[dict[str, Any]]]:
    breakdown: dict[str, list[dict[str, Any]]] = {
        "succeeded": [],
        "failed": [],
        "cancelled": [],
        "pending": [],
    }
    for summary in ordered_delivery_summaries(post):
        if summary["status"] == "posted":
            breakdown["succeeded"].append(summary)
        elif summary["status"] == "failed":
            breakdown["failed"].append(summary)
        elif summary["status"] in {"cancelled", "skipped"}:
            breakdown["cancelled"].append(summary)
        else:
            breakdown["pending"].append(summary)
    return breakdown


def scheduled_post_display_status(post: CanonicalPost) -> str:
    breakdown = scheduled_post_delivery_breakdown(post)
    if breakdown["pending"]:
        return post.status
    if breakdown["succeeded"] and not breakdown["failed"]:
        return "success"
    if breakdown["succeeded"]:
        return "partial_failure"
    if breakdown["failed"]:
        return "failure"
    if breakdown["cancelled"]:
        return "cancelled"
    return post.status


def can_delete_scheduled_post(post: CanonicalPost) -> bool:
    if post.origin_kind != "composer":
        return False
    return scheduled_post_display_status(post) in DELETABLE_DISPLAY_STATUSES


def is_recoverable_post(post: CanonicalPost | None) -> bool:
    if post is None:
        return False
    return post.origin_kind == "composer" or (
        post.origin_kind == "account_import"
        and post.post_type == POST_TYPE_STANDARD
        and (bool((post.metadata_json or {}).get(MANUAL_RECOVERY_KEY)) or any(job.status == "failed" for job in post.delivery_jobs))
    )


def _mark_manual_recovery(post: CanonicalPost) -> None:
    if post.origin_kind == "account_import":
        metadata = dict(post.metadata_json or {})
        metadata[MANUAL_RECOVERY_KEY] = True
        metadata.pop(IMPORT_DELIVERY_SUPPRESSED_REASON_KEY, None)
        post.metadata_json = metadata


def _desired_job_status(post_status: str) -> str:
    if post_status == "queued":
        return "queued"
    if post_status == "scheduled":
        return "scheduled"
    return "draft"


def _giveaway_config_from_campaign(post: CanonicalPost):
    if post.giveaway_campaign is None:
        return None
    return giveaway_config_input_from_json(
        {
            "giveaway_end_at": post.giveaway_campaign.giveaway_end_at.isoformat() if post.giveaway_campaign.giveaway_end_at else None,
            "pool_mode": post.giveaway_campaign.pool_mode,
            "winner_count": post.giveaway_campaign.winner_count,
            "channels": [
                {
                    "service": channel.service,
                    "account_id": channel.account_id,
                    "rules": channel.rules_json,
                }
                for channel in post.giveaway_campaign.channels
            ],
        }
    )


def resolve_target_accounts(session: Session, persona: Persona, target_account_ids: list[str] | None) -> list[Account]:
    if target_account_ids:
        stmt = (
            select(Account)
            .where(Account.persona_id == persona.id, Account.id.in_(target_account_ids))
            .order_by(Account.label, Account.service)
        )
        accounts = list(session.scalars(stmt))
    else:
        accounts = persona_destination_accounts(persona)

    account_by_id = {account.id: account for account in accounts}
    ordered_accounts: list[Account] = []
    for account_id in target_account_ids or [account.id for account in accounts]:
        account = account_by_id.get(account_id)
        if account:
            ordered_accounts.append(account)

    for account in ordered_accounts:
        if account.persona_id != persona.id:
            raise ValueError("Target accounts must belong to the selected persona.")
        if not account.is_enabled or not account.destination_enabled:
            raise ValueError(f"{account.label} is not an enabled destination account.")
    return ordered_accounts


def validate_post_for_target_accounts(post: CanonicalPost, target_accounts: list[Account]) -> list[str]:
    issues: list[str] = []
    if not target_accounts:
        issues.append("Select at least one destination account.")
        return issues
    for account in target_accounts:
        try:
            adapter = get_destination_adapter_for_account(account)
        except KeyError:
            issues.append(f"{account.label}: outbound publishing is not supported.")
            continue
        for issue in adapter.validate(post, post.persona, account):
            issues.append(f"{account.label}: {issue.message}")
    return issues


def _resolve_scheduled_post_edit_targets(
    session: Session, post: CanonicalPost, target_ids: list[str]
) -> tuple[list[Account], list[Account]]:
    successful_accounts = {
        job.target_account_id: job.target_account
        for job in post.delivery_jobs
        if job.status == "posted"
    }
    removed_successful_ids = successful_accounts.keys() - set(target_ids)
    if removed_successful_ids:
        labels = sorted(successful_accounts[account_id].label for account_id in removed_successful_ids)
        raise ValueError(f"Successfully published destinations cannot be removed: {', '.join(labels)}.")
    pending_ids = [account_id for account_id in target_ids if account_id not in successful_accounts]
    pending_accounts = resolve_target_accounts(session, post.persona, pending_ids) if pending_ids else []
    selected_accounts = {account.id: account for account in pending_accounts}
    if set(pending_ids) - selected_accounts.keys():
        raise ValueError("Selected destination accounts must exist and belong to the selected persona.")
    selected_accounts.update(successful_accounts)
    target_accounts = [selected_accounts[account_id] for account_id in target_ids if account_id in selected_accounts]
    return target_accounts, pending_accounts


def _sync_scheduled_post_giveaway(
    session: Session, post: CanonicalPost, target_accounts: list[Account], giveaway_config: GiveawayConfigInput | None
) -> None:
    campaign = post.giveaway_campaign
    if post.post_type == POST_TYPE_GIVEAWAY and not target_accounts:
        if campaign is not None:
            campaign.status = "cancelled"
            for item in [*campaign.channels, *campaign.pools]:
                item.status = "cancelled"
        return
    if post.post_type == POST_TYPE_GIVEAWAY and campaign is not None and campaign.status == "cancelled":
        campaign.status = "scheduled"
        for item in [*campaign.channels, *campaign.pools]:
            if item.status == "cancelled":
                item.status = "scheduled"
    sync_giveaway_campaign(session, post, target_accounts, giveaway_config)


def sync_delivery_jobs(session: Session, post: CanonicalPost, target_accounts: list[Account], desired_status: str, *, reset_failed_retries: bool = False) -> list[DeliveryJob]:
    jobs_by_account = {job.target_account_id: job for job in post.delivery_jobs}
    desired_ids = {account.id for account in target_accounts}
    created: list[DeliveryJob] = []

    for account in target_accounts:
        job = jobs_by_account.get(account.id)
        if job is None:
            job = DeliveryJob(
                post_id=post.id,
                target_account_id=account.id,
                status=desired_status,
                max_retries=persona_max_retries(post.persona),
            )
            job.post = post
            job.target_account = account
            session.add(job)
            created.append(job)
            continue

        if job.status == "posted":
            continue
        job.max_retries = persona_max_retries(post.persona)
        if job.status not in {"posted", "posting"}:
            if reset_failed_retries and job.status == "failed":
                # The user is starting a new recovery cycle; attempts remain
                # in history while the retry budget starts fresh.
                job.attempt_count = 0
                job.last_error = None
                job.last_error_class = None
            job.status = desired_status
        if desired_status == "queued":
            job.queued_at = utcnow()

    for job in post.delivery_jobs:
        if job.target_account_id not in desired_ids and job.status not in {"posted"}:
            job.status = "cancelled"

    session.flush()
    return created


def refresh_post_status(post: CanonicalPost) -> None:
    statuses = {job.status for job in post.delivery_jobs}
    if not statuses:
        if post.origin_kind == "account_import" and post.published_at:
            post.status = "posted"
        return
    if statuses <= {"posted", "skipped", "cancelled"}:
        post.status = "posted" if "posted" in statuses else "cancelled"
    elif "posting" in statuses:
        post.status = "posting"
    elif "queued" in statuses:
        post.status = "queued"
    elif "scheduled" in statuses:
        post.status = "scheduled"
    elif statuses == {"draft"}:
        post.status = "draft"
    elif "failed" in statuses:
        post.status = "failed"


def create_scheduled_post(session: Session, payload: ScheduledPostCreate, media_items: list[MediaItem]) -> CanonicalPost:
    persona = get_persona(session, payload.persona_id)
    if not persona:
        raise ValueError("Persona not found.")

    target_accounts = resolve_target_accounts(session, persona, payload.target_account_ids)
    post = CanonicalPost(
        persona_id=payload.persona_id,
        origin_kind="composer",
        post_type=payload.post_type,
        origin_account_id=None,
        status=payload.status,
        body=payload.body,
        publish_overrides_json=payload.publish_overrides_json,
        metadata_json=payload.metadata_json,
        scheduled_for=normalize_datetime(payload.scheduled_for),
    )
    session.add(post)
    post.persona = persona
    session.flush()

    for item in media_items:
        session.add(_create_attachment(post, item))
    session.flush()
    validate_publish_override_attachment_ids(post, post.publish_overrides_json)

    sync_delivery_jobs(session, post, target_accounts, _desired_job_status(post.status))
    sync_giveaway_campaign(session, post, target_accounts, payload.giveaway)
    validation_errors = validate_post_for_target_accounts(post, target_accounts)
    post.last_error = "; ".join(validation_errors) if validation_errors else None
    if validation_errors and post.status in {"scheduled", "queued", "posting"}:
        raise ValueError(post.last_error)

    refresh_post_status(post)
    session.flush()
    return get_post(session, post.id) or post


def update_scheduled_post(
    session: Session,
    post: CanonicalPost,
    payload: ScheduledPostUpdate,
    media_items: list[MediaItem] | None = None,
) -> CanonicalPost:
    if any(job.status == "posting" for job in post.delivery_jobs):
        raise ValueError("This post is currently being published. Wait for delivery to finish before editing it.")
    if post.origin_kind == "account_import" and payload.post_type not in {None, POST_TYPE_STANDARD}:
        raise ValueError("Imported posts can only be repaired as standard posts.")
    target_ids = payload.target_account_ids
    if target_ids is None:
        target_ids = _active_target_account_ids(post)
    target_accounts, pending_accounts = _resolve_scheduled_post_edit_targets(session, post, target_ids)

    # Preserve the effective published caption and selection for locked targets
    # before applying changes to the common post or service-level overrides.
    successful_overrides = {}
    for job in post.delivery_jobs:
        if job.status != "posted":
            continue
        account = job.target_account
        saved_override = (post.publish_overrides_json or {}).get(account.id)
        override = dict(saved_override) if isinstance(saved_override, dict) else {}
        override["body"] = service_body(post, account)
        override["attachment_ids"] = [attachment.id for attachment in service_attachments(post, account)]
        successful_overrides[account.id] = override

    if payload.body is not None:
        post.body = payload.body
    if payload.post_type is not None:
        post.post_type = payload.post_type
    if payload.status is not None:
        post.status = payload.status
    if post.status == "cancelled" and target_accounts:
        post.status = "draft"
    if payload.publish_overrides_json is not None:
        post.publish_overrides_json = dict(payload.publish_overrides_json)
    post.publish_overrides_json = {**(post.publish_overrides_json or {}), **successful_overrides}
    if payload.metadata_json is not None:
        post.metadata_json = {**(post.metadata_json or {}), **payload.metadata_json} if post.origin_kind == "account_import" else payload.metadata_json
    _mark_manual_recovery(post)
    if "scheduled_for" in payload.model_fields_set:
        post.scheduled_for = normalize_datetime(payload.scheduled_for)

    _sync_post_attachments(
        session,
        post,
        attachment_order=payload.attachment_order,
        deleted_attachment_ids=payload.deleted_attachment_ids,
        attachment_alt_texts=payload.attachment_alt_texts,
    )

    next_sort_order = len(post.attachments)
    new_attachments = []
    for offset, item in enumerate(media_items or []):
        item.sort_order = next_sort_order + offset
        attachment = _create_attachment(post, item)
        new_attachments.append(attachment)
        session.add(attachment)
    session.flush()
    deleted_ids = set(payload.deleted_attachment_ids or [])
    if deleted_ids:
        overrides = {}
        for key, saved_override in (post.publish_overrides_json or {}).items():
            override = dict(saved_override) if isinstance(saved_override, dict) else saved_override
            if isinstance(override, dict) and "attachment_ids" in override:
                selected_ids = [str(item or "").strip() for item in override.get("attachment_ids") or [] if str(item or "").strip()]
                override["attachment_ids"] = [item for item in selected_ids if item not in deleted_ids]
                if key not in successful_overrides and any(item in deleted_ids for item in selected_ids):
                    override["attachment_ids"].extend(item.id for item in new_attachments)
            overrides[key] = override
        post.publish_overrides_json = overrides
    validate_publish_override_attachment_ids(post, post.publish_overrides_json)

    sync_delivery_jobs(session, post, target_accounts, _desired_job_status(post.status), reset_failed_retries=True)
    giveaway_config = payload.giveaway
    if post.post_type == POST_TYPE_GIVEAWAY and giveaway_config is None:
        if post.giveaway_campaign is None and post.instagram_giveaway is not None:
            migrate_legacy_instagram_giveaway(session, post)
        giveaway_config = _giveaway_config_from_campaign(post)
    _sync_scheduled_post_giveaway(session, post, target_accounts, giveaway_config)

    validation_errors = validate_post_for_target_accounts(post, pending_accounts) if pending_accounts else []
    post.last_error = "; ".join(validation_errors) if validation_errors else None
    if validation_errors and post.status in {"scheduled", "queued", "posting"}:
        raise ValueError(post.last_error)

    refresh_post_status(post)
    session.flush()
    return get_post(session, post.id) or post


def delete_scheduled_post(session: Session, post: CanonicalPost) -> None:
    if post.origin_kind != "composer":
        raise ValueError("Only scheduled posts created in the composer can be deleted.")
    if not can_delete_scheduled_post(post):
        raise ValueError("Only draft or needs-attention scheduled posts can be deleted.")
    for attachment in list(post.attachments):
        other_attachment = session.scalar(
            select(MediaAttachment.id)
            .where(
                MediaAttachment.storage_path == attachment.storage_path,
                MediaAttachment.post_id != post.id,
            )
            .limit(1)
        )
        if other_attachment is None:
            delete_managed_media_file(attachment.storage_path)
    session.delete(post)
    session.flush()


def schedule_post_now(session: Session, post: CanonicalPost) -> CanonicalPost:
    if any(job.status == "posting" for job in post.delivery_jobs):
        raise ValueError("This post is currently being published. Wait for delivery to finish before retrying it.")
    target_ids = _active_target_account_ids(post)
    target_accounts, pending_accounts = _resolve_scheduled_post_edit_targets(session, post, target_ids)
    if not target_accounts:
        raise ValueError("Select at least one destination account.")
    _mark_manual_recovery(post)
    post.scheduled_for = utcnow()
    post.status = "queued"
    if post.post_type == POST_TYPE_GIVEAWAY:
        if post.giveaway_campaign is None and post.instagram_giveaway is not None:
            migrate_legacy_instagram_giveaway(session, post)
        _sync_scheduled_post_giveaway(session, post, target_accounts, _giveaway_config_from_campaign(post))

    validation_errors = (
        validate_post_for_target_accounts(post, pending_accounts)
        if pending_accounts or not target_accounts
        else []
    )
    post.last_error = "; ".join(validation_errors) if validation_errors else None
    if validation_errors:
        raise ValueError(post.last_error)

    sync_delivery_jobs(session, post, target_accounts, "queued", reset_failed_retries=True)
    refresh_post_status(post)
    session.flush()
    return get_post(session, post.id) or post


def get_or_create_sync_state(session: Session, source_account: Account) -> AccountSyncState:
    stmt = select(AccountSyncState).where(AccountSyncState.source_account_id == source_account.id)
    state = session.scalar(stmt)
    if state:
        return state
    state = AccountSyncState(source_account_id=source_account.id, state_json={})
    session.add(state)
    session.flush()
    return state


def _resolve_pending_post_id(session: Session, source_account_id: str, external_id: str | None) -> str | None:
    if not source_account_id or not external_id:
        return None
    stmt = select(AccountPostRef).where(
        AccountPostRef.account_id == source_account_id,
        AccountPostRef.external_id == external_id,
    )
    ref = session.scalar(stmt)
    return ref.post_id if ref else None


def upsert_account_post_refs(
    session: Session,
    post: CanonicalPost,
    account: Account,
    external_refs: list[ExternalPostRefPayload],
) -> None:
    seen_ids: set[str] = set()
    for external_ref in external_refs:
        external_id = str(getattr(external_ref, "external_id", "") or "").strip()
        if not external_id or external_id in seen_ids:
            continue
        seen_ids.add(external_id)
        ref = session.scalar(
            select(AccountPostRef).where(
                AccountPostRef.account_id == account.id,
                AccountPostRef.external_id == external_id,
            )
        )
        observed_at = getattr(external_ref, "observed_at", None) or utcnow()
        external_url = getattr(external_ref, "external_url", None)
        if ref:
            ref.post_id = post.id
            ref.external_url = external_url or ref.external_url
            ref.observed_at = observed_at
            continue
        session.add(
            AccountPostRef(
                post_id=post.id,
                account_id=account.id,
                external_id=external_id,
                external_url=external_url,
                observed_at=observed_at,
            )
        )


def _remote_url_host(url: str | None) -> str | None:
    try:
        parsed = urlsplit(str(url or "").strip())
    except ValueError:
        return None
    return parsed.netloc.lower() if parsed.scheme.lower() in {"http", "https"} and parsed.netloc else None


def _find_polled_post(
    session: Session,
    persona: Persona,
    source_account: Account,
    external_refs: list[ExternalPostRefPayload],
) -> CanonicalPost | None:
    refs_by_id = {
        str(ref.external_id or "").strip(): ref
        for ref in external_refs
        if str(ref.external_id or "").strip()
    }
    urls = {
        str(ref.external_url).strip()
        for ref in external_refs
        if _remote_url_host(ref.external_url)
    }
    if not refs_by_id and not urls:
        return None

    def matches(external_id: str | None, external_url: str | None, account_id: str) -> bool:
        if external_url and str(external_url).strip() in urls:
            return True
        incoming = refs_by_id.get(str(external_id or "").strip()) if account_id == source_account.id else None
        if incoming is None:
            return False
        saved_host = _remote_url_host(external_url)
        incoming_host = _remote_url_host(incoming.external_url)
        # Mastodon IDs are local to an instance. Changing a configured instance
        # must not make the same numeric ID identify an unrelated status.
        return not (source_account.service == "mastodon" and saved_host and incoming_host and saved_host != incoming_host)

    identity_clause = or_(
        (AccountPostRef.account_id == source_account.id) & AccountPostRef.external_id.in_(refs_by_id),
        AccountPostRef.external_url.in_(urls),
    )
    saved_refs = session.scalars(
        select(AccountPostRef)
        .join(Account, Account.id == AccountPostRef.account_id)
        .join(CanonicalPost, CanonicalPost.id == AccountPostRef.post_id)
        .where(Account.persona_id == persona.id, Account.service == source_account.service,
               CanonicalPost.persona_id == persona.id, identity_clause)
    )
    for ref in saved_refs:
        if matches(ref.external_id, ref.external_url, ref.account_id):
            return get_post(session, ref.post_id) or ref.post

    # Older/recovered deliveries can have a persisted remote ID without a ref.
    # Repair that lookup during import instead of fanning out a second post.
    delivery_clause = or_(
        (DeliveryJob.target_account_id == source_account.id) & DeliveryJob.external_id.in_(refs_by_id),
        DeliveryJob.external_url.in_(urls),
    )
    saved_jobs = session.scalars(
        select(DeliveryJob)
        .join(Account, Account.id == DeliveryJob.target_account_id)
        .join(CanonicalPost, CanonicalPost.id == DeliveryJob.post_id)
        .where(DeliveryJob.status == "posted", Account.persona_id == persona.id,
               Account.service == source_account.service, CanonicalPost.persona_id == persona.id, delivery_clause)
    )
    for job in saved_jobs:
        if matches(job.external_id, job.external_url, job.target_account_id):
            return get_post(session, job.post_id) or job.post
    return None


def _set_import_delivery_suppression(post: CanonicalPost, reason: str) -> None:
    metadata = dict(post.metadata_json or {})
    metadata[IMPORT_DELIVERY_SUPPRESSED_REASON_KEY] = reason
    post.metadata_json = metadata


def _clear_import_delivery_suppression(post: CanonicalPost) -> None:
    metadata = dict(post.metadata_json or {})
    if IMPORT_DELIVERY_SUPPRESSED_REASON_KEY in metadata:
        metadata.pop(IMPORT_DELIVERY_SUPPRESSED_REASON_KEY, None)
        post.metadata_json = metadata


def _import_delivery_is_suppressed(post: CanonicalPost) -> bool:
    return bool((post.metadata_json or {}).get(IMPORT_DELIVERY_SUPPRESSED_REASON_KEY))


def upsert_polled_post(
    session: Session,
    persona: Persona,
    source_account: Account,
    payload: CanonicalPostPayload,
    *,
    queue_delivery: bool = True,
    delivery_suppressed_reason: str | None = None,
) -> CanonicalPost:
    session.flush()
    existing_post = _find_polled_post(session, persona, source_account, payload.external_refs)

    if existing_post is not None:
        upsert_account_post_refs(session, existing_post, source_account, payload.external_refs)
        if (existing_post.metadata_json or {}).get(MANUAL_RECOVERY_KEY):
            # A source rescan must preserve the user's repaired draft, targets,
            # and explicit retry decision on the existing logical post.
            session.flush()
            return existing_post
        if queue_delivery:
            _clear_import_delivery_suppression(existing_post)
        elif existing_post.origin_kind == "account_import" and existing_post.origin_account_id == source_account.id:
            _set_import_delivery_suppression(
                existing_post,
                delivery_suppressed_reason or "import_delivery_disabled",
            )
        if queue_delivery and existing_post.origin_account_id == source_account.id:
            target_accounts = routed_destination_accounts(session, source_account)
            if target_accounts:
                sync_delivery_jobs(session, existing_post, target_accounts, "queued")
                refresh_post_status(existing_post)
        session.flush()
        return existing_post

    metadata = dict(payload.metadata)
    if payload.reply_to_external:
        metadata["pending_reply_external_id"] = payload.reply_to_external.external_id
    if payload.quote_of_external:
        metadata["pending_quote_external_id"] = payload.quote_of_external.external_id
    if not queue_delivery:
        metadata[IMPORT_DELIVERY_SUPPRESSED_REASON_KEY] = delivery_suppressed_reason or "import_delivery_disabled"

    post = CanonicalPost(
        persona_id=persona.id,
        origin_kind="account_import",
        post_type=POST_TYPE_STANDARD,
        origin_account_id=source_account.id,
        status="queued" if queue_delivery else "posted",
        body=payload.body,
        publish_overrides_json=payload.publish_overrides,
        metadata_json=metadata,
        published_at=payload.published_at,
    )
    session.add(post)
    post.persona = persona
    post.origin_account = source_account
    post.reply_to_post_id = _resolve_pending_post_id(
        session,
        source_account.id,
        metadata.get("pending_reply_external_id"),
    )
    post.quote_of_post_id = _resolve_pending_post_id(
        session,
        source_account.id,
        metadata.get("pending_quote_external_id"),
    )

    session.flush()

    for media in payload.media:
        session.add(_create_attachment(post, media))
    upsert_account_post_refs(session, post, source_account, payload.external_refs)
    session.flush()

    target_accounts = routed_destination_accounts(session, source_account)
    if queue_delivery and target_accounts and (
        not metadata.get("pending_reply_external_id") or post.reply_to_post_id
    ) and (
        not metadata.get("pending_quote_external_id") or post.quote_of_post_id
    ):
        sync_delivery_jobs(session, post, target_accounts, "queued")
    refresh_post_status(post)
    session.flush()
    return get_post(session, post.id) or post


def reconcile_pending_relationships(session: Session, post: CanonicalPost) -> bool:
    if not post.origin_account_id:
        return False
    changed = False
    pending_reply = post.metadata_json.get("pending_reply_external_id")
    pending_quote = post.metadata_json.get("pending_quote_external_id")
    if pending_reply and not post.reply_to_post_id:
        post.reply_to_post_id = _resolve_pending_post_id(session, post.origin_account_id, pending_reply)
        changed = changed or bool(post.reply_to_post_id)
    if pending_quote and not post.quote_of_post_id:
        post.quote_of_post_id = _resolve_pending_post_id(session, post.origin_account_id, pending_quote)
        changed = changed or bool(post.quote_of_post_id)
    if changed and post.origin_account_id:
        source_account = session.get(Account, post.origin_account_id)
        if source_account:
            if _import_delivery_is_suppressed(post):
                return changed
            target_accounts = routed_destination_accounts(session, source_account)
            sync_delivery_jobs(session, post, target_accounts, "queued")
            refresh_post_status(post)
    return changed
