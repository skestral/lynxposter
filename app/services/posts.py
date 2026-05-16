from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session, selectinload

from app.adapters import get_destination_adapter_for_account
from app.adapters.common import delivery_summary
from app.domain import CanonicalPostPayload, ExternalPostRefPayload, MediaItem
from app.models import Account, AccountPostRef, AccountSyncState, CanonicalPost, DeliveryJob, MediaAttachment, Persona
from app.schemas import ScheduledPostCreate, ScheduledPostUpdate
from app.services.giveaway_engine import (
    POST_TYPE_GIVEAWAY,
    POST_TYPE_STANDARD,
    giveaway_config_input_from_json,
    giveaway_selectinloads,
    migrate_legacy_instagram_giveaway,
    sync_giveaway_campaign,
)
from app.services.personas import get_persona, persona_destination_accounts, routed_destination_accounts
from app.services.storage import delete_managed_media_file


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


def get_post(session: Session, post_id: str, *, owner_user_id: str | None = None) -> CanonicalPost | None:
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
    if owner_user_id is not None:
        stmt = stmt.join(Persona, Persona.id == CanonicalPost.persona_id).where(Persona.owner_user_id == owner_user_id)
    return session.scalar(stmt)


def list_scheduled_posts(session: Session, *, owner_user_id: str | None = None) -> list[CanonicalPost]:
    stmt = (
        select(CanonicalPost)
        .options(
            selectinload(CanonicalPost.attachments),
            selectinload(CanonicalPost.delivery_jobs).selectinload(DeliveryJob.target_account),
            selectinload(CanonicalPost.persona),
            selectinload(CanonicalPost.origin_account),
            *giveaway_selectinloads(),
        )
        .where(CanonicalPost.origin_kind == "composer")
        .order_by(desc(CanonicalPost.created_at))
    )
    if owner_user_id is not None:
        stmt = stmt.join(Persona, Persona.id == CanonicalPost.persona_id).where(Persona.owner_user_id == owner_user_id)
    return list(session.scalars(stmt))


def _create_attachment(post: CanonicalPost, item: MediaItem) -> MediaAttachment:
    return MediaAttachment(
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


def _delete_attachment_file_if_unshared(session: Session, attachment: MediaAttachment) -> None:
    other_attachment = session.scalar(
        select(MediaAttachment.id)
        .where(
            MediaAttachment.storage_path == attachment.storage_path,
            MediaAttachment.id != attachment.id,
        )
        .limit(1)
    )
    if other_attachment is None:
        delete_managed_media_file(attachment.storage_path)


def _sync_post_attachments(
    session: Session,
    post: CanonicalPost,
    *,
    attachment_order: list[str] | None,
    deleted_attachment_ids: list[str] | None,
) -> None:
    attachments_by_id = {attachment.id: attachment for attachment in post.attachments}
    deleted_ids = [str(item or "").strip() for item in deleted_attachment_ids or [] if str(item or "").strip()]
    unknown_deleted = [attachment_id for attachment_id in deleted_ids if attachment_id not in attachments_by_id]
    if unknown_deleted:
        raise ValueError("One or more selected media items no longer exist on this post.")

    for attachment_id in dict.fromkeys(deleted_ids):
        attachment = attachments_by_id.pop(attachment_id)
        _delete_attachment_file_if_unshared(session, attachment)
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
    if breakdown["succeeded"] and not breakdown["failed"] and not breakdown["cancelled"]:
        return "success"
    if breakdown["succeeded"]:
        return "partial_failure"
    if breakdown["failed"]:
        return "failure"
    if breakdown["cancelled"]:
        return "cancelled"
    return post.status


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


def sync_delivery_jobs(session: Session, post: CanonicalPost, target_accounts: list[Account], desired_status: str) -> list[DeliveryJob]:
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

        job.max_retries = persona_max_retries(post.persona)
        if job.status not in {"posted", "posting"}:
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
    if payload.body is not None:
        post.body = payload.body
    if payload.post_type is not None:
        post.post_type = payload.post_type
    if payload.status is not None:
        post.status = payload.status
    if payload.publish_overrides_json is not None:
        post.publish_overrides_json = payload.publish_overrides_json
    if payload.metadata_json is not None:
        post.metadata_json = payload.metadata_json
    if "scheduled_for" in payload.model_fields_set:
        post.scheduled_for = normalize_datetime(payload.scheduled_for)

    _sync_post_attachments(
        session,
        post,
        attachment_order=payload.attachment_order,
        deleted_attachment_ids=payload.deleted_attachment_ids,
    )

    next_sort_order = len(post.attachments)
    for offset, item in enumerate(media_items or []):
        item.sort_order = next_sort_order + offset
        session.add(_create_attachment(post, item))
    session.flush()

    target_ids = payload.target_account_ids
    if target_ids is None:
        target_ids = _active_target_account_ids(post)
    target_accounts = resolve_target_accounts(session, post.persona, target_ids)
    sync_delivery_jobs(session, post, target_accounts, _desired_job_status(post.status))
    giveaway_config = payload.giveaway
    if post.post_type == POST_TYPE_GIVEAWAY and giveaway_config is None:
        if post.giveaway_campaign is None and post.instagram_giveaway is not None:
            migrate_legacy_instagram_giveaway(session, post)
        giveaway_config = _giveaway_config_from_campaign(post)
    sync_giveaway_campaign(session, post, target_accounts, giveaway_config)

    validation_errors = validate_post_for_target_accounts(post, target_accounts)
    post.last_error = "; ".join(validation_errors) if validation_errors else None
    if validation_errors and post.status in {"scheduled", "queued", "posting"}:
        raise ValueError(post.last_error)

    refresh_post_status(post)
    session.flush()
    return get_post(session, post.id) or post


def delete_scheduled_post(session: Session, post: CanonicalPost) -> None:
    if post.origin_kind != "composer":
        raise ValueError("Only scheduled posts created in the composer can be deleted.")
    if post.status != "draft":
        raise ValueError("Only draft scheduled posts can be deleted.")
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
    target_ids = _active_target_account_ids(post)
    target_accounts = resolve_target_accounts(session, post.persona, target_ids)
    post.scheduled_for = utcnow()
    post.status = "queued"
    if post.post_type == POST_TYPE_GIVEAWAY:
        if post.giveaway_campaign is None and post.instagram_giveaway is not None:
            migrate_legacy_instagram_giveaway(session, post)
        sync_giveaway_campaign(session, post, target_accounts, _giveaway_config_from_campaign(post))

    validation_errors = validate_post_for_target_accounts(post, target_accounts)
    post.last_error = "; ".join(validation_errors) if validation_errors else None
    if validation_errors:
        raise ValueError(post.last_error)

    sync_delivery_jobs(session, post, target_accounts, "queued")
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


def _upsert_account_post_refs(
    session: Session,
    post: CanonicalPost,
    account: Account,
    external_refs: list[ExternalPostRefPayload],
) -> None:
    for external_ref in external_refs:
        external_id = str(getattr(external_ref, "external_id", "") or "").strip()
        if not external_id:
            continue
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
            ref.external_url = external_url
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


def upsert_polled_post(session: Session, persona: Persona, source_account: Account, payload: CanonicalPostPayload) -> CanonicalPost:
    existing_post: CanonicalPost | None = None
    for external_ref in payload.external_refs:
        stmt = select(AccountPostRef).where(
            AccountPostRef.account_id == source_account.id,
            AccountPostRef.external_id == external_ref.external_id,
        )
        existing_ref = session.scalar(stmt)
        if existing_ref:
            existing_post = get_post(session, existing_ref.post_id) or existing_ref.post
            break

    if existing_post is not None:
        _upsert_account_post_refs(session, existing_post, source_account, payload.external_refs)
        if existing_post.origin_account_id == source_account.id:
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

    post = CanonicalPost(
        persona_id=persona.id,
        origin_kind="account_import",
        post_type=POST_TYPE_STANDARD,
        origin_account_id=source_account.id,
        status="queued",
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
    _upsert_account_post_refs(session, post, source_account, payload.external_refs)
    session.flush()

    target_accounts = routed_destination_accounts(session, source_account)
    if target_accounts and (
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
            target_accounts = routed_destination_accounts(session, source_account)
            sync_delivery_jobs(session, post, target_accounts, "queued")
            refresh_post_status(post)
    return changed
