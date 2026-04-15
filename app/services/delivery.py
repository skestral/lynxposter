from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from sqlalchemy import distinct, func, select
from sqlalchemy.orm import Session, selectinload

from app.adapters import get_destination_adapter_for_account, get_source_adapter_for_account, supports_source
from app.adapters.common import autorun_initial_import_guard_reason, logical_post_limit_reached, looks_like_historical_backfill, now_utc
from app.domain import ExternalPostRefPayload
from app.models import Account, AccountPostRef, AccountRoute, CanonicalPost, DeliveryAttempt, DeliveryJob, Persona
from app.services.alerts import AlertDispatcher
from app.services.events import log_run_event
from app.services.posts import (
    get_or_create_sync_state,
    persona_max_retries,
    reconcile_pending_relationships,
    refresh_post_status,
    sync_delivery_jobs,
    upsert_polled_post,
)


def new_run_id() -> str:
    return str(uuid4())


def _resolve_delivery_context(session: Session, post: CanonicalPost, target_account: Account) -> dict[str, str | None] | None:
    context = {
        "reply_external_id": None,
        "reply_external_url": None,
        "quote_external_id": None,
        "quote_external_url": None,
    }
    if post.reply_to_post_id:
        reply_ref = session.scalar(
            select(AccountPostRef).where(
                AccountPostRef.post_id == post.reply_to_post_id,
                AccountPostRef.account_id == target_account.id,
            )
        )
        if not reply_ref:
            return None
        context["reply_external_id"] = reply_ref.external_id
        context["reply_external_url"] = reply_ref.external_url
    if post.quote_of_post_id:
        quote_ref = session.scalar(
            select(AccountPostRef).where(
                AccountPostRef.post_id == post.quote_of_post_id,
                AccountPostRef.account_id == target_account.id,
            )
        )
        if not quote_ref:
            return None
        context["quote_external_id"] = quote_ref.external_id
        context["quote_external_url"] = quote_ref.external_url
    return context


def _published_posts_within_hour(session: Session, persona_id: str) -> int:
    cutoff = now_utc() - timedelta(hours=1)
    stmt = (
        select(func.count(distinct(DeliveryJob.post_id)))
        .where(
            DeliveryJob.delivered_at.is_not(None),
            DeliveryJob.delivered_at >= cutoff,
        )
        .join(CanonicalPost, CanonicalPost.id == DeliveryJob.post_id)
        .where(CanonicalPost.persona_id == persona_id)
    )
    return int(session.scalar(stmt) or 0)


def poll_sources(session: Session, alerts: AlertDispatcher, *, run_id: str | None = None, trigger: str = "manual") -> str:
    run_id = run_id or new_run_id()
    stmt = select(Persona).options(selectinload(Persona.accounts)).where(Persona.is_enabled.is_(True)).order_by(Persona.name)
    personas = list(session.scalars(stmt))
    for persona in personas:
        for account in persona.accounts:
            if not account.is_enabled or not account.source_enabled or not supports_source(account.service):
                continue
            sync_state = get_or_create_sync_state(session, account)
            guard_reason = autorun_initial_import_guard_reason(persona, account, sync_state) if trigger == "autorun" else None
            if guard_reason:
                log_run_event(
                    session,
                    run_id=run_id,
                    persona_id=persona.id,
                    persona_name=persona.name,
                    account_id=account.id,
                    service=account.service,
                    operation="poll",
                    severity="warning",
                    message=guard_reason,
                    metadata={"trigger": trigger, "autorun_guard": "initial_import_blocked"},
                )
                continue
            try:
                log_run_event(
                    session,
                    run_id=run_id,
                    persona_id=persona.id,
                    persona_name=persona.name,
                    account_id=account.id,
                    service=account.service,
                    operation="poll",
                    message=f"Polling {account.label}",
                    metadata={"trigger": trigger},
                )
                result = get_source_adapter_for_account(account).poll(session, persona, account, sync_state)
                imported = 0
                for payload in result.posts:
                    upsert_polled_post(session, persona, account, payload)
                    imported += 1
                sync_state.state_json = result.next_state
                sync_state.cursor = result.cursor
                sync_state.last_polled_at = now_utc()
                account.last_health_status = "ok"
                account.last_error = None
                log_run_event(
                    session,
                    run_id=run_id,
                    persona_id=persona.id,
                    persona_name=persona.name,
                    account_id=account.id,
                    service=account.service,
                    operation="poll",
                    message=result.note or f"Imported {imported} posts from {account.label}",
                    metadata={"imported_count": imported, "initial_sync_note": result.note, "trigger": trigger},
                )
            except Exception as exc:
                account.last_health_status = "error"
                account.last_error = str(exc)
                log_run_event(
                    session,
                    run_id=run_id,
                    persona_id=persona.id,
                    persona_name=persona.name,
                    account_id=account.id,
                    service=account.service,
                    operation="poll",
                    message=str(exc),
                    severity="error",
                    metadata={"trigger": trigger},
                )
                alerts.emit_hard_failure(
                    session,
                    run_id=run_id,
                    persona=persona,
                    account=account,
                    service=account.service,
                    operation="poll",
                    message=str(exc),
                    error_class=exc.__class__.__name__,
                )
    return run_id


def enqueue_due_scheduled_posts(session: Session, *, run_id: str | None = None) -> str:
    run_id = run_id or new_run_id()
    stmt = (
        select(CanonicalPost)
        .options(
            selectinload(CanonicalPost.delivery_jobs).selectinload(DeliveryJob.target_account),
            selectinload(CanonicalPost.persona),
        )
        .where(
            CanonicalPost.origin_kind == "composer",
            CanonicalPost.status.in_(["scheduled", "queued"]),
            CanonicalPost.scheduled_for.is_not(None),
            CanonicalPost.scheduled_for <= now_utc(),
        )
    )
    for post in session.scalars(stmt):
        post.status = "queued"
        for job in post.delivery_jobs:
            if job.status in {"draft", "scheduled", "failed"} and job.target_account and job.target_account.is_enabled:
                job.status = "queued"
                job.queued_at = now_utc()
        refresh_post_status(post)
        log_run_event(
            session,
            run_id=run_id,
            persona_id=post.persona_id,
            persona_name=post.persona.name if post.persona else None,
            operation="schedule",
            message=f"Scheduled post {post.id} is now queued",
            post_id=post.id,
        )
    return run_id


def reconcile_pending_posts(session: Session, *, run_id: str | None = None) -> str:
    run_id = run_id or new_run_id()
    stmt = (
        select(CanonicalPost)
        .options(selectinload(CanonicalPost.delivery_jobs), selectinload(CanonicalPost.persona))
        .where(CanonicalPost.origin_kind == "account_import")
    )
    for post in session.scalars(stmt):
        if reconcile_pending_relationships(session, post):
            log_run_event(
                session,
                run_id=run_id,
                persona_id=post.persona_id,
                persona_name=post.persona.name if post.persona else None,
                account_id=post.origin_account_id,
                operation="reconcile",
                message=f"Resolved pending relationships for post {post.id}",
                post_id=post.id,
            )
    return run_id


def _route_is_enabled(session: Session, post: CanonicalPost, target_account: Account) -> bool:
    if post.origin_kind != "account_import" or not post.origin_account_id:
        return True
    stmt = select(AccountRoute).where(
        AccountRoute.source_account_id == post.origin_account_id,
        AccountRoute.destination_account_id == target_account.id,
        AccountRoute.is_enabled.is_(True),
    )
    return session.scalar(stmt) is not None


def process_delivery_queue(session: Session, alerts: AlertDispatcher, *, run_id: str | None = None) -> str:
    run_id = run_id or new_run_id()
    stmt = (
        select(DeliveryJob)
        .options(
            selectinload(DeliveryJob.post).selectinload(CanonicalPost.attachments),
            selectinload(DeliveryJob.post).selectinload(CanonicalPost.delivery_jobs).selectinload(DeliveryJob.target_account),
            selectinload(DeliveryJob.post).selectinload(CanonicalPost.persona),
            selectinload(DeliveryJob.post).selectinload(CanonicalPost.origin_account),
            selectinload(DeliveryJob.target_account),
        )
        .where(DeliveryJob.status == "queued")
        .order_by(DeliveryJob.queued_at)
    )

    for job in session.scalars(stmt):
        post = job.post
        persona = post.persona
        target_account = job.target_account
        if not persona or not target_account:
            job.status = "cancelled"
            continue
        if not persona.is_enabled or not target_account.is_enabled or not target_account.destination_enabled:
            job.status = "cancelled"
            refresh_post_status(post)
            continue
        if not _route_is_enabled(session, post, target_account):
            job.status = "cancelled"
            refresh_post_status(post)
            continue
        if post.origin_kind == "account_import" and looks_like_historical_backfill(post, persona, post.origin_account):
            job.status = "cancelled"
            job.last_error = "Skipped historical backfill import to avoid reposting old content."
            post.last_error = job.last_error
            log_run_event(
                session,
                run_id=run_id,
                persona_id=persona.id,
                persona_name=persona.name,
                account_id=target_account.id,
                service=target_account.service,
                operation="publish",
                severity="warning",
                message=job.last_error,
                post_id=post.id,
                delivery_job_id=job.id,
            )
            refresh_post_status(post)
            continue

        overflow_policy = (persona.throttle_settings_json or {}).get("overflow_posts", "retry")
        has_successful_delivery = any(sibling.status == "posted" for sibling in post.delivery_jobs)
        if not has_successful_delivery and logical_post_limit_reached(persona, _published_posts_within_hour(session, persona.id)):
            if overflow_policy == "skip":
                for sibling in post.delivery_jobs:
                    if sibling.status in {"queued", "posting"}:
                        sibling.status = "skipped"
                post.status = "cancelled"
            continue

        context = _resolve_delivery_context(session, post, target_account)
        if context is None and (post.reply_to_post_id or post.quote_of_post_id):
            continue

        try:
            adapter = get_destination_adapter_for_account(target_account)
        except KeyError:
            job.status = "cancelled"
            job.last_error = "Outbound publishing is not supported."
            refresh_post_status(post)
            continue

        issues = adapter.validate(post, persona, target_account)
        if issues:
            message = "; ".join(issue.message for issue in issues)
            post.last_error = message
            _record_attempt_failure(job, message, "ValidationError")
            if job.status == "failed":
                alerts.emit_hard_failure(
                    session,
                    run_id=run_id,
                    persona=persona,
                    account=target_account,
                    service=target_account.service,
                    post=post,
                    delivery_job=job,
                    operation="validate",
                    message=message,
                    error_class="ValidationError",
                    retry_count=job.attempt_count,
                )
            refresh_post_status(post)
            continue

        attempt = DeliveryAttempt(delivery_job_id=job.id, status="started")
        session.add(attempt)
        job.status = "posting"
        job.attempt_count += 1
        job.last_attempt_at = now_utc()
        session.flush()

        try:
            result = adapter.publish(session, post, persona, target_account, context=context)
            attempt.status = "posted"
            attempt.finished_at = now_utc()
            attempt.response_payload = result.raw
            job.status = "posted"
            job.external_id = result.external_id
            job.external_url = result.external_url
            job.delivered_at = now_utc()
            job.last_error = None
            job.last_error_class = None
            post.last_error = None
            target_account.last_health_status = "ok"
            target_account.last_error = None
            if post.published_at is None:
                post.published_at = now_utc()

            external_refs = list(result.external_refs or [])
            if not external_refs:
                external_refs = [
                    ExternalPostRefPayload(
                        external_id=result.external_id,
                        external_url=result.external_url,
                        observed_at=now_utc(),
                    )
                ]
            for external_ref in external_refs:
                ref = session.scalar(
                    select(AccountPostRef).where(
                        AccountPostRef.account_id == target_account.id,
                        AccountPostRef.external_id == external_ref.external_id,
                    )
                )
                if ref:
                    ref.post_id = post.id
                    ref.external_url = external_ref.external_url
                    ref.observed_at = external_ref.observed_at or now_utc()
                else:
                    session.add(
                        AccountPostRef(
                            post_id=post.id,
                            account_id=target_account.id,
                            external_id=external_ref.external_id,
                            external_url=external_ref.external_url,
                            observed_at=external_ref.observed_at or now_utc(),
                        )
                    )

            refresh_post_status(post)
            log_run_event(
                session,
                run_id=run_id,
                persona_id=persona.id,
                persona_name=persona.name,
                account_id=target_account.id,
                service=target_account.service,
                operation="publish",
                message=f"Published post {post.id} to {target_account.label}",
                post_id=post.id,
                delivery_job_id=job.id,
            )
        except Exception as exc:
            attempt.status = "failed"
            attempt.finished_at = now_utc()
            attempt.error_class = exc.__class__.__name__
            attempt.error_message = str(exc)
            post.last_error = str(exc)
            target_account.last_health_status = "error"
            target_account.last_error = str(exc)
            _record_attempt_failure(job, str(exc), exc.__class__.__name__)
            if job.status == "failed":
                alerts.emit_hard_failure(
                    session,
                    run_id=run_id,
                    persona=persona,
                    account=target_account,
                    service=target_account.service,
                    post=post,
                    delivery_job=job,
                    operation="publish",
                    message=str(exc),
                    error_class=exc.__class__.__name__,
                    retry_count=job.attempt_count,
                )
            log_run_event(
                session,
                run_id=run_id,
                persona_id=persona.id,
                persona_name=persona.name,
                account_id=target_account.id,
                service=target_account.service,
                operation="publish",
                message=str(exc),
                severity="error",
                post_id=post.id,
                delivery_job_id=job.id,
            )
            refresh_post_status(post)
    return run_id


def _record_attempt_failure(job: DeliveryJob, message: str, error_class: str) -> None:
    job.last_error = message
    job.last_error_class = error_class
    job.last_attempt_at = now_utc()
    max_retries = job.max_retries or persona_max_retries(job.post.persona)
    if job.attempt_count >= max_retries:
        job.status = "failed"
    else:
        job.status = "queued"
