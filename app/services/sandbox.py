from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.adapters import account_is_configured, get_destination_adapter_for_account
from app.domain import ValidationIssue
from app.models import CanonicalPost, MediaAttachment
from app.schemas import (
    SandboxAccountPreviewRead,
    SandboxExpectationCheckRead,
    SandboxExpectationInput,
    SandboxPreviewRead,
    SandboxPreviewRequest,
    ValidationIssueRead,
)
from app.services.personas import get_persona
from app.services.posts import resolve_target_accounts


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _build_transient_post(request: SandboxPreviewRequest) -> CanonicalPost:
    post = CanonicalPost(
        persona_id=request.persona_id,
        origin_kind="composer",
        origin_account_id=None,
        status="draft",
        body=request.body,
        publish_overrides_json=dict(request.publish_overrides_json or {}),
        metadata_json=dict(request.metadata_json or {}),
    )
    post.attachments = [
        MediaAttachment(
            storage_path=attachment.storage_path or f"sandbox/{attachment.filename}",
            mime_type=attachment.mime_type,
            alt_text=attachment.alt_text,
            size_bytes=attachment.size_bytes,
            checksum="sandbox",
            sort_order=attachment.sort_order,
        )
        for attachment in sorted(request.attachment_inputs, key=lambda item: (item.sort_order, item.filename))
    ]
    return post


def _serialize_issues(issues: list[ValidationIssue]) -> list[ValidationIssueRead]:
    return [
        ValidationIssueRead(
            service=issue.service,
            message=issue.message,
            field=issue.field,
            level=issue.level,
        )
        for issue in issues
    ]


def _build_check(
    *,
    key: str,
    label: str,
    passed: bool,
    expected: Any,
    actual: Any,
    message: str,
) -> SandboxExpectationCheckRead:
    return SandboxExpectationCheckRead(
        key=key,
        label=label,
        passed=passed,
        expected=expected,
        actual=actual,
        message=message,
    )


def _evaluate_global_expectations(expectations: SandboxExpectationInput, target_count: int) -> list[SandboxExpectationCheckRead]:
    checks: list[SandboxExpectationCheckRead] = []
    if expectations.expected_target_count is not None:
        passed = target_count == expectations.expected_target_count
        checks.append(
            _build_check(
                key="expected_target_count",
                label="Target count",
                passed=passed,
                expected=expectations.expected_target_count,
                actual=target_count,
                message=(
                    f"Matched expected target count of {expectations.expected_target_count}."
                    if passed
                    else f"Expected {expectations.expected_target_count} targets but sandbox resolved {target_count}."
                ),
            )
        )
    return checks


def _evaluate_account_expectations(
    *,
    expectations: SandboxExpectationInput,
    rendered_body: str,
    request_shape: dict[str, Any],
    attachment_count: int,
    source_link: str | None,
) -> list[SandboxExpectationCheckRead]:
    checks: list[SandboxExpectationCheckRead] = []
    serialized_request = json.dumps(request_shape, sort_keys=True)

    if expectations.body_must_contain:
        passed = expectations.body_must_contain in rendered_body
        checks.append(
            _build_check(
                key="body_must_contain",
                label="Body contains text",
                passed=passed,
                expected=expectations.body_must_contain,
                actual=rendered_body,
                message=(
                    f"Rendered body contains '{expectations.body_must_contain}'."
                    if passed
                    else f"Rendered body does not contain '{expectations.body_must_contain}'."
                ),
            )
        )

    if expectations.body_must_not_contain:
        passed = expectations.body_must_not_contain not in rendered_body
        checks.append(
            _build_check(
                key="body_must_not_contain",
                label="Body excludes text",
                passed=passed,
                expected=expectations.body_must_not_contain,
                actual=rendered_body,
                message=(
                    f"Rendered body excludes '{expectations.body_must_not_contain}'."
                    if passed
                    else f"Rendered body still contains '{expectations.body_must_not_contain}'."
                ),
            )
        )

    if expectations.max_body_length is not None:
        actual = len(rendered_body)
        passed = actual <= expectations.max_body_length
        checks.append(
            _build_check(
                key="max_body_length",
                label="Max body length",
                passed=passed,
                expected=expectations.max_body_length,
                actual=actual,
                message=(
                    f"Rendered body fits within {expectations.max_body_length} characters."
                    if passed
                    else f"Rendered body is {actual} characters, above the expected max of {expectations.max_body_length}."
                ),
            )
        )

    if expectations.expected_attachment_count is not None:
        passed = attachment_count == expectations.expected_attachment_count
        checks.append(
            _build_check(
                key="expected_attachment_count",
                label="Attachment count",
                passed=passed,
                expected=expectations.expected_attachment_count,
                actual=attachment_count,
                message=(
                    f"Attachment count matches {expectations.expected_attachment_count}."
                    if passed
                    else f"Expected {expectations.expected_attachment_count} attachments but found {attachment_count}."
                ),
            )
        )

    if expectations.expected_visibility is not None:
        actual_visibility = request_shape.get("visibility")
        passed = actual_visibility == expectations.expected_visibility
        checks.append(
            _build_check(
                key="expected_visibility",
                label="Visibility",
                passed=passed,
                expected=expectations.expected_visibility,
                actual=actual_visibility,
                message=(
                    f"Visibility matches {expectations.expected_visibility}."
                    if passed
                    else f"Expected visibility {expectations.expected_visibility} but saw {actual_visibility or 'none'}."
                ),
            )
        )

    if expectations.require_media is not None:
        actual_has_media = attachment_count > 0
        passed = actual_has_media is expectations.require_media
        checks.append(
            _build_check(
                key="require_media",
                label="Media requirement",
                passed=passed,
                expected=expectations.require_media,
                actual=actual_has_media,
                message=(
                    "Media requirement matched the sandbox preview."
                    if passed
                    else "Media requirement did not match the sandbox preview."
                ),
            )
        )

    if expectations.require_source_link_in_payload is not None:
        actual_has_source_link = bool(source_link and (source_link in rendered_body or source_link in serialized_request))
        passed = actual_has_source_link is expectations.require_source_link_in_payload
        checks.append(
            _build_check(
                key="require_source_link_in_payload",
                label="Source link in payload",
                passed=passed,
                expected=expectations.require_source_link_in_payload,
                actual=actual_has_source_link,
                message=(
                    "Source link expectation matched the sandbox payload."
                    if passed
                    else "Source link expectation did not match the sandbox payload."
                ),
            )
        )

    return checks


def build_sandbox_preview(session: Session, request: SandboxPreviewRequest) -> SandboxPreviewRead:
    persona = get_persona(session, request.persona_id)
    if not persona:
        raise ValueError("Persona not found.")

    post = _build_transient_post(request)
    target_accounts = resolve_target_accounts(session, persona, request.target_account_ids)

    global_errors: list[str] = []
    if not target_accounts:
        global_errors.append("Select at least one enabled destination account.")

    previews: list[SandboxAccountPreviewRead] = []
    for account in target_accounts:
        configured = account_is_configured(account)
        notes: list[str] = []
        issues: list[ValidationIssue] = []
        try:
            adapter = get_destination_adapter_for_account(account)
            preview = adapter.preview(post, persona, account)
            issues.extend(adapter.validate(post, persona, account))
        except KeyError:
            preview = None
            issues.append(
                ValidationIssue(
                    service=account.service,
                    field="service",
                    message="Outbound publishing is not supported for this account.",
                    level="error",
                )
            )

        if not configured:
            issues.append(
                ValidationIssue(
                    service=account.service,
                    field="credentials",
                    message="Account credentials are incomplete for live publishing.",
                    level="warning",
                )
            )
            notes.append("This account can be previewed, but it is not configured for a real publish yet.")

        if preview is None:
            rendered_body = post.body
            request_shape: dict[str, Any] = {}
            action = "unsupported"
            endpoint_label = None
        else:
            rendered_body = preview.rendered_body
            request_shape = preview.request_shape
            action = preview.action
            endpoint_label = preview.endpoint_label
            notes.extend(preview.notes)

        expectation_checks = _evaluate_account_expectations(
            expectations=request.expectations,
            rendered_body=rendered_body,
            request_shape=request_shape,
            attachment_count=len(post.attachments),
            source_link=(post.metadata_json or {}).get("link"),
        )
        error_issues = [issue for issue in issues if issue.level == "error"]
        publish_ready = configured and not error_issues

        previews.append(
            SandboxAccountPreviewRead(
                account_id=account.id,
                account_label=account.label,
                service=account.service,
                configured=configured,
                publish_ready=publish_ready,
                action=action,
                endpoint_label=endpoint_label,
                rendered_body=rendered_body,
                body_length=len(rendered_body),
                attachment_count=len(post.attachments),
                request_shape=request_shape,
                validation_issues=_serialize_issues(issues),
                expectation_checks=expectation_checks,
                notes=notes,
            )
        )

    global_checks = _evaluate_global_expectations(request.expectations, len(target_accounts))
    overall_valid = bool(previews) and all(preview.publish_ready for preview in previews) and not global_errors
    overall_expectations_passed = all(check.passed for check in global_checks) and all(
        check.passed for preview in previews for check in preview.expectation_checks
    )

    return SandboxPreviewRead(
        persona_id=persona.id,
        persona_name=persona.name,
        generated_at=_now_utc(),
        target_count=len(target_accounts),
        attachment_count=len(post.attachments),
        overall_valid=overall_valid,
        overall_expectations_passed=overall_expectations_passed,
        global_checks=global_checks,
        previews=previews,
        global_errors=global_errors,
    )
