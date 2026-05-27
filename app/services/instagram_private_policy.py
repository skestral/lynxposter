from __future__ import annotations

from dataclasses import dataclass

from app.config import get_settings


INSTAGRAM_PRIVATE_SCAN_MODE_MANUAL_ONLY = "manual_only"
INSTAGRAM_PRIVATE_SCAN_MODE_END_ONLY = "end_only"
INSTAGRAM_PRIVATE_SCAN_MODE_WEEKLY = "weekly"

INSTAGRAM_PRIVATE_SCAN_MODES = {
    INSTAGRAM_PRIVATE_SCAN_MODE_MANUAL_ONLY,
    INSTAGRAM_PRIVATE_SCAN_MODE_END_ONLY,
    INSTAGRAM_PRIVATE_SCAN_MODE_WEEKLY,
}

INSTAGRAM_PRIVATE_REASON_MANUAL = "manual"
INSTAGRAM_PRIVATE_REASON_END_OF_GIVEAWAY = "end_of_giveaway"
INSTAGRAM_PRIVATE_REASON_WEEKLY_DUE = "weekly_due"
INSTAGRAM_PRIVATE_REASON_DIAGNOSTIC = "diagnostic"


@dataclass(frozen=True)
class InstagramPrivateAccessDecision:
    allowed: bool
    mode: str
    reason: str
    message: str


def normalize_instagram_private_scan_mode(value: str | None) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    aliases = {
        "": INSTAGRAM_PRIVATE_SCAN_MODE_MANUAL_ONLY,
        "manual": INSTAGRAM_PRIVATE_SCAN_MODE_MANUAL_ONLY,
        "manual_only": INSTAGRAM_PRIVATE_SCAN_MODE_MANUAL_ONLY,
        "graph_only": INSTAGRAM_PRIVATE_SCAN_MODE_MANUAL_ONLY,
        "end": INSTAGRAM_PRIVATE_SCAN_MODE_END_ONLY,
        "end_only": INSTAGRAM_PRIVATE_SCAN_MODE_END_ONLY,
        "end_of_giveaway": INSTAGRAM_PRIVATE_SCAN_MODE_END_ONLY,
        "weekly": INSTAGRAM_PRIVATE_SCAN_MODE_WEEKLY,
        "weekly_due": INSTAGRAM_PRIVATE_SCAN_MODE_WEEKLY,
    }
    candidate = aliases.get(normalized, normalized)
    return candidate if candidate in INSTAGRAM_PRIVATE_SCAN_MODES else INSTAGRAM_PRIVATE_SCAN_MODE_MANUAL_ONLY


def instagram_private_scan_mode() -> str:
    return normalize_instagram_private_scan_mode(get_settings().instagram_private_scan_mode)


def instagram_private_scan_mode_label(mode: str | None = None) -> str:
    resolved = normalize_instagram_private_scan_mode(mode or instagram_private_scan_mode())
    if resolved == INSTAGRAM_PRIVATE_SCAN_MODE_WEEKLY:
        return "Weekly private scans allowed"
    if resolved == INSTAGRAM_PRIVATE_SCAN_MODE_END_ONLY:
        return "End-of-giveaway private scans allowed"
    return "Graph-only automation"


def instagram_private_access_decision(reason: str) -> InstagramPrivateAccessDecision:
    resolved_reason = str(reason or "").strip().lower() or INSTAGRAM_PRIVATE_REASON_MANUAL
    mode = instagram_private_scan_mode()
    always_allowed = {INSTAGRAM_PRIVATE_REASON_MANUAL, INSTAGRAM_PRIVATE_REASON_DIAGNOSTIC}
    mode_allowed = {
        INSTAGRAM_PRIVATE_SCAN_MODE_MANUAL_ONLY: always_allowed,
        INSTAGRAM_PRIVATE_SCAN_MODE_END_ONLY: always_allowed | {INSTAGRAM_PRIVATE_REASON_END_OF_GIVEAWAY},
        INSTAGRAM_PRIVATE_SCAN_MODE_WEEKLY: always_allowed | {INSTAGRAM_PRIVATE_REASON_END_OF_GIVEAWAY, INSTAGRAM_PRIVATE_REASON_WEEKLY_DUE},
    }
    if resolved_reason in mode_allowed.get(mode, always_allowed):
        return InstagramPrivateAccessDecision(
            allowed=True,
            mode=mode,
            reason=resolved_reason,
            message="Instagram private verification is allowed for this explicit action.",
        )
    return InstagramPrivateAccessDecision(
        allowed=False,
        mode=mode,
        reason=resolved_reason,
        message=(
            "Instagram private verification was skipped because Instagram safety mode is "
            f"{instagram_private_scan_mode_label(mode)}. Use the manual scan button for intentional private checks."
        ),
    )


def ensure_instagram_private_access_allowed(reason: str) -> InstagramPrivateAccessDecision:
    decision = instagram_private_access_decision(reason)
    if not decision.allowed:
        raise RuntimeError(decision.message)
    return decision
