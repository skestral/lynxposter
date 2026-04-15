from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class MediaItem:
    storage_path: Path
    mime_type: str
    alt_text: str = ""
    size_bytes: int = 0
    checksum: str = ""
    sort_order: int = 0


@dataclass(slots=True)
class ExternalPostRefPayload:
    external_id: str
    external_url: str | None = None
    observed_at: datetime | None = None


@dataclass(slots=True)
class PendingRelationship:
    external_id: str


@dataclass(slots=True)
class CanonicalPostPayload:
    body: str
    media: list[MediaItem] = field(default_factory=list)
    publish_overrides: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    published_at: datetime | None = None
    external_refs: list[ExternalPostRefPayload] = field(default_factory=list)
    reply_to_external: PendingRelationship | None = None
    quote_of_external: PendingRelationship | None = None


@dataclass(slots=True)
class ValidationIssue:
    service: str
    message: str
    field: str | None = None
    level: str = "error"


@dataclass(slots=True)
class PublishResult:
    service: str
    external_id: str
    external_url: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)
    external_refs: list[ExternalPostRefPayload] = field(default_factory=list)


@dataclass(slots=True)
class PublishPreview:
    service: str
    action: str
    rendered_body: str
    endpoint_label: str | None = None
    request_shape: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PollResult:
    posts: list[CanonicalPostPayload]
    next_state: dict[str, Any] = field(default_factory=dict)
    cursor: str | None = None
    note: str | None = None
