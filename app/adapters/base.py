from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from sqlalchemy.orm import Session

from app.domain import PollResult, PublishPreview, PublishResult, ValidationIssue
from app.models import Account, AccountSyncState, CanonicalPost, Persona


class AdapterError(RuntimeError):
    """Base adapter error."""


class ConfigurationError(AdapterError):
    """Raised when an account is missing required configuration."""


class SourceAdapter(ABC):
    service: str

    @abstractmethod
    def poll(
        self,
        session: Session,
        persona: Persona,
        account: Account,
        sync_state: AccountSyncState | None,
    ) -> PollResult:
        raise NotImplementedError


class DestinationAdapter(ABC):
    service: str

    @abstractmethod
    def validate(self, post: CanonicalPost, persona: Persona, account: Account) -> list[ValidationIssue]:
        raise NotImplementedError

    @abstractmethod
    def preview(
        self,
        post: CanonicalPost,
        persona: Persona,
        account: Account,
        *,
        context: dict[str, Any] | None = None,
    ) -> PublishPreview:
        raise NotImplementedError

    @abstractmethod
    def publish(
        self,
        session: Session,
        post: CanonicalPost,
        persona: Persona,
        account: Account,
        *,
        context: dict[str, Any] | None = None,
    ) -> PublishResult:
        raise NotImplementedError


def get_account_credentials(account: Account) -> dict[str, Any]:
    return dict(account.credentials_json or {})


def get_account_source_setting(persona: Persona, account: Account, key: str, default: Any = None) -> Any:
    account_settings = account.source_settings_json or {}
    if key in account_settings:
        value = account_settings[key]
        if value is not None and (not isinstance(value, str) or value.strip() != ""):
            return value
    persona_settings = persona.settings_json or {}
    if key in persona_settings:
        value = persona_settings[key]
        if value is not None and (not isinstance(value, str) or value.strip() != ""):
            return value
    return default


def get_account_publish_setting(
    persona: Persona,
    account: Account,
    key: str,
    default: Any = None,
    *,
    fallback_keys: tuple[str, ...] = (),
) -> Any:
    account_settings = account.publish_settings_json or {}
    if key in account_settings:
        value = account_settings[key]
        if value is not None and (not isinstance(value, str) or value.strip() != ""):
            return value
    persona_settings = persona.settings_json or {}
    for persona_key in (key, *fallback_keys):
        if persona_key not in persona_settings:
            continue
        value = persona_settings[persona_key]
        if value is not None and (not isinstance(value, str) or value.strip() != ""):
            return value
    return default
