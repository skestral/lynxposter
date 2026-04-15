from __future__ import annotations

from typing import Any

INSTAGRAM_INSTAGRAPI_SETTINGS_KEY = "_lynxposter_instagrapi_settings"


def _normalized_settings(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    if not value:
        return None
    return dict(value)


def get_instagram_private_settings(credentials: dict[str, Any] | None) -> dict[str, Any] | None:
    current = dict(credentials or {})
    return _normalized_settings(current.get(INSTAGRAM_INSTAGRAPI_SETTINGS_KEY))


def apply_instagram_private_settings(
    credentials: dict[str, Any] | None,
    *,
    previous_credentials: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current = dict(credentials or {})
    previous = dict(previous_credentials or {})

    resolved = _normalized_settings(settings)
    if resolved is None:
        resolved = get_instagram_private_settings(current) or get_instagram_private_settings(previous)

    if resolved is None:
        current.pop(INSTAGRAM_INSTAGRAPI_SETTINGS_KEY, None)
        return current

    current[INSTAGRAM_INSTAGRAPI_SETTINGS_KEY] = resolved
    return current
