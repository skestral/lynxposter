from __future__ import annotations


DEFAULT_OIDC_SCOPE = "openid profile email"


def oidc_scope_items(raw_value: str | None) -> list[str]:
    if raw_value is None:
        return DEFAULT_OIDC_SCOPE.split()

    collapsed = raw_value.replace(",", " ")
    items: list[str] = []
    seen: set[str] = set()
    for item in collapsed.split():
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        items.append(normalized)
    return items or DEFAULT_OIDC_SCOPE.split()


def normalize_oidc_scope(raw_value: str | None) -> str:
    return " ".join(oidc_scope_items(raw_value))


def oidc_scope_includes_groups(raw_value: str | None) -> bool:
    return "groups" in {item.lower() for item in oidc_scope_items(raw_value)}


def oidc_group_mapping_enabled(admin_groups: str | None, user_groups: str | None) -> bool:
    return bool((admin_groups or "").strip() or (user_groups or "").strip())
