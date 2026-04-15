from __future__ import annotations

from copy import deepcopy

DEFAULT_UI_THEME = "skylight"
DEFAULT_UI_MODE = "light"

_MODE_LABELS = {
    "light": "Light",
    "dark": "Dark",
}

_LIGHT_FOUNDATION = {
    "ink": "#1e2b46",
    "ink_soft": "#5f6f8e",
    "border": "rgba(124, 152, 204, 0.28)",
    "border_soft": "rgba(109, 131, 179, 0.14)",
    "surface": "rgba(255, 255, 255, 0.62)",
    "surface_strong": "rgba(255, 255, 255, 0.8)",
    "muted": "rgba(245, 249, 255, 0.56)",
    "input_bg": "rgba(255, 255, 255, 0.72)",
    "shadow_rgb": "44, 64, 108",
    "overlay_glow": "rgba(255, 255, 255, 0.88)",
    "nav_start": "rgba(255, 255, 255, 0.6)",
    "nav_end": "rgba(255, 255, 255, 0.32)",
    "elevated": "rgba(255, 255, 255, 0.76)",
    "elevated_soft": "rgba(255, 255, 255, 0.38)",
    "menu_bg": "rgba(255, 255, 255, 0.78)",
    "table_striped": "rgba(255, 255, 255, 0.3)",
    "table_hover": "rgba(255, 255, 255, 0.38)",
}

_DARK_FOUNDATION = {
    "ink": "#eef3ff",
    "ink_soft": "#a6b3d5",
    "border": "rgba(127, 149, 199, 0.24)",
    "border_soft": "rgba(115, 130, 169, 0.16)",
    "surface": "rgba(19, 25, 41, 0.68)",
    "surface_strong": "rgba(23, 31, 50, 0.84)",
    "muted": "rgba(20, 28, 48, 0.58)",
    "input_bg": "rgba(14, 20, 35, 0.78)",
    "shadow_rgb": "7, 10, 20",
    "overlay_glow": "rgba(255, 255, 255, 0.08)",
    "nav_start": "rgba(16, 22, 38, 0.78)",
    "nav_end": "rgba(18, 25, 42, 0.58)",
    "elevated": "rgba(24, 32, 52, 0.82)",
    "elevated_soft": "rgba(14, 20, 35, 0.62)",
    "menu_bg": "rgba(14, 20, 35, 0.88)",
    "table_striped": "rgba(255, 255, 255, 0.04)",
    "table_hover": "rgba(255, 255, 255, 0.08)",
}

_UI_THEME_OPTIONS: tuple[dict[str, object], ...] = (
    {
        "id": "skylight",
        "label": "Skylight",
        "collection": "Classic",
        "description": "Icy blue glass with silver light and a crisp cloudbank backdrop.",
        "swatches": ("#d6e6ff", "#f3f7ff", "#b6d3ff"),
        "modes": {
            "light": {
                "accent": "#5674d9",
                "secondary": "#7fb8df",
                "highlight": "#eef5ff",
                "bg_start": "#dbe9ff",
                "bg_mid": "#f7fbff",
                "bg_end": "#e7f1ff",
                "blob_one": "rgba(128, 173, 255, 0.56)",
                "blob_two": "rgba(210, 225, 255, 0.78)",
                "blob_three": "rgba(152, 212, 243, 0.38)",
            },
            "dark": {
                "accent": "#9cb6ff",
                "secondary": "#72c8ff",
                "highlight": "#16223c",
                "bg_start": "#081224",
                "bg_mid": "#0d1831",
                "bg_end": "#11203d",
                "blob_one": "rgba(83, 126, 228, 0.42)",
                "blob_two": "rgba(53, 89, 164, 0.3)",
                "blob_three": "rgba(57, 155, 194, 0.24)",
            },
        },
    },
    {
        "id": "sunrise",
        "label": "Sunrise",
        "collection": "Classic",
        "description": "Soft apricot and blush gradients with warm pearl highlights.",
        "swatches": ("#ffd7c2", "#fff3ea", "#ffc7d4"),
        "modes": {
            "light": {
                "accent": "#d66c74",
                "secondary": "#f2aa6f",
                "highlight": "#fff4ec",
                "bg_start": "#ffe2d3",
                "bg_mid": "#fff8f3",
                "bg_end": "#ffe9ea",
                "blob_one": "rgba(255, 169, 136, 0.5)",
                "blob_two": "rgba(255, 228, 209, 0.86)",
                "blob_three": "rgba(255, 182, 194, 0.44)",
            },
            "dark": {
                "accent": "#ff9e93",
                "secondary": "#ffc279",
                "highlight": "#301d24",
                "bg_start": "#1c1117",
                "bg_mid": "#29171f",
                "bg_end": "#341a21",
                "blob_one": "rgba(215, 115, 87, 0.36)",
                "blob_two": "rgba(120, 54, 49, 0.32)",
                "blob_three": "rgba(171, 74, 112, 0.24)",
            },
        },
    },
    {
        "id": "lagoon",
        "label": "Lagoon",
        "collection": "Classic",
        "description": "Seafoam, aqua, and misted teal for a cooler studio feel.",
        "swatches": ("#bfe8ea", "#eefcfb", "#96d3cf"),
        "modes": {
            "light": {
                "accent": "#1f8b97",
                "secondary": "#6fc8bb",
                "highlight": "#effcfc",
                "bg_start": "#d4f4f1",
                "bg_mid": "#f8fffe",
                "bg_end": "#e4faf7",
                "blob_one": "rgba(101, 209, 212, 0.48)",
                "blob_two": "rgba(217, 248, 241, 0.86)",
                "blob_three": "rgba(128, 198, 181, 0.42)",
            },
            "dark": {
                "accent": "#5fd0d3",
                "secondary": "#8be4c2",
                "highlight": "#10292d",
                "bg_start": "#071619",
                "bg_mid": "#0b2023",
                "bg_end": "#0f292b",
                "blob_one": "rgba(29, 146, 150, 0.38)",
                "blob_two": "rgba(24, 101, 103, 0.28)",
                "blob_three": "rgba(76, 167, 143, 0.22)",
            },
        },
    },
    {
        "id": "twilight",
        "label": "Twilight",
        "collection": "Classic",
        "description": "Lavender-blue glass with rose haze, kept bright rather than dark.",
        "swatches": ("#d9d7ff", "#f8f4ff", "#c8d4ff"),
        "modes": {
            "light": {
                "accent": "#6e71d8",
                "secondary": "#b28ad9",
                "highlight": "#f5f2ff",
                "bg_start": "#e5e2ff",
                "bg_mid": "#fcfaff",
                "bg_end": "#efe7ff",
                "blob_one": "rgba(146, 156, 255, 0.46)",
                "blob_two": "rgba(236, 228, 255, 0.9)",
                "blob_three": "rgba(225, 167, 221, 0.36)",
            },
            "dark": {
                "accent": "#a9acff",
                "secondary": "#dfabff",
                "highlight": "#1c1833",
                "bg_start": "#0d1022",
                "bg_mid": "#161630",
                "bg_end": "#22183a",
                "blob_one": "rgba(107, 109, 219, 0.34)",
                "blob_two": "rgba(91, 73, 158, 0.28)",
                "blob_three": "rgba(157, 92, 163, 0.2)",
            },
        },
    },
    {
        "id": "pride-rainbow",
        "label": "Rainbow Pride",
        "collection": "Pride",
        "description": "A polished rainbow gradient with cool glass layers instead of novelty saturation.",
        "swatches": ("#ff6b6b", "#ffd166", "#4ecdc4", "#5b7cfa"),
        "modes": {
            "light": {
                "accent": "#ff5f7f",
                "secondary": "#5b7cfa",
                "highlight": "#fff7f3",
                "bg_start": "#ffe1de",
                "bg_mid": "#fff8ef",
                "bg_end": "#e4f0ff",
                "blob_one": "rgba(255, 107, 107, 0.38)",
                "blob_two": "rgba(255, 209, 102, 0.34)",
                "blob_three": "rgba(78, 205, 196, 0.32)",
            },
            "dark": {
                "accent": "#ff87a2",
                "secondary": "#7fb5ff",
                "highlight": "#251422",
                "bg_start": "#170d1a",
                "bg_mid": "#201225",
                "bg_end": "#141d32",
                "blob_one": "rgba(216, 74, 123, 0.34)",
                "blob_two": "rgba(205, 156, 67, 0.22)",
                "blob_three": "rgba(56, 168, 162, 0.18)",
            },
        },
    },
    {
        "id": "pride-trans",
        "label": "Trans Pride",
        "collection": "Pride",
        "description": "Sky, blush, and white-light glass with a cooler afterglow in dark mode.",
        "swatches": ("#5bcffb", "#f5abb9", "#ffffff"),
        "modes": {
            "light": {
                "accent": "#57bde8",
                "secondary": "#f29fb4",
                "highlight": "#fbfdff",
                "bg_start": "#dff5ff",
                "bg_mid": "#fff9fb",
                "bg_end": "#ffe7ef",
                "blob_one": "rgba(91, 207, 251, 0.34)",
                "blob_two": "rgba(255, 255, 255, 0.64)",
                "blob_three": "rgba(245, 171, 185, 0.34)",
            },
            "dark": {
                "accent": "#84dcff",
                "secondary": "#ffbbcb",
                "highlight": "#16202b",
                "bg_start": "#091520",
                "bg_mid": "#121d2a",
                "bg_end": "#241723",
                "blob_one": "rgba(63, 177, 219, 0.28)",
                "blob_two": "rgba(255, 255, 255, 0.08)",
                "blob_three": "rgba(223, 135, 161, 0.22)",
            },
        },
    },
    {
        "id": "pride-lesbian",
        "label": "Lesbian Pride",
        "collection": "Pride",
        "description": "Clementine, rose, and plum tones with a peach-glow light mode and berry dark mode.",
        "swatches": ("#d52d00", "#ff9a56", "#d362a4", "#a30262"),
        "modes": {
            "light": {
                "accent": "#d75b3e",
                "secondary": "#c65aa2",
                "highlight": "#fff5f0",
                "bg_start": "#ffe3d7",
                "bg_mid": "#fff5ef",
                "bg_end": "#f9e2ef",
                "blob_one": "rgba(213, 45, 0, 0.3)",
                "blob_two": "rgba(255, 154, 86, 0.28)",
                "blob_three": "rgba(163, 2, 98, 0.24)",
            },
            "dark": {
                "accent": "#ff936d",
                "secondary": "#e07dbd",
                "highlight": "#271317",
                "bg_start": "#1b0d11",
                "bg_mid": "#271116",
                "bg_end": "#301228",
                "blob_one": "rgba(179, 55, 28, 0.28)",
                "blob_two": "rgba(191, 98, 73, 0.24)",
                "blob_three": "rgba(131, 35, 98, 0.2)",
            },
        },
    },
    {
        "id": "pride-pan",
        "label": "Pan Pride",
        "collection": "Pride",
        "description": "Magenta, gold, and cyan layered into bright glass with a lively but balanced finish.",
        "swatches": ("#ff218c", "#ffd800", "#21b1ff"),
        "modes": {
            "light": {
                "accent": "#f14499",
                "secondary": "#29b6ff",
                "highlight": "#fff8ee",
                "bg_start": "#ffe1f1",
                "bg_mid": "#fff9ee",
                "bg_end": "#e3f5ff",
                "blob_one": "rgba(255, 33, 140, 0.3)",
                "blob_two": "rgba(255, 216, 0, 0.24)",
                "blob_three": "rgba(33, 177, 255, 0.28)",
            },
            "dark": {
                "accent": "#ff6aae",
                "secondary": "#5ec9ff",
                "highlight": "#261527",
                "bg_start": "#180c19",
                "bg_mid": "#261421",
                "bg_end": "#0f2340",
                "blob_one": "rgba(199, 45, 124, 0.26)",
                "blob_two": "rgba(189, 150, 19, 0.18)",
                "blob_three": "rgba(24, 111, 170, 0.2)",
            },
        },
    },
    {
        "id": "pride-nonbinary",
        "label": "Nonbinary Pride",
        "collection": "Pride",
        "description": "Yellow, lilac, and charcoal tuned into a sharp but calm glass palette.",
        "swatches": ("#fff430", "#9c59d1", "#2d2d2d"),
        "modes": {
            "light": {
                "accent": "#9a63d4",
                "secondary": "#f0c94a",
                "highlight": "#fffdf1",
                "bg_start": "#fff8cb",
                "bg_mid": "#faf4ff",
                "bg_end": "#ece8f8",
                "blob_one": "rgba(255, 244, 48, 0.28)",
                "blob_two": "rgba(156, 89, 209, 0.22)",
                "blob_three": "rgba(45, 45, 45, 0.12)",
            },
            "dark": {
                "accent": "#c18fff",
                "secondary": "#ffe46d",
                "highlight": "#261f2e",
                "bg_start": "#151116",
                "bg_mid": "#21182a",
                "bg_end": "#131216",
                "blob_one": "rgba(207, 184, 42, 0.16)",
                "blob_two": "rgba(123, 75, 177, 0.22)",
                "blob_three": "rgba(0, 0, 0, 0.24)",
            },
        },
    },
)

_UI_THEME_BY_ID = {str(option["id"]): option for option in _UI_THEME_OPTIONS}


def _hex_to_rgb(value: str) -> str:
    candidate = value.strip().lstrip("#")
    if len(candidate) != 6:
        return "86, 116, 217"
    red = int(candidate[0:2], 16)
    green = int(candidate[2:4], 16)
    blue = int(candidate[4:6], 16)
    return f"{red}, {green}, {blue}"


def normalize_ui_theme(value: str | None) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in _UI_THEME_BY_ID:
        return candidate
    return DEFAULT_UI_THEME


def normalize_ui_mode(value: str | None) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in _MODE_LABELS:
        return candidate
    return DEFAULT_UI_MODE


def ui_mode_label(value: str | None) -> str:
    return _MODE_LABELS[normalize_ui_mode(value)]


def _base_tokens_for_mode(mode: str) -> dict[str, str]:
    return deepcopy(_LIGHT_FOUNDATION if mode == "light" else _DARK_FOUNDATION)


def ui_theme_tokens(value: str | None, mode: str | None = None) -> dict[str, str]:
    theme_id = normalize_ui_theme(value)
    normalized_mode = normalize_ui_mode(mode)
    option = _UI_THEME_BY_ID[theme_id]
    mode_tokens = deepcopy(option["modes"][normalized_mode])
    base_tokens = _base_tokens_for_mode(normalized_mode)
    merged = {**base_tokens, **mode_tokens}
    merged["accent_rgb"] = _hex_to_rgb(merged["accent"])
    return merged


def ui_theme_runtime_style(value: str | None, mode: str | None = None) -> str:
    tokens = ui_theme_tokens(value, mode)
    lines = [":root {"]
    for key, token_value in tokens.items():
        lines.append(f"    --lp-{key.replace('_', '-')}: {token_value};")
    lines.append("}")
    return "\n".join(lines)


def ui_theme_options() -> list[dict[str, object]]:
    options: list[dict[str, object]] = []
    for option in _UI_THEME_OPTIONS:
        item = deepcopy(option)
        item.pop("modes", None)
        item["is_pride"] = item.get("collection") == "Pride"
        options.append(item)
    return options


def ui_theme_catalog_for_client() -> list[dict[str, object]]:
    catalog: list[dict[str, object]] = []
    for option in _UI_THEME_OPTIONS:
        item = deepcopy(option)
        item["is_pride"] = item.get("collection") == "Pride"
        item["modes"] = {
            "light": ui_theme_tokens(str(option["id"]), "light"),
            "dark": ui_theme_tokens(str(option["id"]), "dark"),
        }
        catalog.append(item)
    return catalog


def ui_theme_definition(value: str | None) -> dict[str, object]:
    theme_id = normalize_ui_theme(value)
    option = deepcopy(_UI_THEME_BY_ID[theme_id])
    option.pop("modes", None)
    option["is_pride"] = option.get("collection") == "Pride"
    return option
