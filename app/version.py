from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


VERSION_ENV_KEYS = ("APP_VERSION", "LYNXPOSTER_VERSION")
GIT_SHA_ENV_KEYS = (
    "APP_GIT_SHA",
    "LYNXPOSTER_GIT_SHA",
    "GIT_COMMIT",
    "GITHUB_SHA",
    "SOURCE_VERSION",
    "RENDER_GIT_COMMIT",
    "HEROKU_SLUG_COMMIT",
    "VERCEL_GIT_COMMIT_SHA",
    "RAILWAY_GIT_COMMIT_SHA",
    "COMMIT_SHA",
)
UNKNOWN_BUILD = "unknown"


@dataclass(frozen=True)
class AppVersion:
    version: str
    git_sha: str | None
    git_sha_short: str | None
    git_source: str

    @property
    def label(self) -> str:
        if self.git_sha_short:
            return f"v{self.version} ({self.git_sha_short})"
        return f"v{self.version} ({UNKNOWN_BUILD})"


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _read_version_file() -> str:
    version_file = _project_root() / "VERSION"
    try:
        version = version_file.read_text(encoding="utf-8").strip()
    except OSError:
        return "0.1.0"
    return version or "0.1.0"


def _version_from_environment() -> str | None:
    for key in VERSION_ENV_KEYS:
        value = str(os.getenv(key) or "").strip()
        if value:
            return value
    return None


def _normalize_git_sha(value: str | None) -> str | None:
    sha = str(value or "").strip()
    if not sha or sha.lower() in {"unknown", "none", "null"}:
        return None
    return sha


def _git_sha_from_environment() -> tuple[str | None, str]:
    for key in GIT_SHA_ENV_KEYS:
        sha = _normalize_git_sha(os.getenv(key))
        if sha:
            return sha, key
    return None, UNKNOWN_BUILD


def _git_sha_from_command() -> tuple[str | None, str]:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=_project_root(),
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None, UNKNOWN_BUILD
    return _normalize_git_sha(result.stdout), "git"


@lru_cache(maxsize=1)
def get_app_version() -> AppVersion:
    version = _version_from_environment() or _read_version_file()
    git_sha, git_source = _git_sha_from_environment()
    if git_sha is None:
        git_sha, git_source = _git_sha_from_command()
    git_sha_short = git_sha[:8] if git_sha else None
    return AppVersion(version=version, git_sha=git_sha, git_sha_short=git_sha_short, git_source=git_source)


def app_version_payload() -> dict[str, str | None]:
    version = get_app_version()
    return {
        "version": version.version,
        "git_sha": version.git_sha,
        "git_sha_short": version.git_sha_short,
        "git_source": version.git_source,
        "label": version.label,
    }


def reset_app_version_cache() -> None:
    get_app_version.cache_clear()
