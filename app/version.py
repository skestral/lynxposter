from __future__ import annotations

import os
import re
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
GIT_SHA_BUILD_FILES = ("BUILD_SHA", ".build-sha", "REVISION")
UNKNOWN_BUILD = "unknown"
VERSION_PATTERN = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


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


def _read_version_file(root: Path | None = None) -> str:
    version_file = (root or _project_root()) / "VERSION"
    try:
        version = version_file.read_text(encoding="utf-8").strip()
    except OSError:
        return "0.1.0"
    return version or "0.1.0"


def _version_components(version: str) -> tuple[int, int, int]:
    match = VERSION_PATTERN.fullmatch(version)
    if not match:
        raise ValueError(f"VERSION must use numeric major.minor.patch format, got: {version}")
    return tuple(int(component) for component in match.groups())


def _git_output(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments], cwd=root, check=True, capture_output=True, text=True, timeout=5,
    )
    return result.stdout.strip()


def version_for_checkout(root: Path) -> str:
    """Resolve a checkout version without environment or baked-build overrides.

    A manually edited VERSION is authoritative. Committed checkpoints use the
    largest patch plus first-parent distance within the current major/minor, so
    merging an older VERSION cannot lower the effective version. This requires
    complete history; shallow clones raise rather than inventing a commit count.
    Unversioned directories and environments without Git use the plain file.
    """
    version = _read_version_file(root)
    major, minor, _patch = _version_components(version)
    try:
        committed_version = _git_output(root, "show", "HEAD:VERSION")
    except (OSError, subprocess.SubprocessError):
        return version
    if version != committed_version:
        return version
    try:
        if _git_output(root, "rev-parse", "--is-shallow-repository") == "true":
            raise ValueError("Cannot calculate a commit-based version from a shallow Git checkout. Fetch full history before building.")
        first_parent_commits = _git_output(root, "rev-list", "--first-parent", "HEAD").splitlines()
        distances = {commit: distance for distance, commit in enumerate(first_parent_commits)}
        history = _git_output(
            root, "log", "--first-parent", "--diff-merges=first-parent", "--format=VERSION_COMMIT:%H",
            "--no-ext-diff", "--no-textconv", "-p", "--", "VERSION",
        )
        candidates: list[int] = []
        commit = None
        for line in history.splitlines():
            if line.startswith("VERSION_COMMIT:"):
                commit = line.removeprefix("VERSION_COMMIT:")
            elif line.startswith("+") and commit in distances:
                match = VERSION_PATTERN.fullmatch(line[1:].strip())
                if match:
                    checkpoint_major, checkpoint_minor, checkpoint_patch = (int(value) for value in match.groups())
                    if (checkpoint_major, checkpoint_minor) == (major, minor):
                        candidates.append(checkpoint_patch + distances[commit])
        if not candidates:
            raise ValueError("Cannot locate the VERSION baseline in Git history.")
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError("Cannot calculate a version from this Git checkout.") from exc
    return f"{major}.{minor}.{max(candidates)}"


def _version_from_build_file() -> str | None:
    try:
        version = (_project_root() / "BUILD_VERSION").read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return version or None


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


def _git_sha_from_build_file() -> tuple[str | None, str]:
    project_root = _project_root()
    for filename in GIT_SHA_BUILD_FILES:
        build_file = project_root / filename
        try:
            sha = _normalize_git_sha(build_file.read_text(encoding="utf-8"))
        except OSError:
            continue
        if sha:
            return sha, filename
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
    version = _version_from_environment() or _version_from_build_file()
    if version is None:
        try:
            version = version_for_checkout(_project_root())
        except ValueError:
            version = _read_version_file()
    git_sha, git_source = _git_sha_from_environment()
    if git_sha is None:
        git_sha, git_source = _git_sha_from_build_file()
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


def _main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Calculate LynxPoster versions from this checkout.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--print", action="store_true", help="Print this checkout's effective version.")
    mode.add_argument("--next", action="store_true", help="Print the next patch version without writing files.")
    arguments = parser.parse_args()
    try:
        version = version_for_checkout(_project_root())
        if arguments.next:
            major, minor, patch = _version_components(version)
            version = f"{major}.{minor}.{patch + 1}"
    except ValueError as exc:
        parser.error(str(exc))
    print(version)


if __name__ == "__main__":
    _main()
