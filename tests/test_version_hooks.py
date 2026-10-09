from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys
import venv

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _environment(**overrides):
    environment = dict(os.environ)
    for key in list(environment):
        if key.startswith("GIT_") or key == "LYNXPOSTER_SKIP_VERSION_BUMP":
            environment.pop(key)
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT="0")
    environment.update(overrides)
    return environment


def _git(root, *arguments, check=True, **environment):
    result = subprocess.run(
        ["git", *arguments], cwd=root, env=_environment(**environment),
        capture_output=True, text=True, timeout=30,
    )
    if check:
        assert result.returncode == 0, result.stdout + result.stderr
    return result


def _version(root):
    return (root / "VERSION").read_text(encoding="utf-8").strip()


def _effective_version(root):
    result = subprocess.run(
        [sys.executable, str(root / "app" / "version.py"), "--print"],
        cwd=root, env=_environment(), capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout.strip()


@pytest.fixture()
def hooks_repo(tmp_path):
    if shutil.which("git") is None:
        pytest.skip("Git is needed to exercise actual commit hooks.")
    root = tmp_path / "hook-repository"
    root.mkdir()
    (root / "app").mkdir()
    (root / ".githooks").mkdir()
    shutil.copy2(PROJECT_ROOT / "app" / "version.py", root / "app" / "version.py")
    for name in ("pre-commit",):
        hook = root / ".githooks" / name
        shutil.copy2(PROJECT_ROOT / ".githooks" / name, hook)
        hook.chmod(0o755)
    # Use the same local-venv interpreter discovery as real hooks, without pip
    # or application dependencies. Every Git mutation stays in this temp repo.
    venv.EnvBuilder(with_pip=False).create(root / ".venv")
    (root / ".gitignore").write_text(".venv/\n", encoding="utf-8")
    (root / "VERSION").write_text("0.1.72\n", encoding="utf-8")
    (root / "tracked.txt").write_text("Baseline\n", encoding="utf-8")
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Hook Test")
    _git(root, "config", "user.email", "hook-test@example.invalid")
    _git(root, "config", "commit.gpgsign", "false")
    _git(root, "config", "core.hooksPath", ".githooks")
    _git(root, "add", ".gitignore", "VERSION", "tracked.txt", "app/version.py", ".githooks")
    _git(root, "commit", "-m", "Baseline", LYNXPOSTER_SKIP_VERSION_BUMP="1")
    assert _version(root) == _effective_version(root) == "0.1.72"
    return root


def _stage_change(root, text="Changed\n", filename="tracked.txt"):
    (root / filename).write_text(text, encoding="utf-8")
    _git(root, "add", filename)


def test_normal_commits_bump_once_each_from_72_to_73_to_74(hooks_repo):
    root = hooks_repo
    for patch in (73, 74):
        _stage_change(root, text=f"Change {patch}\n")
        _git(root, "commit", "-m", f"Change {patch}")
        expected = f"0.1.{patch}"
        assert _version(root) == _effective_version(root) == expected
        assert _git(root, "show", "HEAD:VERSION").stdout.strip() == expected
        assert _git(root, "status", "--porcelain").stdout == ""


def test_deletion_only_commit_includes_version_bump(hooks_repo):
    root = hooks_repo
    (root / "tracked.txt").unlink()
    _git(root, "add", "-u")
    _git(root, "commit", "-m", "Delete the only staged file")
    assert _version(root) == _effective_version(root) == "0.1.73"
    assert set(_git(root, "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD").stdout.splitlines()) == {"VERSION", "tracked.txt"}


def test_allow_empty_commit_still_records_next_version(hooks_repo):
    root = hooks_repo
    _git(root, "commit", "--allow-empty", "-m", "Empty commit requested")
    assert _version(root) == _effective_version(root) == "0.1.73"
    assert _git(root, "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD").stdout.strip() == "VERSION"


def test_manually_staged_release_version_is_preserved(hooks_repo):
    root = hooks_repo
    (root / "VERSION").write_text("0.2.0\n", encoding="utf-8")
    _git(root, "add", "VERSION")
    _stage_change(root)
    _git(root, "commit", "-m", "Manual release")
    assert _version(root) == _effective_version(root) == "0.2.0"


def test_retry_after_later_hook_failure_preserves_already_staged_bump(hooks_repo):
    root = hooks_repo
    rejection = root / ".githooks" / "commit-msg"
    rejection.write_text("#!/usr/bin/env sh\necho 'Intentional test rejection' >&2\nexit 1\n", encoding="utf-8", newline="\n")
    rejection.chmod(0o755)
    previous_head = _git(root, "rev-parse", "HEAD").stdout.strip()
    _stage_change(root)
    rejected = _git(root, "commit", "-m", "Rejected after pre-commit", check=False)
    assert rejected.returncode != 0
    assert "Intentional test rejection" in rejected.stderr
    assert _git(root, "rev-parse", "HEAD").stdout.strip() == previous_head
    assert _version(root) == "0.1.73"
    assert _git(root, "show", ":VERSION").stdout.strip() == "0.1.73"
    rejection.unlink()
    _git(root, "commit", "-m", "Retry the same commit")
    assert _version(root) == _effective_version(root) == "0.1.73"


def test_unstaged_version_edit_rejects_commit_without_overwriting_it(hooks_repo):
    root = hooks_repo
    previous_head = _git(root, "rev-parse", "HEAD").stdout.strip()
    (root / "VERSION").write_text("0.9.0\n", encoding="utf-8")
    _stage_change(root)
    rejected = _git(root, "commit", "-m", "Must not discard unstaged version", check=False)
    assert rejected.returncode != 0
    assert "unstaged changes" in rejected.stderr
    assert _git(root, "rev-parse", "HEAD").stdout.strip() == previous_head
    assert _version(root) == "0.9.0"
    assert _git(root, "show", ":VERSION").stdout.strip() == "0.1.72"


def test_skip_keeps_raw_version_but_effective_version_and_next_commit_advance(hooks_repo):
    root = hooks_repo
    _stage_change(root, text="Skipped raw bump\n")
    _git(root, "commit", "-m", "Skip raw bump", LYNXPOSTER_SKIP_VERSION_BUMP="1")
    assert _version(root) == "0.1.72"
    assert _effective_version(root) == "0.1.73"
    _stage_change(root, text="Normal bump after skipped commit\n")
    _git(root, "commit", "-m", "Resume automatic bump")
    assert _version(root) == _effective_version(root) == "0.1.74"
