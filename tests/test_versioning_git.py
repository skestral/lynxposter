from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import app.version as version_module


def _git(root: Path, *arguments: str) -> str:
    environment = os.environ.copy()
    for key in (
        "GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL",
        "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR",
    ):
        environment.pop(key, None)
    result = subprocess.run(
        ["git", "-c", f"core.hooksPath={root / '.no-hooks'}", "-c", "commit.gpgsign=false", *arguments], cwd=root,
        check=True, capture_output=True, text=True, timeout=15, env=environment,
    )
    return result.stdout.strip()


def _commit(root: Path, filename: str, value: str, message: str = "Test commit") -> None:
    (root / filename).write_text(value, encoding="utf-8")
    _git(root, "add", filename)
    _git(root, "commit", "--quiet", "-m", message)


@pytest.fixture
def checkout(tmp_path):
    if not shutil.which("git"):
        pytest.skip("Git is needed to exercise checkout versioning.")
    root = tmp_path / "checkout"
    root.mkdir()
    _git(root, "init", "--quiet", "--initial-branch=main")
    _git(root, "config", "--local", "user.name", "Versioning Test")
    _git(root, "config", "--local", "user.email", "versioning-test@example.invalid")
    _commit(root, "VERSION", "0.1.72\n", "Set version baseline")
    return root


@pytest.fixture(autouse=True)
def clear_version_environment(monkeypatch):
    for key in (*version_module.VERSION_ENV_KEYS, *version_module.GIT_SHA_ENV_KEYS):
        monkeypatch.delenv(key, raising=False)
    version_module.reset_app_version_cache()
    yield
    version_module.reset_app_version_cache()


def test_two_commits_without_hooks_increment_effective_patch(checkout):
    assert version_module.version_for_checkout(checkout) == "0.1.72"
    _commit(checkout, "first.txt", "First change")
    assert version_module.version_for_checkout(checkout) == "0.1.73"
    _commit(checkout, "second.txt", "Second change")
    assert version_module.version_for_checkout(checkout) == "0.1.74"
    assert (checkout / "VERSION").read_text(encoding="utf-8") == "0.1.72\n"


def test_manual_version_edit_resets_baseline_then_counts_following_commits(checkout):
    _commit(checkout, "first.txt", "First change")
    (checkout / "VERSION").write_text("0.2.0\n", encoding="utf-8")
    assert version_module.version_for_checkout(checkout) == "0.2.0"
    _git(checkout, "add", "VERSION")
    assert version_module.version_for_checkout(checkout) == "0.2.0"
    _git(checkout, "commit", "--quiet", "-m", "Change release baseline")
    assert version_module.version_for_checkout(checkout) == "0.2.0"
    _commit(checkout, "second.txt", "Second change")
    assert version_module.version_for_checkout(checkout) == "0.2.1"


def test_first_parent_history_counts_merge_once_without_branch_commit_inflation(checkout):
    _git(checkout, "checkout", "--quiet", "-b", "feature")
    _commit(checkout, "feature-one.txt", "One")
    _commit(checkout, "feature-two.txt", "Two")
    _git(checkout, "checkout", "--quiet", "main")
    _commit(checkout, "main.txt", "Main")
    _git(checkout, "merge", "--no-ff", "--quiet", "feature", "-m", "Merge feature")
    assert version_module.version_for_checkout(checkout) == "0.1.74"


def test_merge_with_older_branch_version_cannot_lower_effective_main_version(checkout):
    _git(checkout, "checkout", "--quiet", "-b", "feature")
    _commit(checkout, "VERSION", "0.1.73\n", "Feature version checkpoint")
    _git(checkout, "checkout", "--quiet", "main")
    _commit(checkout, "main-one.txt", "One")
    _commit(checkout, "main-two.txt", "Two")
    assert version_module.version_for_checkout(checkout) == "0.1.74"
    _git(checkout, "merge", "--no-ff", "--quiet", "feature", "-m", "Merge older branch version")
    assert (checkout / "VERSION").read_text(encoding="utf-8") == "0.1.73\n"
    assert version_module.version_for_checkout(checkout) == "0.1.75"


def test_committed_patch_checkpoints_preserve_monotonicity_and_allow_higher_versions(checkout):
    _commit(checkout, "first.txt", "First")
    _commit(checkout, "VERSION", "0.1.71\n", "Older version checkpoint")
    assert version_module.version_for_checkout(checkout) == "0.1.74"
    _commit(checkout, "VERSION", "0.1.80\n", "Higher version checkpoint")
    assert version_module.version_for_checkout(checkout) == "0.1.80"
    _commit(checkout, "next.txt", "Next")
    assert version_module.version_for_checkout(checkout) == "0.1.81"


@pytest.mark.parametrize("raw", ["1.2", "1.2.3.4", "v1.2.3", "1.2.-1", "1.2.dev", "01.2.3"])
def test_checkout_version_requires_numeric_three_part_version(tmp_path, raw):
    (tmp_path / "VERSION").write_text(raw, encoding="utf-8")
    with pytest.raises(ValueError, match="numeric major.minor.patch"):
        version_module.version_for_checkout(tmp_path)


def test_unversioned_directory_uses_plain_version_and_missing_git_is_supported(tmp_path, monkeypatch):
    (tmp_path / "VERSION").write_text("3.2.1\n", encoding="utf-8")
    assert version_module.version_for_checkout(tmp_path) == "3.2.1"

    def missing_git(*args, **kwargs):
        raise FileNotFoundError("Git is not installed")

    monkeypatch.setattr(version_module.subprocess, "run", missing_git)
    assert version_module.version_for_checkout(tmp_path) == "3.2.1"


def test_runtime_prefers_environment_then_baked_version_over_checkout(checkout, monkeypatch):
    _commit(checkout, "first.txt", "First change")
    monkeypatch.setattr(version_module, "_project_root", lambda: checkout)
    (checkout / "BUILD_VERSION").write_text("0.1.80\n", encoding="utf-8")
    (checkout / "BUILD_SHA").write_text("baked-sha\n", encoding="utf-8")
    assert version_module.get_app_version().version == "0.1.80"
    assert version_module.get_app_version().git_sha == "baked-sha"
    monkeypatch.setenv("APP_VERSION", "9.8.7")
    version_module.reset_app_version_cache()
    assert version_module.get_app_version().version == "9.8.7"
    monkeypatch.delenv("APP_VERSION")
    (checkout / "BUILD_VERSION").unlink()
    version_module.reset_app_version_cache()
    assert version_module.get_app_version().version == "0.1.73"


def test_baked_build_version_and_sha_work_without_git(tmp_path, monkeypatch):
    (tmp_path / "VERSION").write_text("0.1.72\n", encoding="utf-8")
    (tmp_path / "BUILD_VERSION").write_text("0.1.90\n", encoding="utf-8")
    (tmp_path / "BUILD_SHA").write_text("0123456789abcdef\n", encoding="utf-8")
    monkeypatch.setattr(version_module, "_project_root", lambda: tmp_path)

    def no_git(*args, **kwargs):
        raise AssertionError("Baked Docker metadata must not need Git")

    monkeypatch.setattr(version_module.subprocess, "run", no_git)
    version = version_module.get_app_version()
    assert version.version == "0.1.90"
    assert version.git_sha == "0123456789abcdef"
    assert version.git_source == "BUILD_SHA"


def _copy_cli(checkout: Path) -> Path:
    app_dir = checkout / "app"
    app_dir.mkdir(exist_ok=True)
    script = app_dir / "version.py"
    shutil.copyfile(Path(version_module.__file__), script)
    return script


def test_cli_ignores_build_and_environment_overrides_without_writing_files(checkout):
    _commit(checkout, "first.txt", "First change")
    _commit(checkout, "second.txt", "Second change")
    script = _copy_cli(checkout)
    (checkout / "BUILD_VERSION").write_text("8.8.8\n", encoding="utf-8")
    environment = {**os.environ, "APP_VERSION": "9.9.9", "LYNXPOSTER_VERSION": "7.7.7"}
    before = _git(checkout, "status", "--porcelain")
    for argument, expected in (("--print", "0.1.74"), ("--next", "0.1.75")):
        result = subprocess.run([sys.executable, str(script), argument], check=True, capture_output=True, text=True,
                                timeout=15, cwd=checkout, env=environment)
        assert result.stdout.strip() == expected
    assert _git(checkout, "status", "--porcelain") == before
    assert (checkout / "VERSION").read_text(encoding="utf-8") == "0.1.72\n"


def test_shallow_checkout_requires_full_history_for_cli_and_runtime_uses_plain_version(checkout, tmp_path, monkeypatch):
    _commit(checkout, "first.txt", "First change")
    _commit(checkout, "second.txt", "Second change")
    shallow = tmp_path / "shallow"
    _git(tmp_path, "clone", "--quiet", "--depth=1", checkout.as_uri(), str(shallow))
    assert _git(shallow, "rev-parse", "--is-shallow-repository") == "true"
    with pytest.raises(ValueError, match="shallow Git checkout"):
        version_module.version_for_checkout(shallow)
    monkeypatch.setattr(version_module, "_project_root", lambda: shallow)
    assert version_module.get_app_version().version == "0.1.72"
    script = _copy_cli(shallow)
    result = subprocess.run([sys.executable, str(script), "--print"], capture_output=True, text=True,
                            timeout=15, cwd=shallow)
    assert result.returncode == 2
    assert "Fetch full history before building" in result.stderr
    assert result.stdout == ""
    (shallow / "VERSION").write_text("0.2.0\n", encoding="utf-8")
    assert version_module.version_for_checkout(shallow) == "0.2.0"
