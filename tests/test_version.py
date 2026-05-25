from __future__ import annotations

from app.main import version
from app.version import GIT_SHA_ENV_KEYS, app_version_payload, reset_app_version_cache
import app.version as version_module


def test_app_version_payload_uses_environment_build_identity(monkeypatch):
    monkeypatch.setenv("APP_VERSION", "9.8.7")
    monkeypatch.setenv("APP_GIT_SHA", "abcdef1234567890")
    reset_app_version_cache()

    try:
        payload = app_version_payload()
    finally:
        reset_app_version_cache()

    assert payload["version"] == "9.8.7"
    assert payload["git_sha"] == "abcdef1234567890"
    assert payload["git_sha_short"] == "abcdef12"
    assert payload["git_source"] == "APP_GIT_SHA"
    assert payload["label"] == "v9.8.7 (abcdef12)"


def test_version_endpoint_returns_build_identity(monkeypatch):
    monkeypatch.setenv("APP_VERSION", "1.2.3")
    monkeypatch.setenv("APP_GIT_SHA", "123456789abcdef")
    reset_app_version_cache()

    try:
        payload = version()
    finally:
        reset_app_version_cache()

    assert payload["version"] == "1.2.3"
    assert payload["git_sha_short"] == "12345678"


def test_app_version_payload_uses_baked_build_sha(monkeypatch, tmp_path):
    for key in GIT_SHA_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(version_module, "_project_root", lambda: tmp_path)
    (tmp_path / "VERSION").write_text("2.0.0\n", encoding="utf-8")
    (tmp_path / "BUILD_SHA").write_text("fedcba9876543210\n", encoding="utf-8")
    reset_app_version_cache()

    try:
        payload = app_version_payload()
    finally:
        reset_app_version_cache()

    assert payload["version"] == "2.0.0"
    assert payload["git_sha"] == "fedcba9876543210"
    assert payload["git_sha_short"] == "fedcba98"
    assert payload["git_source"] == "BUILD_SHA"
