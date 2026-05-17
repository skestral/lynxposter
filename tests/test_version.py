from __future__ import annotations

from app.main import version
from app.version import app_version_payload, reset_app_version_cache


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
