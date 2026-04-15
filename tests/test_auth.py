from __future__ import annotations

import asyncio
import base64
import json

import httpx
from authlib.integrations.base_client.errors import OAuthError
from fastapi import HTTPException
from starlette.requests import Request

from app.main import app, auth_callback, auth_error_page, auth_login
from app.services.auth import (
    _claims_with_userinfo,
    describe_auth_failure,
    describe_group_mapping_failure,
    describe_oidc_connectivity_failure,
    store_auth_debug_snapshot,
)


def _jwt(payload: dict[str, object]) -> str:
    header = {"alg": "none", "typ": "JWT"}

    def _encode(data: dict[str, object]) -> str:
        raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return f"{_encode(header)}.{_encode(payload)}.signature"


def test_describe_auth_failure_explains_groups_scope_issue():
    message = describe_auth_failure(
        "invalid_scope",
        "The OAuth 2.0 Client is not allowed to request scope 'groups'.",
    )

    assert "openid profile email" in message
    assert "groups" in message


def test_describe_oidc_connectivity_failure_mentions_dev_container_reachability(monkeypatch):
    class _Settings:
        auth_oidc_issuer_url = "https://auth.den.camp"

    monkeypatch.setattr("app.services.auth.get_settings", lambda: _Settings())

    message = describe_oidc_connectivity_failure(httpx.ConnectTimeout("timed out"))

    assert ".well-known/openid-configuration" in message
    assert "dev container" in message
    assert "timing out" in message


def test_auth_callback_redirects_to_error_page_on_oauth_failure(monkeypatch):
    async def _raise_oauth_error(request):
        raise OAuthError(
            error="invalid_scope",
            description="The OAuth 2.0 Client is not allowed to request scope 'groups'.",
        )

    monkeypatch.setattr("app.main.auth_enabled", lambda: True)
    monkeypatch.setattr("app.main.complete_oidc_login", _raise_oauth_error)
    request = Request({"type": "http", "headers": [], "session": {"post_login_redirect": "/"}})

    response = asyncio.run(auth_callback(request))

    assert response.status_code == 303
    assert response.headers["location"].startswith("/auth/error?")
    assert "openid+profile+email" in response.headers["location"]


def test_auth_login_redirects_to_error_page_on_connectivity_failure(monkeypatch):
    async def _raise_connectivity_error(request):
        raise HTTPException(
            status_code=503,
            detail="OIDC login failed because LynxPoster could not reach the provider discovery endpoint before timing out.",
        )

    monkeypatch.setattr("app.main.auth_enabled", lambda: True)
    monkeypatch.setattr("app.main.begin_oidc_login", _raise_connectivity_error)
    monkeypatch.setattr("app.main.get_request_principal", lambda request: type("P", (), {"is_authenticated": False})())
    request = Request({"type": "http", "headers": [], "query_string": b"next=%2F", "session": {}})

    response = asyncio.run(auth_login(request))

    assert response.status_code == 303
    assert response.headers["location"].startswith("/auth/error?")
    assert "timing+out" in response.headers["location"]


def test_describe_group_mapping_failure_mentions_missing_groups(monkeypatch):
    class _Settings:
        auth_oidc_admin_groups = "admins"
        auth_oidc_user_groups = "users"

    monkeypatch.setattr("app.services.auth.get_settings", lambda: _Settings())

    message = describe_group_mapping_failure([])

    assert "No groups were received" in message
    assert "admins, users" in message
    assert "Authelia" in message


def test_describe_group_mapping_failure_mentions_received_groups(monkeypatch):
    class _Settings:
        auth_oidc_admin_groups = "admins"
        auth_oidc_user_groups = "users"

    monkeypatch.setattr("app.services.auth.get_settings", lambda: _Settings())

    message = describe_group_mapping_failure(["staff", "editors"])

    assert "Received groups: editors, staff" in message
    assert "Configured admin/user groups: admins, users" in message


def test_auth_error_page_includes_oidc_debug_snapshot(monkeypatch):
    class _Settings:
        auth_oidc_enabled = True
        auth_oidc_issuer_url = "https://auth.example.com"
        auth_oidc_client_id = "lynxposter"
        auth_oidc_scope = "openid profile email"
        auth_oidc_groups_claim = "groups"
        auth_oidc_username_claim = "preferred_username"
        auth_oidc_admin_groups = "admins"
        auth_oidc_user_groups = "users"

    monkeypatch.setattr("app.services.auth.get_settings", lambda: _Settings())
    app.state.last_auth_debug = None
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/auth/error",
            "query_string": b"message=test",
            "headers": [],
            "session": {},
            "app": app,
        }
    )

    store_auth_debug_snapshot(
        request,
        claims={"sub": "abc123", "preferred_username": "lynx", "groups": ["staff"]},
        groups=["staff"],
        token={
            "id_token": _jwt({"sub": "abc123", "preferred_username": "lynx", "groups": ["staff"]}),
            "access_token": _jwt({"sub": "abc123", "scope": "openid profile email groups"}),
            "token_type": "Bearer",
        },
    )
    response = auth_error_page(request)

    assert "lynxposter_config" in response.context["auth_debug_json"]
    assert "provider_claims" in response.context["auth_debug_json"]
    assert "provider_tokens" in response.context["auth_debug_json"]
    assert "\"preferred_username\": \"lynx\"" in response.context["auth_debug_json"]
    assert "\"resolved_groups\": [" in response.context["auth_debug_json"]
    assert "\"admin_groups\": [" in response.context["auth_debug_json"]
    assert "\"id_token\": {" in response.context["auth_debug_json"]
    assert "\"decoded_payload\": {" in response.context["auth_debug_json"]
    app.state.last_auth_debug = None


def test_claims_with_userinfo_merges_groups_from_userinfo():
    class _Client:
        async def userinfo(self, **kwargs):
            return {
                "sub": "abc123",
                "preferred_username": "lynx",
                "groups": ["admins"],
            }

    token = {
        "access_token": _jwt({"sub": "abc123"}),
        "userinfo": {"sub": "abc123", "preferred_username": "lynx"},
    }

    claims = asyncio.run(_claims_with_userinfo(_Client(), token))

    assert claims["groups"] == ["admins"]
    assert token["userinfo"]["groups"] == ["admins"]
    assert token["_lynxposter_userinfo_fetch"]["status"] == "fetched"


def test_claims_with_userinfo_keeps_existing_claims_when_fetch_fails():
    class _Client:
        async def userinfo(self, **kwargs):
            raise RuntimeError("userinfo unavailable")

    token = {
        "access_token": _jwt({"sub": "abc123"}),
        "userinfo": {"sub": "abc123", "preferred_username": "lynx"},
    }

    claims = asyncio.run(_claims_with_userinfo(_Client(), token))

    assert claims["preferred_username"] == "lynx"
    assert "groups" not in claims
    assert token["_lynxposter_userinfo_fetch"]["status"] == "error"
