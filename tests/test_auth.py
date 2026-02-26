"""Tests for the mstransfer auth module."""

from __future__ import annotations

import pytest

from mstransfer.server.auth import (
    APIKeyAuthProvider,
    AuthContext,
    AuthProvider,
    NoAuthProvider,
)

# ---------------------------------------------------------------------------
# AuthContext
# ---------------------------------------------------------------------------


class TestAuthContext:
    def test_defaults(self):
        ctx = AuthContext()
        assert ctx.identity == "anonymous"
        assert ctx.scopes == set()
        assert ctx.extra == {}

    def test_custom_values(self):
        ctx = AuthContext(
            identity="user-1",
            scopes={"read", "write"},
            extra={"role": "admin"},
        )
        assert ctx.identity == "user-1"
        assert ctx.scopes == {"read", "write"}
        assert ctx.extra == {"role": "admin"}


# ---------------------------------------------------------------------------
# NoAuthProvider
# ---------------------------------------------------------------------------


class TestNoAuthProvider:
    @pytest.mark.asyncio
    async def test_returns_anonymous(self):
        provider = NoAuthProvider()
        # NoAuthProvider doesn't actually inspect the request,
        # so we can pass a mock or None cast.
        ctx = await provider.authenticate(None)  # type: ignore[arg-type]
        assert ctx.identity == "anonymous"

    def test_satisfies_protocol(self):
        assert isinstance(NoAuthProvider(), AuthProvider)


# ---------------------------------------------------------------------------
# APIKeyAuthProvider
# ---------------------------------------------------------------------------


class TestAPIKeyAuthProvider:
    @pytest.mark.asyncio
    async def test_valid_bearer_token(self, authed_client):
        resp = await authed_client.get(
            "/v1/transfer/nonexistent/status",
            headers={"Authorization": "Bearer test-secret"},
        )
        # 404 means auth passed, route logic ran
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_wrong_bearer_token(self, authed_client):
        resp = await authed_client.post(
            "/v1/upload",
            content=b"data",
            headers={
                "Authorization": "Bearer wrong-key",
                "X-Transfer-ID": "t1",
                "X-Original-Filename": "f.msz",
            },
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_missing_auth(self, authed_client):
        resp = await authed_client.post(
            "/v1/upload",
            content=b"data",
            headers={
                "X-Transfer-ID": "t1",
                "X-Original-Filename": "f.msz",
            },
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_query_param_fallback(self, authed_client):
        resp = await authed_client.get(
            "/v1/transfer/nonexistent/status?api_key=test-secret",
        )
        # 404 means auth passed
        assert resp.status_code == 404

    def test_satisfies_protocol(self):
        assert isinstance(APIKeyAuthProvider("key"), AuthProvider)


# ---------------------------------------------------------------------------
# Integration: health is always unauthenticated
# ---------------------------------------------------------------------------


class TestAuthIntegration:
    @pytest.mark.asyncio
    async def test_health_no_auth_required(self, authed_client):
        """Health endpoint should work without any credentials."""
        resp = await authed_client.get("/v1/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    @pytest.mark.asyncio
    async def test_upload_requires_auth(self, authed_client):
        """Upload without credentials should return 401."""
        resp = await authed_client.post(
            "/v1/upload",
            content=b"data",
            headers={
                "X-Transfer-ID": "t1",
                "X-Original-Filename": "f.msz",
            },
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_upload_with_valid_auth(self, authed_client, test_msz):
        """Upload with valid credentials should succeed."""
        resp = await authed_client.post(
            "/v1/upload",
            content=test_msz.read_bytes(),
            headers={
                "Authorization": "Bearer test-secret",
                "X-Transfer-ID": "auth-upload-test",
                "X-Original-Filename": "test.msz",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["state"] == "done"

    @pytest.mark.asyncio
    async def test_status_requires_auth(self, authed_client):
        """Status endpoint without credentials should return 401."""
        resp = await authed_client.get("/v1/transfer/some-id/status")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_status_with_valid_auth(self, authed_client, test_msz):
        """Status endpoint with valid credentials should work."""
        # First upload a file with auth
        await authed_client.post(
            "/v1/upload",
            content=test_msz.read_bytes(),
            headers={
                "Authorization": "Bearer test-secret",
                "X-Transfer-ID": "auth-status-test",
                "X-Original-Filename": "test.msz",
            },
        )
        # Then check status with auth
        resp = await authed_client.get(
            "/v1/transfer/auth-status-test/status",
            headers={"Authorization": "Bearer test-secret"},
        )
        assert resp.status_code == 200
        assert resp.json()["state"] == "done"

    @pytest.mark.asyncio
    async def test_no_auth_app_still_works(self, msz_client, test_msz):
        """Apps created without auth should still work (backward-compat)."""
        resp = await msz_client.post(
            "/v1/upload",
            content=test_msz.read_bytes(),
            headers={
                "X-Transfer-ID": "no-auth-test",
                "X-Original-Filename": "test.msz",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["state"] == "done"
