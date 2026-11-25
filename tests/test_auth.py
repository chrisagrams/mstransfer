"""Tests for the mstransfer auth module."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from mstransfer.server.app import create_app
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

    @pytest.mark.asyncio
    async def test_non_ascii_bearer_token_rejected(self, authed_client):
        resp = await authed_client.get(
            "/v1/transfer/nonexistent/status",
            headers={"Authorization": "Bearer café".encode()},
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_non_ascii_query_param_rejected(self, authed_client):
        resp = await authed_client.get(
            "/v1/transfer/nonexistent/status", params={"api_key": "café"}
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("headers", "params", "expected"),
        [
            ({"Authorization": "Bearer café".encode()}, None, 404),
            (None, {"api_key": "café"}, 404),
            ({"Authorization": "Bearer wrong"}, None, 401),
            (None, {"api_key": "wrong"}, 401),
        ],
    )
    async def test_non_ascii_configured_key(
        self, tmp_output, headers, params, expected
    ):
        app = create_app(output_dir=str(tmp_output), auth=APIKeyAuthProvider("café"))
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            resp = await c.get(
                "/v1/transfer/nonexistent/status", headers=headers, params=params
            )
        # 404 means auth passed
        assert resp.status_code == expected

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


# ---------------------------------------------------------------------------
# Per-direction auth (upload vs. download)
# ---------------------------------------------------------------------------

UPLOAD_ROUTES = [
    ("POST", "/v1/upload"),
    ("GET", "/v1/transfer/{transfer_id}/status"),
]
DOWNLOAD_ROUTES = [
    ("GET", "/v1/files"),
    ("GET", "/v1/files/{filename}"),
]


async def _unauthenticated_codes(app) -> dict[str, int]:
    """Hit every protected-capable route without credentials."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        upload = await c.post(
            "/v1/upload",
            content=b"data",
            headers={"X-Transfer-ID": "t1", "X-Original-Filename": "f.msz"},
        )
        return {
            "upload": upload.status_code,
            "status": (await c.get("/v1/transfer/nope/status")).status_code,
            "list": (await c.get("/v1/files")).status_code,
            "download": (await c.get("/v1/files/nope.msz")).status_code,
        }


def _documents_401(app, method: str, path: str) -> bool:
    op = app.openapi()["paths"][path][method.lower()]
    return "401" in op["responses"]


class TestPerDirectionAuth:
    @pytest.mark.asyncio
    async def test_auth_protects_everything(self, tmp_output):
        app = create_app(output_dir=str(tmp_output), auth=APIKeyAuthProvider("k"))
        codes = await _unauthenticated_codes(app)
        assert codes == {"upload": 401, "status": 401, "list": 401, "download": 401}

    @pytest.mark.asyncio
    async def test_upload_only(self, tmp_output):
        app = create_app(
            output_dir=str(tmp_output), upload_auth=APIKeyAuthProvider("k")
        )
        codes = await _unauthenticated_codes(app)
        assert codes == {"upload": 401, "status": 401, "list": 200, "download": 404}

    @pytest.mark.asyncio
    async def test_download_only(self, tmp_output):
        app = create_app(
            output_dir=str(tmp_output), download_auth=APIKeyAuthProvider("k")
        )
        codes = await _unauthenticated_codes(app)
        # Upload passes auth and fails on the bogus body instead.
        assert codes["upload"] != 401
        assert codes["status"] == 404
        assert codes["list"] == 401
        assert codes["download"] == 401

    @pytest.mark.asyncio
    async def test_separate_keys(self, tmp_output):
        app = create_app(
            output_dir=str(tmp_output),
            upload_auth=APIKeyAuthProvider("up"),
            download_auth=APIKeyAuthProvider("down"),
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            up = {"Authorization": "Bearer up"}
            down = {"Authorization": "Bearer down"}
            assert (await c.get("/v1/files", headers=up)).status_code == 401
            assert (await c.get("/v1/files", headers=down)).status_code == 200
            status = "/v1/transfer/nope/status"
            assert (await c.get(status, headers=down)).status_code == 401
            assert (await c.get(status, headers=up)).status_code == 404

    @pytest.mark.asyncio
    async def test_health_open_with_both(self, tmp_output):
        app = create_app(
            output_dir=str(tmp_output),
            upload_auth=APIKeyAuthProvider("up"),
            download_auth=APIKeyAuthProvider("down"),
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            assert (await c.get("/v1/health")).status_code == 200

    @pytest.mark.parametrize("kwarg", ["upload_auth", "download_auth"])
    def test_auth_combined_with_per_direction_raises(self, tmp_output, kwarg):
        kwargs: dict[str, Any] = {kwarg: APIKeyAuthProvider("k2")}
        with pytest.raises(TypeError, match="can't be combined"):
            create_app(
                output_dir=str(tmp_output), auth=APIKeyAuthProvider("k"), **kwargs
            )

    @pytest.mark.parametrize("kwarg", ["auth", "upload_auth", "download_auth"])
    def test_bad_type_raises(self, tmp_output, kwarg):
        kwargs: dict[str, Any] = {kwarg: 42}
        with pytest.raises(TypeError, match="must be an AuthProvider"):
            create_app(output_dir=str(tmp_output), **kwargs)

    def test_openapi_401_follows_dependency(self, tmp_output):
        app = create_app(
            output_dir=str(tmp_output), upload_auth=APIKeyAuthProvider("k")
        )
        for method, path in UPLOAD_ROUTES:
            assert _documents_401(app, method, path), path
        for method, path in DOWNLOAD_ROUTES:
            assert not _documents_401(app, method, path), path
        assert not _documents_401(app, "GET", "/v1/health")

    def test_openapi_no_401_without_auth(self, tmp_output):
        app = create_app(output_dir=str(tmp_output))
        for method, path in UPLOAD_ROUTES + DOWNLOAD_ROUTES:
            assert not _documents_401(app, method, path), path
