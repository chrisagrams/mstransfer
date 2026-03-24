"""Tests for the include_router integration pattern."""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx
import pytest
from fastapi import FastAPI

from mstransfer.server.auth import APIKeyAuthProvider, make_auth_dependency
from mstransfer.server.fileprovider import DirectoryFileProvider
from mstransfer.server.routes import make_router
from mstransfer.server.state import AppState

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture()
def embedded_state(tmp_output: Path) -> AppState:
    """AppState instance for embedded-router tests."""
    return AppState(
        output_dir=tmp_output,
        store_as="msz",
        files=DirectoryFileProvider(tmp_output),
    )


@pytest.fixture()
def embedded_app(embedded_state: AppState) -> FastAPI:
    """A bare FastAPI app with the transfer router included (not mounted)."""
    app = FastAPI()
    router = make_router(state=embedded_state)
    app.include_router(router, prefix="/transfer/v1")
    return app


@pytest.fixture()
def embedded_client(embedded_app: FastAPI) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=embedded_app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


# ---------------------------------------------------------------------------
# Basic endpoint tests via include_router
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health(embedded_client: httpx.AsyncClient) -> None:
    resp = await embedded_client.get("/transfer/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["store_as"] == "msz"


@pytest.mark.asyncio
async def test_upload_and_status(
    embedded_client: httpx.AsyncClient, test_msz: Path, tmp_output: Path
) -> None:
    payload = test_msz.read_bytes()
    resp = await embedded_client.post(
        "/transfer/v1/upload",
        content=payload,
        headers={
            "X-Transfer-ID": "embedded-upload",
            "X-Original-Filename": "test.msz",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "done"
    assert data["bytes_received"] == len(payload)

    # Verify file was written
    assert (tmp_output / "test.msz").exists()

    # Verify status endpoint works
    status = await embedded_client.get("/transfer/v1/transfer/embedded-upload/status")
    assert status.status_code == 200
    assert status.json()["state"] == "done"


@pytest.mark.asyncio
async def test_list_and_download_files(
    embedded_client: httpx.AsyncClient, test_msz: Path
) -> None:
    payload = test_msz.read_bytes()
    await embedded_client.post(
        "/transfer/v1/upload",
        content=payload,
        headers={
            "X-Transfer-ID": "embedded-files",
            "X-Original-Filename": "test.msz",
        },
    )

    # List files
    resp = await embedded_client.get("/transfer/v1/files")
    assert resp.status_code == 200
    files = resp.json()["files"]
    assert len(files) == 1
    assert files[0]["name"] == "test.msz"

    # Download file
    resp = await embedded_client.get("/transfer/v1/files/test.msz")
    assert resp.status_code == 200
    assert resp.content == payload


# ---------------------------------------------------------------------------
# Auth with include_router
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auth_with_include_router(
    embedded_state: AppState, test_msz: Path
) -> None:
    """Auth dependency works when router is included with explicit state."""
    app = FastAPI()
    auth_dep = make_auth_dependency(APIKeyAuthProvider("secret"))
    router = make_router(auth_dep=auth_dep, state=embedded_state)
    app.include_router(router, prefix="/transfer/v1")

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Unauthenticated request should fail
        resp = await client.get("/transfer/v1/files")
        assert resp.status_code == 401

        # Authenticated request should succeed
        resp = await client.get(
            "/transfer/v1/files",
            headers={"Authorization": "Bearer secret"},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Backwards compatibility: make_router() without state still works
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_router_without_state_backwards_compat(
    msz_client: httpx.AsyncClient,
) -> None:
    """make_router() without state argument still works via request.app.state."""
    resp = await msz_client.get("/v1/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Routes appear in parent OpenAPI schema
# ---------------------------------------------------------------------------


def test_routes_in_parent_openapi(embedded_app: FastAPI) -> None:
    """When included via include_router, routes appear in the parent schema."""
    schema = embedded_app.openapi()
    paths = list(schema["paths"].keys())
    assert "/transfer/v1/health" in paths
    assert "/transfer/v1/upload" in paths
    assert "/transfer/v1/files" in paths
