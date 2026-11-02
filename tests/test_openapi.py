"""The OpenAPI schema documents auth, headers and error responses (#83)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from fastapi import FastAPI

from mstransfer.server.app import create_app
from mstransfer.server.auth import APIKeyAuthProvider, make_auth_dependency
from mstransfer.server.routes import make_router

if TYPE_CHECKING:
    from pathlib import Path

PROTECTED = [
    ("get", "/v1/transfer/{transfer_id}/status"),
    ("post", "/v1/upload"),
    ("get", "/v1/files"),
    ("get", "/v1/files/{filename}"),
]


def _op(schema: dict[str, Any], method: str, path: str) -> dict[str, Any]:
    op: dict[str, Any] = schema["paths"][path][method]
    return op


@pytest.fixture()
def authed_schema(tmp_output: Path) -> dict[str, Any]:
    return create_app(
        output_dir=str(tmp_output), auth=APIKeyAuthProvider("k")
    ).openapi()


def test_api_key_schemes_declared(authed_schema: dict[str, Any]) -> None:
    assert authed_schema["components"]["securitySchemes"] == {
        "HTTPBearer": {
            "type": "http",
            "scheme": "bearer",
            "description": "The API key as a bearer token.",
        },
        "APIKeyQuery": {
            "type": "apiKey",
            "in": "query",
            "name": "api_key",
            "description": "The API key as a query parameter.",
        },
    }


@pytest.mark.parametrize(("method", "path"), PROTECTED)
def test_protected_routes_document_auth(
    authed_schema: dict[str, Any], method: str, path: str
) -> None:
    op = _op(authed_schema, method, path)
    # Either scheme is enough: separate requirement objects mean OR.
    assert op["security"] == [{"HTTPBearer": []}, {"APIKeyQuery": []}]
    assert op["x-requires-auth"] is True
    assert "401" in op["responses"]
    # The security schemes must not leak in as ordinary parameters.
    assert all(p["name"] != "api_key" for p in op.get("parameters", []))


def test_health_documents_no_auth(authed_schema: dict[str, Any]) -> None:
    op = _op(authed_schema, "get", "/v1/health")
    assert "security" not in op
    assert "x-requires-auth" not in op
    assert "401" not in op["responses"]


def test_no_auth_app_documents_no_auth(tmp_output: Path) -> None:
    schema = create_app(output_dir=str(tmp_output)).openapi()
    assert "securitySchemes" not in schema.get("components", {})
    for method, path in PROTECTED:
        op = _op(schema, method, path)
        assert "security" not in op
        assert "x-requires-auth" not in op
        assert "401" not in op["responses"]


def test_raw_callable_auth_marked_without_scheme(tmp_output: Path) -> None:
    async def custom_auth() -> None:
        return None

    schema = create_app(output_dir=str(tmp_output), auth=custom_auth).openapi()
    assert "securitySchemes" not in schema.get("components", {})
    for method, path in PROTECTED:
        op = _op(schema, method, path)
        assert op["x-requires-auth"] is True
        assert "401" in op["responses"]
        assert "security" not in op


def test_make_router_accepts_security_schemes() -> None:
    provider = APIKeyAuthProvider("k")
    app = FastAPI()
    app.include_router(
        make_router(
            auth_dep=make_auth_dependency(provider),
            security_schemes=provider.security_schemes,
        ),
        prefix="/v1",
    )
    op = _op(app.openapi(), "get", "/v1/files")
    assert op["security"] == [{"HTTPBearer": []}, {"APIKeyQuery": []}]


def test_upload_documents_headers_and_body(authed_schema: dict[str, Any]) -> None:
    op = _op(authed_schema, "post", "/v1/upload")
    headers = {p["name"]: p for p in op["parameters"] if p["in"] == "header"}
    assert headers["X-Transfer-ID"]["required"] is True
    assert headers["X-Original-Filename"]["required"] is True
    assert headers["X-Source-Format"]["required"] is False
    assert headers["X-Source-Format"]["schema"]["enum"] == ["msz", "mzml", "mszx"]
    assert op["requestBody"]["content"] == {
        "application/octet-stream": {"schema": {"type": "string", "format": "binary"}}
    }
    assert {"200", "400", "401", "409", "500"} <= set(op["responses"])


@pytest.mark.parametrize(
    ("method", "path", "codes"),
    [
        ("get", "/v1/transfer/{transfer_id}/status", {"404"}),
        ("get", "/v1/files/{filename}", {"404"}),
    ],
)
def test_error_responses_documented(
    authed_schema: dict[str, Any], method: str, path: str, codes: set[str]
) -> None:
    responses = _op(authed_schema, method, path)["responses"]
    assert codes <= set(responses)
    for code in codes:
        ref = responses[code]["content"]["application/json"]["schema"]["$ref"]
        assert ref == "#/components/schemas/ErrorResponse"


def test_download_documents_binary_content(authed_schema: dict[str, Any]) -> None:
    ok = _op(authed_schema, "get", "/v1/files/{filename}")["responses"]["200"]
    assert set(ok["content"]) == {"application/octet-stream", "application/xml"}
