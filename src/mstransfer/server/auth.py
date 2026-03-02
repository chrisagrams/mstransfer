"""Extensible authentication for mstransfer."""

from __future__ import annotations

import hmac
from typing import Any, Protocol, runtime_checkable

from fastapi import HTTPException, Request
from pydantic import BaseModel, Field


class AuthContext(BaseModel):
    """Identity returned by an auth provider."""

    identity: str = "anonymous"
    scopes: set[str] = Field(default_factory=set)
    extra: dict[str, Any] = Field(default_factory=dict)


@runtime_checkable
class AuthProvider(Protocol):
    """Contract that auth providers must satisfy."""

    async def authenticate(self, request: Request) -> AuthContext: ...


class NoAuthProvider:
    """Default no-op provider — allows all requests."""

    async def authenticate(self, request: Request) -> AuthContext:
        return AuthContext()


class APIKeyAuthProvider:
    """Validates ``Authorization: Bearer <key>`` or ``?api_key=`` query param."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    async def authenticate(self, request: Request) -> AuthContext:
        # Try Authorization header first
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header.removeprefix("Bearer ")
            if hmac.compare_digest(token, self._api_key):
                return AuthContext(identity="api-key")

        # Fall back to query parameter
        query_key = request.query_params.get("api_key")
        if query_key is not None and hmac.compare_digest(query_key, self._api_key):
            return AuthContext(identity="api-key")

        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def make_auth_dependency(provider: AuthProvider):
    """Convert an AuthProvider into a FastAPI dependency callable."""

    async def _dependency(request: Request) -> AuthContext:
        return await provider.authenticate(request)

    return _dependency
