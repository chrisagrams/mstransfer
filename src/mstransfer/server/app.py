from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

from mstransfer.server.auth import AuthProvider, make_auth_dependency
from mstransfer.server.fileprovider import DirectoryFileProvider, FileProvider

if TYPE_CHECKING:
    from collections.abc import Callable

    from mstransfer.server.models import StoreFormat
from mstransfer.server.routes import make_router
from mstransfer.server.state import AppState


def _resolve_auth(
    name: str, auth: AuthProvider | Callable[..., Any] | None
) -> Callable[..., Any] | None:
    """Turn an `AuthProvider`, callable, or *None* into a FastAPI dependency."""
    if auth is None:
        return None
    if isinstance(auth, AuthProvider):
        return make_auth_dependency(auth)
    if callable(auth):
        return auth
    msg = f"{name} must be an AuthProvider, callable, or None — got {type(auth)}"
    raise TypeError(msg)


def create_app(
    output_dir: str = "./received",
    store_as: StoreFormat = "msz",
    auth: AuthProvider | Callable[..., Any] | None = None,
    file_provider: FileProvider | None = None,
    *,
    upload_auth: AuthProvider | Callable[..., Any] | None = None,
    download_auth: AuthProvider | Callable[..., Any] | None = None,
) -> FastAPI:
    """Create a configured mstransfer FastAPI application.

    Args:
        output_dir: Directory where received files are stored.
        store_as: Output format — "msz" or "mzml".
        auth: Authentication provider, raw FastAPI dependency, or *None*
              (no auth, backward-compatible default).  Protects both uploads
              and downloads.  Can't be combined with *upload_auth* or
              *download_auth*.
        file_provider: File discovery provider, or *None* to default to
              scanning *output_dir* on disk.
        upload_auth: Like *auth*, but only for `POST /v1/upload` and
              `GET /v1/transfer/{id}/status`.
        download_auth: Like *auth*, but only for `GET /v1/files` and
              `GET /v1/files/{filename}`.

    Raises:
        TypeError: If *auth* is combined with *upload_auth* or
              *download_auth*, or any of them has an unsupported type.
    """
    if auth is not None and (upload_auth is not None or download_auth is not None):
        msg = (
            "auth can't be combined with upload_auth or download_auth; "
            "pass the per-direction auth instead"
        )
        raise TypeError(msg)
    if auth is not None:
        upload_dep = download_dep = _resolve_auth("auth", auth)
    else:
        upload_dep = _resolve_auth("upload_auth", upload_auth)
        download_dep = _resolve_auth("download_auth", download_auth)

    app = FastAPI(title="mstransfer")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    if file_provider is None:
        file_provider = DirectoryFileProvider(out)

    app.state = AppState(output_dir=out, store_as=store_as, files=file_provider)
    app.include_router(
        make_router(upload_auth_dep=upload_dep, download_auth_dep=download_dep),
        prefix="/v1",
    )
    return app
