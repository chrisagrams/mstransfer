from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

from mstransfer.server.auth import AuthProvider, make_auth_dependency

if TYPE_CHECKING:
    from collections.abc import Callable
from mstransfer.server.routes import make_router
from mstransfer.server.state import AppState


def create_app(
    output_dir: str = "./received",
    store_as: str = "msz",
    auth: AuthProvider | Callable[..., Any] | None = None,
) -> FastAPI:
    """Create a configured mstransfer FastAPI application.

    Args:
        output_dir: Directory where received files are stored.
        store_as: Output format — "msz" or "mzml".
        auth: Authentication provider, raw FastAPI dependency, or *None*
              (no auth, backward-compatible default).
    """
    # Resolve the auth dependency
    if auth is None:
        auth_dep = None
    elif isinstance(auth, AuthProvider):
        auth_dep = make_auth_dependency(auth)
    elif callable(auth):
        auth_dep = auth
    else:
        msg = f"auth must be an AuthProvider, callable, or None — got {type(auth)}"
        raise TypeError(msg)

    app = FastAPI(title="mstransfer")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    app.state = AppState(output_dir=out, store_as=store_as)
    app.include_router(make_router(auth_dep=auth_dep), prefix="/v1")
    return app
