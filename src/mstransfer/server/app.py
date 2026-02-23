from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI

from mstransfer.server.routes import router
from mstransfer.server.state import TransferRegistry


def create_app(
    output_dir: str = "./received",
    store_as: str = "msz",
) -> FastAPI:
    """Create a configured mstransfer FastAPI application.

    Args:
        output_dir: Directory where received files are stored.
        store_as: Output format — "msz" or "mzml".
    """
    app = FastAPI(title="mstransfer")
    app.state.output_dir = Path(output_dir)
    app.state.store_as = store_as
    app.state.output_dir.mkdir(parents=True, exist_ok=True)
    app.state.transfers = TransferRegistry()
    app.include_router(router, prefix="/v1")
    return app
