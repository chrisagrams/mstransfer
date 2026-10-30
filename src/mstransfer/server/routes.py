from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import aiofiles
import aiofiles.os
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from mstransfer import __version__
from mstransfer.server.models import (
    FileListResponse,
    HealthResponse,
    TransferRecord,
    TransferState,
    UploadResponse,
)
from mstransfer.store import MSFormat, StorePlan

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from pathlib import Path

    from mstransfer.server.state import AppState


def _state_from_app(request: Request) -> AppState:
    state: AppState = request.app.state
    return state


logger = logging.getLogger(__name__)


def make_router(
    auth_dep: Callable[..., Any] | None = None,
    state: AppState | None = None,
) -> APIRouter:
    """Create the API router, optionally protecting routes with *auth_dep*.

    Parameters
    ----------
    auth_dep:
        Optional FastAPI dependency for authentication.
    state:
        Optional :class:`AppState` instance.  When provided the router
        uses it directly (via a closure) instead of reading
        ``request.app.state``.  This allows the router to be included
        in a parent app with ``app.include_router(...)`` without
        requiring the parent to set up ``app.state``.
    """

    if state is not None:
        _fixed = state

        def _get_state() -> AppState:
            return _fixed

        state_dep = Depends(_get_state)
    else:
        state_dep = Depends(_state_from_app)

    router = APIRouter()
    protected: list[Any] = [Depends(auth_dep)] if auth_dep else []

    @router.get("/health", response_model=HealthResponse)
    async def health(state: AppState = state_dep) -> HealthResponse:
        """
        Simple health check endpoint that returns the server status, version,
          and storage configuration.
        """
        return HealthResponse(
            status="ok",
            version=__version__,
            store_as=state.store_as,
        )

    @router.get(
        "/transfer/{transfer_id}/status",
        response_model=TransferRecord,
        dependencies=protected,
    )
    async def transfer_status(
        transfer_id: str, state: AppState = state_dep
    ) -> TransferRecord:
        """
        Endpoint to check the status of an ongoing or completed transfer by its ID.
        """
        # Get the corresponding transfer record from the registry.
        record = state.transfers.get(transfer_id)

        # If no record is found, return a 404 error.
        if record is None:
            raise HTTPException(status_code=404, detail="Transfer not found")
        return record

    @router.post("/upload", response_model=UploadResponse, dependencies=protected)
    async def upload(request: Request, state: AppState = state_dep) -> UploadResponse:
        """
        Endpoint to handle file uploads. Expects the file content in the request body
        and requires the following headers:

            - X-Transfer-ID: A unique identifier for the transfer.
            - X-Original-Filename: The original filename of the uploaded file.

        and optionally:

            - X-Source-Format: ``mzML``, ``msz`` or ``mszx``.  Inferred from
              the filename extension when absent.

        Responds 409 Conflict, before reading the request body, if the output
        file already exists or another transfer is writing it.

        The server will stream the incoming data to a file, update the transfer registry
        with the progress, and optionally decompress if configured to do so.  MSZX
        archives are always stored as-is, regardless of ``store_as``.
        """
        # Get the transfer ID, raise if missing.
        transfer_id = request.headers.get("X-Transfer-ID")
        if not transfer_id:
            raise HTTPException(status_code=400, detail="Missing X-Transfer-ID header")

        # Get the original filename, raise if missing.
        original_filename = request.headers.get("X-Original-Filename")
        if not original_filename:
            raise HTTPException(
                status_code=400, detail="Missing X-Original-Filename header"
            )

        # Determine the payload format, raise if unrecognised.
        # Without the header, the format is inferred from the filename.
        source_header = request.headers.get("X-Source-Format")
        try:
            source = (
                MSFormat.from_header(source_header)
                if source_header is not None
                else None
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported X-Source-Format: {source_header!r}",
            ) from exc

        registry = state.transfers
        plan = StorePlan.for_upload(
            state.output_dir, original_filename, source, MSFormat(state.store_as)
        )

        # Refuse an upload that would replace a stored file, or one another
        # transfer is still writing, before any of the body is read.
        existing = plan.existing()
        if existing is not None:
            raise HTTPException(
                status_code=409,
                detail=f"{existing.name} already exists on the server",
            )
        dest = plan.final_dest
        if not state.outputs.claim(dest):
            raise HTTPException(
                status_code=409,
                detail=f"{dest.name} is already being written by another transfer",
            )
        try:
            # Create a new transfer record in the registry.
            registry.create(transfer_id, original_filename)
            logger.info("Receiving %s (transfer_id=%s)", original_filename, transfer_id)

            # Stream the incoming data to a temp .part file so a partial upload
            # is never visible under its final name.
            part_path = plan.unique_part_path()
            bytes_received = 0
            update_every = 64  # throttle registry updates to reduce lock overhead
            chunk_count = 0
            try:
                async with aiofiles.open(part_path, "wb") as f:
                    async for chunk in request.stream():
                        await f.write(chunk)
                        bytes_received += len(chunk)
                        chunk_count += 1
                        if chunk_count % update_every == 0:
                            registry.update(transfer_id, bytes_received=bytes_received)

                # Final update to ensure accurate total
                registry.update(transfer_id, bytes_received=bytes_received)

            except Exception as exc:
                part_path.unlink(missing_ok=True)
                registry.update(transfer_id, state=TransferState.ERROR, error=str(exc))
                raise HTTPException(
                    status_code=500, detail=f"Error receiving data: {exc}"
                ) from exc

            registry.update(transfer_id, state=TransferState.RECEIVED)
            logger.info(
                "Received %s (%d bytes, transfer_id=%s)",
                original_filename,
                bytes_received,
                transfer_id,
            )

            # Move the .part file into place, converting it if store_as asks for a
            # different format.  Archives are always kept as-is (see for_upload).
            if plan.conversion is not None:
                registry.update(transfer_id, state=TransferState.DECOMPRESSING)
            try:
                stored = await plan.finalize(part_path)
                registry.update(
                    transfer_id,
                    state=TransferState.DONE,
                    stored_as=str(stored),
                    bytes_received=bytes_received,
                )
            except Exception as exc:
                # On error, update the registry w/ error and log the failure.
                part_path.unlink(missing_ok=True)
                registry.update(transfer_id, state=TransferState.ERROR, error=str(exc))
                logger.error("Storing %s failed: %s", transfer_id, exc)

            # Return the final transfer record as the response.
            final = registry.get(transfer_id)

            # This should not be None,
            #  however if it is, return a 500 error indicating an unexpected state.
            if final is None:
                raise HTTPException(
                    status_code=500,
                    detail="Unexpected error: transfer record missing after processing",
                )
            return UploadResponse(
                transfer_id=final.transfer_id,
                filename=final.filename,
                stored_as=final.stored_as,
                state=final.state,
                bytes_received=final.bytes_received,
            )
        finally:
            state.outputs.release(dest)

    @router.get("/files", dependencies=protected)
    async def list_files(state: AppState = state_dep) -> FileListResponse:
        """List all files available for download.

        Note: ``response_model`` is intentionally omitted so that
        custom :class:`~mstransfer.server.models.FileInfo` subclass
        fields (added by a custom :class:`FileProvider`) are preserved
        in the response via ``SerializeAsAny``.
        """
        files = await state.files.list_files()
        return FileListResponse(files=files)

    @router.get("/files/{filename}", dependencies=protected)
    async def download_file(
        filename: str, state: AppState = state_dep
    ) -> StreamingResponse:
        """Download a single file by name."""
        path = await state.files.get_file(filename)
        if path is None:
            raise HTTPException(status_code=404, detail="File not found")

        stat = await aiofiles.os.stat(path)
        content_type = (
            "application/xml"
            if path.suffix.lower() == ".mzml"
            else "application/octet-stream"
        )

        return StreamingResponse(
            _stream_file(path),
            media_type=content_type,
            headers={
                "Content-Length": str(stat.st_size),
                "Content-Disposition": f'attachment; filename="{path.name}"',
            },
        )

    return router


async def _stream_file(
    path: Path, chunk_size: int = 1_048_576
) -> AsyncGenerator[bytes, None]:
    """Async generator that reads a file in chunks."""
    async with aiofiles.open(path, "rb") as f:
        while True:
            chunk = await f.read(chunk_size)
            if not chunk:
                break
            yield chunk
