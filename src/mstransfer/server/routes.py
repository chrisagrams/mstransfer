from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiofiles
import aiofiles.os
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from mscompress import MSZFile

from mstransfer import __version__
from mstransfer.server.models import (
    FileListResponse,
    HealthResponse,
    TransferRecord,
    TransferState,
    UploadResponse,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

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

        The server will stream the incoming data to a file, update the transfer registry
        with the progress, and optionally decompress if configured to do so.
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

        output_dir = state.output_dir
        store_as = state.store_as
        registry = state.transfers

        # Create a new transfer record in the registry.
        registry.create(transfer_id, original_filename)
        logger.info("Receiving %s (transfer_id=%s)", original_filename, transfer_id)

        # Use the filename stem for the output file.
        stem = Path(original_filename).stem
        msz_path = output_dir / f"{stem}.msz"
        bytes_received = 0

        # Stream the incoming data to a file asynchronously.
        update_every = 64  # throttle registry updates to reduce lock overhead
        chunk_count = 0
        try:
            async with aiofiles.open(msz_path, "wb") as f:
                async for chunk in request.stream():
                    await f.write(chunk)
                    bytes_received += len(chunk)
                    chunk_count += 1
                    if chunk_count % update_every == 0:
                        registry.update(transfer_id, bytes_received=bytes_received)

            # Final update to ensure accurate total
            registry.update(transfer_id, bytes_received=bytes_received)

        except Exception as exc:
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

        if store_as == "msz":
            # If we're storing as .msz, we're done at this point.
            # Just update the registry with the final path and state.
            registry.update(
                transfer_id,
                state=TransferState.DONE,
                stored_as=str(msz_path),
                bytes_received=bytes_received,
            )
        elif store_as == "mzml":
            # If we need to decompress to .mzML, update the state and then
            # offload the decompression to a thread to avoid blocking the event loop.
            registry.update(transfer_id, state=TransferState.DECOMPRESSING)
            try:
                # Open the .msz file
                msz_file = MSZFile(str(msz_path).encode())

                # Construct the output path for the decompressed .mzML file
                mzml_path = output_dir / f"{stem}.mzML"

                # Offload decompression to a thread to avoid blocking the event loop
                await asyncio.to_thread(msz_file.decompress, str(mzml_path))

                # Clean up the original .msz file after successful decompression
                await aiofiles.os.remove(str(msz_path))

                # Update the registry with the final state and path
                # to the decompressed file
                registry.update(
                    transfer_id,
                    state=TransferState.DONE,
                    stored_as=str(mzml_path),
                    bytes_received=bytes_received,
                )
                logger.info("Decompressed to %s", mzml_path)
            except Exception as exc:
                # On error, update the registry w/ error and log the failure.
                registry.update(
                    transfer_id,
                    state=TransferState.ERROR,
                    error=str(exc),
                )
                logger.error("Decompression failed for %s: %s", transfer_id, exc)

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

    @router.get("/files/{filename}", dependencies=protected, response_model=None)
    async def download_file(
        filename: str, request: Request, state: AppState = state_dep
    ) -> StreamingResponse | Response:
        """Download a single file by name.

        Supports single-range ``Range: bytes=...`` requests (``a-b``,
        ``a-`` and suffix ``-n``), answering with ``206 Partial Content``.
        Unsatisfiable ranges get ``416``.  Multi-range requests and
        malformed headers are ignored and the full file is returned.
        """
        path = await state.files.get_file(filename)
        if path is None:
            raise HTTPException(status_code=404, detail="File not found")

        stat = await aiofiles.os.stat(path)
        size = stat.st_size
        content_type = (
            "application/xml"
            if path.suffix.lower() == ".mzml"
            else "application/octet-stream"
        )
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Disposition": f'attachment; filename="{path.name}"',
        }

        range_header = request.headers.get("range")
        byte_range = _parse_range(range_header, size) if range_header else None

        if byte_range == _UNSATISFIABLE:
            return Response(
                status_code=416,
                headers={**headers, "Content-Range": f"bytes */{size}"},
            )

        if byte_range is not None:
            start, end = byte_range
            length = end - start + 1
            return StreamingResponse(
                _stream_file(path, start=start, length=length),
                status_code=206,
                media_type=content_type,
                headers={
                    **headers,
                    "Content-Length": str(length),
                    "Content-Range": f"bytes {start}-{end}/{size}",
                },
            )

        return StreamingResponse(
            _stream_file(path),
            media_type=content_type,
            headers={**headers, "Content-Length": str(size)},
        )

    return router


# Sentinel returned by _parse_range for a syntactically valid but
# unsatisfiable range (answered with 416).
_UNSATISFIABLE: tuple[int, int] = (-1, -1)


def _parse_range(header: str, size: int) -> tuple[int, int] | None:
    """Parse a single ``bytes=`` range against a file of *size* bytes.

    Returns an inclusive ``(start, end)`` tuple, :data:`_UNSATISFIABLE`
    when the range cannot be served, or *None* when the header should be
    ignored (unknown unit, malformed, or multi-range) and the full file
    returned instead.
    """
    unit, _, spec = header.partition("=")
    if unit.strip().lower() != "bytes" or not spec or "," in spec:
        return None

    first, sep, last = spec.strip().partition("-")
    if not sep:
        return None
    first, last = first.strip(), last.strip()
    if (first and not first.isdigit()) or (last and not last.isdigit()):
        return None

    if not first:
        # Suffix range: the final *n* bytes of the file.
        if not last:
            return None
        suffix = int(last)
        if suffix == 0 or size == 0:
            return _UNSATISFIABLE
        return max(size - suffix, 0), size - 1

    start = int(first)
    if last and int(last) < start:
        return None
    if start >= size:
        return _UNSATISFIABLE
    end = int(last) if last else size - 1
    return start, min(end, size - 1)


async def _stream_file(
    path: Path,
    chunk_size: int = 1_048_576,
    *,
    start: int = 0,
    length: int | None = None,
) -> AsyncGenerator[bytes, None]:
    """Async generator that reads a file in chunks.

    When *length* is given, only ``length`` bytes starting at *start*
    are yielded.
    """
    remaining = length
    async with aiofiles.open(path, "rb") as f:
        if start:
            await f.seek(start)
        while remaining is None or remaining > 0:
            to_read = chunk_size if remaining is None else min(chunk_size, remaining)
            chunk = await f.read(to_read)
            if not chunk:
                break
            if remaining is not None:
                remaining -= len(chunk)
            yield chunk
