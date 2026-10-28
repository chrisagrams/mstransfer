from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiofiles
import aiofiles.os
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from mscompress import MSZFile

from mstransfer import __version__
from mstransfer.server.models import (
    MAX_UPLOAD_PARTS,
    FileListResponse,
    HealthResponse,
    MultipartInitRequest,
    MultipartInitResponse,
    PartUploadResponse,
    TransferRecord,
    TransferState,
    UploadResponse,
)
from mstransfer.server.state import PartError

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
        return await _finalize_upload(state, transfer_id, msz_path)

    @router.post(
        "/uploads", response_model=MultipartInitResponse, dependencies=protected
    )
    async def init_multipart_upload(
        body: MultipartInitRequest, state: AppState = state_dep
    ) -> MultipartInitResponse:
        """
        Initiate a multipart upload of a file whose total size is known up front
        (MSZ / MSZX). The file is split into ``part_size`` byte parts which may be
        sent concurrently and in any order via ``PUT /uploads/{id}/parts/{n}``,
        then assembled with ``POST /uploads/{id}/complete``.

        The output file is preallocated to ``total_size`` so that each part can
        be written in place at its offset.
        """
        registry = state.transfers

        # Refuse to clobber an existing transfer with the same ID.
        if registry.get(body.transfer_id) is not None:
            raise HTTPException(status_code=409, detail="Transfer ID already exists")

        part_count = -(-body.total_size // body.part_size)  # ceil division
        if part_count > MAX_UPLOAD_PARTS:
            raise HTTPException(
                status_code=400,
                detail=f"Too many parts ({part_count} > {MAX_UPLOAD_PARTS}); "
                "increase part_size",
            )

        stem = Path(body.filename).stem
        msz_path = state.output_dir / f"{stem}.msz"

        # Register before awaiting so a concurrent init with the same ID fails.
        registry.create_multipart(
            body.transfer_id,
            body.filename,
            path=msz_path,
            total_size=body.total_size,
            part_size=body.part_size,
        )

        # Preallocate the output file (sparse where the filesystem supports it).
        try:
            async with aiofiles.open(msz_path, "wb") as f:
                await f.truncate(body.total_size)
        except Exception as exc:
            registry.abort_multipart(body.transfer_id, reason=str(exc))
            raise HTTPException(
                status_code=500, detail=f"Error allocating file: {exc}"
            ) from exc

        logger.info(
            "Receiving %s in %d part(s) (%d bytes, transfer_id=%s)",
            body.filename,
            part_count,
            body.total_size,
            body.transfer_id,
        )
        return MultipartInitResponse(
            transfer_id=body.transfer_id,
            filename=body.filename,
            total_size=body.total_size,
            part_size=body.part_size,
            part_count=part_count,
        )

    @router.put(
        "/uploads/{transfer_id}/parts/{index}",
        response_model=PartUploadResponse,
        dependencies=protected,
    )
    async def upload_part(
        transfer_id: str,
        index: int,
        request: Request,
        state: AppState = state_dep,
    ) -> PartUploadResponse:
        """
        Receive part *index* of a multipart upload. The body must be exactly the
        bytes of that part's range. Re-sending a part overwrites it, so retries
        are safe; concurrent requests for the same part are rejected with 409.
        """
        registry = state.transfers
        try:
            upload, part = registry.begin_part(transfer_id, index)
        except PartError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

        received = 0
        try:
            # Reject a mismatched body size up front when the client declares it.
            declared = request.headers.get("Content-Length")
            if declared is not None and declared != str(part.size):
                raise HTTPException(
                    status_code=400,
                    detail=f"Part {index} must be {part.size} bytes "
                    f"(Content-Length: {declared})",
                )

            update_every = 64  # throttle registry updates
            chunk_count = 0
            async with aiofiles.open(upload.path, "r+b") as f:
                await f.seek(part.offset)
                async for chunk in request.stream():
                    # Never write past this part's range into a neighbouring part.
                    if received + len(chunk) > part.size:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Part {index} exceeds expected size "
                            f"of {part.size} bytes",
                        )
                    await f.write(chunk)
                    received += len(chunk)
                    chunk_count += 1
                    if chunk_count % update_every == 0:
                        registry.update_part(transfer_id, index, received)

            if received != part.size:
                raise HTTPException(
                    status_code=400,
                    detail=f"Part {index} incomplete: received {received} "
                    f"of {part.size} bytes",
                )

            # The upload may have been aborted while this part was streaming.
            if registry.get_multipart(transfer_id) is not upload:
                raise HTTPException(
                    status_code=409, detail="Transfer is no longer accepting parts"
                )
            registry.update_part(transfer_id, index, received)

        except HTTPException:
            registry.update_part(transfer_id, index, 0)
            raise
        except Exception as exc:
            registry.update_part(transfer_id, index, 0)
            raise HTTPException(
                status_code=500, detail=f"Error receiving part {index}: {exc}"
            ) from exc
        finally:
            registry.end_part(transfer_id, index)

        return PartUploadResponse(
            transfer_id=transfer_id,
            index=index,
            offset=part.offset,
            size=part.size,
            bytes_received=received,
        )

    @router.post(
        "/uploads/{transfer_id}/complete",
        response_model=UploadResponse,
        dependencies=protected,
    )
    async def complete_multipart_upload(
        transfer_id: str, state: AppState = state_dep
    ) -> UploadResponse:
        """
        Finish a multipart upload. Fails with 409 unless every part has been
        fully received. On success the file is processed exactly like a
        single-stream ``POST /upload`` (optionally decompressed to mzML).
        """
        registry = state.transfers
        try:
            upload = registry.complete_multipart(transfer_id)
        except PartError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

        logger.info(
            "Received %s (%d bytes, transfer_id=%s)",
            upload.path.name,
            upload.total_size,
            transfer_id,
        )
        return await _finalize_upload(state, transfer_id, upload.path)

    @router.delete(
        "/uploads/{transfer_id}",
        response_model=TransferRecord,
        dependencies=protected,
    )
    async def abort_multipart_upload(
        transfer_id: str, state: AppState = state_dep
    ) -> TransferRecord:
        """Abort a multipart upload, marking it ERROR and removing partial data."""
        registry = state.transfers
        try:
            upload = registry.abort_multipart(transfer_id, reason="Aborted by client")
        except PartError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

        with contextlib.suppress(FileNotFoundError):
            await aiofiles.os.remove(str(upload.path))
        logger.info("Aborted multipart upload (transfer_id=%s)", transfer_id)

        record = registry.get(transfer_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Transfer not found")
        return record

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


async def _finalize_upload(
    state: AppState, transfer_id: str, msz_path: Path
) -> UploadResponse:
    """Store or decompress a fully received ``.msz`` file and build the response.

    Shared by the single-stream and multipart upload paths. Expects the
    transfer to already be in the RECEIVED state.
    """
    registry = state.transfers
    record = registry.get(transfer_id)
    bytes_received = record.bytes_received if record is not None else 0

    if state.store_as == "msz":
        # If we're storing as .msz, we're done at this point.
        # Just update the registry with the final path and state.
        registry.update(
            transfer_id,
            state=TransferState.DONE,
            stored_as=str(msz_path),
            bytes_received=bytes_received,
        )
    elif state.store_as == "mzml":
        # If we need to decompress to .mzML, update the state and then
        # offload the decompression to a thread to avoid blocking the event loop.
        registry.update(transfer_id, state=TransferState.DECOMPRESSING)
        try:
            # Open the .msz file
            msz_file = MSZFile(str(msz_path).encode())

            # Construct the output path for the decompressed .mzML file
            mzml_path = msz_path.with_suffix(".mzML")

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
