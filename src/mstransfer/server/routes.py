from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import aiofiles
import aiofiles.os
from fastapi import APIRouter, Depends, HTTPException, Request
from mscompress import MSZFile

from mstransfer import __version__
from mstransfer.server.models import (
    HealthResponse,
    TransferRecord,
    TransferState,
    UploadResponse,
)

if TYPE_CHECKING:
    from mstransfer.server.state import AppState


def get_state(request: Request) -> AppState:
    return request.app.state


StateDep = Depends(get_state)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health(state: AppState = StateDep) -> HealthResponse:
    """
    Simple health check endpoint that returns the server status, version,
      and storage configuration.
    """
    return HealthResponse(
        status="ok",
        version=__version__,
        store_as=state.store_as,
    )


@router.get("/transfer/{transfer_id}/status", response_model=TransferRecord)
async def transfer_status(
    transfer_id: str, state: AppState = StateDep
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


@router.post("/upload", response_model=UploadResponse)
async def upload(
    request: Request, state: AppState = StateDep
) -> UploadResponse:
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
        raise HTTPException(
            status_code=400,
            detail="Missing X-Transfer-ID header"
        )

    # Get the original filename, raise if missing.
    original_filename = request.headers.get("X-Original-Filename")
    if not original_filename:
        raise HTTPException(
            status_code=400,
            detail="Missing X-Original-Filename header"
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

            # Update the registry with the final state and path to the decompressed file
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
