from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from mscompress import MSZFile

from mstransfer import __version__
from mstransfer.server.models import (
    HealthResponse,
    TransferState,
    UploadResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    """
    Simple health check endpoint that returns the server status, version,
      and storage configuration.
    """
    return HealthResponse(
        status="ok",
        version=__version__,
        store_as=request.app.state.store_as,
    )


@router.get("/transfer/{transfer_id}/status")
async def transfer_status(transfer_id: str, request: Request) -> dict:
    record = request.app.state.transfers.get(transfer_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Transfer not found")
    return record.model_dump(mode="json")


@router.post("/upload", response_model=UploadResponse)
async def upload(request: Request) -> UploadResponse:
    transfer_id = request.headers.get("X-Transfer-ID")
    if not transfer_id:
        raise HTTPException(status_code=400, detail="Missing X-Transfer-ID header")

    original_filename = request.headers.get("X-Original-Filename", "unknown.msz")
    output_dir: Path = request.app.state.output_dir
    store_as: str = request.app.state.store_as
    registry = request.app.state.transfers

    registry.create(transfer_id, original_filename)
    logger.info("Receiving %s (transfer_id=%s)", original_filename, transfer_id)

    stem = Path(original_filename).stem
    msz_path = output_dir / f"{stem}.msz"
    bytes_received = 0
    try:
        with open(msz_path, "wb") as f:
            async for chunk in request.stream():
                f.write(chunk)
                bytes_received += len(chunk)
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
        registry.update(
            transfer_id,
            state=TransferState.DONE,
            stored_as=str(msz_path),
            bytes_received=bytes_received,
        )
    elif store_as == "mzml":
        registry.update(transfer_id, state=TransferState.DECOMPRESSING)
        try:
            # Open the .msz file
            msz_file = MSZFile(str(msz_path).encode())

            # Construct the output path for the decompressed .mzML file
            mzml_path = output_dir / f"{stem}.mzML"

            # Offload decompression to a thread to avoid blocking the event loop
            await asyncio.to_thread(msz_file.decompress, str(mzml_path))

            # Clean up the original .msz file after successful decompression
            msz_path.unlink(missing_ok=True)

            # Update the registry with the final state and path to the decompressed file
            registry.update(
                transfer_id,
                state=TransferState.DONE,
                stored_as=str(mzml_path),
                bytes_received=bytes_received,
            )
            logger.info("Decompressed to %s", mzml_path)
        except Exception as exc:
            registry.update(
                transfer_id,
                state=TransferState.ERROR,
                error=str(exc),
            )
            logger.error("Decompression failed for %s: %s", transfer_id, exc)

    final = registry.get(transfer_id)
    return UploadResponse(
        transfer_id=final.transfer_id,
        filename=final.filename,
        stored_as=final.stored_as,
        state=final.state,
        bytes_received=final.bytes_received,
    )
