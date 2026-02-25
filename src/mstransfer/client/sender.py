from __future__ import annotations

import logging
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable

import httpx
from mscompress import MZMLFile
from mscompress.utils import detect_filetype

from mstransfer.server.models import TransferRecord, TransferState, UploadResponse

logger = logging.getLogger(__name__)

VALID_EXTENSIONS = {".mzml", ".msz", ".mszx"}
VALID_FORMATS = {"mzML", "msz", "mszx"}


@runtime_checkable
class BatchProgressCallback(Protocol):
    """Callback protocol for observing batch upload progress."""

    def file_started(
            self,
            index: int,
            file_path: Path,
            total_bytes: int | None
        ) -> None: ...

    def file_progress(self, index: int, delta: int) -> None: ...
    def file_done(self, index: int, result: UploadResponse) -> None: ...
    def file_error(self, index: int, exc: Exception) -> None: ...


@dataclass
class FileResult:
    """Result of uploading a single file in a batch."""

    filename: str
    response: UploadResponse | None = field(default=None)
    error: str | None = field(default=None)


def resolve_inputs(paths: list[str], recursive: bool = False) -> list[Path]:
    """Resolve files and directories into a sorted list of valid MS files."""
    result: list[Path] = []
    for p in paths:
        path = Path(p)
        if path.is_file():
            if path.suffix.lower() in VALID_EXTENSIONS:
                result.append(path)
            else:
                logger.warning("Skipping unsupported file: %s", path)
        elif path.is_dir():
            for ext in ("*.mzML", "*.msz", "*.mzml", "*.MSZ"):
                if recursive:
                    result.extend(path.rglob(ext))
                else:
                    result.extend(path.glob(ext))
            result = list(dict.fromkeys(result))
        else:
            logger.warning("Path does not exist: %s", path)
    if not result:
        raise FileNotFoundError("No valid .mzML or .msz files found in the given paths")
    return sorted(set(result))


def _counting_generator(iterator, callback: Callable[[int], None] | None = None):
    """Wrap an iterator, calling callback with byte count per chunk."""
    for chunk in iterator:
        if callback:
            callback(len(chunk))
        yield chunk


def _file_chunk_generator(
    file_path: Path,
    chunk_size: int = 1_048_576,
    callback: Callable[[int], None] | None = None,
):
    """Read a file in chunks, calling callback with each chunk's size."""
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            if callback:
                callback(len(chunk))
            yield chunk


def send_file(
    file_path: Path,
    base_url: str,
    progress_callback: Callable[[int], None] | None = None,
    timeout: float = 3600.0,
    chunk_size: int = 1_048_576,
) -> UploadResponse:
    """Send a single file to the mstransfer listener.

    Returns the final transfer status dict from the server.
    """

    # Generate a unique transfer ID.
    transfer_id = str(uuid.uuid4())

    # Detect file type w/ mscompress
    filetype = detect_filetype(str(file_path))

    if filetype not in VALID_FORMATS:
        raise ValueError(f"Unsupported file type for {file_path}: {filetype}")

    # Construct headers with metadata for the server.
    headers = {
        "X-Transfer-ID": transfer_id,
        "X-Original-Filename": file_path.name,
        "X-Source-Format": filetype,
        "Content-Type": "application/octet-stream",
    }

    if filetype == "mzML":
        # For .mzML, set up a compression stream with a progress callback.
        mzml = MZMLFile(str(file_path).encode())
        stream = _counting_generator(
            mzml.compress_stream(chunk_size=chunk_size),
            progress_callback,
        )
    elif filetype in ("msz", "mszx"):
        # For .msz, just stream the file directly with a progress callback.
        stream = _file_chunk_generator(
            file_path,
            chunk_size=chunk_size,
            callback=progress_callback
        )
    else:
        raise ValueError(f"Unsupported file type: {filetype} for {file_path}")

    # Send the POST request with streaming upload and handle the response.
    with httpx.Client(timeout=httpx.Timeout(timeout, connect=10.0)) as client:
        resp = client.post(
            f"{base_url}/v1/upload",
            headers=headers,
            content=stream,
        )
        resp.raise_for_status()
        upload_result = UploadResponse.model_validate(resp.json())

    # Poll for server-side processing completion
    if upload_result.state not in (TransferState.DONE, TransferState.ERROR):
        state = _poll_status(base_url, transfer_id, timeout=timeout)
        upload_result.state = state

    return upload_result


def _poll_status(
    base_url: str,
    transfer_id: str,
    timeout: float = 300.0,
    interval: float = 0.5,
) -> TransferState:
    """Poll transfer status until terminal state or timeout."""

    # Configure a deadline for the polling operation.
    deadline = time.monotonic() + timeout

    # Keep track of the last seen state and bytes to detect progress.
    last_state: TransferState | None = None
    last_bytes: int = 0

    # Individual request timeout should be reasonably short.
    with httpx.Client(timeout=10.0) as client:
        # Continously poll until we hit a terminal state or exceed the deadline.
        while time.monotonic() < deadline:
            # Make a GET request to the status endpoint for this transfer ID.
            resp = client.get(f"{base_url}/v1/transfer/{transfer_id}/status")
            if resp.status_code == 200:
                record = TransferRecord.model_validate(resp.json())

                # If the transfer is done or errored, return the final state.
                if record.state in (TransferState.DONE, TransferState.ERROR):
                    return record.state

                # Reset deadline if the server is still making progress
                # Either by a state change or receiving more bytes.
                if record.state != last_state or record.bytes_received > last_bytes:
                    last_state = record.state
                    last_bytes = record.bytes_received
                    deadline = time.monotonic() + timeout
            # Sleep until the next poll interval before checking again.
            time.sleep(interval)
    raise TimeoutError(
        f"Transfer {transfer_id} did not complete within {timeout}s"
    )


def send_batch(
    file_paths: list[Path],
    base_url: str,
    parallel: int = 4,
    chunk_size: int = 1_048_576,
    progress: BatchProgressCallback | None = None,
) -> list[FileResult]:
    """Send multiple files with configurable parallelism."""
    # Set the number of workers.
    workers = min(parallel, len(file_paths))

    results: list[FileResult] = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        # Keep track of futures and their corresponding index + file path.
        futures: dict[Future[UploadResponse], tuple[int, Path]] = {}

        for idx, fpath in enumerate(file_paths):
            # Currently, we can only determine total bytes for .msz files
            #  since the .mzML is compressed on the fly.
            is_msz = fpath.suffix.lower() == ".msz"
            total_bytes = fpath.stat().st_size if is_msz else None

            # If the progress callback is provided, notify that this file is starting.
            if progress:
                progress.file_started(idx, fpath, total_bytes)

            def make_callback(i: int):
                """
                Create a callback function that captures the file index for progress.
                """
                def cb(delta: int):
                    if progress:
                        progress.file_progress(i, delta)
                return cb

            # Submit the file upload task to the thread pool and store the future.
            future = pool.submit(
                send_file,
                fpath,
                base_url,
                progress_callback=make_callback(idx),
                chunk_size=chunk_size,
            )
            futures[future] = (idx, fpath)

        # As the futures complete,
        for future in as_completed(futures):
            # Unpack the index and file path for this future to report progress callback
            idx, fpath = futures[future]
            try:
                result = future.result()
                results.append(FileResult(filename=fpath.name, response=result))
                if progress:
                    progress.file_done(idx, result)
            # On exception, append a FileResult with the error message.
            except Exception as exc:
                results.append(FileResult(filename=fpath.name, error=str(exc)))
                if progress:
                    progress.file_error(idx, exc)
                logger.error("Failed to send %s: %s", fpath, exc)

    return results
