from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

import httpx
from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile

from mstransfer.client.utils import (
    ThrottledCallback,
    async_counting_generator,
    async_file_chunk_generator,
    async_iter_from_sync,
    normalize_source,
    optional_client,
)
from mstransfer.server.models import TransferRecord, TransferState, UploadResponse

logger = logging.getLogger(__name__)


@runtime_checkable
class BatchProgressCallback(Protocol):
    """Callback protocol for observing batch upload progress."""

    def file_started(
        self, index: int, file_path: Path, total_bytes: int | None
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


async def async_send_file(
    source: Path | MZMLFile | MSZFile | MSZXFile,
    base_url: str,
    progress_callback: Callable[[int], None] | None = None,
    timeout: httpx.Timeout | None = None,
    poll_timeout: float | None = 3600.0,
    chunk_size: int = 1_048_576,
    api_key: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> UploadResponse:
    """Send a single file to the mstransfer listener (async).

    Accepts a file path, or an already-opened MZMLFile / MSZFile / MSZXFile
    from mscompress.  Returns the final transfer status from the server.

    *timeout* replaces *client*'s timeouts for the upload request; pass
    ``httpx.Timeout(None)`` for no limit.  If *None*, *client*'s timeouts
    are used (10 s connect, 3600 s otherwise, without *client*).

    *poll_timeout* is how long to wait without server-side progress while
    polling for completion, in seconds, or *None* to wait forever.  Status
    polls use *client*'s timeout, or 10 s without *client*.
    """

    # Generate a unique transfer ID.
    transfer_id = str(uuid.uuid4())

    # Normalize source into (file_path, format, mzml_obj | None).
    file_path, fmt, mzml_obj = normalize_source(source)

    # Throttle progress callbacks to reduce overhead (default: every 8 MiB).
    throttled = ThrottledCallback(progress_callback) if progress_callback else None

    # Build the upload stream.
    # If its an mzML file, we can use the compress_stream for on-the-fly compression.
    if mzml_obj is not None:
        stream = async_counting_generator(
            async_iter_from_sync(mzml_obj.compress_stream(chunk_size=chunk_size)),
            throttled,
        )
    # Otherwise, we stream the file in chunks.
    else:
        stream = async_file_chunk_generator(
            file_path,
            chunk_size=chunk_size,
            callback=throttled,
        )

    # Construct headers with metadata for the server.
    headers = {
        "X-Transfer-ID": transfer_id,
        "X-Original-Filename": file_path.name,
        "X-Source-Format": fmt.value,
        "Content-Type": "application/octet-stream",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    # Send the POST request with streaming upload and handle the response.
    async with optional_client(
        client, timeout=httpx.Timeout(3600.0, connect=10.0)
    ) as c:
        resp = await c.post(
            f"{base_url}/v1/upload",
            headers=headers,
            content=stream,
            timeout=httpx.USE_CLIENT_DEFAULT if timeout is None else timeout,
        )
        resp.raise_for_status()
        upload_result = UploadResponse.model_validate(resp.json())

    # Flush any remaining accumulated progress.
    if throttled:
        throttled.flush()

    # Poll for server-side processing completion
    if upload_result.state not in (TransferState.DONE, TransferState.ERROR):
        poll_headers = {"Authorization": f"Bearer {api_key}"} if api_key else None
        state = await _async_poll_status(
            base_url,
            transfer_id,
            timeout=poll_timeout,
            headers=poll_headers,
            client=client,
        )
        upload_result.state = state

    return upload_result


async def _async_poll_status(
    base_url: str,
    transfer_id: str,
    timeout: float | None = 300.0,
    interval: float = 0.5,
    headers: dict[str, str] | None = None,
    client: httpx.AsyncClient | None = None,
) -> TransferState:
    """Poll transfer status until terminal state or timeout (async).

    *timeout* is how long to wait without progress; *None* waits forever.
    """

    # Configure a deadline for the polling operation.
    limit = math.inf if timeout is None else timeout
    deadline = time.monotonic() + limit

    # Keep track of the last seen state and bytes to detect progress.
    last_state: TransferState | None = None
    last_bytes: int = 0

    # Individual request timeout should be reasonably short.
    async with optional_client(client, timeout=10.0) as c:
        # Continuously poll until we hit a terminal state or exceed the deadline.
        while time.monotonic() < deadline:
            # Make a GET request to the status endpoint for this transfer ID.
            resp = await c.get(
                f"{base_url}/v1/transfer/{transfer_id}/status",
                headers=headers,
            )
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
                    deadline = time.monotonic() + limit
            # Sleep until the next poll interval before checking again.
            await asyncio.sleep(interval)
    raise TimeoutError(f"Transfer {transfer_id} did not complete within {timeout}s")


async def async_send_batch(
    sources: Sequence[Path | MZMLFile | MSZFile | MSZXFile],
    base_url: str,
    parallel: int = 4,
    chunk_size: int = 1_048_576,
    progress: BatchProgressCallback | None = None,
    api_key: str | None = None,
    client: httpx.AsyncClient | None = None,
    timeout: httpx.Timeout | None = None,
    poll_timeout: float | None = 3600.0,
) -> list[FileResult]:
    """Send multiple files with configurable parallelism (async).

    *timeout* and *poll_timeout* are passed to :func:`async_send_file` for
    each file.
    """
    workers = min(parallel, len(sources))
    sem = asyncio.Semaphore(workers)

    # Pre-allocate results list indexed by position for deterministic ordering.
    results: list[FileResult | None] = [None] * len(sources)

    async def _upload_one(idx: int, src: Path | MZMLFile | MSZFile | MSZXFile) -> None:
        # Extract a Path for progress reporting and metadata.
        if isinstance(src, MSZXFile):
            fpath = src.archive_path
        elif isinstance(src, MZMLFile | MSZFile):
            fpath = Path(src.path.decode())
        else:
            fpath = src

        async with sem:

            def make_callback(i: int) -> Callable[[int], None]:
                """Create a callback that captures the file index for progress."""

                def cb(delta: int) -> None:
                    if progress:
                        progress.file_progress(i, delta)

                return cb

            started = False
            try:
                # We can determine total bytes for compressed files (MSZ/MSZX).
                # For mzML, compression is on-the-fly so the total is unknown.
                # stat() is inside the try so a missing or unreadable file
                # becomes a per-file error instead of aborting the batch.
                if isinstance(src, MSZFile | MSZXFile):
                    total_bytes: int | None = fpath.stat().st_size
                elif isinstance(src, MZMLFile):
                    total_bytes = None
                else:
                    is_compressed = fpath.suffix.lower() in (".msz", ".mszx")
                    total_bytes = fpath.stat().st_size if is_compressed else None

                # Notify the progress callback that this file is starting.
                if progress:
                    progress.file_started(idx, fpath, total_bytes)
                started = True

                result = await async_send_file(
                    src,
                    base_url,
                    progress_callback=make_callback(idx),
                    chunk_size=chunk_size,
                    api_key=api_key,
                    client=client,
                    timeout=timeout,
                    poll_timeout=poll_timeout,
                )
                results[idx] = FileResult(filename=fpath.name, response=result)
                if progress:
                    progress.file_done(idx, result)
            except Exception as exc:
                results[idx] = FileResult(filename=fpath.name, error=str(exc))
                if progress:
                    # Progress listeners expect file_started before file_error.
                    if not started:
                        progress.file_started(idx, fpath, None)
                    progress.file_error(idx, exc)
                logger.error("Failed to send %s: %s", fpath, exc)

    await asyncio.gather(*[_upload_one(i, s) for i, s in enumerate(sources)])

    return [r for r in results if r is not None]


def send_file(
    source: Path | MZMLFile | MSZFile | MSZXFile,
    base_url: str,
    progress_callback: Callable[[int], None] | None = None,
    timeout: httpx.Timeout | None = None,
    poll_timeout: float | None = 3600.0,
    chunk_size: int = 1_048_576,
    api_key: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> UploadResponse:
    """Send a single file to the mstransfer listener.

    Accepts a file path, or an already-opened MZMLFile / MSZFile / MSZXFile
    from mscompress.  Returns the final transfer status from the server.

    This is a synchronous wrapper around :func:`async_send_file`.
    """
    return asyncio.run(
        async_send_file(
            source,
            base_url,
            progress_callback=progress_callback,
            timeout=timeout,
            poll_timeout=poll_timeout,
            chunk_size=chunk_size,
            api_key=api_key,
            client=client,
        )
    )


def send_batch(
    sources: Sequence[Path | MZMLFile | MSZFile | MSZXFile],
    base_url: str,
    parallel: int = 4,
    chunk_size: int = 1_048_576,
    progress: BatchProgressCallback | None = None,
    api_key: str | None = None,
    client: httpx.AsyncClient | None = None,
    timeout: httpx.Timeout | None = None,
    poll_timeout: float | None = 3600.0,
) -> list[FileResult]:
    """Send multiple files with configurable parallelism.

    *timeout* and *poll_timeout* are passed to :func:`async_send_file` for
    each file.

    This is a synchronous wrapper around :func:`async_send_batch`.
    """
    return asyncio.run(
        async_send_batch(
            sources,
            base_url,
            parallel=parallel,
            chunk_size=chunk_size,
            progress=progress,
            api_key=api_key,
            client=client,
            timeout=timeout,
            poll_timeout=poll_timeout,
        )
    )
