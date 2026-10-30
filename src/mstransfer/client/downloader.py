from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import aiofiles
import httpx

from mstransfer.client.utils import ThrottledCallback, optional_client
from mstransfer.store import DownloadFormat, StorePlan

if TYPE_CHECKING:
    from pathlib import Path

__all__ = [
    "BatchDownloadProgress",
    "DownloadFormat",
    "DownloadProgressCallback",
    "DownloadRequest",
    "async_download_batch",
    "async_download_file",
    "download_batch",
    "download_file",
]

logger = logging.getLogger(__name__)


@runtime_checkable
class DownloadProgressCallback(Protocol):
    """Callback protocol for observing single-file download progress."""

    def on_progress(self, bytes_delta: int) -> None: ...


@runtime_checkable
class BatchDownloadProgress(Protocol):
    """Callback protocol for observing batch download progress."""

    def on_file_start(self, filename: str, total_bytes: int | None) -> None: ...
    def on_file_progress(self, filename: str, bytes_delta: int) -> None: ...
    def on_file_complete(self, filename: str) -> None: ...
    def on_file_error(self, filename: str, error: Exception) -> None: ...


@dataclass
class DownloadRequest:
    """A single download request: URL to fetch and local destination path."""

    url: str
    dest: Path


async def async_download_file(
    url: str,
    dest: Path,
    *,
    store_as: DownloadFormat | None = None,
    chunk_size: int = 1_048_576,
    connect_timeout: float = 10.0,
    read_timeout: float = 300.0,
    progress_callback: DownloadProgressCallback | None = None,
    skip_existing: bool = False,
    force: bool = False,
    client: httpx.AsyncClient | None = None,
) -> Path:
    """Download a single file from *url* to *dest* (async).

    The file is streamed to a temporary ``.part`` file first, then atomically
    renamed (or converted) to the final destination on success.

    Parameters
    ----------
    url:
        The URL to download from.
    dest:
        The local path where the file should be saved.  If *store_as*
        requires a format conversion, the extension will be adjusted
        automatically.
    store_as:
        Desired output format — ``"msz"``, ``"mzml"``, ``"mszx"``, or
        *None* to keep the file as-is.  When set, the downloaded file
        is converted after download using **mscompress**.  Conversion
        *to* ``"mszx"`` is only supported when the source is already
        ``.mszx`` (no-op); other combos raise :class:`NotImplementedError`.
        When a multi-member (batch) ``.mszx`` is stored as ``"msz"`` or
        ``"mzml"``, its members are written to a directory named after
        the archive, ``dest.parent/<dest stem>/<entry stem>.<ext>``.  The
        directory appears only once every member is written, and replaces
        any existing directory of that name.  Members whose names collide
        raise :class:`ValueError` before anything is written.
    chunk_size:
        Number of bytes per read chunk (default 1 MiB).
    connect_timeout:
        Timeout in seconds for establishing the connection.
    read_timeout:
        Timeout in seconds for reading the response body.
    progress_callback:
        Optional callback invoked with byte deltas per chunk.
    skip_existing:
        If *True* and the final destination already exists, skip the
        download.  For an ``.mszx`` stored as ``"msz"`` or ``"mzml"``, an
        existing archive directory (see *store_as*) also counts.
    force:
        If *True*, re-download even if the destination already exists
        (overrides *skip_existing*).

    Returns
    -------
    Path
        The final destination path (may differ from *dest* if the
        extension was adjusted for format conversion), or the archive
        directory for a multi-member ``.mszx``.
    """
    plan = StorePlan.for_download(url, dest, store_as)

    # Skip if the output already exists and we're not forcing.
    if not force and skip_existing and (existing := plan.existing()) is not None:
        logger.info("Skipping existing output: %s", existing)
        return existing

    # Ensure the parent directory exists.
    plan.final_dest.parent.mkdir(parents=True, exist_ok=True)

    # Write to a temp .part file to avoid partial downloads.
    part_path = plan.part_path

    timeout = httpx.Timeout(read_timeout, connect=connect_timeout)

    # Throttle progress callbacks to reduce overhead (default: every 8 MiB).
    throttled = (
        ThrottledCallback(progress_callback.on_progress) if progress_callback else None
    )

    async with (
        optional_client(client, timeout=timeout) as c,
        c.stream("GET", url) as resp,
    ):
        resp.raise_for_status()

        async with aiofiles.open(part_path, "wb") as f:
            async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                await f.write(chunk)
                if throttled:
                    throttled(len(chunk))

        if throttled:
            throttled.flush()

    return await plan.finalize(part_path)


async def async_download_batch(
    files: list[DownloadRequest],
    *,
    store_as: DownloadFormat | None = None,
    parallel: int = 4,
    chunk_size: int = 1_048_576,
    connect_timeout: float = 10.0,
    read_timeout: float = 300.0,
    progress: BatchDownloadProgress | None = None,
    skip_existing: bool = False,
    force: bool = False,
    client: httpx.AsyncClient | None = None,
) -> list[Path]:
    """Download multiple files in parallel (async).

    Parameters
    ----------
    files:
        List of :class:`DownloadRequest` objects (url + dest pairs).
    store_as:
        Desired output format — ``"msz"``, ``"mzml"``, ``"mszx"``, or
        *None* to keep files as-is.  Forwarded to each
        :func:`async_download_file` call.
    parallel:
        Maximum number of concurrent downloads (capped to file count).
    chunk_size:
        Number of bytes per read chunk.
    connect_timeout:
        Timeout in seconds for establishing each connection.
    read_timeout:
        Timeout in seconds for reading each response body.
    progress:
        Optional batch progress callback.
    skip_existing:
        If *True*, skip files whose destination already exists.
    force:
        If *True*, re-download even if destination exists.

    Returns
    -------
    list[Path]
        List of final destination paths (in input order, errors excluded).
        A multi-member ``.mszx`` contributes its archive directory.
    """
    workers = min(parallel, len(files))
    sem = asyncio.Semaphore(workers)

    # Pre-allocate results list indexed by position for deterministic ordering.
    results: list[Path | None] = [None] * len(files)

    async def _download_one(idx: int, req: DownloadRequest) -> None:
        filename = req.dest.name

        async with sem:
            # Try to determine total bytes from a HEAD request (best-effort).
            total_bytes: int | None = None
            if progress:
                try:
                    async with optional_client(
                        client, timeout=connect_timeout
                    ) as head_client:
                        head_resp = await head_client.head(
                            req.url, follow_redirects=True
                        )
                        cl = head_resp.headers.get("content-length")
                        if cl is not None:
                            total_bytes = int(cl)
                except Exception:  # noqa: BLE001
                    pass
                progress.on_file_start(filename, total_bytes)

            # Build a per-file progress adapter that implements
            # DownloadProgressCallback by forwarding to BatchDownloadProgress.
            file_progress: _FileProgressAdapter | None = None
            if progress:
                file_progress = _FileProgressAdapter(filename, progress)

            try:
                result = await async_download_file(
                    req.url,
                    req.dest,
                    store_as=store_as,
                    chunk_size=chunk_size,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    progress_callback=file_progress,
                    skip_existing=skip_existing,
                    force=force,
                    client=client,
                )
                results[idx] = result
                if progress:
                    progress.on_file_complete(filename)
            except Exception as exc:
                if progress:
                    progress.on_file_error(filename, exc)
                logger.error("Failed to download %s: %s", filename, exc)

    await asyncio.gather(*[_download_one(i, req) for i, req in enumerate(files)])

    return [r for r in results if r is not None]


def download_file(
    url: str,
    dest: Path,
    *,
    store_as: DownloadFormat | None = None,
    chunk_size: int = 1_048_576,
    connect_timeout: float = 10.0,
    read_timeout: float = 300.0,
    progress_callback: DownloadProgressCallback | None = None,
    skip_existing: bool = False,
    force: bool = False,
    client: httpx.AsyncClient | None = None,
) -> Path:
    """Download a single file from *url* to *dest*.

    This is a synchronous wrapper around :func:`async_download_file`.
    """
    return asyncio.run(
        async_download_file(
            url,
            dest,
            store_as=store_as,
            chunk_size=chunk_size,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            progress_callback=progress_callback,
            skip_existing=skip_existing,
            force=force,
            client=client,
        )
    )


def download_batch(
    files: list[DownloadRequest],
    *,
    store_as: DownloadFormat | None = None,
    parallel: int = 4,
    chunk_size: int = 1_048_576,
    connect_timeout: float = 10.0,
    read_timeout: float = 300.0,
    progress: BatchDownloadProgress | None = None,
    skip_existing: bool = False,
    force: bool = False,
    client: httpx.AsyncClient | None = None,
) -> list[Path]:
    """Download multiple files in parallel.

    This is a synchronous wrapper around :func:`async_download_batch`.
    """
    return asyncio.run(
        async_download_batch(
            files,
            store_as=store_as,
            parallel=parallel,
            chunk_size=chunk_size,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            progress=progress,
            skip_existing=skip_existing,
            force=force,
            client=client,
        )
    )


class _FileProgressAdapter:
    """Adapts :class:`BatchDownloadProgress` to :class:`DownloadProgressCallback`."""

    def __init__(self, filename: str, batch: BatchDownloadProgress) -> None:
        self._filename = filename
        self._batch = batch

    def on_progress(self, bytes_delta: int) -> None:
        self._batch.on_file_progress(self._filename, bytes_delta)
