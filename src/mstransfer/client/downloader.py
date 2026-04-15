from __future__ import annotations

import asyncio
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable

import aiofiles
import httpx
from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile

from mstransfer.client.utils import ThrottledCallback, optional_client

DownloadFormat = Literal["msz", "mzml", "mszx"]
"""Output formats the client can produce on disk after download."""

logger = logging.getLogger(__name__)

_FORMAT_EXT: dict[DownloadFormat, str] = {
    "msz": ".msz",
    "mzml": ".mzML",
    "mszx": ".mszx",
}


def _detect_source_format(url: str) -> DownloadFormat | None:
    """Infer the source format from the URL filename extension.

    Returns ``"msz"``, ``"mzml"``, ``"mszx"``, or *None* if the format
    cannot be determined.
    """
    # Strip query params and fragments before inspecting extension.
    path = url.split("?", 1)[0].split("#", 1)[0]
    ext = Path(path).suffix.lower()
    if ext == ".msz":
        return "msz"
    if ext == ".mzml":
        return "mzml"
    if ext == ".mszx":
        return "mszx"
    return None


def _resolve_dest(
    dest: Path,
    store_as: DownloadFormat | None,
    source_fmt: DownloadFormat | None,
) -> Path:
    """Adjust *dest* extension when *store_as* differs from the source."""
    if store_as is None or source_fmt is None or store_as == source_fmt:
        return dest
    return dest.with_suffix(_FORMAT_EXT[store_as])


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
        download.
    force:
        If *True*, re-download even if the destination already exists
        (overrides *skip_existing*).

    Returns
    -------
    Path
        The final destination path (may differ from *dest* if the
        extension was adjusted for format conversion).
    """
    source_fmt = _detect_source_format(url)
    final_dest = _resolve_dest(dest, store_as, source_fmt)

    # Fail fast on unsupported conversion combinations before any I/O.
    if store_as == "mszx" and source_fmt is not None and source_fmt != "mszx":
        raise NotImplementedError(
            f"Conversion from {source_fmt} to mszx is not supported"
        )

    # Skip if file already exists and we're not forcing.
    if not force and skip_existing and final_dest.exists():
        logger.info("Skipping existing file: %s", final_dest)
        return final_dest

    # Ensure the parent directory exists.
    final_dest.parent.mkdir(parents=True, exist_ok=True)

    # Write to a temp .part file to avoid partial downloads.
    part_path = dest.with_suffix(dest.suffix + ".part")

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

    # No conversion needed — atomic rename to final destination.
    if store_as is None or source_fmt is None or store_as == source_fmt:
        part_path.rename(final_dest)
        return final_dest

    # Conversion required — give the temp file the correct source extension
    # so mscompress can recognise its format, then convert.
    tmp_source = part_path.with_suffix(_FORMAT_EXT[source_fmt])
    part_path.rename(tmp_source)

    # Stage converted output; atomically rename to final_dest on success.
    staging = final_dest.with_suffix(final_dest.suffix + ".part")

    try:
        if source_fmt == "msz" and store_as == "mzml":
            msz = MSZFile(str(tmp_source).encode())
            await asyncio.to_thread(msz.decompress, str(staging))
        elif source_fmt == "mzml" and store_as == "msz":
            mzml = MZMLFile(str(tmp_source).encode())
            await asyncio.to_thread(mzml.compress, str(staging))
        elif source_fmt == "mszx" and store_as == "mzml":

            def _decompress_mszx() -> None:
                with MSZXFile.open(tmp_source) as mszx:
                    mszx.decompress(str(staging))

            await asyncio.to_thread(_decompress_mszx)
        elif source_fmt == "mszx" and store_as == "msz":

            def _extract_inner_msz() -> None:
                with MSZXFile.open(tmp_source) as mszx:
                    shutil.move(mszx.path.decode(), staging)

            await asyncio.to_thread(_extract_inner_msz)
        else:
            raise NotImplementedError(
                f"Conversion from {source_fmt} to {store_as} is not supported"
            )
        staging.replace(final_dest)
        logger.info("Converted %s → %s", tmp_source.name, final_dest.name)
    finally:
        if staging.exists():
            staging.unlink()
        if tmp_source.exists():
            os.remove(tmp_source)

    return final_dest


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
