from __future__ import annotations

import asyncio
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

import aiofiles
import httpx
from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile

from mstransfer.client.utils import ThrottledCallback, optional_client

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# Output formats the client can produce on disk after download.
DownloadFormat = Literal["msz", "mzml", "mszx"]

# Mapping of format to canonical file extension for naming converted outputs.
_FORMAT_EXT: dict[DownloadFormat, str] = {
    "msz": ".msz",
    "mzml": ".mzML",
    "mszx": ".mszx",
}

# Reverse mapping of lowercase extension to format for source format inference.
_EXT_FORMAT: dict[str, DownloadFormat] = {v.lower(): k for k, v in _FORMAT_EXT.items()}


def _msz_to_mzml(src: Path, dst: Path) -> None:
    """Lambda wrapper msz -> mzml for use in async thread executor."""
    MSZFile(str(src).encode()).decompress(str(dst))


def _mzml_to_msz(src: Path, dst: Path) -> None:
    """Lambda wrapper mzml -> msz for use in async thread executor."""
    MZMLFile(str(src).encode()).compress(str(dst))


def _mszx_to_mzml(src: Path, dst: Path) -> None:
    """Lambda wrapper mszx -> mzml for use in async thread executor."""
    with MSZXFile.open(src) as mszx:
        mszx.decompress(str(dst))


def _mszx_to_msz(src: Path, dst: Path) -> None:
    """Lambda wrapper mszx -> msz for use in async thread executor."""
    with MSZXFile.open(src) as mszx:
        shutil.move(mszx.path.decode(), dst)


# Mapping of (source_format, target_format) to converter function.
_CONVERTERS: dict[
    tuple[DownloadFormat, DownloadFormat], Callable[[Path, Path], None]
] = {
    ("msz", "mzml"): _msz_to_mzml,
    ("mzml", "msz"): _mzml_to_msz,
    ("mszx", "mzml"): _mszx_to_mzml,
    ("mszx", "msz"): _mszx_to_msz,
}


def _detect_source_format(url: str) -> DownloadFormat | None:
    """Infer the source format from the URL filename extension.

    Returns ``"msz"``, ``"mzml"``, ``"mszx"``, or *None* if the format
    cannot be determined.
    """
    # Strip query params and fragments before inspecting extension.
    path = url.split("?", 1)[0].split("#", 1)[0]
    return _EXT_FORMAT.get(Path(path).suffix.lower())


def _resolve_dest(
    dest: Path,
    store_as: DownloadFormat | None,
    source_fmt: DownloadFormat | None,
) -> Path:
    """Adjust *dest* extension when *store_as* differs from the source."""
    if store_as is None or source_fmt is None or store_as == source_fmt:
        return dest
    return dest.with_suffix(_FORMAT_EXT[store_as])


class PartialDownloadError(RuntimeError):
    """Raised when a ranged part does not match the requested byte range."""


def _parse_content_range(value: str | None) -> tuple[int, int, int] | None:
    """Parse ``bytes start-end/size`` into a tuple, or *None* if invalid."""
    if not value:
        return None
    unit, _, spec = value.strip().partition(" ")
    span, _, total = spec.partition("/")
    first, _, last = span.partition("-")
    if unit.lower() != "bytes" or not (
        first.isdigit() and last.isdigit() and total.isdigit()
    ):
        return None
    return int(first), int(last), int(total)


def _split_ranges(size: int, parts: int) -> list[tuple[int, int]]:
    """Split *size* bytes into *parts* contiguous inclusive byte ranges."""
    step = -(-size // parts)  # ceil division
    return [(s, min(s + step, size) - 1) for s in range(0, size, step)]


async def _write_stream(
    resp: httpx.Response,
    part_path: Path,
    chunk_size: int,
    throttled: ThrottledCallback | None,
) -> None:
    """Stream the full body of *resp* into *part_path*."""
    async with aiofiles.open(part_path, "wb") as f:
        async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
            await f.write(chunk)
            if throttled:
                throttled(len(chunk))


async def _download_range(
    client: httpx.AsyncClient,
    url: str,
    part_path: Path,
    start: int,
    end: int,
    chunk_size: int,
    throttled: ThrottledCallback | None,
) -> None:
    """Fetch bytes ``start..end`` (inclusive) and write them at *start*."""
    expected = end - start + 1
    written = 0
    headers = {"Range": f"bytes={start}-{end}"}
    async with client.stream("GET", url, headers=headers) as resp:
        resp.raise_for_status()
        content_range = _parse_content_range(resp.headers.get("content-range"))
        if resp.status_code != 206 or content_range is None:
            raise PartialDownloadError(
                f"Server did not honor range {start}-{end} (status {resp.status_code})"
            )
        if content_range[:2] != (start, end):
            raise PartialDownloadError(
                f"Requested range {start}-{end}, got "
                f"{content_range[0]}-{content_range[1]}"
            )

        # Each part has its own handle so writes at different offsets
        # never share a file position.
        async with aiofiles.open(part_path, "r+b") as f:
            await f.seek(start)
            async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                written += len(chunk)
                if written > expected:
                    raise PartialDownloadError(
                        f"Range {start}-{end} returned more than {expected} bytes"
                    )
                await f.write(chunk)
                if throttled:
                    throttled(len(chunk))

    if written != expected:
        raise PartialDownloadError(
            f"Range {start}-{end} returned {written} of {expected} bytes"
        )


async def _download_split(
    client: httpx.AsyncClient,
    url: str,
    part_path: Path,
    size: int,
    parts: int,
    chunk_size: int,
    throttled: ThrottledCallback | None,
) -> None:
    """Download *url* as *parts* concurrent ranged GETs into *part_path*."""
    # Preallocate so every part can seek to its offset independently.
    async with aiofiles.open(part_path, "wb") as f:
        await f.truncate(size)

    tasks = [
        asyncio.ensure_future(
            _download_range(client, url, part_path, start, end, chunk_size, throttled)
        )
        for start, end in _split_ranges(size, parts)
    ]

    # Fail fast: on the first error, cancel the remaining parts.
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    for t in done:
        exc = t.exception()
        if exc is not None:
            raise exc


async def _fetch_to_part(
    client: httpx.AsyncClient,
    url: str,
    part_path: Path,
    *,
    connections: int,
    min_part_size: int,
    chunk_size: int,
    throttled: ThrottledCallback | None,
) -> None:
    """Download *url* into *part_path*, splitting across connections if possible.

    With ``connections > 1`` a ``Range: bytes=0-0`` probe discovers the
    file size and whether the server supports ranges.  If the server
    ignores the range (``200``), that response is consumed directly as
    the single-stream download so no extra request is made.
    """
    if connections > 1:
        size: int | None = None
        async with client.stream("GET", url, headers={"Range": "bytes=0-0"}) as resp:
            if resp.status_code != 416:
                resp.raise_for_status()
            if resp.status_code == 200:
                logger.debug("Server ignored Range for %s; single stream", url)
                await _write_stream(resp, part_path, chunk_size, throttled)
                return
            if resp.status_code == 206:
                content_range = _parse_content_range(resp.headers.get("content-range"))
                if content_range is not None:
                    size = content_range[2]

        parts = min(connections, size // min_part_size) if size else 0
        if size is not None and parts >= 2:
            logger.debug("Downloading %s in %d parts (%d bytes)", url, parts, size)
            await _download_split(
                client, url, part_path, size, parts, chunk_size, throttled
            )
            return

    async with client.stream("GET", url) as resp:
        resp.raise_for_status()
        await _write_stream(resp, part_path, chunk_size, throttled)


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
    connections: int = 1,
    min_part_size: int = 8_388_608,
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
    connections:
        Number of concurrent HTTP connections used to fetch this file
        via ``Range`` requests (default 1, a single stream).  Falls back
        to a single stream if the server does not support ranges or the
        file is too small to split.
    min_part_size:
        Minimum bytes per ranged part (default 8 MiB); limits how many
        connections a small file is split across.
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
    if connections < 1:
        raise ValueError(f"connections must be >= 1, got {connections}")
    if min_part_size < 1:
        raise ValueError(f"min_part_size must be >= 1, got {min_part_size}")

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

    try:
        async with optional_client(client, timeout=timeout) as c:
            await _fetch_to_part(
                c,
                url,
                part_path,
                connections=connections,
                min_part_size=min_part_size,
                chunk_size=chunk_size,
                throttled=throttled,
            )
    except BaseException:
        part_path.unlink(missing_ok=True)
        raise

    if throttled:
        throttled.flush()

    # No conversion needed — atomic rename to final destination.
    if store_as is None or source_fmt is None or store_as == source_fmt:
        part_path.rename(final_dest)
        return final_dest

    converter = _CONVERTERS.get((source_fmt, store_as))
    if converter is None:
        part_path.unlink(missing_ok=True)
        raise NotImplementedError(
            f"Conversion from {source_fmt} to {store_as} is not supported"
        )

    # Give the temp file the correct source extension so mscompress can
    # recognise its format, then convert.
    tmp_source = part_path.with_suffix(_FORMAT_EXT[source_fmt])
    part_path.rename(tmp_source)

    # Stage converted output; atomically rename to final_dest on success.
    staging = final_dest.with_suffix(final_dest.suffix + ".part")

    try:
        await asyncio.to_thread(converter, tmp_source, staging)
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
    connections: int = 1,
    min_part_size: int = 8_388_608,
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
    connections:
        Concurrent ranged connections *per file*, independent of
        *parallel*.  See :func:`async_download_file`.
    min_part_size:
        Minimum bytes per ranged part.
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
                        # Ignore error responses (e.g. 405 for GET-only
                        # routes) whose Content-Length is the error body.
                        cl = head_resp.headers.get("content-length")
                        if head_resp.is_success and cl is not None:
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
                    connections=connections,
                    min_part_size=min_part_size,
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
    connections: int = 1,
    min_part_size: int = 8_388_608,
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
            connections=connections,
            min_part_size=min_part_size,
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
    connections: int = 1,
    min_part_size: int = 8_388_608,
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
            connections=connections,
            min_part_size=min_part_size,
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
