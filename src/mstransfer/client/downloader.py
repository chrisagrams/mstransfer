from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tarfile
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

import aiofiles
import httpx
from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile
from mscompress.mszx.batch import MSZXBatchFile, read_archive_manifest

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


def _extract_msz_entry(tar: tarfile.TarFile, entry: str, dst: Path) -> None:
    """Stream the MSZ member *entry* of an open .mszx tar to *dst*."""
    member = tar.extractfile(entry)
    if member is None:
        raise ValueError(f"Missing MSZ entry {entry!r}")
    with member, dst.open("wb") as out:
        shutil.copyfileobj(member, out)


def _mszx_to_msz(src: Path, dst: Path) -> None:
    """Extract the embedded .msz payload from an .mszx archive."""
    entries = read_archive_manifest(src).spectra_files
    if len(entries) != 1:
        raise NotImplementedError(
            f"Cannot store a {len(entries)}-member .mszx as a single .msz"
        )
    with tarfile.open(src) as tar:
        _extract_msz_entry(tar, entries[0].entry, dst)


def _member_stem(entry: str) -> str:
    """Return the output stem for archive *entry*, without any directory part.

    Raises `ValueError` for names that would be empty or hidden.
    """
    # PureWindowsPath splits on both "/" and "\\", so no entry can name a
    # path outside the archive directory on any platform.
    name = PureWindowsPath(entry).name
    if name.lower().endswith(".msz"):
        name = name[: -len(".msz")]
    if not name or name.startswith("."):
        raise ValueError(f"Unsafe .mszx entry name {entry!r}")
    return name


def _mszx_member_targets(
    entries: list[str], store_as: DownloadFormat
) -> list[tuple[str, str]]:
    """Map each .mszx spectra entry to an output file name.

    Raises `ValueError` if two entries map to the same output name.
    """
    targets: list[tuple[str, str]] = []
    seen: dict[str, str] = {}
    for entry in entries:
        name = f"{_member_stem(entry)}{_FORMAT_EXT[store_as]}"
        # Compare case-insensitively: macOS and Windows filesystems are.
        key = name.casefold()
        if key in seen:
            raise ValueError(
                f"Archive entries {seen[key]!r} and {entry!r} both map to {name}"
            )
        seen[key] = entry
        targets.append((entry, name))
    return targets


def _mszx_extract_members(
    src: Path, targets: list[tuple[str, Path]], store_as: DownloadFormat
) -> None:
    """Write each (entry, path) in *targets* out of *src* as msz or mzML."""
    if store_as == "msz":
        with tarfile.open(src) as tar:
            for entry, target in targets:
                _extract_msz_entry(tar, entry, target)
    else:
        with MSZXBatchFile.open(src) as batch:
            for entry, target in targets:
                batch.get(entry).decompress(str(target))


def _replace_dir(staging: Path, target: Path) -> None:
    """Move directory *staging* to *target*, replacing any existing directory.

    An existing *target* is moved aside first and restored if the swap
    fails, so *target* is never left half-written.
    """
    if not target.exists():
        staging.rename(target)
        return
    if not target.is_dir():
        raise FileExistsError(f"{target} exists and is not a directory")
    backup = target.with_name(f".{target.name}.{uuid.uuid4().hex}.old")
    target.rename(backup)
    try:
        staging.rename(target)
    except BaseException:
        backup.rename(target)
        raise
    shutil.rmtree(backup, ignore_errors=True)


async def _store_mszx_members(
    src: Path,
    targets: list[tuple[str, str]],
    archive_dir: Path,
    store_as: DownloadFormat,
) -> Path:
    """Store every member of a multi-member .mszx in *archive_dir*.

    Members are extracted into a uniquely named staging directory that is
    renamed to *archive_dir* only once all of them succeed, so a failure
    leaves no partial output behind and concurrent downloads never share
    staging files.
    """
    staging = Path(
        tempfile.mkdtemp(
            dir=archive_dir.parent, prefix=f".{archive_dir.name}.", suffix=".part"
        )
    )
    try:
        staged = [(entry, staging / name) for entry, name in targets]
        await asyncio.to_thread(_mszx_extract_members, src, staged, store_as)
        _replace_dir(staging, archive_dir)
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    logger.info(
        "Extracted %d members from %s into %s", len(targets), src.name, archive_dir
    )
    return archive_dir


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

    Returns `"msz"`, `"mzml"`, `"mszx"`, or *None* if the format
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

    The file is streamed to a temporary `.part` file first, then atomically
    renamed (or converted) to the final destination on success.

    Args:
        url: The URL to download from.
        dest: The local path where the file should be saved.  If *store_as*
            requires a format conversion, the extension will be adjusted
            automatically.
        store_as: Desired output format — `"msz"`, `"mzml"`, `"mszx"`, or
            *None* to keep the file as-is.  When set, the downloaded file
            is converted after download using **mscompress**.  Conversion
            *to* `"mszx"` is only supported when the source is already
            `.mszx` (no-op).  When a multi-member (batch) `.mszx` is
            stored as `"msz"` or `"mzml"`, its members are written to a
            directory named after the archive,
            `dest.parent/<dest stem>/<entry stem>.<ext>`.  The directory
            appears only once every member is written, and replaces any
            existing directory of that name.
        chunk_size: Number of bytes per read chunk (default 1 MiB).
        connect_timeout: Timeout in seconds for establishing the connection.
        read_timeout: Timeout in seconds for reading the response body.
        progress_callback: Optional callback invoked with byte deltas per
            chunk.
        skip_existing: If *True* and the final destination already exists,
            skip the download.  For an `.mszx` stored as `"msz"` or
            `"mzml"`, an existing archive directory (see *store_as*) also
            counts.
        force: If *True*, re-download even if the destination already exists
            (overrides *skip_existing*).
        client: Optional `httpx.AsyncClient` to reuse; a temporary one is
            created and closed otherwise.

    Returns:
        The final destination path (may differ from *dest* if the extension
        was adjusted for format conversion), or the archive directory for a
        multi-member `.mszx`.

    Raises:
        NotImplementedError: The source cannot be converted to *store_as*.
        ValueError: Two members of a multi-member `.mszx` map to the same
            output name; raised before anything is written.
    """
    source_fmt = _detect_source_format(url)
    final_dest = _resolve_dest(dest, store_as, source_fmt)

    # Fail fast on unsupported conversion combinations before any I/O.
    if store_as == "mszx" and source_fmt is not None and source_fmt != "mszx":
        raise NotImplementedError(
            f"Conversion from {source_fmt} to mszx is not supported"
        )

    # Where the members of a multi-member .mszx go, if this turns out to be one.
    archive_dir = final_dest.with_suffix("")
    may_expand = source_fmt == "mszx" and store_as in ("msz", "mzml")

    # Skip if file already exists and we're not forcing.
    if not force and skip_existing:
        if final_dest.exists():
            logger.info("Skipping existing file: %s", final_dest)
            return final_dest
        # The archive directory only ever appears complete, so it is safe
        # to trust without downloading the archive again.
        if may_expand and archive_dir.is_dir():
            logger.info("Skipping existing archive directory: %s", archive_dir)
            return archive_dir

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
        if may_expand:
            manifest = await asyncio.to_thread(read_archive_manifest, tmp_source)
            entries = [spectra.entry for spectra in manifest.spectra_files]
            if len(entries) > 1:
                targets = _mszx_member_targets(entries, store_as)
                return await _store_mszx_members(
                    tmp_source, targets, archive_dir, store_as
                )
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
    connect_timeout: float = 10.0,
    read_timeout: float = 300.0,
    progress: BatchDownloadProgress | None = None,
    skip_existing: bool = False,
    force: bool = False,
    client: httpx.AsyncClient | None = None,
) -> list[Path]:
    """Download multiple files in parallel (async).

    Args:
        files: List of `DownloadRequest` objects (url + dest pairs).
        store_as: Desired output format — `"msz"`, `"mzml"`, `"mszx"`, or
            *None* to keep files as-is.  Forwarded to each
            `async_download_file` call.
        parallel: Maximum number of concurrent downloads (capped to file
            count).
        chunk_size: Number of bytes per read chunk.
        connect_timeout: Timeout in seconds for establishing each connection.
        read_timeout: Timeout in seconds for reading each response body.
        progress: Optional batch progress callback.
        skip_existing: If *True*, skip files whose destination already exists.
        force: If *True*, re-download even if destination exists.
        client: Optional `httpx.AsyncClient` shared by every download; a
            temporary one is created per request otherwise.

    Returns:
        List of final destination paths (in input order, errors excluded).
        A multi-member `.mszx` contributes its archive directory.
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

    This is a synchronous wrapper around `async_download_file`.
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

    This is a synchronous wrapper around `async_download_batch`.
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
    """Adapts `BatchDownloadProgress` to `DownloadProgressCallback`."""

    def __init__(self, filename: str, batch: BatchDownloadProgress) -> None:
        self._filename = filename
        self._batch = batch

    def on_progress(self, bytes_delta: int) -> None:
        self._batch.on_file_progress(self._filename, bytes_delta)
