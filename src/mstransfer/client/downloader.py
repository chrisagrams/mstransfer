from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

import aiofiles
import httpx
from filelock import BaseFileLock, FileLock, Timeout
from pydantic import BaseModel, Field, ValidationError

from mstransfer.client.results import FileResult
from mstransfer.client.utils import ThrottledCallback, optional_client
from mstransfer.store import DownloadFormat, StorePlan

__all__ = [
    "BatchDownloadProgress",
    "DownloadFormat",
    "DownloadProgressCallback",
    "DownloadRequest",
    "DownloadResult",
    "async_download_batch",
    "async_download_file",
    "download_batch",
    "download_file",
]

logger = logging.getLogger(__name__)


@runtime_checkable
class DownloadProgressCallback(Protocol):
    """Callback protocol for observing single-file download progress."""

    def on_total(self, total_bytes: int | None) -> None: ...
    def on_progress(self, bytes_delta: int) -> None: ...


@runtime_checkable
class BatchDownloadProgress(Protocol):
    """Callback protocol for observing batch download progress."""

    def on_file_start(self, filename: str, total_bytes: int | None) -> None: ...
    def on_file_total(self, filename: str, total_bytes: int | None) -> None: ...
    def on_file_progress(self, filename: str, bytes_delta: int) -> None: ...
    def on_file_complete(self, filename: str) -> None: ...
    def on_file_error(self, filename: str, error: Exception) -> None: ...


@dataclass
class DownloadRequest:
    """A single download request: URL to fetch and local destination path."""

    url: str
    dest: Path


@dataclass
class DownloadResult:
    """A completed download: the request and where its output was written."""

    request: DownloadRequest
    path: Path


class _PartialInfo(BaseModel):
    """What a resumable .part file holds: the start of *url*'s *total*-byte body.

    *etag* is the strong ``ETag`` the body was sent with, so a resumed range
    can be checked against it.
    """

    url: str
    etag: str
    total: int = Field(gt=0)


async def async_download_file(
    url: str,
    dest: Path,
    *,
    store_as: DownloadFormat | None = None,
    chunk_size: int = 1_048_576,
    timeout: httpx.Timeout | None = None,
    progress_callback: DownloadProgressCallback | None = None,
    skip_existing: bool = False,
    force: bool = False,
    resume: bool = True,
    api_key: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> Path:
    """Download a single file from *url* to *dest* (async).

    The file is streamed to a temporary ``.part`` file first, then atomically
    renamed (or converted) to the final destination on success.

    With *resume*, an interrupted download keeps its partial ``.part`` file
    (and a ``.part.json`` describing it) next to *dest*, and a later call
    continues from the bytes already received.  A partial is only kept when
    the server sent a strong ``ETag`` and the body size, and it is only
    resumed with an ``If-Range`` request carrying that ``ETag``.  If the
    server does not support ranges, or the file changed, the download
    restarts from zero.  The finished file must have
    exactly the expected size.

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
    timeout:
        Timeouts for the request, replacing *client*'s; pass
        ``httpx.Timeout(None)`` for no limit.  If *None*, *client*'s
        timeouts are used (10 s connect, 300 s otherwise, without
        *client*).
    progress_callback:
        Optional callback.  ``on_total`` is invoked once the server
        responds, with the file size (or *None* if unknown); ``on_progress``
        is invoked with byte deltas per chunk.  A resumed download first
        reports the bytes it already had as one delta.
    skip_existing:
        If *True* and the final destination already exists, skip the
        download.  For an ``.mszx`` stored as ``"msz"`` or ``"mzml"``, an
        existing archive directory (see *store_as*) also counts.
    force:
        If *True*, re-download even if the destination already exists
        (overrides *skip_existing*), discarding any partial download.
    resume:
        If *True* (the default), resume a partial download of *url* to
        *dest* and keep this one's partial if it is interrupted.  Only one
        download to *dest* at a time can do so; a concurrent one uses a
        private ``.part`` file that is never resumed.  If *False*,
        start from zero and keep nothing on failure.
    api_key:
        Optional API key, sent as ``Authorization: Bearer <key>``.
    client:
        Optional :class:`httpx.AsyncClient` to reuse.

    Returns
    -------
    Path
        The final destination path (may differ from *dest* if the
        extension was adjusted for format conversion), or the archive
        directory for a multi-member ``.mszx``.

    Raises
    ------
    ValueError
        If the downloaded size does not match the size the server announced,
        or the server sent part of the file when it was not asked to.
    """
    plan = StorePlan.for_download(url, dest, store_as)

    # Skip if the output already exists and we're not forcing.
    if not force and skip_existing and (existing := plan.existing()) is not None:
        logger.info("Skipping existing output: %s", existing)
        return existing

    # Ensure the parent directory exists.
    plan.final_dest.parent.mkdir(parents=True, exist_ok=True)

    # A resumable download streams to plan.part_path, described by a metadata
    # file, while it holds an exclusive lock, so only one download to *dest* at
    # a time owns them.  Any other download writes to a unique .part file that
    # is never resumed, so concurrent downloads to one dest never share it.
    part_path = plan.unique_part_path()
    meta_path = plan.part_path.with_name(plan.part_path.name + ".json")
    lock: BaseFileLock | None = None
    if resume:
        candidate = FileLock(
            plan.part_path.with_name(plan.part_path.name + ".lock"), timeout=0
        )
        try:
            candidate.acquire()
        except Timeout:
            pass
        else:
            lock = candidate
            part_path = plan.part_path

    # What a partial from an earlier attempt claims to be: the bytes of a
    # *total*-byte body of *url*, as identified by *etag*.
    offset = 0
    etag: str | None = None
    total: int | None = None
    expected_range = ""
    if lock is not None and not force:
        info: _PartialInfo | None
        try:
            info = _PartialInfo.model_validate_json(meta_path.read_text())
            offset = part_path.stat().st_size
        except (OSError, ValidationError):
            info = None
        if info is not None and info.url == url and 0 < offset < info.total:
            etag, total = info.etag, info.total
            expected_range = f"bytes {offset}-{total - 1}/{total}"
        else:
            offset = 0

    # Whether the .part file is worth keeping if this attempt fails.
    retain = offset > 0

    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    # Throttle progress callbacks to reduce overhead (default: every 8 MiB).
    throttled = (
        ThrottledCallback(progress_callback.on_progress) if progress_callback else None
    )

    try:
        async with optional_client(
            client, timeout=httpx.Timeout(300.0, connect=10.0)
        ) as c:
            while True:
                request_headers = dict(headers)
                if offset:
                    request_headers["Range"] = f"bytes={offset}-"
                    request_headers["If-Range"] = str(etag)
                async with c.stream(
                    "GET",
                    url,
                    headers=request_headers,
                    timeout=httpx.USE_CLIENT_DEFAULT if timeout is None else timeout,
                ) as resp:
                    # Content-Length counts encoded bytes, but the body is read
                    # decoded, so neither its size nor byte ranges can be used.
                    encoded = "content-encoding" in resp.headers
                    resp_etag = resp.headers.get("etag")
                    if resp_etag and resp_etag.startswith("W/"):
                        resp_etag = None  # If-Range needs a strong ETag.

                    if offset and resp.status_code == 416:
                        # The server no longer has our range: start over.
                        logger.info("Range rejected for %s, restarting", url)
                        offset, retain = 0, False
                        continue
                    resp.raise_for_status()

                    if offset and resp.status_code == 206:
                        # The range must continue our partial of the same file.
                        if (
                            encoded
                            or resp_etag != etag
                            or resp.headers.get("content-range") != expected_range
                        ):
                            logger.info("Partial of %s is stale, restarting", url)
                            offset, retain = 0, False
                            continue
                        logger.info("Resuming %s from byte %d", url, offset)
                    else:
                        if resp.status_code == 206 and not offset:
                            raise ValueError(
                                f"Got part of {url} without asking for a range"
                            )
                        # A full body, either asked for or because the server
                        # ignored the range or the file changed.
                        if offset:
                            logger.info("Cannot resume %s, restarting", url)
                        offset = 0
                        # Drop any old bytes before the metadata describes the
                        # new body, so a failure in between cannot leave them
                        # labelled as its start.
                        part_path.unlink(missing_ok=True)
                        cl = resp.headers.get("content-length")
                        total = int(cl) if cl and not encoded else None
                        etag = resp_etag
                        # An empty body has nothing to resume.
                        retain = False
                        if lock is not None and etag is not None and total:
                            meta_path.write_text(
                                _PartialInfo(
                                    url=url, etag=etag, total=total
                                ).model_dump_json()
                            )
                            retain = True

                    if progress_callback:
                        progress_callback.on_total(total)
                        if offset:
                            progress_callback.on_progress(offset)

                    size = offset
                    async with aiofiles.open(part_path, "ab" if offset else "wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                            await f.write(chunk)
                            size += len(chunk)
                            if throttled:
                                throttled(len(chunk))

                    if throttled:
                        throttled.flush()
                break

        if total is not None and size != total:
            retain = False
            raise ValueError(f"Downloaded {size} bytes of {url}, but expected {total}")

        return await plan.finalize(part_path)
    finally:
        # Keep a resumable partial for a later attempt.  Anything else, like
        # the .part of a cancelled download without an ETag, is dead
        # weight.  Cancellation is covered too, as it is not an Exception.
        try:
            try:
                keep = retain and part_path.stat().st_size > 0
            except FileNotFoundError:
                keep = False
            if not keep:
                part_path.unlink(missing_ok=True)
                if lock is not None:
                    meta_path.unlink(missing_ok=True)
        finally:
            # Release even if cleanup fails, so the lock is never left held.
            if lock is not None:
                # filelock tries to remove the lock file after release on
                # Windows, but not on Unix, so remove it here while still
                # holding the lock.  A download that opened the old file
                # meanwhile then fails to lock it, as filelock rejects a lock
                # on an unlinked file.  A file left behind is harmless: a later
                # download removes it.
                if sys.platform != "win32":
                    with contextlib.suppress(OSError):
                        Path(lock.lock_file).unlink(missing_ok=True)
                lock.release()


async def async_download_batch(
    files: list[DownloadRequest],
    *,
    store_as: DownloadFormat | None = None,
    parallel: int = 4,
    chunk_size: int = 1_048_576,
    timeout: httpx.Timeout | None = None,
    progress: BatchDownloadProgress | None = None,
    skip_existing: bool = False,
    force: bool = False,
    resume: bool = True,
    api_key: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> list[FileResult[DownloadResult]]:
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
    timeout:
        Timeouts for each request, replacing *client*'s; pass
        ``httpx.Timeout(None)`` for no limit.  If *None*, *client*'s
        timeouts are used (10 s connect, 300 s otherwise, without
        *client*).
    progress:
        Optional batch progress callback.
    skip_existing:
        If *True*, skip files whose destination already exists.
    force:
        If *True*, re-download even if destination exists.
    resume:
        If *True* (the default), resume partial downloads and keep
        resumable partials of failed ones (see :func:`async_download_file`).
    api_key:
        Optional API key, sent as ``Authorization: Bearer <key>``.
    client:
        Optional :class:`httpx.AsyncClient` shared by all downloads.

    Returns
    -------
    list[FileResult[DownloadResult]]
        One result per request, in input order, named after the request's
        *dest*, carrying either a :class:`DownloadResult` or the error.  A
        multi-member ``.mszx`` yields its archive directory as the path.
    """
    workers = min(parallel, len(files))
    sem = asyncio.Semaphore(workers)

    # Pre-allocate results list indexed by position for deterministic ordering.
    results: list[FileResult[DownloadResult]] = [
        FileResult(filename=req.dest.name) for req in files
    ]

    async def _download_one(idx: int, req: DownloadRequest) -> None:
        filename = req.dest.name

        async with sem:
            # The total is unknown until the GET response arrives.
            if progress:
                progress.on_file_start(filename, None)

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
                    timeout=timeout,
                    progress_callback=file_progress,
                    skip_existing=skip_existing,
                    force=force,
                    resume=resume,
                    api_key=api_key,
                    client=client,
                )
            except Exception as exc:
                results[idx].error = exc
                if progress:
                    progress.on_file_error(filename, exc)
                logger.error("Failed to download %s: %s", filename, exc)
            else:
                results[idx].response = DownloadResult(request=req, path=result)
                # A raising callback must not turn a finished download into
                # a failure, or abort the rest of the batch.
                if progress:
                    try:
                        progress.on_file_complete(filename)
                    except Exception:
                        logger.exception("on_file_complete failed for %s", filename)

    await asyncio.gather(*[_download_one(i, req) for i, req in enumerate(files)])

    return results


def download_file(
    url: str,
    dest: Path,
    *,
    store_as: DownloadFormat | None = None,
    chunk_size: int = 1_048_576,
    timeout: httpx.Timeout | None = None,
    progress_callback: DownloadProgressCallback | None = None,
    skip_existing: bool = False,
    force: bool = False,
    resume: bool = True,
    api_key: str | None = None,
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
            timeout=timeout,
            progress_callback=progress_callback,
            skip_existing=skip_existing,
            force=force,
            resume=resume,
            api_key=api_key,
            client=client,
        )
    )


def download_batch(
    files: list[DownloadRequest],
    *,
    store_as: DownloadFormat | None = None,
    parallel: int = 4,
    chunk_size: int = 1_048_576,
    timeout: httpx.Timeout | None = None,
    progress: BatchDownloadProgress | None = None,
    skip_existing: bool = False,
    force: bool = False,
    resume: bool = True,
    api_key: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> list[FileResult[DownloadResult]]:
    """Download multiple files in parallel.

    This is a synchronous wrapper around :func:`async_download_batch`.
    """
    return asyncio.run(
        async_download_batch(
            files,
            store_as=store_as,
            parallel=parallel,
            chunk_size=chunk_size,
            timeout=timeout,
            progress=progress,
            skip_existing=skip_existing,
            force=force,
            resume=resume,
            api_key=api_key,
            client=client,
        )
    )


class _FileProgressAdapter:
    """Adapts :class:`BatchDownloadProgress` to :class:`DownloadProgressCallback`."""

    def __init__(self, filename: str, batch: BatchDownloadProgress) -> None:
        self._filename = filename
        self._batch = batch

    def on_total(self, total_bytes: int | None) -> None:
        self._batch.on_file_total(self._filename, total_bytes)

    def on_progress(self, bytes_delta: int) -> None:
        self._batch.on_file_progress(self._filename, bytes_delta)
