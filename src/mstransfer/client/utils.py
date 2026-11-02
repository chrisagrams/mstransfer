from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile
from mscompress.utils import detect_filetype

from mstransfer.store import MSFormat

logger = logging.getLogger(__name__)


def resolve_inputs(paths: list[str], recursive: bool = False) -> list[Path]:
    """Resolve files and directories into a sorted list of valid MS files."""
    result: list[Path] = []
    for p in paths:
        path = Path(p)
        if path.is_file():
            if MSFormat.from_path(path.name) is not None:
                result.append(path)
            else:
                logger.warning("Skipping unsupported file: %s", path)
        elif path.is_dir():
            entries = path.rglob("*") if recursive else path.glob("*")
            result.extend(
                entry
                for entry in entries
                if entry.is_file() and MSFormat.from_path(entry.name) is not None
            )
        else:
            logger.warning("Path does not exist: %s", path)
    if not result:
        raise FileNotFoundError(
            "No valid .mzML, .msz or .mszx files found in the given paths"
        )
    return sorted(set(result))


def normalize_source(
    source: Path | MZMLFile | MSZFile | MSZXFile,
) -> tuple[Path, MSFormat, MZMLFile | None]:
    """Return ``(file_path, format, mzml_obj | None)`` from *source*.

    For a :class:`~pathlib.Path`, the format is detected from the file
    contents rather than its extension.
    """
    if isinstance(source, MZMLFile):
        return Path(source.path.decode()), MSFormat.MZML, source
    # MSZXFile subclasses MSZFile, so it must be checked first.
    if isinstance(source, MSZXFile):
        return source.archive_path, MSFormat.MSZX, None
    if isinstance(source, MSZFile):
        return Path(source.path.decode()), MSFormat.MSZ, None
    if isinstance(source, Path):
        filetype = detect_filetype(str(source))
        if filetype is None:
            raise ValueError(f"Unsupported file type for {source}")
        fmt = MSFormat(filetype.lower())
        mzml_obj = MZMLFile(str(source).encode()) if fmt is MSFormat.MZML else None
        return source, fmt, mzml_obj
    raise TypeError(f"Unsupported source type: {type(source)}")


@asynccontextmanager
async def optional_client(
    client: httpx.AsyncClient | None,
    **kwargs: Any,
) -> AsyncIterator[httpx.AsyncClient]:
    """Yield *client* if provided, otherwise create and close a temporary one."""
    if client is not None:
        yield client
    else:
        async with httpx.AsyncClient(**kwargs) as c:
            yield c


async def async_iter_from_sync(
    sync_iter: Iterator[bytes],
) -> AsyncIterator[bytes]:
    """Bridge a blocking sync iterator into an async iterator.

    Each ``next()`` call is offloaded to a thread via ``asyncio.to_thread``
    so that blocking reads (e.g. OS-pipe reads from ``compress_stream``)
    do not stall the event loop.
    """

    def _next() -> tuple[bool, bytes]:
        try:
            return True, next(sync_iter)
        except StopIteration:
            return False, b""

    while True:
        has_value, chunk = await asyncio.to_thread(_next)
        if not has_value:
            break
        yield chunk


class ThrottledCallback:
    """Accumulate byte deltas and forward only after *threshold* bytes."""

    def __init__(
        self, callback: Callable[[int], None], threshold: int = 8_388_608
    ) -> None:
        self._callback = callback
        self._threshold = threshold
        self._accumulated = 0

    def __call__(self, delta: int) -> None:
        self._accumulated += delta
        if self._accumulated >= self._threshold:
            self._callback(self._accumulated)
            self._accumulated = 0

    def flush(self) -> None:
        if self._accumulated:
            self._callback(self._accumulated)
            self._accumulated = 0


async def async_counting_generator(
    async_iter: AsyncIterator[bytes],
    callback: Callable[[int], None] | None = None,
) -> AsyncIterator[bytes]:
    """Wrap an async byte iterator, calling *callback* with each chunk's length."""
    async for chunk in async_iter:
        if callback:
            callback(len(chunk))
        yield chunk


async def async_file_chunk_generator(
    file_path: Path,
    chunk_size: int = 1_048_576,
    callback: Callable[[int], None] | None = None,
) -> AsyncIterator[bytes]:
    """Read a file in chunks as an async iterator."""

    def _read_chunks() -> Iterator[bytes]:
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    async for chunk in async_iter_from_sync(_read_chunks()):
        if callback:
            callback(len(chunk))
        yield chunk
