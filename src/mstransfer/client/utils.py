from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile
from mscompress.utils import detect_filetype

logger = logging.getLogger(__name__)

VALID_EXTENSIONS = {".mzml", ".msz", ".mszx"}
VALID_FORMATS = {"mzML", "msz", "mszx"}


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


def normalize_source(
    source: Path | MZMLFile | MSZFile | MSZXFile,
) -> tuple[Path, str, MZMLFile | None]:
    """Return ``(file_path, filetype, mzml_obj | None)`` from *source*."""
    if isinstance(source, MZMLFile):
        return Path(source.path.decode()), "mzML", source
    if isinstance(source, MSZFile):
        return Path(source.path.decode()), "msz", None
    if isinstance(source, MSZXFile):
        return source.archive_path, "mszx", None
    if isinstance(source, Path):
        filetype = detect_filetype(str(source)) or ""
        if filetype not in VALID_FORMATS:
            raise ValueError(f"Unsupported file type for {source}: {filetype}")
        mzml_obj = MZMLFile(str(source).encode()) if filetype == "mzML" else None
        return source, filetype, mzml_obj
    raise TypeError(f"Unsupported source type: {type(source)}")


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
