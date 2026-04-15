"""Extensible file discovery for mstransfer."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Sequence

from mstransfer.server.models import FileInfo, StoreFormat

VALID_EXTENSIONS: dict[str, StoreFormat] = {
    ".msz": "msz",
    ".mzml": "mzml",
}


@runtime_checkable
class FileProvider(Protocol):
    """Contract that file providers must satisfy.

    Implementations may return :class:`FileInfo` subclasses from
    :meth:`list_files`; the return type is :class:`~collections.abc.Sequence`
    so that subclass lists are accepted without type errors.
    """

    async def list_files(self) -> Sequence[FileInfo]: ...

    async def get_file(self, filename: str) -> Path | None: ...


class DirectoryFileProvider:
    """Default provider — scans a directory for .msz/.mzML files."""

    def __init__(self, directory: Path) -> None:
        self._directory = directory

    async def list_files(self) -> Sequence[FileInfo]:
        """Return metadata for every .msz / .mzML file in the directory."""
        return await asyncio.to_thread(self._scan)

    async def get_file(self, filename: str) -> Path | None:
        """Resolve *filename* to a path inside the directory.

        Returns *None* if the file does not exist or the name attempts
        directory traversal.
        """
        if not self._is_safe_filename(filename):
            return None

        path = (self._directory / filename).resolve()

        # Ensure the resolved path is still inside the directory.
        if not path.is_relative_to(self._directory.resolve()):
            return None

        if not path.is_file():
            return None

        return path

    def _scan(self) -> list[FileInfo]:
        """Scan the directory for valid files and return their metadata."""
        results: list[FileInfo] = []
        for entry in os.scandir(self._directory):
            if not entry.is_file():
                continue
            ext = Path(entry.name).suffix.lower()
            fmt = VALID_EXTENSIONS.get(ext)
            if fmt is None:
                continue
            results.append(
                FileInfo(
                    name=entry.name,
                    size_bytes=entry.stat().st_size,
                    format=fmt,
                )
            )
        return sorted(results, key=lambda f: f.name)

    @staticmethod
    def _is_safe_filename(filename: str) -> bool:
        """Reject filenames that could escape the directory."""
        if not filename:
            return False
        if "/" in filename or "\\" in filename:
            return False
        return ".." not in filename
