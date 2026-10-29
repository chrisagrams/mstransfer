"""Decide where and in what format a downloaded file is stored on disk."""

from __future__ import annotations

import asyncio
import logging
import shutil
import tarfile
import tempfile
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, ClassVar, Literal
from urllib.parse import unquote, urlsplit

from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile
from mscompress.mszx.batch import MSZXBatchFile, read_archive_manifest

from mstransfer.client.utils import replace_dir

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# Output formats callers can request via ``store_as``.
DownloadFormat = Literal["msz", "mzml", "mszx"]


class MSFormat(str, Enum):
    """An on-disk MS data format.

    Members compare equal to their ``DownloadFormat`` string values.
    """

    MSZ = "msz"
    MZML = "mzml"
    MSZX = "mszx"

    def __str__(self) -> str:
        return self.value

    @property
    def ext(self) -> str:
        """The canonical file extension, used to name converted outputs."""
        return ".mzML" if self is MSFormat.MZML else f".{self.value}"

    @classmethod
    def from_url(cls, url: str) -> MSFormat | None:
        """Infer the format from the URL filename extension, or *None*."""
        suffix = PurePosixPath(unquote(urlsplit(url).path)).suffix
        # Every format's lowercased extension is "." + its value.
        try:
            return cls(suffix.lower().removeprefix("."))
        except ValueError:
            return None


def _extract_msz_entry(tar: tarfile.TarFile, entry: str, dst: Path) -> None:
    """Stream the MSZ member *entry* of an open .mszx tar to *dst*."""
    member = tar.extractfile(entry)
    if member is None:
        raise ValueError(f"Missing MSZ entry {entry!r}")
    with member, dst.open("wb") as out:
        shutil.copyfileobj(member, out)


@dataclass(frozen=True)
class Conversion:
    """Turning a *source*-format file into a *target*-format file."""

    source: MSFormat
    target: MSFormat

    @property
    def supported(self) -> bool:
        """Whether a converter exists for this pair of formats."""
        return (self.source, self.target) in self._CONVERTERS

    def run(self, src: Path, dst: Path) -> None:
        """Convert *src* to *dst* (blocking; run in a worker thread)."""
        self._CONVERTERS[(self.source, self.target)](src, dst)

    @staticmethod
    def _msz_to_mzml(src: Path, dst: Path) -> None:
        """Decompress an .msz file to mzML."""
        MSZFile(str(src).encode()).decompress(str(dst))

    @staticmethod
    def _mzml_to_msz(src: Path, dst: Path) -> None:
        """Compress an mzML file to .msz."""
        MZMLFile(str(src).encode()).compress(str(dst))

    @staticmethod
    def _mszx_to_mzml(src: Path, dst: Path) -> None:
        """Decompress the spectra of a single-member .mszx archive to mzML."""
        with MSZXFile.open(src) as mszx:
            mszx.decompress(str(dst))

    @staticmethod
    def _mszx_to_msz(src: Path, dst: Path) -> None:
        """Extract the embedded .msz payload from an .mszx archive."""
        entries = read_archive_manifest(src).spectra_files
        if len(entries) != 1:
            raise NotImplementedError(
                f"Cannot store a {len(entries)}-member .mszx as a single .msz"
            )
        with tarfile.open(src) as tar:
            _extract_msz_entry(tar, entries[0].entry, dst)

    # Mapping of (source_format, target_format) to converter function.
    _CONVERTERS: ClassVar[
        dict[tuple[MSFormat, MSFormat], Callable[[Path, Path], None]]
    ] = {
        (MSFormat.MSZ, MSFormat.MZML): _msz_to_mzml,
        (MSFormat.MZML, MSFormat.MSZ): _mzml_to_msz,
        (MSFormat.MSZX, MSFormat.MZML): _mszx_to_mzml,
        (MSFormat.MSZX, MSFormat.MSZ): _mszx_to_msz,
    }


@dataclass(frozen=True)
class MszxExpansion:
    """Writes every member of a multi-member .mszx into one directory.

    Each spectra *entry* of *src* becomes ``<entry stem>.<ext>`` stored as
    *target* (:attr:`MSFormat.MSZ` or :attr:`MSFormat.MZML`).
    """

    src: Path
    entries: tuple[str, ...]
    target: MSFormat

    @cached_property
    def targets(self) -> list[tuple[str, str]]:
        """Map each spectra entry to an output file name.

        Raises :class:`ValueError` for unsafe names, or if two entries map
        to the same output name.
        """
        targets: list[tuple[str, str]] = []
        seen: dict[str, str] = {}
        for entry in self.entries:
            name = f"{self._member_stem(entry)}{self.target.ext}"
            # Compare case-insensitively: macOS and Windows filesystems are.
            key = name.casefold()
            if key in seen:
                raise ValueError(
                    f"Archive entries {seen[key]!r} and {entry!r} both map to {name}"
                )
            seen[key] = entry
            targets.append((entry, name))
        return targets

    def extract_to(self, directory: Path) -> None:
        """Write every member into *directory* (blocking)."""
        if self.target is MSFormat.MSZ:
            with tarfile.open(self.src) as tar:
                for entry, name in self.targets:
                    _extract_msz_entry(tar, entry, directory / name)
        else:
            with MSZXBatchFile.open(self.src) as batch:
                for entry, name in self.targets:
                    batch.get(entry).decompress(str(directory / name))

    async def write(self, archive_dir: Path) -> Path:
        """Store every member in *archive_dir*.

        Members are extracted into a uniquely named staging directory that
        is renamed to *archive_dir* only once all of them succeed, so a
        failure leaves no partial output behind and concurrent downloads
        never share staging files.  Names are validated before anything
        is written.
        """
        targets = self.targets
        staging = Path(
            tempfile.mkdtemp(
                dir=archive_dir.parent, prefix=f".{archive_dir.name}.", suffix=".part"
            )
        )
        try:
            await asyncio.to_thread(self.extract_to, staging)
            replace_dir(staging, archive_dir)
        finally:
            shutil.rmtree(staging, ignore_errors=True)

        logger.info(
            "Extracted %d members from %s into %s",
            len(targets),
            self.src.name,
            archive_dir,
        )
        return archive_dir

    @staticmethod
    def _member_stem(entry: str) -> str:
        """Return the output stem for archive *entry*, without any directory part.

        Raises :class:`ValueError` for names that would be empty or hidden.
        """
        # PureWindowsPath splits on both "/" and "\\", so no entry can name a
        # path outside the archive directory on any platform.
        name = PureWindowsPath(entry).name
        if name.lower().endswith(".msz"):
            name = name[: -len(".msz")]
        if not name or name.startswith("."):
            raise ValueError(f"Unsafe .mszx entry name {entry!r}")
        return name


@dataclass(frozen=True)
class StorePlan:
    """How a file downloaded to *dest* ends up on disk.

    *source* is the format of the downloaded bytes (or *None* if unknown)
    and *target* the requested ``store_as`` format (or *None* to keep the
    file as-is).  Build one with :meth:`for_download` to also validate the
    combination before any I/O.
    """

    dest: Path
    source: MSFormat | None
    target: MSFormat | None

    @classmethod
    def for_download(
        cls, url: str, dest: Path, store_as: DownloadFormat | None
    ) -> StorePlan:
        """Plan storage for *url*, failing fast on unsupported conversions."""
        target = MSFormat(store_as) if store_as is not None else None
        plan = cls(dest, MSFormat.from_url(url), target)
        conversion = plan.conversion
        if conversion is not None and not conversion.supported:
            raise NotImplementedError(
                f"Conversion from {conversion.source} to {conversion.target} "
                "is not supported"
            )
        return plan

    @property
    def conversion(self) -> Conversion | None:
        """The conversion needed, or *None* if the file is stored as-is."""
        if self.source is None or self.target is None or self.source == self.target:
            return None
        return Conversion(self.source, self.target)

    @property
    def final_dest(self) -> Path:
        """The output path, with its extension adjusted for any conversion."""
        conversion = self.conversion
        if conversion is None:
            return self.dest
        return self.dest.with_suffix(conversion.target.ext)

    @property
    def part_path(self) -> Path:
        """Where the download is streamed before it is finalized."""
        return self.dest.with_suffix(self.dest.suffix + ".part")

    @property
    def may_expand(self) -> bool:
        """Whether a multi-member .mszx would be expanded into a directory."""
        return self.source is MSFormat.MSZX and self.target in (
            MSFormat.MSZ,
            MSFormat.MZML,
        )

    @property
    def archive_dir(self) -> Path:
        """Where the members of a multi-member .mszx are written."""
        return self.final_dest.with_suffix("")

    def existing(self) -> Path | None:
        """Return the output already on disk, if any."""
        if self.final_dest.exists():
            return self.final_dest
        # The archive directory only ever appears complete, so it is safe
        # to trust without downloading the archive again.
        if self.may_expand and self.archive_dir.is_dir():
            return self.archive_dir
        return None

    async def finalize(self, part: Path) -> Path:
        """Move or convert the downloaded *part* file into its final form.

        Returns the final destination, or the archive directory for a
        multi-member ``.mszx``.
        """
        conversion = self.conversion
        if conversion is None:
            part.rename(self.final_dest)
            return self.final_dest

        # Give the temp file the correct source extension so mscompress can
        # recognise its format, then convert.
        src = part.with_suffix(conversion.source.ext)
        part.rename(src)
        try:
            if self.may_expand:
                manifest = await asyncio.to_thread(read_archive_manifest, src)
                entries = tuple(spectra.entry for spectra in manifest.spectra_files)
                if len(entries) > 1:
                    expansion = MszxExpansion(src, entries, conversion.target)
                    return await expansion.write(self.archive_dir)
            return await self._convert(src, conversion)
        finally:
            src.unlink(missing_ok=True)

    async def _convert(self, src: Path, conversion: Conversion) -> Path:
        """Convert *src* into :attr:`final_dest` via a staging file."""
        final_dest = self.final_dest
        staging = final_dest.with_suffix(final_dest.suffix + ".part")
        try:
            await asyncio.to_thread(conversion.run, src, staging)
            staging.replace(final_dest)
        finally:
            staging.unlink(missing_ok=True)
        logger.info("Converted %s → %s", src.name, final_dest.name)
        return final_dest
