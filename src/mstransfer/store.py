"""Decide where and in what format a transferred file is stored on disk."""

from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from enum import StrEnum
from functools import cached_property
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, ClassVar, Literal
from urllib.parse import unquote, urlsplit

from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile
from mscompress.mszx.batch import MSZXBatchFile, read_archive_manifest

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# Output formats callers can request via `store_as`.
DownloadFormat = Literal["msz", "mzml", "mszx"]


class MSFormat(StrEnum):
    """An on-disk MS data format.

    Members compare equal to their `DownloadFormat` string values.
    """

    MSZ = "msz"
    MZML = "mzml"
    MSZX = "mszx"

    @property
    def ext(self) -> str:
        """The canonical file extension, used to name converted outputs."""
        return ".mzML" if self is MSFormat.MZML else f".{self.value}"

    @classmethod
    def from_path(cls, name: str) -> MSFormat | None:
        """Infer the format from a filename extension, or *None*."""
        suffix = PurePosixPath(name).suffix
        # Every format's lowercased extension is "." + its value.
        try:
            return cls(suffix.lower().removeprefix("."))
        except ValueError:
            return None

    @classmethod
    def from_url(cls, url: str) -> MSFormat | None:
        """Infer the format from the URL filename extension, or *None*."""
        return cls.from_path(unquote(urlsplit(url).path))

    @classmethod
    def from_header(cls, value: str) -> MSFormat:
        """Parse an `X-Source-Format` header value, case-insensitively.

        Raises `ValueError` for an unrecognised value.
        """
        return cls(value.strip().lower())


@dataclass(frozen=True)
class Conversion:
    """Turning a *source*-format file into a *target*-format file."""

    source: MSFormat
    target: MSFormat

    @property
    def supported(self) -> bool:
        """Whether a converter exists for this pair of formats."""
        return (self.source, self.target) in self._CONVERTERS

    async def run(self, src: Path, dst: Path) -> None:
        """Convert *src* to *dst* in a worker thread."""
        converter = self._CONVERTERS[(self.source, self.target)]
        await asyncio.to_thread(converter, src, dst)

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
        with MSZXBatchFile.open(src) as batch:
            if len(batch) != 1:
                raise NotImplementedError(
                    f"Cannot store a {len(batch)}-member .mszx as a single .msz"
                )
            batch.extract_msz(0, dst)

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

    Each spectra *entry* of *src* becomes `<entry stem>.<ext>` stored as
    *target* (`MSFormat.MSZ` or `MSFormat.MZML`).
    """

    src: Path
    entries: tuple[str, ...]
    target: MSFormat

    @cached_property
    def targets(self) -> list[tuple[str, str]]:
        """Map each spectra entry to an output file name.

        Raises `ValueError` for unsafe names, or if two entries map
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

    def _extract_to(self, directory: Path) -> None:
        """Write every member into *directory* (blocking)."""
        with MSZXBatchFile.open(self.src) as batch:
            for entry, name in self.targets:
                if self.target is MSFormat.MSZ:
                    batch.extract_msz(entry, directory / name)
                else:
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
            await asyncio.to_thread(self._extract_to, staging)
            self._swap_into(staging, archive_dir)
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
    def _swap_into(staging: Path, target: Path) -> None:
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

    @staticmethod
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


@dataclass(frozen=True)
class StorePlan:
    """How a file downloaded to *dest* ends up on disk.

    *source* is the format of the downloaded bytes (or *None* if unknown)
    and *target* the requested `store_as` format (or *None* to keep the
    file as-is).  Build one with `for_download` to also validate the
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

    @classmethod
    def for_upload(
        cls,
        dest_dir: Path,
        filename: str,
        source: MSFormat | None,
        store_as: MSFormat,
    ) -> StorePlan:
        """Plan storage for an upload of *filename* into *dest_dir*.

        *source* is the format the client declared, or *None* to infer it
        from *filename*.  mzML is compressed on the wire, so anything but an
        .mszx archive arrives as MSZ.  Archives are always kept as-is,
        whatever *store_as* says.
        """
        if source is None:
            source = MSFormat.from_path(filename)
        wire = MSFormat.MSZX if source is MSFormat.MSZX else MSFormat.MSZ
        target = None if wire is MSFormat.MSZX else store_as
        return cls(dest_dir / f"{Path(filename).stem}{wire.ext}", wire, target)

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

    def unique_part_path(self) -> Path:
        """A `part_path` variant that concurrent writers never share.

        Two uploads can target the same *dest* (e.g. `run.msz` and
        `run.mzML` both arrive as `run.msz`), so each needs its own file.
        """
        return self.dest.with_name(f"{self.dest.name}.{uuid.uuid4().hex}.part")

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
        multi-member `.mszx`.
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
        """Convert *src* into `final_dest` via a staging file."""
        final_dest = self.final_dest
        # A unique name, so concurrent conversions to one output never share it.
        staging = final_dest.with_name(f"{final_dest.name}.{uuid.uuid4().hex}.part")
        try:
            await conversion.run(src, staging)
            staging.replace(final_dest)
        finally:
            staging.unlink(missing_ok=True)
        logger.info("Converted %s → %s", src.name, final_dest.name)
        return final_dest
