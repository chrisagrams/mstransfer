"""Decide where and in what format a downloaded file is stored on disk."""

from __future__ import annotations

import asyncio
import logging
import shutil
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from typing import TYPE_CHECKING, Literal

from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile
from mscompress.mszx.batch import MSZXBatchFile, read_archive_manifest

from mstransfer.client.utils import replace_dir

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# Output formats the client can produce on disk after download.
DownloadFormat = Literal["msz", "mzml", "mszx"]

# Mapping of format to canonical file extension for naming converted outputs.
FORMAT_EXT: dict[DownloadFormat, str] = {
    "msz": ".msz",
    "mzml": ".mzML",
    "mszx": ".mszx",
}

# Reverse mapping of lowercase extension to format for source format inference.
_EXT_FORMAT: dict[str, DownloadFormat] = {v.lower(): k for k, v in FORMAT_EXT.items()}


def detect_source_format(url: str) -> DownloadFormat | None:
    """Infer the source format from the URL filename extension.

    Returns ``"msz"``, ``"mzml"``, ``"mszx"``, or *None* if the format
    cannot be determined.
    """
    # Strip query params and fragments before inspecting extension.
    path = url.split("?", 1)[0].split("#", 1)[0]
    return _EXT_FORMAT.get(Path(path).suffix.lower())


# ---------------------------------------------------------------------------
# Single-file converters (run in a worker thread)
# ---------------------------------------------------------------------------


def msz_to_mzml(src: Path, dst: Path) -> None:
    """Decompress an .msz file to mzML."""
    MSZFile(str(src).encode()).decompress(str(dst))


def mzml_to_msz(src: Path, dst: Path) -> None:
    """Compress an mzML file to .msz."""
    MZMLFile(str(src).encode()).compress(str(dst))


def mszx_to_mzml(src: Path, dst: Path) -> None:
    """Decompress the spectra of a single-member .mszx archive to mzML."""
    with MSZXFile.open(src) as mszx:
        mszx.decompress(str(dst))


def mszx_to_msz(src: Path, dst: Path) -> None:
    """Extract the embedded .msz payload from an .mszx archive."""
    entries = read_archive_manifest(src).spectra_files
    if len(entries) != 1:
        raise NotImplementedError(
            f"Cannot store a {len(entries)}-member .mszx as a single .msz"
        )
    with tarfile.open(src) as tar:
        _extract_msz_entry(tar, entries[0].entry, dst)


# Mapping of (source_format, target_format) to converter function.
CONVERTERS: dict[
    tuple[DownloadFormat, DownloadFormat], Callable[[Path, Path], None]
] = {
    ("msz", "mzml"): msz_to_mzml,
    ("mzml", "msz"): mzml_to_msz,
    ("mszx", "mzml"): mszx_to_mzml,
    ("mszx", "msz"): mszx_to_msz,
}


# ---------------------------------------------------------------------------
# Multi-member .mszx helpers
# ---------------------------------------------------------------------------


def _extract_msz_entry(tar: tarfile.TarFile, entry: str, dst: Path) -> None:
    """Stream the MSZ member *entry* of an open .mszx tar to *dst*."""
    member = tar.extractfile(entry)
    if member is None:
        raise ValueError(f"Missing MSZ entry {entry!r}")
    with member, dst.open("wb") as out:
        shutil.copyfileobj(member, out)


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


def _member_targets(
    entries: list[str], store_as: DownloadFormat
) -> list[tuple[str, str]]:
    """Map each .mszx spectra entry to an output file name.

    Raises :class:`ValueError` if two entries map to the same output name.
    """
    targets: list[tuple[str, str]] = []
    seen: dict[str, str] = {}
    for entry in entries:
        name = f"{_member_stem(entry)}{FORMAT_EXT[store_as]}"
        # Compare case-insensitively: macOS and Windows filesystems are.
        key = name.casefold()
        if key in seen:
            raise ValueError(
                f"Archive entries {seen[key]!r} and {entry!r} both map to {name}"
            )
        seen[key] = entry
        targets.append((entry, name))
    return targets


def _extract_members(
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


# ---------------------------------------------------------------------------
# Store plan
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StorePlan:
    """How a file downloaded to *dest* ends up on disk.

    *source* is the format of the downloaded bytes (or *None* if unknown)
    and *target* the requested ``store_as`` format (or *None* to keep the
    file as-is).  Build one with :meth:`for_download` to also validate the
    combination before any I/O.
    """

    dest: Path
    source: DownloadFormat | None
    target: DownloadFormat | None

    @classmethod
    def for_download(
        cls, url: str, dest: Path, store_as: DownloadFormat | None
    ) -> StorePlan:
        """Plan storage for *url*, failing fast on unsupported conversions."""
        plan = cls(dest, detect_source_format(url), store_as)
        conversion = plan.conversion
        if conversion is not None and conversion not in CONVERTERS:
            raise NotImplementedError(
                f"Conversion from {plan.source} to {plan.target} is not supported"
            )
        return plan

    @property
    def conversion(self) -> tuple[DownloadFormat, DownloadFormat] | None:
        """The ``(source, target)`` conversion needed, or *None* if none is."""
        if self.source is None or self.target is None or self.source == self.target:
            return None
        return self.source, self.target

    @property
    def final_dest(self) -> Path:
        """The output path, with its extension adjusted for any conversion."""
        if self.conversion is None:
            return self.dest
        return self.dest.with_suffix(FORMAT_EXT[self.conversion[1]])

    @property
    def part_path(self) -> Path:
        """Where the download is streamed before it is finalized."""
        return self.dest.with_suffix(self.dest.suffix + ".part")

    @property
    def may_expand(self) -> bool:
        """Whether a multi-member .mszx would be expanded into a directory."""
        return self.source == "mszx" and self.target in ("msz", "mzml")

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

        source, target = conversion
        # Give the temp file the correct source extension so mscompress can
        # recognise its format, then convert.
        src = part.with_suffix(FORMAT_EXT[source])
        part.rename(src)
        try:
            if self.may_expand:
                manifest = await asyncio.to_thread(read_archive_manifest, src)
                entries = [spectra.entry for spectra in manifest.spectra_files]
                if len(entries) > 1:
                    return await self._expand(src, entries, target)
            return await self._convert(src, CONVERTERS[conversion])
        finally:
            src.unlink(missing_ok=True)

    async def _convert(
        self, src: Path, converter: Callable[[Path, Path], None]
    ) -> Path:
        """Convert *src* into :attr:`final_dest` via a staging file."""
        final_dest = self.final_dest
        staging = final_dest.with_suffix(final_dest.suffix + ".part")
        try:
            await asyncio.to_thread(converter, src, staging)
            staging.replace(final_dest)
        finally:
            staging.unlink(missing_ok=True)
        logger.info("Converted %s → %s", src.name, final_dest.name)
        return final_dest

    async def _expand(
        self, src: Path, entries: list[str], target: DownloadFormat
    ) -> Path:
        """Store every member of a multi-member .mszx in :attr:`archive_dir`.

        Members are extracted into a uniquely named staging directory that
        is renamed to :attr:`archive_dir` only once all of them succeed, so
        a failure leaves no partial output behind and concurrent downloads
        never share staging files.
        """
        targets = _member_targets(entries, target)
        archive_dir = self.archive_dir
        staging = Path(
            tempfile.mkdtemp(
                dir=archive_dir.parent, prefix=f".{archive_dir.name}.", suffix=".part"
            )
        )
        try:
            staged = [(entry, staging / name) for entry, name in targets]
            await asyncio.to_thread(_extract_members, src, staged, target)
            replace_dir(staging, archive_dir)
        finally:
            shutil.rmtree(staging, ignore_errors=True)

        logger.info(
            "Extracted %d members from %s into %s", len(targets), src.name, archive_dir
        )
        return archive_dir
