"""Tests for the mstransfer download client."""

from __future__ import annotations

import asyncio
import http.server
import os
import posixpath
import shutil
import threading
import urllib.parse
from unittest.mock import patch

import httpx
import pytest

from mstransfer.client.downloader import (
    DownloadRequest,
    PartialDownloadError,
    _detect_source_format,
    _FileProgressAdapter,
    _parse_content_range,
    _resolve_dest,
    _split_ranges,
    async_download_batch,
    async_download_file,
    download_batch,
    download_file,
)
from mstransfer.server.app import create_app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_http_server(serve_dir):
    """Start a local HTTP server serving files from *serve_dir*."""
    handler = http.server.SimpleHTTPRequestHandler

    class QuietHandler(handler):
        """Suppress request logging."""

        def log_message(self, format, *args):  # noqa: A002
            pass

        def translate_path(self, path):
            path = urllib.parse.unquote(posixpath.normpath(path))
            parts = path.split("/")
            result = serve_dir
            for part in parts:
                if not part or part == ".":
                    continue
                result = result / part
            return str(result)

    server = http.server.HTTPServer(("127.0.0.1", 0), QuietHandler)
    port = server.server_address[1]

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    return server, thread, f"http://127.0.0.1:{port}"


@pytest.fixture()
def served_files(tmp_path):
    """Create files and serve them via a local HTTP server.

    Returns a dict with ``base_url`` and ``serve_dir``.
    """
    serve_dir = tmp_path / "serve"
    serve_dir.mkdir()

    # Create some test files to serve.
    (serve_dir / "small.bin").write_bytes(b"hello world")
    (serve_dir / "medium.bin").write_bytes(b"x" * 10_000)

    server, thread, base_url = _make_http_server(serve_dir)

    yield {"base_url": base_url, "serve_dir": serve_dir}

    server.shutdown()
    thread.join(timeout=5)


@pytest.fixture()
def served_ms_files(tmp_path, test_msz, test_mzml, test_mszx):
    """Serve real .msz, .mzML, and .mszx files via a local HTTP server."""
    serve_dir = tmp_path / "ms_serve"
    serve_dir.mkdir()

    shutil.copy(test_msz, serve_dir / "test.msz")
    shutil.copy(test_mzml, serve_dir / "test.mzML")
    shutil.copy(test_mszx, serve_dir / "test.mszx")

    server, thread, base_url = _make_http_server(serve_dir)

    yield {"base_url": base_url, "serve_dir": serve_dir}

    server.shutdown()
    thread.join(timeout=5)


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------


class TestDownloadFile:
    def test_basic_download(self, served_files, tmp_path):
        """Download a file and verify contents."""
        dest = tmp_path / "out" / "small.bin"
        result = download_file(
            f"{served_files['base_url']}/small.bin",
            dest,
        )
        assert result == dest
        assert dest.exists()
        assert dest.read_bytes() == b"hello world"

    def test_creates_parent_directories(self, served_files, tmp_path):
        """Parent directories are created automatically."""
        dest = tmp_path / "a" / "b" / "c" / "small.bin"
        download_file(f"{served_files['base_url']}/small.bin", dest)
        assert dest.exists()

    def test_atomic_write_no_partial_file(self, served_files, tmp_path):
        """The .part file should not exist after a successful download."""
        dest = tmp_path / "small.bin"
        download_file(f"{served_files['base_url']}/small.bin", dest)
        part_path = dest.with_suffix(dest.suffix + ".part")
        assert not part_path.exists()
        assert dest.exists()

    def test_progress_callback(self, served_files, tmp_path):
        """Progress callback is invoked with byte deltas."""
        dest = tmp_path / "medium.bin"
        deltas: list[int] = []

        class Tracker:
            def on_progress(self, bytes_delta: int) -> None:
                deltas.append(bytes_delta)

        download_file(
            f"{served_files['base_url']}/medium.bin",
            dest,
            progress_callback=Tracker(),
            chunk_size=1024,
        )
        assert len(deltas) > 0
        assert sum(deltas) == 10_000

    def test_small_chunk_size(self, served_files, tmp_path):
        """Using a small chunk size still produces the correct file."""
        dest = tmp_path / "medium.bin"
        download_file(
            f"{served_files['base_url']}/medium.bin",
            dest,
            chunk_size=128,
        )
        assert dest.read_bytes() == b"x" * 10_000

    def test_skip_existing(self, served_files, tmp_path):
        """When skip_existing=True and file exists, download is skipped."""
        dest = tmp_path / "small.bin"
        dest.write_bytes(b"already here")

        result = download_file(
            f"{served_files['base_url']}/small.bin",
            dest,
            skip_existing=True,
        )
        assert result == dest
        # Content should NOT have been overwritten.
        assert dest.read_bytes() == b"already here"

    def test_force_overrides_skip_existing(self, served_files, tmp_path):
        """force=True re-downloads even when file exists."""
        dest = tmp_path / "small.bin"
        dest.write_bytes(b"old content")

        download_file(
            f"{served_files['base_url']}/small.bin",
            dest,
            skip_existing=True,
            force=True,
        )
        assert dest.read_bytes() == b"hello world"

    def test_force_overwrites_existing(self, served_files, tmp_path):
        """force=True overwrites an existing file."""
        dest = tmp_path / "small.bin"
        dest.write_bytes(b"old content")

        download_file(
            f"{served_files['base_url']}/small.bin",
            dest,
            force=True,
        )
        assert dest.read_bytes() == b"hello world"

    def test_http_404_raises(self, served_files, tmp_path):
        """HTTP 404 raises an httpx.HTTPStatusError."""
        dest = tmp_path / "missing.bin"
        with pytest.raises(httpx.HTTPStatusError):
            download_file(f"{served_files['base_url']}/nonexistent", dest)
        # No partial file should remain.
        assert not dest.exists()

    def test_no_progress_callback(self, served_files, tmp_path):
        """Download works fine without a progress callback."""
        dest = tmp_path / "small.bin"
        result = download_file(
            f"{served_files['base_url']}/small.bin",
            dest,
            progress_callback=None,
        )
        assert result == dest
        assert dest.read_bytes() == b"hello world"


# ---------------------------------------------------------------------------
# download_batch
# ---------------------------------------------------------------------------


class TestDownloadBatch:
    def test_single_file(self, served_files, tmp_path):
        """download_batch with a single file."""
        dest = tmp_path / "small.bin"
        requests = [
            DownloadRequest(url=f"{served_files['base_url']}/small.bin", dest=dest),
        ]
        results = download_batch(requests)
        assert len(results) == 1
        assert results[0] == dest
        assert dest.read_bytes() == b"hello world"

    def test_multiple_files(self, served_files, tmp_path):
        """download_batch downloads multiple files in parallel."""
        requests = [
            DownloadRequest(
                url=f"{served_files['base_url']}/small.bin",
                dest=tmp_path / "small.bin",
            ),
            DownloadRequest(
                url=f"{served_files['base_url']}/medium.bin",
                dest=tmp_path / "medium.bin",
            ),
        ]
        results = download_batch(requests, parallel=2)
        assert len(results) == 2
        assert (tmp_path / "small.bin").read_bytes() == b"hello world"
        assert (tmp_path / "medium.bin").read_bytes() == b"x" * 10_000

    def test_parallel_capped_to_file_count(self, served_files, tmp_path):
        """Semaphore should not exceed the number of files."""
        requests = [
            DownloadRequest(
                url=f"{served_files['base_url']}/small.bin",
                dest=tmp_path / "small.bin",
            ),
        ]
        with patch(
            "asyncio.Semaphore",
            wraps=asyncio.Semaphore,
        ) as mock_sem:
            download_batch(requests, parallel=8)
            mock_sem.assert_called_once_with(1)

    def test_batch_progress_callback(self, served_files, tmp_path):
        """Batch progress callbacks are invoked correctly."""
        started: list[str] = []
        completed: list[str] = []
        progress_deltas: dict[str, list[int]] = {}

        class Tracker:
            def on_file_start(self, filename: str, total_bytes: int | None) -> None:
                started.append(filename)

            def on_file_progress(self, filename: str, bytes_delta: int) -> None:
                progress_deltas.setdefault(filename, []).append(bytes_delta)

            def on_file_complete(self, filename: str) -> None:
                completed.append(filename)

            def on_file_error(self, filename: str, error: Exception) -> None:
                pass

        requests = [
            DownloadRequest(
                url=f"{served_files['base_url']}/small.bin",
                dest=tmp_path / "small.bin",
            ),
        ]
        download_batch(requests, parallel=1, progress=Tracker())
        assert "small.bin" in started
        assert "small.bin" in completed
        assert sum(progress_deltas.get("small.bin", [])) == 11  # len(b"hello world")

    def test_error_does_not_block_others(self, served_files, tmp_path):
        """One failed download does not prevent others from completing."""
        requests = [
            DownloadRequest(
                url=f"{served_files['base_url']}/small.bin",
                dest=tmp_path / "small.bin",
            ),
            DownloadRequest(
                url=f"{served_files['base_url']}/nonexistent",
                dest=tmp_path / "missing.bin",
            ),
        ]

        errors: list[str] = []

        class Tracker:
            def on_file_start(self, filename: str, total_bytes: int | None) -> None:
                pass

            def on_file_progress(self, filename: str, bytes_delta: int) -> None:
                pass

            def on_file_complete(self, filename: str) -> None:
                pass

            def on_file_error(self, filename: str, error: Exception) -> None:
                errors.append(filename)

        results = download_batch(requests, parallel=2, progress=Tracker())
        # Only the successful download appears in results.
        assert len(results) == 1
        assert results[0] == tmp_path / "small.bin"
        assert (tmp_path / "small.bin").read_bytes() == b"hello world"
        assert "missing.bin" in errors

    def test_skip_existing_in_batch(self, served_files, tmp_path):
        """skip_existing is forwarded to individual downloads."""
        dest = tmp_path / "small.bin"
        dest.write_bytes(b"pre-existing")

        requests = [
            DownloadRequest(url=f"{served_files['base_url']}/small.bin", dest=dest),
        ]
        results = download_batch(requests, skip_existing=True)
        assert len(results) == 1
        assert dest.read_bytes() == b"pre-existing"


# ---------------------------------------------------------------------------
# _FileProgressAdapter
# ---------------------------------------------------------------------------


class TestFileProgressAdapter:
    def test_forwards_to_batch(self):
        """Adapter forwards on_progress to batch on_file_progress."""
        calls: list[tuple[str, int]] = []

        class FakeBatch:
            def on_file_start(self, filename, total_bytes):
                pass

            def on_file_progress(self, filename, bytes_delta):
                calls.append((filename, bytes_delta))

            def on_file_complete(self, filename):
                pass

            def on_file_error(self, filename, error):
                pass

        adapter = _FileProgressAdapter("test.bin", FakeBatch())
        adapter.on_progress(1024)
        adapter.on_progress(512)
        assert calls == [("test.bin", 1024), ("test.bin", 512)]


# ---------------------------------------------------------------------------
# Format detection and destination resolution
# ---------------------------------------------------------------------------


class TestDetectSourceFormat:
    def test_msz_url(self):
        assert _detect_source_format("http://host/data/file.msz") == "msz"

    def test_mzml_url(self):
        assert _detect_source_format("http://host/data/file.mzML") == "mzml"

    def test_mzml_lowercase(self):
        assert _detect_source_format("http://host/data/file.mzml") == "mzml"

    def test_unknown_extension(self):
        assert _detect_source_format("http://host/data/file.bin") is None

    def test_query_params_stripped(self):
        assert _detect_source_format("http://h/f.msz?token=abc") == "msz"

    def test_fragment_stripped(self):
        assert _detect_source_format("http://h/f.mzML#section") == "mzml"

    def test_mszx_url(self):
        assert _detect_source_format("http://host/data/file.mszx") == "mszx"

    def test_mszx_uppercase(self):
        assert _detect_source_format("http://host/data/file.MSZX") == "mszx"


class TestResolveDest:
    def test_no_store_as(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert _resolve_dest(dest, None, "msz") == dest

    def test_same_format(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert _resolve_dest(dest, "msz", "msz") == dest

    def test_msz_to_mzml(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert _resolve_dest(dest, "mzml", "msz") == tmp_path / "file.mzML"

    def test_mzml_to_msz(self, tmp_path):
        dest = tmp_path / "file.mzML"
        assert _resolve_dest(dest, "msz", "mzml") == tmp_path / "file.msz"

    def test_unknown_source_no_change(self, tmp_path):
        dest = tmp_path / "file.bin"
        assert _resolve_dest(dest, "msz", None) == dest

    def test_mszx_same_format(self, tmp_path):
        dest = tmp_path / "file.mszx"
        assert _resolve_dest(dest, "mszx", "mszx") == dest

    def test_mszx_to_msz(self, tmp_path):
        dest = tmp_path / "file.mszx"
        assert _resolve_dest(dest, "msz", "mszx") == tmp_path / "file.msz"

    def test_mszx_to_mzml(self, tmp_path):
        dest = tmp_path / "file.mszx"
        assert _resolve_dest(dest, "mzml", "mszx") == tmp_path / "file.mzML"

    def test_msz_to_mszx(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert _resolve_dest(dest, "mszx", "msz") == tmp_path / "file.mszx"


# ---------------------------------------------------------------------------
# Format conversion (integration with mscompress)
# ---------------------------------------------------------------------------


class TestFormatConversion:
    def test_download_msz_store_as_mzml(self, served_ms_files, test_mzml, tmp_path):
        """Download .msz and decompress to .mzML."""
        dest = tmp_path / "result.msz"
        result = download_file(
            f"{served_ms_files['base_url']}/test.msz",
            dest,
            store_as="mzml",
        )
        # Extension should have been changed to .mzML.
        assert result == tmp_path / "result.mzML"
        assert result.exists()
        # Decompressed file should match the original mzML.
        assert result.stat().st_size == test_mzml.stat().st_size

    def test_download_mzml_store_as_msz(self, served_ms_files, test_msz, tmp_path):
        """Download .mzML and compress to .msz."""
        dest = tmp_path / "result.mzML"
        result = download_file(
            f"{served_ms_files['base_url']}/test.mzML",
            dest,
            store_as="msz",
        )
        assert result == tmp_path / "result.msz"
        assert result.exists()
        assert result.stat().st_size > 0

    def test_download_msz_store_as_msz_no_conversion(
        self, served_ms_files, test_msz, tmp_path
    ):
        """Download .msz with store_as='msz' — no conversion needed."""
        dest = tmp_path / "result.msz"
        result = download_file(
            f"{served_ms_files['base_url']}/test.msz",
            dest,
            store_as="msz",
        )
        assert result == dest
        assert dest.exists()
        assert dest.read_bytes() == test_msz.read_bytes()

    def test_download_store_as_none_keeps_original(
        self, served_ms_files, test_msz, tmp_path
    ):
        """Download with store_as=None keeps file as-is."""
        dest = tmp_path / "result.msz"
        result = download_file(
            f"{served_ms_files['base_url']}/test.msz",
            dest,
            store_as=None,
        )
        assert result == dest
        assert dest.read_bytes() == test_msz.read_bytes()

    def test_no_intermediate_file_left(self, served_ms_files, tmp_path):
        """Intermediate temp file is cleaned up after conversion."""
        out_dir = tmp_path / "clean_output"
        out_dir.mkdir()
        dest = out_dir / "result.msz"
        download_file(
            f"{served_ms_files['base_url']}/test.msz",
            dest,
            store_as="mzml",
        )
        # No .part or temp .msz file should remain.
        remaining = list(out_dir.iterdir())
        assert len(remaining) == 1
        assert remaining[0].suffix == ".mzML"

    def test_batch_store_as_forwarded(self, served_ms_files, test_mzml, tmp_path):
        """download_batch forwards store_as to download_file."""
        requests = [
            DownloadRequest(
                url=f"{served_ms_files['base_url']}/test.msz",
                dest=tmp_path / "result.msz",
            ),
        ]
        results = download_batch(requests, store_as="mzml")
        assert len(results) == 1
        assert results[0] == tmp_path / "result.mzML"
        assert results[0].stat().st_size == test_mzml.stat().st_size

    def test_download_mszx_store_as_mzml(self, served_ms_files, test_mzml, tmp_path):
        """Download .mszx and decompress inner spectra to .mzML."""
        dest = tmp_path / "result.mszx"
        result = download_file(
            f"{served_ms_files['base_url']}/test.mszx",
            dest,
            store_as="mzml",
        )
        assert result == tmp_path / "result.mzML"
        assert result.exists()
        assert result.stat().st_size == test_mzml.stat().st_size

    def test_download_mszx_store_as_msz(self, served_ms_files, test_msz, tmp_path):
        """Download .mszx and extract inner .msz — bytes match the original."""
        dest = tmp_path / "result.mszx"
        result = download_file(
            f"{served_ms_files['base_url']}/test.mszx",
            dest,
            store_as="msz",
        )
        assert result == tmp_path / "result.msz"
        assert result.exists()
        # Inner spectra.msz is a byte-for-byte copy of test.msz (conftest).
        assert result.read_bytes() == test_msz.read_bytes()

    def test_download_mszx_store_as_mszx_no_conversion(
        self, served_ms_files, test_mszx, tmp_path
    ):
        """Download .mszx with store_as='mszx' — no conversion, bytes match."""
        dest = tmp_path / "result.mszx"
        result = download_file(
            f"{served_ms_files['base_url']}/test.mszx",
            dest,
            store_as="mszx",
        )
        assert result == dest
        assert dest.read_bytes() == test_mszx.read_bytes()

    def test_download_mszx_store_as_none_keeps_original(
        self, served_ms_files, test_mszx, tmp_path
    ):
        """Download .mszx with store_as=None keeps file as-is."""
        dest = tmp_path / "result.mszx"
        result = download_file(
            f"{served_ms_files['base_url']}/test.mszx",
            dest,
            store_as=None,
        )
        assert result == dest
        assert dest.read_bytes() == test_mszx.read_bytes()

    def test_no_intermediate_file_left_mszx(self, served_ms_files, tmp_path):
        """After mszx→mzml, only the final .mzML remains in the output dir."""
        out_dir = tmp_path / "clean_output_mszx"
        out_dir.mkdir()
        dest = out_dir / "result.mszx"
        download_file(
            f"{served_ms_files['base_url']}/test.mszx",
            dest,
            store_as="mzml",
        )
        remaining = list(out_dir.iterdir())
        assert len(remaining) == 1
        assert remaining[0].suffix == ".mzML"

    def test_batch_mszx_store_as_mzml_converts(
        self, served_ms_files, test_mzml, tmp_path
    ):
        """Regression: batch .mszx URL + store_as='mzml' must convert, not no-op."""
        requests = [
            DownloadRequest(
                url=f"{served_ms_files['base_url']}/test.mszx",
                dest=tmp_path / "result.mszx",
            ),
        ]
        results = download_batch(requests, store_as="mzml")
        assert len(results) == 1
        assert results[0] == tmp_path / "result.mzML"
        # Not silently kept as mszx.
        assert not (tmp_path / "result.mszx").exists()
        assert results[0].stat().st_size == test_mzml.stat().st_size

    def test_msz_to_mszx_raises(self, served_ms_files, tmp_path):
        """Converting msz → mszx is out of scope and raises NotImplementedError."""
        dest = tmp_path / "result.msz"
        with pytest.raises(NotImplementedError):
            download_file(
                f"{served_ms_files['base_url']}/test.msz",
                dest,
                store_as="mszx",
            )

    def test_mzml_to_mszx_raises(self, served_ms_files, tmp_path):
        """Converting mzml → mszx is out of scope and raises NotImplementedError."""
        dest = tmp_path / "result.mzML"
        with pytest.raises(NotImplementedError):
            download_file(
                f"{served_ms_files['base_url']}/test.mzML",
                dest,
                store_as="mszx",
            )

    def test_malformed_mszx_cleans_up(self, served_ms_files, tmp_path):
        """A malformed .mszx archive raises and leaves no files behind."""
        # Overwrite the served file with invalid tar bytes.
        (served_ms_files["serve_dir"] / "broken.mszx").write_bytes(b"not a tar")

        out_dir = tmp_path / "broken_output"
        out_dir.mkdir()
        dest = out_dir / "broken.mszx"

        with pytest.raises(Exception):  # noqa: B017, PT011
            download_file(
                f"{served_ms_files['base_url']}/broken.mszx",
                dest,
                store_as="mzml",
            )

        # No .part, no partial mzML, no leftover mszx in the output dir.
        assert list(out_dir.iterdir()) == []


# ---------------------------------------------------------------------------
# async_download_file
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestAsyncDownloadFile:
    async def test_basic_download(self, served_files, tmp_path):
        """Download a file and verify contents (async)."""
        dest = tmp_path / "out" / "small.bin"
        result = await async_download_file(
            f"{served_files['base_url']}/small.bin",
            dest,
        )
        assert result == dest
        assert dest.exists()
        assert dest.read_bytes() == b"hello world"

    async def test_progress_callback(self, served_files, tmp_path):
        """Progress callback is invoked with byte deltas (async)."""
        dest = tmp_path / "medium.bin"
        deltas: list[int] = []

        class Tracker:
            def on_progress(self, bytes_delta: int) -> None:
                deltas.append(bytes_delta)

        await async_download_file(
            f"{served_files['base_url']}/medium.bin",
            dest,
            progress_callback=Tracker(),
            chunk_size=1024,
        )
        assert len(deltas) > 0
        assert sum(deltas) == 10_000

    async def test_skip_existing(self, served_files, tmp_path):
        """When skip_existing=True and file exists, download is skipped (async)."""
        dest = tmp_path / "small.bin"
        dest.write_bytes(b"already here")

        result = await async_download_file(
            f"{served_files['base_url']}/small.bin",
            dest,
            skip_existing=True,
        )
        assert result == dest
        assert dest.read_bytes() == b"already here"

    async def test_format_conversion_msz_to_mzml(
        self, served_ms_files, test_mzml, tmp_path
    ):
        """Download .msz and decompress to .mzML (async)."""
        dest = tmp_path / "result.msz"
        result = await async_download_file(
            f"{served_ms_files['base_url']}/test.msz",
            dest,
            store_as="mzml",
        )
        assert result == tmp_path / "result.mzML"
        assert result.exists()
        assert result.stat().st_size == test_mzml.stat().st_size

    async def test_format_conversion_mszx_to_mzml(
        self, served_ms_files, test_mzml, tmp_path
    ):
        """Download .mszx and decompress inner spectra to .mzML (async)."""
        dest = tmp_path / "result.mszx"
        result = await async_download_file(
            f"{served_ms_files['base_url']}/test.mszx",
            dest,
            store_as="mzml",
        )
        assert result == tmp_path / "result.mzML"
        assert result.stat().st_size == test_mzml.stat().st_size

    async def test_format_conversion_mszx_to_msz(
        self, served_ms_files, test_msz, tmp_path
    ):
        """Download .mszx and extract inner .msz (async)."""
        dest = tmp_path / "result.mszx"
        result = await async_download_file(
            f"{served_ms_files['base_url']}/test.mszx",
            dest,
            store_as="msz",
        )
        assert result == tmp_path / "result.msz"
        assert result.read_bytes() == test_msz.read_bytes()

    async def test_http_404_raises(self, served_files, tmp_path):
        """HTTP 404 raises an httpx.HTTPStatusError (async)."""
        dest = tmp_path / "missing.bin"
        with pytest.raises(httpx.HTTPStatusError):
            await async_download_file(f"{served_files['base_url']}/nonexistent", dest)
        assert not dest.exists()


# ---------------------------------------------------------------------------
# async_download_batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestAsyncDownloadBatch:
    async def test_single_file(self, served_files, tmp_path):
        """async_download_batch with a single file."""
        dest = tmp_path / "small.bin"
        requests = [
            DownloadRequest(url=f"{served_files['base_url']}/small.bin", dest=dest),
        ]
        results = await async_download_batch(requests)
        assert len(results) == 1
        assert results[0] == dest
        assert dest.read_bytes() == b"hello world"

    async def test_multiple_files(self, served_files, tmp_path):
        """async_download_batch downloads multiple files concurrently."""
        requests = [
            DownloadRequest(
                url=f"{served_files['base_url']}/small.bin",
                dest=tmp_path / "small.bin",
            ),
            DownloadRequest(
                url=f"{served_files['base_url']}/medium.bin",
                dest=tmp_path / "medium.bin",
            ),
        ]
        results = await async_download_batch(requests, parallel=2)
        assert len(results) == 2
        assert (tmp_path / "small.bin").read_bytes() == b"hello world"
        assert (tmp_path / "medium.bin").read_bytes() == b"x" * 10_000

    async def test_error_does_not_block_others(self, served_files, tmp_path):
        """One failed download does not prevent others from completing (async)."""
        requests = [
            DownloadRequest(
                url=f"{served_files['base_url']}/small.bin",
                dest=tmp_path / "small.bin",
            ),
            DownloadRequest(
                url=f"{served_files['base_url']}/nonexistent",
                dest=tmp_path / "missing.bin",
            ),
        ]

        errors: list[str] = []

        class Tracker:
            def on_file_start(self, filename: str, total_bytes: int | None) -> None:
                pass

            def on_file_progress(self, filename: str, bytes_delta: int) -> None:
                pass

            def on_file_complete(self, filename: str) -> None:
                pass

            def on_file_error(self, filename: str, error: Exception) -> None:
                errors.append(filename)

        results = await async_download_batch(requests, parallel=2, progress=Tracker())
        assert len(results) == 1
        assert results[0] == tmp_path / "small.bin"
        assert (tmp_path / "small.bin").read_bytes() == b"hello world"
        assert "missing.bin" in errors


# ---------------------------------------------------------------------------
# Split (ranged) downloads
# ---------------------------------------------------------------------------


@pytest.fixture()
def range_server(tmp_path, test_msz):
    """mstransfer app (which supports Range) serving a few files over ASGI.

    Returns a dict with an ``httpx.AsyncClient`` factory, the served
    payloads, and a list recording the ``Range`` header of every request.
    """
    serve_dir = tmp_path / "range_serve"
    serve_dir.mkdir()
    big = os.urandom(200_003)  # odd size so parts are uneven
    (serve_dir / "big.msz").write_bytes(big)
    shutil.copy(test_msz, serve_dir / "test.msz")

    app = create_app(output_dir=str(serve_dir), store_as="msz")
    ranges: list[str | None] = []

    async def _record(request: httpx.Request) -> None:
        ranges.append(request.headers.get("range"))

    def make_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            event_hooks={"request": [_record]},
        )

    return {"client": make_client, "big": big, "ranges": ranges}


class TestRangeHelpers:
    def test_split_ranges_even(self):
        assert _split_ranges(100, 4) == [(0, 24), (25, 49), (50, 74), (75, 99)]

    def test_split_ranges_uneven_covers_all_bytes(self):
        ranges = _split_ranges(10, 3)
        assert ranges == [(0, 3), (4, 7), (8, 9)]

    def test_parse_content_range(self):
        assert _parse_content_range("bytes 0-0/1234") == (0, 0, 1234)

    @pytest.mark.parametrize(
        "value", [None, "", "bytes */1234", "items 0-1/2", "bytes 0-1/*"]
    )
    def test_parse_content_range_invalid(self, value):
        assert _parse_content_range(value) is None


@pytest.mark.asyncio
class TestSplitDownload:
    async def test_split_download_byte_identical(self, range_server, tmp_path):
        """A file split across N connections matches the source exactly."""
        dest = tmp_path / "out" / "big.msz"
        async with range_server["client"]() as client:
            result = await async_download_file(
                "http://test/v1/files/big.msz",
                dest,
                connections=4,
                min_part_size=1024,
                chunk_size=4096,
                client=client,
            )

        assert result == dest
        assert dest.read_bytes() == range_server["big"]
        assert not dest.with_suffix(".msz.part").exists()

        # One probe followed by one ranged GET per part.
        size = len(range_server["big"])
        expected_parts = [f"bytes={s}-{e}" for s, e in _split_ranges(size, 4)]
        assert range_server["ranges"][0] == "bytes=0-0"
        assert sorted(range_server["ranges"][1:]) == sorted(expected_parts)

    async def test_split_progress_sums_to_size(self, range_server, tmp_path):
        """Progress deltas from all parts add up to the file size."""
        deltas: list[int] = []

        class Tracker:
            def on_progress(self, bytes_delta: int) -> None:
                deltas.append(bytes_delta)

        async with range_server["client"]() as client:
            await async_download_file(
                "http://test/v1/files/big.msz",
                tmp_path / "big.msz",
                connections=3,
                min_part_size=1024,
                progress_callback=Tracker(),
                client=client,
            )
        assert sum(deltas) == len(range_server["big"])

    async def test_parts_capped_by_min_part_size(self, range_server, tmp_path):
        """Fewer parts than connections are used for smaller files."""
        async with range_server["client"]() as client:
            await async_download_file(
                "http://test/v1/files/big.msz",
                tmp_path / "big.msz",
                connections=16,
                min_part_size=100_000,
                client=client,
            )
        # 200_003 // 100_000 == 2 parts, plus the probe.
        assert len(range_server["ranges"]) == 3
        assert (tmp_path / "big.msz").read_bytes() == range_server["big"]

    async def test_small_file_falls_back_to_single_stream(self, range_server, tmp_path):
        """Files below 2 * min_part_size are fetched with one plain GET."""
        async with range_server["client"]() as client:
            await async_download_file(
                "http://test/v1/files/big.msz",
                tmp_path / "big.msz",
                connections=4,
                client=client,
            )
        # Probe, then a single un-ranged GET.
        assert range_server["ranges"] == ["bytes=0-0", None]
        assert (tmp_path / "big.msz").read_bytes() == range_server["big"]

    async def test_split_with_conversion(self, range_server, test_mzml, tmp_path):
        """Split downloads still go through the store_as conversion step."""
        async with range_server["client"]() as client:
            result = await async_download_file(
                "http://test/v1/files/test.msz",
                tmp_path / "test.msz",
                store_as="mzml",
                connections=4,
                min_part_size=1024,
                client=client,
            )
        assert result == tmp_path / "test.mzML"
        assert result.stat().st_size == test_mzml.stat().st_size
        assert len(range_server["ranges"]) > 2
        assert list(tmp_path.glob("*.part")) == []
        assert not (tmp_path / "test.msz").exists()

    async def test_fallback_when_server_ignores_range(self, served_files, tmp_path):
        """A server without Range support is downloaded in a single request."""
        ranges: list[str | None] = []

        async def _record(request: httpx.Request) -> None:
            ranges.append(request.headers.get("range"))

        dest = tmp_path / "medium.bin"
        async with httpx.AsyncClient(event_hooks={"request": [_record]}) as client:
            await async_download_file(
                f"{served_files['base_url']}/medium.bin",
                dest,
                connections=4,
                min_part_size=100,
                client=client,
            )
        assert dest.read_bytes() == b"x" * 10_000
        # The 200 probe response is reused as the download itself.
        assert ranges == ["bytes=0-0"]

    async def test_short_part_raises_and_cleans_up(self, tmp_path):
        """A part with too few bytes fails the download and removes .part."""
        size = 4096

        def handler(request: httpx.Request) -> httpx.Response:
            header = request.headers["range"]
            if header == "bytes=0-0":
                return httpx.Response(
                    206,
                    content=b"x",
                    headers={"Content-Range": f"bytes 0-0/{size}"},
                )
            start, end = (int(v) for v in header.removeprefix("bytes=").split("-"))
            # Return one byte short of the requested range.
            return httpx.Response(
                206,
                content=b"x" * (end - start),
                headers={"Content-Range": f"bytes {start}-{end}/{size}"},
            )

        dest = tmp_path / "f.msz"
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            with pytest.raises(PartialDownloadError):
                await async_download_file(
                    "http://test/f.msz",
                    dest,
                    connections=4,
                    min_part_size=1024,
                    client=c,
                )
        assert not dest.exists()
        assert not dest.with_suffix(".msz.part").exists()

    async def test_mismatched_content_range_raises(self, tmp_path):
        """A part whose Content-Range differs from the request is rejected."""
        size = 4096

        def handler(request: httpx.Request) -> httpx.Response:
            # Always answer with the first bytes regardless of the range.
            return httpx.Response(
                206,
                content=b"x",
                headers={"Content-Range": f"bytes 0-0/{size}"},
            )

        dest = tmp_path / "f.msz"
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            with pytest.raises(PartialDownloadError, match="Requested range"):
                await async_download_file(
                    "http://test/f.msz",
                    dest,
                    connections=2,
                    min_part_size=1024,
                    client=c,
                )
        assert not dest.with_suffix(".msz.part").exists()

    async def test_invalid_connections_raises(self, tmp_path):
        with pytest.raises(ValueError, match="connections"):
            await async_download_file(
                "http://test/f.msz", tmp_path / "f.msz", connections=0
            )

    async def test_batch_forwards_connections(self, range_server, tmp_path):
        """Batch downloads split each file independently of `parallel`."""
        requests = [
            DownloadRequest(
                url="http://test/v1/files/big.msz", dest=tmp_path / "a.msz"
            ),
            DownloadRequest(
                url="http://test/v1/files/big.msz", dest=tmp_path / "b.msz"
            ),
        ]
        async with range_server["client"]() as client:
            results = await async_download_batch(
                requests,
                parallel=2,
                connections=2,
                min_part_size=1024,
                client=client,
            )
        assert len(results) == 2
        for path in results:
            assert path.read_bytes() == range_server["big"]
        # Two files × (probe + 2 parts) — the progress-less batch skips HEAD.
        assert len(range_server["ranges"]) == 6

    async def test_batch_ignores_failed_head_content_length(
        self, range_server, tmp_path
    ):
        """A non-2xx HEAD (405 on GET-only routes) does not set total_bytes."""
        totals: list[int | None] = []

        class Tracker:
            def on_file_start(self, filename: str, total_bytes: int | None) -> None:
                totals.append(total_bytes)

            def on_file_progress(self, filename: str, bytes_delta: int) -> None:
                pass

            def on_file_complete(self, filename: str) -> None:
                pass

            def on_file_error(self, filename: str, error: Exception) -> None:
                pass

        async with range_server["client"]() as client:
            await async_download_batch(
                [
                    DownloadRequest(
                        url="http://test/v1/files/big.msz", dest=tmp_path / "a.msz"
                    )
                ],
                progress=Tracker(),
                client=client,
            )
        assert totals == [None]
