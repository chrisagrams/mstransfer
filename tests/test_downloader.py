"""Tests for the mstransfer download client."""

from __future__ import annotations

import asyncio
import http.server
import posixpath
import shutil
import threading
import urllib.parse
from unittest.mock import patch

import httpx
import pytest

from mstransfer.client.downloader import (
    DownloadRequest,
    _detect_source_format,
    _FileProgressAdapter,
    _resolve_dest,
    async_download_batch,
    async_download_file,
    download_batch,
    download_file,
)

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
