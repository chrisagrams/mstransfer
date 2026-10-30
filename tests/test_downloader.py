"""Tests for the mstransfer download client."""

from __future__ import annotations

import asyncio
import http.server
import io
import json
import posixpath
import shutil
import tarfile
import threading
import urllib.parse
from unittest.mock import patch

import httpx
import pytest
from mscompress.mszx import compress_batch

from mstransfer.client.downloader import (
    DownloadRequest,
    _FileProgressAdapter,
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
# Multi-member (batch v2) .mszx archives
# ---------------------------------------------------------------------------


def _write_batch_mszx(path, members):
    """Write a v2 batch .mszx whose *members* map entry name → source file."""
    manifest = json.dumps(
        {
            "version": "2.0",
            "container": "batch",
            "spectra_files": [{"entry": entry} for entry in members],
        }
    ).encode()
    with tarfile.open(path, "w") as tar:
        info = tarfile.TarInfo(name="manifest.json")
        info.size = len(manifest)
        tar.addfile(info, io.BytesIO(manifest))
        for entry, src in members.items():
            tar.add(str(src), arcname=entry)


@pytest.fixture()
def served_batch_mszx(served_ms_files, test_mzml, tmp_path):
    """Serve batch archives built by mscompress.

    ``batch.mszx`` has members ``a.msz`` and ``b.msz``; ``batch2.mszx`` has
    ``a.msz`` and ``z.msz``, so the two share a member name.
    """
    inputs = tmp_path / "batch_inputs"
    inputs.mkdir()
    for name in ("a", "b", "z"):
        (inputs / name).mkdir()
        shutil.copy(test_mzml, inputs / name / f"{name}.mzML")
    compress_batch(
        [inputs / "a" / "a.mzML", inputs / "b" / "b.mzML"],
        served_ms_files["serve_dir"] / "batch.mszx",
    )
    compress_batch(
        [inputs / "a" / "a.mzML", inputs / "z" / "z.mzML"],
        served_ms_files["serve_dir"] / "batch2.mszx",
    )
    return served_ms_files


class TestBatchMszx:
    def test_store_as_msz_writes_archive_dir(self, served_batch_mszx, tmp_path):
        """Members are written to dest.parent/<dest stem>/<entry stem>.msz."""
        out_dir = tmp_path / "out"
        result = download_file(
            f"{served_batch_mszx['base_url']}/batch.mszx",
            out_dir / "batch.mszx",
            store_as="msz",
        )
        assert result == out_dir / "batch"
        # Only the archive directory remains: no staging dirs, no archive.
        assert list(out_dir.iterdir()) == [result]
        assert sorted(p.name for p in result.iterdir()) == ["a.msz", "b.msz"]

        archive = served_batch_mszx["serve_dir"] / "batch.mszx"
        with tarfile.open(archive) as tar:
            for path in result.iterdir():
                member = tar.extractfile(path.name)
                assert member is not None
                assert path.read_bytes() == member.read()

    def test_store_as_mzml_writes_archive_dir(
        self, served_batch_mszx, test_mzml, tmp_path
    ):
        """Members are decompressed to dest.parent/<dest stem>/<entry stem>.mzML."""
        out_dir = tmp_path / "out"
        result = download_file(
            f"{served_batch_mszx['base_url']}/batch.mszx",
            out_dir / "batch.mszx",
            store_as="mzml",
        )
        assert result == out_dir / "batch"
        assert list(out_dir.iterdir()) == [result]
        assert sorted(p.name for p in result.iterdir()) == ["a.mzML", "b.mzML"]
        for path in result.iterdir():
            assert path.read_bytes() == test_mzml.read_bytes()

    def test_store_as_none_keeps_archive(self, served_batch_mszx, tmp_path):
        """Without store_as the batch archive is saved as-is."""
        dest = tmp_path / "batch.mszx"
        result = download_file(
            f"{served_batch_mszx['base_url']}/batch.mszx",
            dest,
        )
        assert result == dest
        archive = served_batch_mszx["serve_dir"] / "batch.mszx"
        assert dest.read_bytes() == archive.read_bytes()

    def test_does_not_touch_sibling_files(self, served_batch_mszx, tmp_path):
        """Files next to the archive directory are left alone."""
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        (out_dir / "a.msz").write_bytes(b"unrelated")
        download_file(
            f"{served_batch_mszx['base_url']}/batch.mszx",
            out_dir / "batch.mszx",
            store_as="msz",
        )
        assert (out_dir / "a.msz").read_bytes() == b"unrelated"

    def test_replaces_existing_archive_dir(self, served_batch_mszx, tmp_path):
        """An existing archive directory is replaced as a whole."""
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        old = out_dir / "batch"
        old.mkdir()
        (old / "a.msz").write_bytes(b"stale")
        (old / "old.msz").write_bytes(b"stale")
        result = download_file(
            f"{served_batch_mszx['base_url']}/batch.mszx",
            out_dir / "batch.mszx",
            store_as="msz",
        )
        assert sorted(p.name for p in result.iterdir()) == ["a.msz", "b.msz"]
        assert (result / "a.msz").read_bytes() != b"stale"
        assert list(out_dir.iterdir()) == [result]

    def test_skip_existing_archive_dir_skips_download(
        self, served_batch_mszx, tmp_path
    ):
        """An existing archive directory is trusted without fetching the URL."""
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        existing = out_dir / "gone"
        existing.mkdir()
        (existing / "a.msz").write_bytes(b"existing")
        # gone.mszx isn't served, so any request would fail with a 404.
        result = download_file(
            f"{served_batch_mszx['base_url']}/gone.mszx",
            out_dir / "gone.mszx",
            store_as="msz",
            skip_existing=True,
        )
        assert result == existing
        assert (existing / "a.msz").read_bytes() == b"existing"

    def test_force_overrides_skip_existing(self, served_batch_mszx, tmp_path):
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        existing = out_dir / "batch"
        existing.mkdir()
        (existing / "a.msz").write_bytes(b"existing")
        download_file(
            f"{served_batch_mszx['base_url']}/batch.mszx",
            out_dir / "batch.mszx",
            store_as="msz",
            skip_existing=True,
            force=True,
        )
        assert (existing / "a.msz").read_bytes() != b"existing"
        assert (existing / "b.msz").exists()

    def test_archive_dir_path_is_a_file(self, served_batch_mszx, tmp_path):
        """A regular file where the archive directory should go is an error."""
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        (out_dir / "batch").write_bytes(b"in the way")
        with pytest.raises(FileExistsError):
            download_file(
                f"{served_batch_mszx['base_url']}/batch.mszx",
                out_dir / "batch.mszx",
                store_as="msz",
            )
        assert (out_dir / "batch").read_bytes() == b"in the way"
        assert list(out_dir.iterdir()) == [out_dir / "batch"]

    def test_colliding_member_names_raise(self, served_ms_files, test_msz, tmp_path):
        """Entries that map to the same output name fail before writing."""
        _write_batch_mszx(
            served_ms_files["serve_dir"] / "dup.mszx",
            {"a.msz": test_msz, "sub/A.msz": test_msz},
        )
        out_dir = tmp_path / "out"
        with pytest.raises(ValueError, match="both map to"):
            download_file(
                f"{served_ms_files['base_url']}/dup.mszx",
                out_dir / "dup.mszx",
                store_as="msz",
            )
        assert list(out_dir.iterdir()) == []

    def test_member_dirs_are_stripped(self, served_ms_files, test_msz, tmp_path):
        r"""Directory parts of entry names, "/" or "\", never escape the dir."""
        _write_batch_mszx(
            served_ms_files["serve_dir"] / "nested.mszx",
            {"../x.msz": test_msz, "sub/y.msz": test_msz, "..\\w.msz": test_msz},
        )
        out_dir = tmp_path / "out"
        result = download_file(
            f"{served_ms_files['base_url']}/nested.mszx",
            out_dir / "nested.mszx",
            store_as="msz",
        )
        assert result == out_dir / "nested"
        assert list(out_dir.iterdir()) == [result]
        assert sorted(p.name for p in result.iterdir()) == ["w.msz", "x.msz", "y.msz"]

    def test_uppercase_msz_suffix_is_replaced(
        self, served_ms_files, test_msz, tmp_path
    ):
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        _write_batch_mszx(
            served_ms_files["serve_dir"] / "upper.mszx",
            {"A.MSZ": test_msz, "b.msz": test_msz},
        )
        result = download_file(
            f"{served_ms_files['base_url']}/upper.mszx",
            out_dir / "upper.mszx",
            store_as="msz",
        )
        assert sorted(p.name for p in result.iterdir()) == ["A.msz", "b.msz"]

    @pytest.mark.parametrize("entry", [".msz", "..", "sub/.hidden.msz"])
    def test_unsafe_member_names_raise(
        self, served_ms_files, test_msz, tmp_path, entry
    ):
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        _write_batch_mszx(
            served_ms_files["serve_dir"] / "unsafe.mszx",
            {"a.msz": test_msz, entry: test_msz},
        )
        with pytest.raises(ValueError, match="Unsafe"):
            download_file(
                f"{served_ms_files['base_url']}/unsafe.mszx",
                out_dir / "unsafe.mszx",
                store_as="msz",
            )
        assert list(out_dir.iterdir()) == []

    def test_failed_member_leaves_existing_dir(
        self, served_ms_files, test_msz, tmp_path
    ):
        """A bad member aborts the archive and keeps the previous output."""
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        src = served_ms_files["serve_dir"] / "bad.mszx"
        # Declare a second member in the manifest that isn't in the tar.
        manifest = json.dumps(
            {
                "version": "2.0",
                "container": "batch",
                "spectra_files": [{"entry": "a.msz"}, {"entry": "missing.msz"}],
            }
        ).encode()
        with tarfile.open(src, "w") as tar:
            info = tarfile.TarInfo(name="manifest.json")
            info.size = len(manifest)
            tar.addfile(info, io.BytesIO(manifest))
            tar.add(str(test_msz), arcname="a.msz")

        previous = out_dir / "bad"
        previous.mkdir()
        (previous / "old.msz").write_bytes(b"previous")
        with pytest.raises(ValueError, match="missing from the archive"):
            download_file(
                f"{served_ms_files['base_url']}/bad.mszx",
                out_dir / "bad.mszx",
                store_as="msz",
            )
        assert list(out_dir.iterdir()) == [previous]
        assert [p.name for p in previous.iterdir()] == ["old.msz"]

    def test_download_batch_returns_archive_dirs(self, served_batch_mszx, tmp_path):
        """download_batch returns one path per request, in input order."""
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        base = served_batch_mszx["base_url"]
        requests = [
            DownloadRequest(url=f"{base}/batch.mszx", dest=out_dir / "batch.mszx"),
            DownloadRequest(url=f"{base}/test.mszx", dest=out_dir / "single.mszx"),
        ]
        results = download_batch(requests, store_as="msz", parallel=1)
        assert results == [out_dir / "batch", out_dir / "single.msz"]

    def test_parallel_archives_sharing_member_name(self, served_batch_mszx, tmp_path):
        """Archives with a common member name don't clobber each other."""
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        base = served_batch_mszx["base_url"]
        requests = [
            DownloadRequest(url=f"{base}/batch.mszx", dest=out_dir / "batch.mszx"),
            DownloadRequest(url=f"{base}/batch2.mszx", dest=out_dir / "batch2.mszx"),
        ]
        results = download_batch(requests, store_as="msz", parallel=2)
        assert results == [out_dir / "batch", out_dir / "batch2"]
        assert sorted(p.name for p in results[0].iterdir()) == ["a.msz", "b.msz"]
        assert sorted(p.name for p in results[1].iterdir()) == ["a.msz", "z.msz"]
        assert sorted(out_dir.iterdir()) == results


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
