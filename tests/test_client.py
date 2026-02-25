"""Tests for the mstransfer client (resolve_inputs, helpers, send_file)."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
import uvicorn

from mstransfer.client.sender import (
    _counting_generator,
    _file_chunk_generator,
    resolve_inputs,
    send_batch,
    send_file,
)
from mstransfer.server.app import create_app

# ---------------------------------------------------------------------------
# resolve_inputs
# ---------------------------------------------------------------------------


class TestResolveInputs:
    def test_single_mzml_file(self, sample_files):
        result = resolve_inputs([str(sample_files / "a.mzML")])
        assert len(result) == 1
        assert result[0].name == "a.mzML"

    def test_single_msz_file(self, sample_files):
        result = resolve_inputs([str(sample_files / "b.msz")])
        assert len(result) == 1
        assert result[0].name == "b.msz"

    def test_skips_unsupported_extension(self, sample_files):
        with pytest.raises(FileNotFoundError):
            resolve_inputs([str(sample_files / "c.txt")])

    def test_directory_non_recursive(self, sample_files):
        result = resolve_inputs([str(sample_files)])
        names = {p.name for p in result}
        assert "a.mzML" in names
        assert "b.msz" in names
        assert "d.mzML" not in names

    def test_directory_recursive(self, sample_files):
        result = resolve_inputs([str(sample_files)], recursive=True)
        names = {p.name for p in result}
        assert "a.mzML" in names
        assert "b.msz" in names
        assert "d.mzML" in names
        assert "e.msz" in names

    def test_mixed_files_and_dirs(self, sample_files):
        result = resolve_inputs(
            [str(sample_files / "a.mzML"), str(sample_files / "sub")],
        )
        names = {p.name for p in result}
        assert "a.mzML" in names
        assert "d.mzML" in names
        assert "e.msz" in names

    def test_no_valid_files_raises(self, tmp_path):
        (tmp_path / "empty_dir").mkdir()
        with pytest.raises(FileNotFoundError, match="No valid"):
            resolve_inputs([str(tmp_path / "empty_dir")])

    def test_nonexistent_path(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            resolve_inputs([str(tmp_path / "does_not_exist")])

    def test_results_are_sorted(self, sample_files):
        result = resolve_inputs([str(sample_files)], recursive=True)
        assert result == sorted(result)

    def test_no_duplicates(self, sample_files):
        result = resolve_inputs([str(sample_files), str(sample_files)], recursive=True)
        assert len(result) == len(set(result))


# ---------------------------------------------------------------------------
# _counting_generator
# ---------------------------------------------------------------------------


class TestCountingGenerator:
    def test_yields_all_chunks(self):
        chunks = [b"aaa", b"bb", b"c"]
        result = list(_counting_generator(iter(chunks)))
        assert result == chunks

    def test_callback_called_with_lengths(self):
        chunks = [b"aaa", b"bb", b"c"]
        sizes = []
        list(_counting_generator(iter(chunks), callback=sizes.append))
        assert sizes == [3, 2, 1]

    def test_no_callback(self):
        chunks = [b"hello"]
        result = list(_counting_generator(iter(chunks), callback=None))
        assert result == chunks


# ---------------------------------------------------------------------------
# _file_chunk_generator
# ---------------------------------------------------------------------------


class TestFileChunkGenerator:
    def test_reads_full_file(self, tmp_path):
        f = tmp_path / "data.bin"
        content = b"x" * 100
        f.write_bytes(content)
        result = b"".join(_file_chunk_generator(f, chunk_size=1024))
        assert result == content

    def test_chunked_reads(self, tmp_path):
        f = tmp_path / "data.bin"
        content = b"abcdefghij"
        f.write_bytes(content)
        chunks = list(_file_chunk_generator(f, chunk_size=3))
        assert len(chunks) == 4  # 3+3+3+1
        assert b"".join(chunks) == content

    def test_callback_reports_sizes(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"abcdefghij")
        sizes = []
        list(_file_chunk_generator(f, chunk_size=4, callback=sizes.append))
        assert sizes == [4, 4, 2]


# ---------------------------------------------------------------------------
# send_file (integration with real server + real mscompress files)
# ---------------------------------------------------------------------------


@pytest.fixture()
def _live_server(tmp_path):
    """Start a real mstransfer server on a free port in a background thread."""
    output_dir = tmp_path / "server_output"
    output_dir.mkdir()
    app = create_app(output_dir=str(output_dir), store_as="msz")

    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for server to be ready
    for _ in range(50):
        if server.started:
            break
        time.sleep(0.1)
    else:
        raise RuntimeError("Server did not start in time")

    # Extract the actual bound port
    sockets = server.servers[0].sockets
    port = sockets[0].getsockname()[1]

    yield {"host": "127.0.0.1", "port": port, "output_dir": output_dir}

    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture()
def _live_server_mzml(tmp_path):
    """Start a real mstransfer server in mzml mode."""
    output_dir = tmp_path / "server_output_mzml"
    output_dir.mkdir()
    app = create_app(output_dir=str(output_dir), store_as="mzml")

    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    for _ in range(50):
        if server.started:
            break
        time.sleep(0.1)
    else:
        raise RuntimeError("Server did not start in time")

    sockets = server.servers[0].sockets
    port = sockets[0].getsockname()[1]

    yield {"host": "127.0.0.1", "port": port, "output_dir": output_dir}

    server.should_exit = True
    thread.join(timeout=5)


class TestSendFile:
    def test_send_msz_file(self, test_msz, _live_server):
        """Send a real .msz file to the server."""
        result = send_file(
            test_msz,
            _live_server["host"],
            _live_server["port"],
        )
        assert result["state"] == "done"
        assert result["filename"] == "test.msz"
        assert result["bytes_received"] == test_msz.stat().st_size

        written = _live_server["output_dir"] / "test.msz"
        assert written.exists()
        assert written.read_bytes() == test_msz.read_bytes()

    def test_send_mzml_file(self, test_mzml, _live_server):
        """Send a real .mzML file — sender compresses on the fly."""
        result = send_file(
            test_mzml,
            _live_server["host"],
            _live_server["port"],
        )
        assert result["state"] == "done"
        assert result["filename"] == "test.mzML"
        assert result["bytes_received"] > 0

        # Server stored as msz
        written = _live_server["output_dir"] / "test.msz"
        assert written.exists()
        assert written.stat().st_size > 0

    def test_send_mzml_file_server_decompresses(self, test_mzml, _live_server_mzml):
        """Send .mzML → server compresses to msz in transit, decompresses back."""
        result = send_file(
            test_mzml,
            _live_server_mzml["host"],
            _live_server_mzml["port"],
        )
        assert result["state"] == "done"

        # Server should have decompressed back to mzML
        mzml_out = _live_server_mzml["output_dir"] / "test.mzML"
        assert mzml_out.exists()
        assert mzml_out.stat().st_size == test_mzml.stat().st_size

    def test_send_file_progress_callback(self, test_msz, _live_server):
        """Progress callback should be invoked with byte deltas."""
        deltas = []
        send_file(
            test_msz,
            _live_server["host"],
            _live_server["port"],
            progress_callback=deltas.append,
        )
        assert len(deltas) > 0
        assert sum(deltas) == test_msz.stat().st_size

    def test_send_msz_file_custom_chunk_size(self, test_msz, _live_server):
        """Send a .msz file with a small custom chunk_size."""
        result = send_file(
            test_msz,
            _live_server["host"],
            _live_server["port"],
            chunk_size=512,
        )
        assert result["state"] == "done"
        assert result["bytes_received"] == test_msz.stat().st_size

    def test_send_mzml_file_custom_chunk_size(self, test_mzml, _live_server):
        """Send a .mzML file with a custom chunk_size passed to compress_stream."""
        result = send_file(
            test_mzml,
            _live_server["host"],
            _live_server["port"],
            chunk_size=2048,
        )
        assert result["state"] == "done"
        assert result["bytes_received"] > 0

    def test_chunk_size_affects_generator(self, test_msz, _live_server):
        """Smaller chunk_size should produce more progress callbacks."""
        small_deltas = []
        send_file(
            test_msz,
            _live_server["host"],
            _live_server["port"],
            progress_callback=small_deltas.append,
            chunk_size=256,
        )
        large_deltas = []
        send_file(
            test_msz,
            _live_server["host"],
            _live_server["port"],
            progress_callback=large_deltas.append,
            chunk_size=1_048_576,
        )
        # Smaller chunks should produce at least as many callbacks
        assert len(small_deltas) >= len(large_deltas)
        # Both should transfer the full file
        assert sum(small_deltas) == test_msz.stat().st_size
        assert sum(large_deltas) == test_msz.stat().st_size


class TestSendBatch:
    def test_single_file(self, test_msz, _live_server):
        """send_batch with a single file returns a one-element result list."""
        results = send_batch(
            [test_msz],
            _live_server["host"],
            _live_server["port"],
            parallel=1,
        )
        assert len(results) == 1
        r = results[0]
        assert r is not None
        assert r["state"] == "done"
        assert r["filename"] == "test.msz"

    def test_multiple_msz_files(self, test_msz, _live_server, tmp_path):
        """send_batch sends multiple files and returns results for each."""
        copies = []
        for i in range(3):
            copy = tmp_path / f"copy_{i}.msz"
            copy.write_bytes(test_msz.read_bytes())
            copies.append(copy)

        results = send_batch(
            copies,
            _live_server["host"],
            _live_server["port"],
            parallel=2,
        )
        assert len(results) == 3
        for r in results:
            assert r is not None
            assert r["state"] == "done"

    def test_results_preserve_input_order(self, test_msz, _live_server, tmp_path):
        """Results list indices match the input file_paths indices."""
        files = []
        for name in ["alpha.msz", "beta.msz", "gamma.msz"]:
            f = tmp_path / name
            f.write_bytes(test_msz.read_bytes())
            files.append(f)

        results = send_batch(
            files,
            _live_server["host"],
            _live_server["port"],
            parallel=1,
        )
        for i, f in enumerate(files):
            r = results[i]
            assert r is not None
            assert r["filename"] == f.name

    def test_mixed_msz_and_mzml(self, test_msz, test_mzml, _live_server):
        """send_batch handles a mix of .msz and .mzML files."""
        results = send_batch(
            [test_msz, test_mzml],
            _live_server["host"],
            _live_server["port"],
            parallel=2,
        )
        assert len(results) == 2
        r0, r1 = results[0], results[1]
        assert r0 is not None
        assert r1 is not None
        assert r0["state"] == "done"
        assert r1["state"] == "done"

    def test_error_captured_in_results(self, test_msz, _live_server):
        """When send_file raises, the error is captured in the results list."""
        with patch(
            "mstransfer.client.sender.send_file",
            side_effect=ConnectionError("server exploded"),
        ):
            results = send_batch(
                [test_msz],
                _live_server["host"],
                _live_server["port"],
                parallel=1,
            )
        assert len(results) == 1
        r = results[0]
        assert r is not None
        assert "error" in r
        assert "server exploded" in r["error"]
        assert r["filename"] == "test.msz"

    def test_partial_failure(self, test_msz, _live_server, tmp_path):
        """One failure does not prevent other files from succeeding."""
        good_file = tmp_path / "good.msz"
        good_file.write_bytes(test_msz.read_bytes())

        original_send = send_file

        def flaky_send(file_path, *args, **kwargs):
            if file_path.name == "bad.msz":
                raise ConnectionError("boom")
            return original_send(file_path, *args, **kwargs)

        bad_file = tmp_path / "bad.msz"
        bad_file.write_bytes(test_msz.read_bytes())

        with patch("mstransfer.client.sender.send_file", side_effect=flaky_send):
            results = send_batch(
                [good_file, bad_file],
                _live_server["host"],
                _live_server["port"],
                parallel=2,
            )
        r0, r1 = results[0], results[1]
        assert r0 is not None
        assert r1 is not None
        assert r0["state"] == "done"
        assert "error" in r1
        assert r1["filename"] == "bad.msz"

    def test_parallel_capped_to_file_count(self, test_msz, _live_server):
        """Workers should not exceed the number of files."""
        with patch(
            "mstransfer.client.sender.ThreadPoolExecutor",
            wraps=ThreadPoolExecutor,
        ) as mock_pool:
            send_batch(
                [test_msz],
                _live_server["host"],
                _live_server["port"],
                parallel=8,
            )
            mock_pool.assert_called_once_with(max_workers=1)

    def test_chunk_size_passed_to_send_file(self, test_msz, _live_server):
        """send_batch should forward chunk_size to send_file."""
        with patch(
            "mstransfer.client.sender.send_file", wraps=send_file,
        ) as mock_send:
            send_batch(
                [test_msz],
                _live_server["host"],
                _live_server["port"],
                parallel=1,
                chunk_size=4096,
            )
            mock_send.assert_called_once()
            _, kwargs = mock_send.call_args
            assert kwargs["chunk_size"] == 4096
