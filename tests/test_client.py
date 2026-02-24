"""Tests for the mstransfer client (resolve_inputs, helpers, send_file)."""

from __future__ import annotations

import threading
import time

import pytest
import uvicorn

from mstransfer.client.sender import (
    _counting_generator,
    _file_chunk_generator,
    resolve_inputs,
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
