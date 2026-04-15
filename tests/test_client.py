"""Tests for the mstransfer client (resolve_inputs, helpers, send_file)."""

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest
import uvicorn
from mscompress import MSZFile, MZMLFile
from mscompress.mszx import MSZXFile

from mstransfer.client.sender import (
    async_send_batch,
    async_send_file,
    send_batch,
    send_file,
)
from mstransfer.client.utils import ThrottledCallback, resolve_inputs
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
# ThrottledCallback
# ---------------------------------------------------------------------------


class TestThrottledCallback:
    def test_accumulates_below_threshold(self):
        """Deltas below threshold are accumulated, not forwarded."""
        forwarded: list[int] = []
        cb = ThrottledCallback(forwarded.append, threshold=100)
        cb(30)
        cb(30)
        assert forwarded == []
        assert cb._accumulated == 60

    def test_forwards_at_threshold(self):
        """Callback fires once accumulated bytes reach the threshold."""
        forwarded: list[int] = []
        cb = ThrottledCallback(forwarded.append, threshold=100)
        cb(60)
        cb(50)  # 110 >= 100 → forward
        assert forwarded == [110]
        assert cb._accumulated == 0

    def test_flush_sends_remainder(self):
        """flush() forwards any leftover accumulated bytes."""
        forwarded: list[int] = []
        cb = ThrottledCallback(forwarded.append, threshold=100)
        cb(40)
        cb.flush()
        assert forwarded == [40]
        assert cb._accumulated == 0

    def test_flush_noop_when_empty(self):
        """flush() does nothing if there are no accumulated bytes."""
        forwarded: list[int] = []
        cb = ThrottledCallback(forwarded.append, threshold=100)
        cb.flush()
        assert forwarded == []

    def test_total_bytes_preserved(self):
        """Sum of forwarded deltas equals sum of input deltas after flush."""
        forwarded: list[int] = []
        cb = ThrottledCallback(forwarded.append, threshold=50)
        for _ in range(100):
            cb(7)
        cb.flush()
        assert sum(forwarded) == 700


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

    yield {"base_url": f"http://127.0.0.1:{port}", "output_dir": output_dir}

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

    yield {"base_url": f"http://127.0.0.1:{port}", "output_dir": output_dir}

    server.should_exit = True
    thread.join(timeout=5)


class TestSendFile:
    def test_send_msz_file(self, test_msz, _live_server):
        """Send a real .msz file to the server."""
        result = send_file(
            test_msz,
            _live_server["base_url"],
        )
        assert result.state == "done"
        assert result.filename == "test.msz"
        assert result.bytes_received == test_msz.stat().st_size

        written = _live_server["output_dir"] / "test.msz"
        assert written.exists()
        assert written.read_bytes() == test_msz.read_bytes()

    def test_send_mzml_file(self, test_mzml, _live_server):
        """Send a real .mzML file — sender compresses on the fly."""
        result = send_file(
            test_mzml,
            _live_server["base_url"],
        )
        assert result.state == "done"
        assert result.filename == "test.mzML"
        assert result.bytes_received > 0

        # Server stored as msz
        written = _live_server["output_dir"] / "test.msz"
        assert written.exists()
        assert written.stat().st_size > 0

    def test_send_mzml_file_server_decompresses(self, test_mzml, _live_server_mzml):
        """Send .mzML → server compresses to msz in transit, decompresses back."""
        result = send_file(
            test_mzml,
            _live_server_mzml["base_url"],
        )
        assert result.state == "done"

        # Server should have decompressed back to mzML
        mzml_out = _live_server_mzml["output_dir"] / "test.mzML"
        assert mzml_out.exists()
        assert mzml_out.stat().st_size == test_mzml.stat().st_size

    def test_send_file_progress_callback(self, test_msz, _live_server):
        """Progress callback should be invoked with byte deltas."""
        deltas = []
        send_file(
            test_msz,
            _live_server["base_url"],
            progress_callback=deltas.append,
        )
        assert len(deltas) > 0
        assert sum(deltas) == test_msz.stat().st_size

    def test_send_msz_file_custom_chunk_size(self, test_msz, _live_server):
        """Send a .msz file with a small custom chunk_size."""
        result = send_file(
            test_msz,
            _live_server["base_url"],
            chunk_size=512,
        )
        assert result.state == "done"
        assert result.bytes_received == test_msz.stat().st_size

    def test_send_mzml_file_custom_chunk_size(self, test_mzml, _live_server):
        """Send a .mzML file with a custom chunk_size passed to compress_stream."""
        result = send_file(
            test_mzml,
            _live_server["base_url"],
            chunk_size=2048,
        )
        assert result.state == "done"
        assert result.bytes_received > 0

    def test_chunk_size_affects_generator(self, test_msz, _live_server):
        """Both small and large chunk sizes should transfer the full file."""
        small_deltas: list[int] = []
        send_file(
            test_msz,
            _live_server["base_url"],
            progress_callback=small_deltas.append,
            chunk_size=256,
        )
        large_deltas: list[int] = []
        send_file(
            test_msz,
            _live_server["base_url"],
            progress_callback=large_deltas.append,
            chunk_size=1_048_576,
        )
        # Both should transfer the full file regardless of chunk size
        assert sum(small_deltas) == test_msz.stat().st_size
        assert sum(large_deltas) == test_msz.stat().st_size

    def test_send_mszfile_object(self, test_msz, _live_server):
        """send_file accepts an MSZFile object directly."""
        msz = MSZFile(str(test_msz).encode())
        result = send_file(msz, _live_server["base_url"])
        assert result.state == "done"
        assert result.filename == "test.msz"
        assert result.bytes_received == test_msz.stat().st_size

    def test_send_mzmlfile_object(self, test_mzml, _live_server):
        """send_file accepts an MZMLFile object directly."""
        mzml = MZMLFile(str(test_mzml).encode())
        result = send_file(mzml, _live_server["base_url"])
        assert result.state == "done"
        assert result.filename == "test.mzML"
        assert result.bytes_received > 0

    def test_send_mzmlfile_object_server_decompresses(
        self,
        test_mzml,
        _live_server_mzml,
    ):
        """MZMLFile object → server decompresses back to mzML."""
        mzml = MZMLFile(str(test_mzml).encode())
        result = send_file(mzml, _live_server_mzml["base_url"])
        assert result.state == "done"

        mzml_out = _live_server_mzml["output_dir"] / "test.mzML"
        assert mzml_out.exists()
        assert mzml_out.stat().st_size == test_mzml.stat().st_size

    def test_send_mszxfile_object(self, test_mszx, _live_server):
        """send_file accepts an MSZXFile object directly."""
        mszx = MSZXFile.open(test_mszx)
        result = send_file(mszx, _live_server["base_url"])
        assert result.state == "done"
        assert result.filename == "test.mszx"
        assert result.bytes_received == test_mszx.stat().st_size
        mszx.close()

    def test_send_file_rejects_invalid_type(self, _live_server):
        """send_file raises TypeError for unsupported input types."""
        with pytest.raises(TypeError, match="Unsupported source type"):
            send_file("not_a_path_or_file", _live_server["base_url"])  # type: ignore[arg-type]


class TestSendBatch:
    def test_single_file(self, test_msz, _live_server):
        """send_batch with a single file returns a one-element result list."""
        results = send_batch(
            [test_msz],
            _live_server["base_url"],
            parallel=1,
        )
        assert len(results) == 1
        r = results[0]
        assert r.response is not None
        assert r.response.state == "done"
        assert r.filename == "test.msz"

    def test_multiple_msz_files(self, test_msz, _live_server, tmp_path):
        """send_batch sends multiple files and returns results for each."""
        copies = []
        for i in range(3):
            copy = tmp_path / f"copy_{i}.msz"
            copy.write_bytes(test_msz.read_bytes())
            copies.append(copy)

        results = send_batch(
            copies,
            _live_server["base_url"],
            parallel=2,
        )
        assert len(results) == 3
        for r in results:
            assert r.response is not None
            assert r.response.state == "done"

    def test_results_contain_all_filenames(self, test_msz, _live_server, tmp_path):
        """Results contain an entry for every input file."""
        files = []
        for name in ["alpha.msz", "beta.msz", "gamma.msz"]:
            f = tmp_path / name
            f.write_bytes(test_msz.read_bytes())
            files.append(f)

        results = send_batch(
            files,
            _live_server["base_url"],
            parallel=1,
        )
        result_names = {r.filename for r in results}
        assert result_names == {f.name for f in files}

    def test_mixed_msz_and_mzml(self, test_msz, test_mzml, _live_server):
        """send_batch handles a mix of .msz and .mzML files."""
        results = send_batch(
            [test_msz, test_mzml],
            _live_server["base_url"],
            parallel=2,
        )
        assert len(results) == 2
        r0, r1 = results[0], results[1]
        assert r0.response is not None
        assert r1.response is not None
        assert r0.response.state == "done"
        assert r1.response.state == "done"

    def test_error_captured_in_results(self, test_msz, _live_server):
        """When async_send_file raises, the error is captured in the results list."""

        async def _explode(*args, **kwargs):
            raise ConnectionError("server exploded")

        with patch(
            "mstransfer.client.sender.async_send_file",
            side_effect=_explode,
        ):
            results = send_batch(
                [test_msz],
                _live_server["base_url"],
                parallel=1,
            )
        assert len(results) == 1
        r = results[0]
        assert r.error is not None
        assert "server exploded" in r.error
        assert r.filename == "test.msz"

    def test_partial_failure(self, test_msz, _live_server, tmp_path):
        """One failure does not prevent other files from succeeding."""
        good_file = tmp_path / "good.msz"
        good_file.write_bytes(test_msz.read_bytes())

        original_send = async_send_file

        async def flaky_send(source, *args, **kwargs):
            if isinstance(source, MSZXFile):
                name = source.archive_path.name
            elif isinstance(source, Path):
                name = source.name
            else:
                name = Path(source.path.decode()).name
            if name == "bad.msz":
                raise ConnectionError("boom")
            return await original_send(source, *args, **kwargs)

        bad_file = tmp_path / "bad.msz"
        bad_file.write_bytes(test_msz.read_bytes())

        with patch("mstransfer.client.sender.async_send_file", side_effect=flaky_send):
            results = send_batch(
                [good_file, bad_file],
                _live_server["base_url"],
                parallel=2,
            )
        by_name = {r.filename: r for r in results}
        assert by_name["good.msz"].response is not None
        assert by_name["good.msz"].response.state == "done"
        assert by_name["bad.msz"].error is not None

    def test_parallel_capped_to_file_count(self, test_msz, _live_server):
        """Semaphore should not exceed the number of files."""
        with patch(
            "asyncio.Semaphore",
            wraps=asyncio.Semaphore,
        ) as mock_sem:
            send_batch(
                [test_msz],
                _live_server["base_url"],
                parallel=8,
            )
            mock_sem.assert_called_once_with(1)

    def test_chunk_size_passed_to_send_file(self, test_msz, _live_server):
        """send_batch should forward chunk_size to async_send_file."""
        with patch(
            "mstransfer.client.sender.async_send_file",
            wraps=async_send_file,
        ) as mock_send:
            send_batch(
                [test_msz],
                _live_server["base_url"],
                parallel=1,
                chunk_size=4096,
            )
            mock_send.assert_called_once()
            _, kwargs = mock_send.call_args
            assert kwargs["chunk_size"] == 4096

    def test_batch_with_mscompress_objects(self, test_msz, test_mzml, _live_server):
        """send_batch accepts MSZFile and MZMLFile objects."""
        msz = MSZFile(str(test_msz).encode())
        mzml = MZMLFile(str(test_mzml).encode())
        results = send_batch(
            [msz, mzml],
            _live_server["base_url"],
            parallel=2,
        )
        assert len(results) == 2
        for r in results:
            assert r.response is not None
            assert r.response.state == "done"

    def test_batch_mixed_paths_and_objects(self, test_msz, test_mzml, _live_server):
        """send_batch accepts a mix of Path and mscompress objects."""
        mzml = MZMLFile(str(test_mzml).encode())
        results = send_batch(
            [test_msz, mzml],
            _live_server["base_url"],
            parallel=2,
        )
        assert len(results) == 2
        names = {r.filename for r in results}
        assert names == {"test.msz", "test.mzML"}
        for r in results:
            assert r.response is not None
            assert r.response.state == "done"

    def test_batch_with_mszxfile_object(self, test_msz, test_mszx, _live_server):
        """send_batch accepts MSZXFile objects alongside Paths."""
        mszx = MSZXFile.open(test_mszx)
        results = send_batch(
            [test_msz, mszx],
            _live_server["base_url"],
            parallel=2,
        )
        mszx.close()
        assert len(results) == 2
        names = {r.filename for r in results}
        assert names == {"test.msz", "test.mszx"}
        for r in results:
            assert r.response is not None
            assert r.response.state == "done"


# ---------------------------------------------------------------------------
# async_send_file
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestAsyncSendFile:
    async def test_send_msz_file(self, test_msz, _live_server):
        """Send a real .msz file to the server (async)."""
        result = await async_send_file(
            test_msz,
            _live_server["base_url"],
        )
        assert result.state == "done"
        assert result.filename == "test.msz"
        assert result.bytes_received == test_msz.stat().st_size

        written = _live_server["output_dir"] / "test.msz"
        assert written.exists()
        assert written.read_bytes() == test_msz.read_bytes()

    async def test_send_mzml_file(self, test_mzml, _live_server):
        """Send a real .mzML file — sender compresses on the fly (async)."""
        result = await async_send_file(
            test_mzml,
            _live_server["base_url"],
        )
        assert result.state == "done"
        assert result.filename == "test.mzML"
        assert result.bytes_received > 0

        written = _live_server["output_dir"] / "test.msz"
        assert written.exists()
        assert written.stat().st_size > 0

    async def test_send_file_progress_callback(self, test_msz, _live_server):
        """Progress callback should be invoked with byte deltas (async)."""
        deltas: list[int] = []
        await async_send_file(
            test_msz,
            _live_server["base_url"],
            progress_callback=deltas.append,
        )
        assert len(deltas) > 0
        assert sum(deltas) == test_msz.stat().st_size

    async def test_send_mzmlfile_object(self, test_mzml, _live_server):
        """async_send_file accepts an MZMLFile object directly."""
        mzml = MZMLFile(str(test_mzml).encode())
        result = await async_send_file(mzml, _live_server["base_url"])
        assert result.state == "done"
        assert result.filename == "test.mzML"
        assert result.bytes_received > 0

    async def test_send_mszfile_object(self, test_msz, _live_server):
        """async_send_file accepts an MSZFile object directly."""
        msz = MSZFile(str(test_msz).encode())
        result = await async_send_file(msz, _live_server["base_url"])
        assert result.state == "done"
        assert result.filename == "test.msz"
        assert result.bytes_received == test_msz.stat().st_size

    async def test_send_mszxfile_object(self, test_mszx, _live_server):
        """async_send_file accepts an MSZXFile object directly."""
        mszx = MSZXFile.open(test_mszx)
        result = await async_send_file(mszx, _live_server["base_url"])
        assert result.state == "done"
        assert result.filename == "test.mszx"
        assert result.bytes_received == test_mszx.stat().st_size
        mszx.close()

    async def test_send_file_rejects_invalid_type(self, _live_server):
        """async_send_file raises TypeError for unsupported input types."""
        with pytest.raises(TypeError, match="Unsupported source type"):
            await async_send_file("not_a_path_or_file", _live_server["base_url"])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# async_send_batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestAsyncSendBatch:
    async def test_single_file(self, test_msz, _live_server):
        """async_send_batch with a single file returns a one-element result list."""
        results = await async_send_batch(
            [test_msz],
            _live_server["base_url"],
            parallel=1,
        )
        assert len(results) == 1
        r = results[0]
        assert r.response is not None
        assert r.response.state == "done"
        assert r.filename == "test.msz"

    async def test_multiple_files(self, test_msz, _live_server, tmp_path):
        """async_send_batch sends multiple files concurrently."""
        copies = []
        for i in range(3):
            copy = tmp_path / f"copy_{i}.msz"
            copy.write_bytes(test_msz.read_bytes())
            copies.append(copy)

        results = await async_send_batch(
            copies,
            _live_server["base_url"],
            parallel=2,
        )
        assert len(results) == 3
        for r in results:
            assert r.response is not None
            assert r.response.state == "done"

    async def test_mixed_msz_and_mzml(self, test_msz, test_mzml, _live_server):
        """async_send_batch handles a mix of .msz and .mzML files."""
        results = await async_send_batch(
            [test_msz, test_mzml],
            _live_server["base_url"],
            parallel=2,
        )
        assert len(results) == 2
        for r in results:
            assert r.response is not None
            assert r.response.state == "done"
