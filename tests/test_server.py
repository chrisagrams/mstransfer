"""Tests for the mstransfer server (routes, state, models)."""

from __future__ import annotations

import asyncio

import pytest
from mscompress import MZMLFile

from mstransfer.server.fileprovider import DirectoryFileProvider
from mstransfer.server.models import TransferState
from mstransfer.server.state import OutputClaims, TransferRegistry

# ---------------------------------------------------------------------------
# TransferRegistry unit tests
# ---------------------------------------------------------------------------


class TestTransferRegistry:
    def test_create_and_get(self):
        reg = TransferRegistry()
        rec = reg.create("t1", "file.msz")
        assert rec.transfer_id == "t1"
        assert rec.filename == "file.msz"
        assert rec.state == TransferState.RECEIVING

        fetched = reg.get("t1")
        assert fetched is rec

    def test_get_missing(self):
        reg = TransferRegistry()
        assert reg.get("nonexistent") is None

    def test_update(self):
        reg = TransferRegistry()
        reg.create("t1", "file.msz")
        reg.update("t1", state=TransferState.DONE, bytes_received=1024)
        rec = reg.get("t1")
        assert rec is not None
        assert rec.state == TransferState.DONE
        assert rec.bytes_received == 1024

    def test_update_missing(self):
        reg = TransferRegistry()
        assert reg.update("nope", state=TransferState.DONE) is None

    def test_evicts_oldest_terminal_when_full(self):
        reg = TransferRegistry(maxsize=2)
        reg.create("t1", "a.msz")
        reg.update("t1", state=TransferState.DONE)
        reg.create("t2", "b.msz")
        reg.update("t2", state=TransferState.DONE)
        # Creating a third record should evict t1 (oldest terminal).
        reg.create("t3", "c.msz")
        assert reg.get("t1") is None
        assert reg.get("t2") is not None
        assert reg.get("t3") is not None

    def test_never_evicts_in_progress(self):
        reg = TransferRegistry(maxsize=2)
        reg.create("t1", "a.msz")  # RECEIVING (in-progress)
        reg.create("t2", "b.msz")  # RECEIVING (in-progress)
        reg.create("t3", "c.msz")  # RECEIVING (in-progress)
        # All three should be kept — no terminal records to evict.
        assert reg.get("t1") is not None
        assert reg.get("t2") is not None
        assert reg.get("t3") is not None

    def test_evicts_least_recently_used(self):
        reg = TransferRegistry(maxsize=2)
        reg.create("t1", "a.msz")
        reg.update("t1", state=TransferState.DONE)
        reg.create("t2", "b.msz")
        reg.update("t2", state=TransferState.DONE)
        # Access t1 to refresh it (move to end).
        reg.get("t1")
        # Now t2 is the least-recently-used terminal record.
        reg.create("t3", "c.msz")
        assert reg.get("t1") is not None  # refreshed, kept
        assert reg.get("t2") is None  # LRU, evicted
        assert reg.get("t3") is not None

    def test_get_refreshes_order(self):
        reg = TransferRegistry(maxsize=3)
        reg.create("t1", "a.msz")
        reg.update("t1", state=TransferState.DONE)
        reg.create("t2", "b.msz")
        reg.update("t2", state=TransferState.DONE)
        reg.create("t3", "c.msz")
        reg.update("t3", state=TransferState.DONE)
        # Refresh t1 so it's no longer the oldest.
        reg.get("t1")
        # Evict twice by adding two new records.
        reg.create("t4", "d.msz")
        reg.create("t5", "e.msz")
        # t2 and t3 should be evicted (oldest), t1 kept.
        assert reg.get("t2") is None
        assert reg.get("t3") is None
        assert reg.get("t1") is not None


class TestOutputClaims:
    def test_claim_is_exclusive_until_released(self, tmp_path):
        claims = OutputClaims()
        assert claims.claim(tmp_path / "run.msz")
        assert not claims.claim(tmp_path / "run.msz")
        claims.release(tmp_path / "run.msz")
        assert claims.claim(tmp_path / "run.msz")

    def test_claims_compare_case_insensitively(self, tmp_path):
        claims = OutputClaims()
        assert claims.claim(tmp_path / "run.mzML")
        assert not claims.claim(tmp_path / "RUN.mzml")


# ---------------------------------------------------------------------------
# Server route tests (via ASGI transport)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health(msz_client):
    resp = await msz_client.get("/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["store_as"] == "msz"
    assert "version" in data


@pytest.mark.asyncio
async def test_health_mzml_mode(mzml_client):
    resp = await mzml_client.get("/v1/health")
    assert resp.status_code == 200
    assert resp.json()["store_as"] == "mzml"


@pytest.mark.asyncio
async def test_upload_msz_store_as_msz(msz_client, tmp_output, test_msz):
    """Upload a real .msz file; server stores as msz (passthrough)."""
    payload = test_msz.read_bytes()
    resp = await msz_client.post(
        "/v1/upload",
        content=payload,
        headers={
            "X-Transfer-ID": "msz-store-msz",
            "X-Original-Filename": "test.msz",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["transfer_id"] == "msz-store-msz"
    assert data["filename"] == "test.msz"
    assert data["state"] == "done"
    assert data["bytes_received"] == len(payload)
    # Verify written file is byte-identical
    written = tmp_output / "test.msz"
    assert written.exists()
    assert written.read_bytes() == payload


@pytest.mark.asyncio
async def test_upload_msz_store_as_mzml(mzml_client, tmp_output, test_msz):
    """Upload a real .msz file; server decompresses to mzML."""
    payload = test_msz.read_bytes()
    resp = await mzml_client.post(
        "/v1/upload",
        content=payload,
        headers={
            "X-Transfer-ID": "msz-store-mzml",
            "X-Original-Filename": "test.msz",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["transfer_id"] == "msz-store-mzml"
    assert data["state"] == "done"
    assert data["bytes_received"] == len(payload)

    # The temp .msz should be cleaned up
    assert not (tmp_output / "test.msz").exists()
    # The decompressed mzML should exist and be valid
    mzml_out = tmp_output / "test.mzML"
    assert mzml_out.exists()
    assert mzml_out.stat().st_size > 0
    # Sanity check: decompressed mzML should be larger than the compressed msz
    assert mzml_out.stat().st_size > len(payload)


@pytest.mark.asyncio
async def test_upload_mzml_stream_store_as_msz(msz_client, tmp_output, test_mzml):
    """Simulate sender compressing mzML → msz on the fly, server stores msz."""
    mzml = MZMLFile(str(test_mzml).encode())
    compressed = b"".join(mzml.compress_stream(chunk_size=1_048_576))

    resp = await msz_client.post(
        "/v1/upload",
        content=compressed,
        headers={
            "X-Transfer-ID": "mzml-stream-msz",
            "X-Original-Filename": "test.mzML",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "done"
    assert data["bytes_received"] == len(compressed)

    written = tmp_output / "test.msz"
    assert written.exists()
    assert written.stat().st_size == len(compressed)


@pytest.mark.asyncio
async def test_upload_mzml_stream_store_as_mzml(mzml_client, tmp_output, test_mzml):
    """Sender compresses mzML → msz, server decompresses back to mzML."""
    mzml = MZMLFile(str(test_mzml).encode())
    compressed = b"".join(mzml.compress_stream(chunk_size=1_048_576))

    resp = await mzml_client.post(
        "/v1/upload",
        content=compressed,
        headers={
            "X-Transfer-ID": "mzml-roundtrip",
            "X-Original-Filename": "test.mzML",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "done"

    # Round-tripped mzML should exist
    mzml_out = tmp_output / "test.mzML"
    assert mzml_out.exists()
    # Should be approximately the same size as the original
    original_size = test_mzml.stat().st_size
    output_size = mzml_out.stat().st_size
    assert output_size == original_size


@pytest.mark.asyncio
@pytest.mark.parametrize("client_fixture", ["msz_client", "mzml_client"])
@pytest.mark.parametrize("source_format", ["mszx", "MSZX", None])
async def test_upload_mszx_stored_as_is(
    request, client_fixture, source_format, tmp_output, test_mszx
):
    """An .mszx archive is kept byte-for-byte as .mszx, whatever store_as says."""
    client = request.getfixturevalue(client_fixture)
    payload = test_mszx.read_bytes()
    headers = {
        "X-Transfer-ID": "mszx-upload",
        "X-Original-Filename": "run.mszx",
    }
    if source_format is not None:
        headers["X-Source-Format"] = source_format
    resp = await client.post("/v1/upload", content=payload, headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "done"
    assert data["stored_as"] == str(tmp_output / "run.mszx")
    assert (tmp_output / "run.mszx").read_bytes() == payload
    assert sorted(p.name for p in tmp_output.iterdir()) == ["run.mszx"]


@pytest.mark.asyncio
async def test_upload_source_format_header_overrides_extension(
    msz_client, tmp_output, test_mszx
):
    """X-Source-Format wins over the filename extension."""
    payload = test_mszx.read_bytes()
    resp = await msz_client.post(
        "/v1/upload",
        content=payload,
        headers={
            "X-Transfer-ID": "mszx-renamed",
            "X-Original-Filename": "run.bin",
            "X-Source-Format": "mszx",
        },
    )
    assert resp.status_code == 200
    assert (tmp_output / "run.mszx").read_bytes() == payload


@pytest.mark.asyncio
async def test_upload_mzml_source_format_header(mzml_client, tmp_output, test_mzml):
    """The client's X-Source-Format: mzML still means an MSZ stream."""
    mzml = MZMLFile(str(test_mzml).encode())
    compressed = b"".join(mzml.compress_stream(chunk_size=1_048_576))
    resp = await mzml_client.post(
        "/v1/upload",
        content=compressed,
        headers={
            "X-Transfer-ID": "mzml-header",
            "X-Original-Filename": "test.mzML",
            "X-Source-Format": "mzML",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["state"] == "done"
    assert (tmp_output / "test.mzML").stat().st_size == test_mzml.stat().st_size


@pytest.mark.asyncio
async def test_upload_unknown_source_format_rejected(msz_client, tmp_output):
    resp = await msz_client.post(
        "/v1/upload",
        content=b"data",
        headers={
            "X-Transfer-ID": "bad-format",
            "X-Original-Filename": "run.raw",
            "X-Source-Format": "raw",
        },
    )
    assert resp.status_code == 400
    assert list(tmp_output.iterdir()) == []
    status = await msz_client.get("/v1/transfer/bad-format/status")
    assert status.status_code == 404


@pytest.mark.asyncio
async def test_upload_receive_failure_marks_error(msz_client, tmp_output, test_msz):
    """If the upload can't be written, the transfer ends in ERROR with a 500."""
    tmp_output.rmdir()
    resp = await msz_client.post(
        "/v1/upload",
        content=test_msz.read_bytes(),
        headers={
            "X-Transfer-ID": "receive-fail",
            "X-Original-Filename": "test.msz",
            "X-Source-Format": "msz",
        },
    )
    assert resp.status_code == 500
    assert "Error receiving data" in resp.json()["detail"]

    status = (await msz_client.get("/v1/transfer/receive-fail/status")).json()
    assert status["state"] == "error"
    assert status["error"]


@pytest.mark.asyncio
async def test_upload_conversion_failure_marks_error(mzml_client, tmp_output):
    """A payload that can't be decompressed ends in ERROR and leaves no files."""
    resp = await mzml_client.post(
        "/v1/upload",
        content=b"not an msz file" * 256,
        headers={
            "X-Transfer-ID": "convert-fail",
            "X-Original-Filename": "bad.msz",
            "X-Source-Format": "msz",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "error"
    assert data["stored_as"] == ""

    status = (await mzml_client.get("/v1/transfer/convert-fail/status")).json()
    assert status["state"] == "error"
    assert status["error"]
    # Neither the .part file, the renamed source nor a partial mzML remain.
    assert list(tmp_output.iterdir()) == []


@pytest.mark.asyncio
async def test_upload_missing_transfer_id(msz_client):
    resp = await msz_client.post(
        "/v1/upload",
        content=b"data",
        headers={"X-Original-Filename": "test.msz"},
    )
    assert resp.status_code == 400
    assert "X-Transfer-ID" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_transfer_status(msz_client, test_msz):
    await msz_client.post(
        "/v1/upload",
        content=test_msz.read_bytes(),
        headers={
            "X-Transfer-ID": "status-test",
            "X-Original-Filename": "status.msz",
        },
    )
    resp = await msz_client.get("/v1/transfer/status-test/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["transfer_id"] == "status-test"
    assert data["state"] == "done"
    assert data["bytes_received"] == test_msz.stat().st_size


@pytest.mark.asyncio
async def test_transfer_status_not_found(msz_client):
    resp = await msz_client.get("/v1/transfer/nonexistent/status")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_upload_preserves_filename_stem(msz_client, tmp_output, test_msz):
    """Uploaded file should use the original filename's stem."""
    await msz_client.post(
        "/v1/upload",
        content=test_msz.read_bytes(),
        headers={
            "X-Transfer-ID": "stem-test",
            "X-Original-Filename": "my_experiment.mzML",
        },
    )
    assert (tmp_output / "my_experiment.msz").exists()


@pytest.mark.asyncio
async def test_upload_missing_filename(msz_client, tmp_output, test_msz):
    """Missing X-Original-Filename header should return 400."""
    resp = await msz_client.post(
        "/v1/upload",
        content=test_msz.read_bytes(),
        headers={"X-Transfer-ID": "default-name-test"},
    )
    assert resp.status_code == 400
    assert "X-Original-Filename" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_decompress_does_not_block_event_loop(mzml_client, tmp_output, test_msz):
    """Decompression should be offloaded so concurrent requests aren't blocked."""
    payload = test_msz.read_bytes()

    upload_task = asyncio.create_task(
        mzml_client.post(
            "/v1/upload",
            content=payload,
            headers={
                "X-Transfer-ID": "blocking-test-upload",
                "X-Original-Filename": "blocking.msz",
            },
        )
    )

    # Give the upload a moment to start processing
    await asyncio.sleep(0.05)

    # Health check should respond promptly even while decompression runs
    health_resp = await asyncio.wait_for(
        mzml_client.get("/v1/health"),
        timeout=2.0,
    )
    assert health_resp.status_code == 200

    upload_resp = await upload_task
    assert upload_resp.status_code == 200
    assert upload_resp.json()["state"] == "done"


def _upload_headers(transfer_id, filename, **extra):
    return {"X-Transfer-ID": transfer_id, "X-Original-Filename": filename, **extra}


@pytest.mark.asyncio
async def test_upload_conflicting_with_stored_file_rejected(
    msz_client, tmp_output, test_msz
):
    """An upload whose output already exists gets a 409 and changes nothing."""
    stored = tmp_output / "test.msz"
    stored.write_bytes(b"stored earlier")
    consumed = False

    async def body():
        nonlocal consumed
        consumed = True
        yield test_msz.read_bytes()

    resp = await msz_client.post(
        "/v1/upload",
        content=body(),
        headers=_upload_headers("conflict", "test.mzML"),
    )
    assert resp.status_code == 409
    assert "test.msz already exists" in resp.json()["detail"]
    # Rejected before the body was read; no transfer record or files left.
    assert not consumed
    assert stored.read_bytes() == b"stored earlier"
    assert list(tmp_output.iterdir()) == [stored]
    status = await msz_client.get("/v1/transfer/conflict/status")
    assert status.status_code == 404


@pytest.mark.asyncio
async def test_upload_same_stem_different_format_rejected(msz_client, test_msz):
    """test.mzML and test.msz both map to test.msz; the second is refused."""
    first = await msz_client.post(
        "/v1/upload",
        content=test_msz.read_bytes(),
        headers=_upload_headers("first", "test.msz"),
    )
    assert first.json()["state"] == "done"

    second = await msz_client.post(
        "/v1/upload",
        content=test_msz.read_bytes(),
        headers=_upload_headers("second", "test.mzML"),
    )
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_upload_conflicting_with_in_flight_transfer_rejected(
    msz_client, tmp_output, test_msz
):
    """Two concurrent uploads to one output can't interleave."""
    payload = test_msz.read_bytes()
    release = asyncio.Event()

    async def slow_body():
        yield payload[:1024]
        await release.wait()
        yield payload[1024:]

    first = asyncio.create_task(
        msz_client.post(
            "/v1/upload",
            content=slow_body(),
            headers=_upload_headers("in-flight", "run1.mzML"),
        )
    )
    # Wait until the first upload has claimed its output.
    for _ in range(100):
        status = await msz_client.get("/v1/transfer/in-flight/status")
        if status.status_code == 200:
            break
        await asyncio.sleep(0.01)

    second = await msz_client.post(
        "/v1/upload",
        content=payload,
        headers=_upload_headers("racing", "run1.msz"),
    )
    assert second.status_code == 409
    assert "already being written" in second.json()["detail"]

    release.set()
    first_resp = await first
    assert first_resp.json()["state"] == "done"
    assert (tmp_output / "run1.msz").read_bytes() == payload

    # The claim is released once the first transfer finishes.
    (tmp_output / "run1.msz").unlink()
    third = await msz_client.post(
        "/v1/upload",
        content=payload,
        headers=_upload_headers("after", "run1.msz"),
    )
    assert third.json()["state"] == "done"


@pytest.mark.asyncio
async def test_upload_store_as_mzml_leaves_unrelated_msz_alone(
    mzml_client, tmp_output, test_msz
):
    """With store_as=mzml, a stored <stem>.msz is neither replaced nor removed."""
    stored_msz = tmp_output / "test.msz"
    stored_msz.write_bytes(b"stored earlier")
    resp = await mzml_client.post(
        "/v1/upload",
        content=test_msz.read_bytes(),
        headers=_upload_headers("mzml-side", "test.msz"),
    )
    assert resp.json()["state"] == "done"
    assert stored_msz.read_bytes() == b"stored earlier"
    assert (tmp_output / "test.mzML").exists()

    # The mzML output now exists, so a second upload of the same stem conflicts.
    again = await mzml_client.post(
        "/v1/upload",
        content=test_msz.read_bytes(),
        headers=_upload_headers("mzml-again", "test.mzML"),
    )
    assert again.status_code == 409
    assert "test.mzML already exists" in again.json()["detail"]


# ---------------------------------------------------------------------------
# DirectoryFileProvider unit tests
# ---------------------------------------------------------------------------


class TestDirectoryFileProvider:
    @pytest.mark.asyncio
    async def test_list_files_empty(self, tmp_path):
        provider = DirectoryFileProvider(tmp_path)
        files = await provider.list_files()
        assert files == []

    @pytest.mark.asyncio
    async def test_list_files_finds_msz_and_mzml(self, tmp_path):
        (tmp_path / "a.msz").write_bytes(b"msz data")
        (tmp_path / "b.mzML").write_bytes(b"mzml data")
        (tmp_path / "c.txt").write_bytes(b"not a ms file")

        provider = DirectoryFileProvider(tmp_path)
        files = await provider.list_files()
        names = [f.name for f in files]
        assert "a.msz" in names
        assert "b.mzML" in names
        assert "c.txt" not in names

    @pytest.mark.asyncio
    async def test_list_files_finds_mszx(self, tmp_path):
        (tmp_path / "a.mszx").write_bytes(b"mszx data")

        provider = DirectoryFileProvider(tmp_path)
        files = await provider.list_files()
        assert [(f.name, f.format) for f in files] == [("a.mszx", "mszx")]

    @pytest.mark.asyncio
    async def test_list_files_reports_correct_metadata(self, tmp_path):
        content = b"x" * 100
        (tmp_path / "test.msz").write_bytes(content)

        provider = DirectoryFileProvider(tmp_path)
        files = await provider.list_files()
        assert len(files) == 1
        assert files[0].name == "test.msz"
        assert files[0].size_bytes == 100
        assert files[0].format == "msz"

    @pytest.mark.asyncio
    async def test_list_files_sorted(self, tmp_path):
        (tmp_path / "z.msz").write_bytes(b"z")
        (tmp_path / "a.msz").write_bytes(b"a")

        provider = DirectoryFileProvider(tmp_path)
        files = await provider.list_files()
        assert [f.name for f in files] == ["a.msz", "z.msz"]

    @pytest.mark.asyncio
    async def test_get_file_exists(self, tmp_path):
        (tmp_path / "test.msz").write_bytes(b"data")
        provider = DirectoryFileProvider(tmp_path)
        path = await provider.get_file("test.msz")
        assert path is not None
        assert path.name == "test.msz"

    @pytest.mark.asyncio
    async def test_get_file_not_found(self, tmp_path):
        provider = DirectoryFileProvider(tmp_path)
        assert await provider.get_file("nonexistent.msz") is None

    @pytest.mark.asyncio
    async def test_get_file_rejects_traversal(self, tmp_path):
        provider = DirectoryFileProvider(tmp_path)
        assert await provider.get_file("../etc/passwd") is None
        assert await provider.get_file("sub/file.msz") is None
        assert await provider.get_file("..\\secret") is None

    @pytest.mark.asyncio
    async def test_get_file_rejects_empty(self, tmp_path):
        provider = DirectoryFileProvider(tmp_path)
        assert await provider.get_file("") is None


# ---------------------------------------------------------------------------
# File listing and download route tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_files_empty(msz_client):
    """Fresh server with no uploads returns empty file list."""
    resp = await msz_client.get("/v1/files")
    assert resp.status_code == 200
    assert resp.json() == {"files": []}


@pytest.mark.asyncio
async def test_list_files_after_upload(msz_client, test_msz):
    """Uploaded file appears in the file listing."""
    payload = test_msz.read_bytes()
    await msz_client.post(
        "/v1/upload",
        content=payload,
        headers={
            "X-Transfer-ID": "list-test",
            "X-Original-Filename": "test.msz",
        },
    )

    resp = await msz_client.get("/v1/files")
    assert resp.status_code == 200
    files = resp.json()["files"]
    assert len(files) == 1
    assert files[0]["name"] == "test.msz"
    assert files[0]["size_bytes"] == len(payload)
    assert files[0]["format"] == "msz"


@pytest.mark.asyncio
async def test_list_files_after_mszx_upload(msz_client, test_mszx):
    """An uploaded archive is listed as mszx and downloads unchanged."""
    payload = test_mszx.read_bytes()
    await msz_client.post(
        "/v1/upload",
        content=payload,
        headers={
            "X-Transfer-ID": "list-mszx",
            "X-Original-Filename": "run.mszx",
            "X-Source-Format": "mszx",
        },
    )

    files = (await msz_client.get("/v1/files")).json()["files"]
    assert files == [{"name": "run.mszx", "size_bytes": len(payload), "format": "mszx"}]

    resp = await msz_client.get("/v1/files/run.mszx")
    assert resp.status_code == 200
    assert resp.content == payload


@pytest.mark.asyncio
async def test_download_msz(msz_client, test_msz):
    """Download a previously uploaded .msz file."""
    payload = test_msz.read_bytes()
    await msz_client.post(
        "/v1/upload",
        content=payload,
        headers={
            "X-Transfer-ID": "dl-msz",
            "X-Original-Filename": "test.msz",
        },
    )

    resp = await msz_client.get("/v1/files/test.msz")
    assert resp.status_code == 200
    assert resp.content == payload
    assert resp.headers["content-disposition"] == 'attachment; filename="test.msz"'
    assert resp.headers["content-length"] == str(len(payload))


@pytest.mark.asyncio
async def test_download_mzml(mzml_client, test_msz, test_mzml):
    """Download from an mzml-mode server returns the decompressed file."""
    payload = test_msz.read_bytes()
    await mzml_client.post(
        "/v1/upload",
        content=payload,
        headers={
            "X-Transfer-ID": "dl-mzml",
            "X-Original-Filename": "test.msz",
        },
    )

    resp = await mzml_client.get("/v1/files/test.mzML")
    assert resp.status_code == 200
    assert len(resp.content) == test_mzml.stat().st_size
    assert resp.headers["content-type"] == "application/xml"


@pytest.mark.asyncio
async def test_download_not_found(msz_client):
    """Requesting a file that doesn't exist returns 404."""
    resp = await msz_client.get("/v1/files/nonexistent.msz")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_download_directory_traversal(msz_client):
    """Path traversal attempts are rejected."""
    resp = await msz_client.get("/v1/files/..%2F..%2Fetc%2Fpasswd")
    assert resp.status_code == 404

    resp = await msz_client.get("/v1/files/..%5Csecret")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_files_auth_required(authed_client):
    """File listing requires auth when configured."""
    resp = await authed_client.get("/v1/files")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_download_auth_required(authed_client):
    """File download requires auth when configured."""
    resp = await authed_client.get("/v1/files/test.msz")
    assert resp.status_code == 401
