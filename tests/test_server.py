"""Tests for the mstransfer server (routes, state, models)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest
from mscompress import MZMLFile

from mstransfer.server.fileprovider import DirectoryFileProvider
from mstransfer.server.models import TransferState
from mstransfer.server.state import TransferRegistry

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

    def test_cleanup(self):
        reg = TransferRegistry()
        rec = reg.create("t1", "old.msz")
        reg.update("t1", state=TransferState.DONE)
        rec.created_at = datetime.now() - timedelta(seconds=600)
        removed = reg.cleanup(max_age_seconds=300)
        assert removed == 1
        assert reg.get("t1") is None

    def test_cleanup_keeps_recent(self):
        reg = TransferRegistry()
        reg.create("t1", "new.msz")
        reg.update("t1", state=TransferState.DONE)
        removed = reg.cleanup(max_age_seconds=300)
        assert removed == 0
        assert reg.get("t1") is not None

    def test_cleanup_keeps_in_progress(self):
        reg = TransferRegistry()
        rec = reg.create("t1", "active.msz")
        rec.created_at = datetime.now() - timedelta(seconds=600)
        removed = reg.cleanup(max_age_seconds=300)
        assert removed == 0


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
