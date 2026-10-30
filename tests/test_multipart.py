"""Tests for multipart (split, multi-connection) uploads."""

from __future__ import annotations

import argparse
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from mstransfer.cli import cmd_upload, main
from mstransfer.client import sender
from mstransfer.client.sender import (
    async_send_batch,
    async_send_file,
    plan_part_size,
)
from mstransfer.client.utils import async_file_range_generator
from mstransfer.server.models import MAX_UPLOAD_PARTS, TransferState
from mstransfer.server.state import PartError, TransferRegistry
from tests.test_client import _live_server, _live_server_mzml  # noqa: F401

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _split(payload: bytes, part_size: int) -> list[bytes]:
    return [payload[i : i + part_size] for i in range(0, len(payload), part_size)]


async def _init(
    client: httpx.AsyncClient,
    transfer_id: str,
    total_size: int,
    part_size: int,
    filename: str = "test.msz",
) -> httpx.Response:
    return await client.post(
        "/v1/uploads",
        json={
            "transfer_id": transfer_id,
            "filename": filename,
            "total_size": total_size,
            "part_size": part_size,
        },
    )


async def _put_part(
    client: httpx.AsyncClient, transfer_id: str, index: int, data: bytes
) -> httpx.Response:
    return await client.put(f"/v1/uploads/{transfer_id}/parts/{index}", content=data)


# ---------------------------------------------------------------------------
# TransferRegistry multipart bookkeeping
# ---------------------------------------------------------------------------


class TestRegistryMultipart:
    def test_create_multipart_computes_parts(self, tmp_path):
        reg = TransferRegistry()
        rec = reg.create_multipart("t1", "f.msz", tmp_path / "f.msz", 10, 4)
        assert rec.total_bytes == 10
        assert rec.parts is not None
        assert [(p.offset, p.size) for p in rec.parts] == [(0, 4), (4, 4), (8, 2)]

    def test_aggregate_bytes_received(self, tmp_path):
        reg = TransferRegistry()
        reg.create_multipart("t1", "f.msz", tmp_path / "f.msz", 10, 4)
        reg.begin_part("t1", 0)
        reg.begin_part("t1", 2)
        reg.update_part("t1", 0, 3)
        reg.update_part("t1", 2, 2)
        rec = reg.get("t1")
        assert rec is not None
        assert rec.bytes_received == 5
        # Retrying a part resets only that part's contribution.
        reg.end_part("t1", 0)
        reg.begin_part("t1", 0)
        assert rec.bytes_received == 2

    def test_concurrent_same_part_rejected(self, tmp_path):
        reg = TransferRegistry()
        reg.create_multipart("t1", "f.msz", tmp_path / "f.msz", 10, 4)
        reg.begin_part("t1", 1)
        with pytest.raises(PartError) as exc_info:
            reg.begin_part("t1", 1)
        assert exc_info.value.status_code == 409
        reg.end_part("t1", 1)
        reg.begin_part("t1", 1)  # released, may be claimed again

    def test_complete_requires_all_parts(self, tmp_path):
        reg = TransferRegistry()
        reg.create_multipart("t1", "f.msz", tmp_path / "f.msz", 8, 4)
        reg.begin_part("t1", 0)
        reg.update_part("t1", 0, 4)
        reg.end_part("t1", 0)
        with pytest.raises(PartError) as exc_info:
            reg.complete_multipart("t1")
        assert exc_info.value.status_code == 409
        reg.begin_part("t1", 1)
        reg.update_part("t1", 1, 4)
        reg.end_part("t1", 1)
        reg.complete_multipart("t1")
        rec = reg.get("t1")
        assert rec is not None
        assert rec.state == TransferState.RECEIVED

    def test_unknown_transfer(self):
        reg = TransferRegistry()
        with pytest.raises(PartError) as exc_info:
            reg.begin_part("nope", 0)
        assert exc_info.value.status_code == 404

    def test_eviction_drops_multipart_state(self, tmp_path):
        reg = TransferRegistry(maxsize=1)
        reg.create_multipart("t1", "a.msz", tmp_path / "a.msz", 8, 4)
        reg.abort_multipart("t1", reason="test")
        reg.create("t2", "b.msz")
        assert reg.get("t1") is None
        assert reg.get_multipart("t1") is None

    def test_update_part_unknown_transfer_is_noop(self):
        reg = TransferRegistry()
        reg.update_part("nope", 0, 4)
        assert reg.get("nope") is None

    def test_complete_rejected_while_part_in_flight(self, tmp_path):
        reg = TransferRegistry()
        reg.create_multipart("t1", "f.msz", tmp_path / "f.msz", 4, 4)
        reg.begin_part("t1", 0)
        reg.update_part("t1", 0, 4)
        with pytest.raises(PartError) as exc_info:
            reg.complete_multipart("t1")
        assert exc_info.value.status_code == 409
        assert "still uploading" in exc_info.value.detail


# ---------------------------------------------------------------------------
# Server routes (via ASGI transport)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multipart_happy_path(msz_client, tmp_output, test_msz):
    payload = test_msz.read_bytes()
    part_size = 300_000
    parts = _split(payload, part_size)

    resp = await _init(msz_client, "mp1", len(payload), part_size)
    assert resp.status_code == 200
    assert resp.json()["part_count"] == len(parts)

    for i, data in enumerate(parts):
        resp = await _put_part(msz_client, "mp1", i, data)
        assert resp.status_code == 200
        assert resp.json()["bytes_received"] == len(data)

    resp = await msz_client.post("/v1/uploads/mp1/complete")
    assert resp.status_code == 200
    data = resp.json()
    assert data["state"] == "done"
    assert data["bytes_received"] == len(payload)
    assert (tmp_output / "test.msz").read_bytes() == payload

    # Status exposes per-part progress.
    status = (await msz_client.get("/v1/transfer/mp1/status")).json()
    assert status["total_bytes"] == len(payload)
    assert [p["bytes_received"] for p in status["parts"]] == [len(p) for p in parts]


@pytest.mark.asyncio
async def test_multipart_out_of_order(msz_client, tmp_output, test_msz):
    payload = test_msz.read_bytes()
    part_size = 200_000
    parts = _split(payload, part_size)

    await _init(msz_client, "mp2", len(payload), part_size)
    for i in reversed(range(len(parts))):
        resp = await _put_part(msz_client, "mp2", i, parts[i])
        assert resp.status_code == 200

    resp = await msz_client.post("/v1/uploads/mp2/complete")
    assert resp.status_code == 200
    assert (tmp_output / "test.msz").read_bytes() == payload


@pytest.mark.asyncio
async def test_multipart_store_as_mzml(mzml_client, tmp_output, test_msz):
    """Completed multipart uploads go through the same decompress path."""
    payload = test_msz.read_bytes()
    part_size = 400_000
    await _init(mzml_client, "mp3", len(payload), part_size)
    for i, data in enumerate(_split(payload, part_size)):
        assert (await _put_part(mzml_client, "mp3", i, data)).status_code == 200

    resp = await mzml_client.post("/v1/uploads/mp3/complete")
    assert resp.status_code == 200
    assert resp.json()["state"] == "done"
    assert resp.json()["stored_as"].endswith("test.mzML")
    assert (tmp_output / "test.mzML").exists()
    assert not (tmp_output / "test.msz").exists()


@pytest.mark.asyncio
async def test_complete_before_all_parts_rejected(msz_client, tmp_output, test_msz):
    payload = test_msz.read_bytes()
    part_size = 500_000
    parts = _split(payload, part_size)
    await _init(msz_client, "mp4", len(payload), part_size)
    await _put_part(msz_client, "mp4", 0, parts[0])

    resp = await msz_client.post("/v1/uploads/mp4/complete")
    assert resp.status_code == 409
    assert "1" in resp.json()["detail"]

    # Still receiving; sending the last part lets completion succeed.
    status = (await msz_client.get("/v1/transfer/mp4/status")).json()
    assert status["state"] == "receiving"
    await _put_part(msz_client, "mp4", 1, parts[1])
    resp = await msz_client.post("/v1/uploads/mp4/complete")
    assert resp.status_code == 200
    assert (tmp_output / "test.msz").read_bytes() == payload


@pytest.mark.asyncio
async def test_part_index_out_of_range(msz_client):
    await _init(msz_client, "mp5", 10, 4)
    assert (await _put_part(msz_client, "mp5", 3, b"xx")).status_code == 400
    assert (await _put_part(msz_client, "mp5", -1, b"xxxx")).status_code == 400


@pytest.mark.asyncio
async def test_part_wrong_size_rejected(msz_client):
    await _init(msz_client, "mp6", 10, 4)
    # Too long (declared via Content-Length).
    assert (await _put_part(msz_client, "mp6", 0, b"x" * 5)).status_code == 400
    # Too short.
    assert (await _put_part(msz_client, "mp6", 0, b"x" * 3)).status_code == 400
    status = (await msz_client.get("/v1/transfer/mp6/status")).json()
    assert status["bytes_received"] == 0


@pytest.mark.asyncio
async def test_part_overflow_without_content_length(msz_client, tmp_output):
    """A streamed (chunked) body that overruns its part must not touch neighbours."""
    await _init(msz_client, "mp7", 8, 4)
    assert (await _put_part(msz_client, "mp7", 1, b"BBBB")).status_code == 200

    async def _body():
        yield b"AA"
        yield b"AAAA"  # would spill into part 1

    resp = await msz_client.put("/v1/uploads/mp7/parts/0", content=_body())
    assert resp.status_code == 400
    assert (tmp_output / "test.msz").read_bytes()[4:] == b"BBBB"
    status = (await msz_client.get("/v1/transfer/mp7/status")).json()
    assert status["parts"][0]["bytes_received"] == 0


@pytest.mark.asyncio
async def test_part_retry_overwrites(msz_client, tmp_output, test_msz):
    payload = test_msz.read_bytes()
    part_size = 500_000
    parts = _split(payload, part_size)
    await _init(msz_client, "mp8", len(payload), part_size)

    # First attempt sends garbage of the right length; the retry fixes it.
    await _put_part(msz_client, "mp8", 0, b"\0" * len(parts[0]))
    await _put_part(msz_client, "mp8", 1, parts[1])
    assert (await _put_part(msz_client, "mp8", 0, parts[0])).status_code == 200

    status = (await msz_client.get("/v1/transfer/mp8/status")).json()
    assert status["bytes_received"] == len(payload)

    assert (await msz_client.post("/v1/uploads/mp8/complete")).status_code == 200
    assert (tmp_output / "test.msz").read_bytes() == payload


@pytest.mark.asyncio
async def test_parts_rejected_after_complete(msz_client):
    await _init(msz_client, "mp9", 4, 4)
    await _put_part(msz_client, "mp9", 0, b"abcd")
    assert (await msz_client.post("/v1/uploads/mp9/complete")).status_code == 200
    assert (await _put_part(msz_client, "mp9", 0, b"abcd")).status_code == 409
    assert (await msz_client.post("/v1/uploads/mp9/complete")).status_code == 409


@pytest.mark.asyncio
async def test_abort(msz_client, tmp_output):
    await _init(msz_client, "mp10", 8, 4)
    await _put_part(msz_client, "mp10", 0, b"abcd")

    resp = await msz_client.delete("/v1/uploads/mp10")
    assert resp.status_code == 200
    assert resp.json()["state"] == "error"
    assert not (tmp_output / "test.msz").exists()
    assert (await _put_part(msz_client, "mp10", 1, b"efgh")).status_code == 409


@pytest.mark.asyncio
async def test_init_rejects_duplicate_and_invalid(msz_client):
    assert (await _init(msz_client, "mp11", 8, 4)).status_code == 200
    assert (await _init(msz_client, "mp11", 8, 4)).status_code == 409
    assert (await _init(msz_client, "mp12", 0, 4)).status_code == 422
    assert (await _init(msz_client, "mp13", 8, 0)).status_code == 422
    too_many = MAX_UPLOAD_PARTS + 1
    assert (await _init(msz_client, "mp14", too_many, 1)).status_code == 400


@pytest.mark.asyncio
async def test_unknown_transfer_404(msz_client):
    assert (await _put_part(msz_client, "nope", 0, b"x")).status_code == 404
    assert (await msz_client.post("/v1/uploads/nope/complete")).status_code == 404
    assert (await msz_client.delete("/v1/uploads/nope")).status_code == 404


@pytest.mark.asyncio
async def test_parts_rejected_for_legacy_upload(msz_client):
    await msz_client.post(
        "/v1/upload",
        content=b"data",
        headers={"X-Transfer-ID": "legacy", "X-Original-Filename": "x.msz"},
    )
    assert (await _put_part(msz_client, "legacy", 0, b"data")).status_code == 404


@pytest.mark.asyncio
async def test_multipart_auth_required(authed_client):
    assert (await _init(authed_client, "a1", 8, 4)).status_code == 401
    assert (await _put_part(authed_client, "a1", 0, b"abcd")).status_code == 401
    assert (await authed_client.post("/v1/uploads/a1/complete")).status_code == 401
    assert (await authed_client.delete("/v1/uploads/a1")).status_code == 401

    auth = {"Authorization": "Bearer test-secret"}
    resp = await authed_client.post(
        "/v1/uploads",
        json={
            "transfer_id": "a1",
            "filename": "a.msz",
            "total_size": 4,
            "part_size": 4,
        },
        headers=auth,
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_legacy_upload_still_works(msz_client, tmp_output, test_msz):
    payload = test_msz.read_bytes()
    resp = await msz_client.post(
        "/v1/upload",
        content=payload,
        headers={"X-Transfer-ID": "legacy2", "X-Original-Filename": "test.msz"},
    )
    assert resp.status_code == 200
    assert resp.json()["state"] == "done"
    assert (tmp_output / "test.msz").read_bytes() == payload
    status = (await msz_client.get("/v1/transfer/legacy2/status")).json()
    assert status["parts"] is None


@pytest.mark.asyncio
async def test_init_allocation_failure(msz_client, tmp_output):
    # A directory in the way makes opening the output file fail.
    (tmp_output / "test.msz").mkdir()
    resp = await _init(msz_client, "mp15", 8, 4)
    assert resp.status_code == 500
    status = (await msz_client.get("/v1/transfer/mp15/status")).json()
    assert status["state"] == "error"


@pytest.mark.asyncio
async def test_part_progress_updated_while_streaming(msz_client, tmp_output):
    """Bodies with many chunks report progress before the part finishes."""
    await _init(msz_client, "mp16", 100, 100)

    async def _body():
        for _ in range(100):
            yield b"x"

    resp = await msz_client.put("/v1/uploads/mp16/parts/0", content=_body())
    assert resp.status_code == 200
    assert resp.json()["bytes_received"] == 100
    assert (tmp_output / "test.msz").read_bytes() == b"x" * 100


@pytest.mark.asyncio
async def test_part_short_without_content_length(msz_client):
    await _init(msz_client, "mp17", 8, 4)

    async def _body():
        yield b"AA"

    resp = await msz_client.put("/v1/uploads/mp17/parts/0", content=_body())
    assert resp.status_code == 400
    assert "incomplete" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_part_rejected_if_aborted_mid_stream(msz_app, msz_client):
    await _init(msz_client, "mp18", 4, 4)

    async def _body():
        yield b"AB"
        msz_app.state.transfers.abort_multipart("mp18", reason="test")
        yield b"CD"

    resp = await msz_client.put("/v1/uploads/mp18/parts/0", content=_body())
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_abort_record_missing(msz_app, msz_client):
    await _init(msz_client, "mp19", 4, 4)
    registry = msz_app.state.transfers
    real_abort = registry.abort_multipart

    def _abort_then_forget(transfer_id, reason):
        # Simulate the record vanishing (e.g. evicted) right after the abort.
        upload = real_abort(transfer_id, reason=reason)
        registry._records.pop(transfer_id)
        return upload

    with patch.object(registry, "abort_multipart", _abort_then_forget):
        resp = await msz_client.delete("/v1/uploads/mp19")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Transfer not found"


@pytest.mark.asyncio
async def test_complete_decompression_failure(mzml_client, test_msz):
    payload = test_msz.read_bytes()
    await _init(mzml_client, "mp20", len(payload), len(payload))
    await _put_part(mzml_client, "mp20", 0, payload)

    with patch("mstransfer.server.routes.MSZFile", side_effect=RuntimeError("corrupt")):
        resp = await mzml_client.post("/v1/uploads/mp20/complete")
    assert resp.status_code == 200
    assert resp.json()["state"] == "error"
    status = (await mzml_client.get("/v1/transfer/mp20/status")).json()
    assert status["error"] == "corrupt"


@pytest.mark.asyncio
async def test_complete_record_missing(msz_app, msz_client):
    await _init(msz_client, "mp21", 4, 4)
    await _put_part(msz_client, "mp21", 0, b"abcd")
    registry = msz_app.state.transfers
    real_complete = registry.complete_multipart

    def _complete_then_forget(transfer_id):
        # Simulate the record vanishing (e.g. evicted) during finalization.
        upload = real_complete(transfer_id)
        registry._records.pop(transfer_id)
        return upload

    with patch.object(registry, "complete_multipart", _complete_then_forget):
        resp = await msz_client.post("/v1/uploads/mp21/complete")
    assert resp.status_code == 500


# ---------------------------------------------------------------------------
# Client helpers
# ---------------------------------------------------------------------------


class TestPlanPartSize:
    def test_single_connection(self):
        assert plan_part_size(100, 1, 10) is None

    def test_below_threshold(self):
        assert plan_part_size(19, 4, 10) is None

    def test_capped_by_min_part_size(self):
        # 25 bytes / min 10 → only 2 parts worthwhile.
        assert plan_part_size(25, 8, 10) == 13

    def test_even_split(self):
        assert plan_part_size(100, 4, 10) == 25


@pytest.mark.asyncio
async def test_file_range_generator(tmp_path):
    path = tmp_path / "f.bin"
    path.write_bytes(bytes(range(100)))
    seen: list[int] = []
    chunks = [
        c
        async for c in async_file_range_generator(
            path, 10, 25, chunk_size=7, callback=seen.append
        )
    ]
    assert b"".join(chunks) == bytes(range(10, 35))
    assert seen == [7, 7, 7, 4]


@pytest.mark.asyncio
async def test_file_range_generator_stops_at_eof(tmp_path):
    path = tmp_path / "f.bin"
    path.write_bytes(bytes(range(20)))
    chunks = [c async for c in async_file_range_generator(path, 15, 50, chunk_size=4)]
    assert b"".join(chunks) == bytes(range(15, 20))


# ---------------------------------------------------------------------------
# Client end-to-end (live server)
# ---------------------------------------------------------------------------


async def _status(base_url: str, transfer_id: str) -> dict[str, object]:
    async with httpx.AsyncClient() as c:
        resp = await c.get(f"{base_url}/v1/transfer/{transfer_id}/status")
        resp.raise_for_status()
        data: dict[str, object] = resp.json()
        return data


class TestClientMultipart:
    @pytest.mark.asyncio
    async def test_send_file_with_connections(self, test_msz, _live_server):  # noqa: F811
        total = test_msz.stat().st_size
        deltas: list[int] = []
        result = await async_send_file(
            test_msz,
            _live_server["base_url"],
            progress_callback=deltas.append,
            chunk_size=65_536,
            connections=4,
            min_part_size=100_000,
        )
        assert result.state == "done"
        assert result.bytes_received == total
        assert sum(deltas) == total
        written = _live_server["output_dir"] / "test.msz"
        assert written.read_bytes() == test_msz.read_bytes()

        status = await _status(_live_server["base_url"], result.transfer_id)
        parts = status["parts"]
        assert isinstance(parts, list)
        assert len(parts) == 4

    @pytest.mark.asyncio
    async def test_send_mszx_with_connections(self, test_mszx, _live_server):  # noqa: F811
        result = await async_send_file(
            test_mszx,
            _live_server["base_url"],
            connections=3,
            min_part_size=100_000,
        )
        assert result.state == "done"
        written = _live_server["output_dir"] / "test.msz"
        assert written.read_bytes() == test_mszx.read_bytes()

    @pytest.mark.asyncio
    async def test_send_file_store_as_mzml(self, test_msz, _live_server_mzml):  # noqa: F811
        result = await async_send_file(
            test_msz,
            _live_server_mzml["base_url"],
            connections=2,
            min_part_size=100_000,
        )
        assert result.state == "done"
        assert (_live_server_mzml["output_dir"] / "test.mzML").exists()

    @pytest.mark.asyncio
    async def test_small_file_uses_single_stream(self, test_msz, _live_server):  # noqa: F811
        # Default MIN_PART_SIZE (8 MiB) exceeds the ~1 MB test file.
        result = await async_send_file(
            test_msz, _live_server["base_url"], connections=4
        )
        assert result.state == "done"
        status = await _status(_live_server["base_url"], result.transfer_id)
        assert status["parts"] is None

    @pytest.mark.asyncio
    async def test_mzml_ignores_connections(self, test_mzml, _live_server):  # noqa: F811
        result = await async_send_file(
            test_mzml,
            _live_server["base_url"],
            connections=4,
            min_part_size=1,
        )
        assert result.state == "done"
        status = await _status(_live_server["base_url"], result.transfer_id)
        assert status["parts"] is None

    @pytest.mark.asyncio
    async def test_part_failure_aborts_upload(self, test_msz, _live_server):  # noqa: F811
        transfer_id = uuid.UUID(int=42)
        real_range = async_file_range_generator

        async def _flaky(path, offset, length, **kwargs):
            if offset > 0:
                raise OSError("disk on fire")
            async for chunk in real_range(path, offset, length, **kwargs):
                yield chunk

        with (
            patch.object(sender.uuid, "uuid4", return_value=transfer_id),
            patch.object(sender, "async_file_range_generator", _flaky),
            pytest.raises(OSError, match="disk on fire"),
        ):
            await async_send_file(
                test_msz,
                _live_server["base_url"],
                connections=4,
                min_part_size=100_000,
            )

        status = await _status(_live_server["base_url"], str(transfer_id))
        assert status["state"] == "error"
        assert not (_live_server["output_dir"] / "test.msz").exists()

    @pytest.mark.asyncio
    async def test_abort_failure_still_raises_part_error(
        self,
        test_msz,
        _live_server,  # noqa: F811
    ):
        """A failed abort request is logged; the original part error propagates."""

        async def _broken(path, offset, length, **kwargs):
            raise OSError("disk on fire")
            yield b""  # pragma: no cover - makes this an async generator

        with (
            patch.object(sender, "async_file_range_generator", _broken),
            patch.object(
                httpx.AsyncClient,
                "delete",
                AsyncMock(side_effect=httpx.ConnectError("unreachable")),
            ),
            pytest.raises(OSError, match="disk on fire"),
        ):
            await async_send_file(
                test_msz,
                _live_server["base_url"],
                connections=4,
                min_part_size=100_000,
            )

    @pytest.mark.asyncio
    async def test_batch_forwards_connections(self, test_msz, _live_server):  # noqa: F811
        with patch(
            "mstransfer.client.sender.async_send_file",
            wraps=async_send_file,
        ) as mock_send:
            results = await async_send_batch(
                [test_msz], _live_server["base_url"], connections=3
            )
        assert results[0].response is not None
        assert mock_send.call_args.kwargs["connections"] == 3


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


class TestCliConnections:
    @patch("mstransfer.cli.async_send_batch", new_callable=AsyncMock)
    @patch("mstransfer.cli.httpx.get")
    @patch("mstransfer.cli.resolve_inputs")
    def test_upload_passes_connections(self, mock_resolve, mock_get, mock_batch):
        mock_resolve.return_value = [Path("/data/a.msz")]
        mock_get.return_value = MagicMock(status_code=200)
        mock_batch.return_value = []
        args = argparse.Namespace(
            targets=["/data/a.msz", "host"],
            recursive=False,
            parallel=2,
            chunk_size=1_048_576,
            api_key=None,
            connections=8,
        )
        cmd_upload(args)
        _, kwargs = mock_batch.call_args
        assert kwargs["connections"] == 8
        assert kwargs["parallel"] == 2

    @patch("mstransfer.cli.cmd_upload")
    def test_main_parses_connections(self, mock_cmd_upload):
        with patch("sys.argv", ["mstransfer", "upload", "a.msz", "host", "-c", "6"]):
            main()
        assert mock_cmd_upload.call_args.args[0].connections == 6

    @patch("mstransfer.cli.cmd_upload")
    def test_main_defaults_connections(self, mock_cmd_upload):
        with patch("sys.argv", ["mstransfer", "upload", "a.msz", "host"]):
            main()
        assert mock_cmd_upload.call_args.args[0].connections == 1

    def test_main_rejects_zero_connections(self):
        with (
            patch("sys.argv", ["mstransfer", "upload", "a.msz", "host", "-c", "0"]),
            pytest.raises(SystemExit),
        ):
            main()
