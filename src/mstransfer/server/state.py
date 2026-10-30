from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from starlette.datastructures import State

from mstransfer.server.models import (
    PartStatus,
    StoreFormat,
    TransferRecord,
    TransferState,
)

if TYPE_CHECKING:
    from pathlib import Path

    from mstransfer.server.fileprovider import FileProvider


class AppState(State):
    """Application state object to hold configuration and transfer registry."""

    output_dir: Path
    store_as: StoreFormat
    transfers: TransferRegistry
    files: FileProvider

    def __init__(
        self,
        output_dir: Path,
        store_as: StoreFormat,
        files: FileProvider,
    ) -> None:
        super().__init__()
        self.output_dir = output_dir
        self.store_as = store_as
        self.transfers = TransferRegistry()
        self.files = files


@dataclass
class MultipartUpload:
    """Server-side bookkeeping for an in-progress multipart upload."""

    path: Path
    total_size: int
    part_size: int
    # Indices of parts with a request currently streaming into the file.
    in_flight: set[int] = field(default_factory=set)


class PartError(Exception):
    """Raised when a part operation is invalid for the transfer's state."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class TransferRegistry:
    """In-memory LRU registry of transfer records.

    When the number of records exceeds *maxsize*, the least-recently-used
    **terminal** (DONE / ERROR) record is evicted.  In-progress transfers
    are never evicted; the registry may temporarily exceed *maxsize* if all
    records are active.

    The registry is only accessed from the server's event loop and none of
    its methods await, so each method call is atomic with respect to other
    request handlers (no explicit locking is required).
    """

    _TERMINAL = frozenset({TransferState.DONE, TransferState.ERROR})

    def __init__(self, maxsize: int = 1024) -> None:
        self._records: OrderedDict[str, TransferRecord] = OrderedDict()
        self._multipart: dict[str, MultipartUpload] = {}
        self._maxsize = maxsize

    def create(self, transfer_id: str, filename: str) -> TransferRecord:
        record = TransferRecord(transfer_id=transfer_id, filename=filename)
        self._records[transfer_id] = record
        self._multipart.pop(transfer_id, None)
        self._records.move_to_end(transfer_id)
        self._evict()
        return record

    def get(self, transfer_id: str) -> TransferRecord | None:
        record = self._records.get(transfer_id)
        if record is not None:
            self._records.move_to_end(transfer_id)
        return record

    def update(self, transfer_id: str, **kwargs: object) -> TransferRecord | None:
        record = self._records.get(transfer_id)
        if record is None:
            return None
        for key, value in kwargs.items():
            setattr(record, key, value)
        self._records.move_to_end(transfer_id)
        return record

    # -- multipart uploads -------------------------------------------------

    def create_multipart(
        self,
        transfer_id: str,
        filename: str,
        path: Path,
        total_size: int,
        part_size: int,
    ) -> TransferRecord:
        """Create a record for a multipart upload split into fixed-size parts."""
        part_count = -(-total_size // part_size)  # ceil division
        parts = [
            PartStatus(
                index=i,
                offset=i * part_size,
                size=min(part_size, total_size - i * part_size),
            )
            for i in range(part_count)
        ]
        record = self.create(transfer_id, filename)
        record.total_bytes = total_size
        record.parts = parts
        self._multipart[transfer_id] = MultipartUpload(
            path=path, total_size=total_size, part_size=part_size
        )
        return record

    def get_multipart(self, transfer_id: str) -> MultipartUpload | None:
        return self._multipart.get(transfer_id)

    def begin_part(
        self, transfer_id: str, index: int
    ) -> tuple[MultipartUpload, PartStatus]:
        """Claim part *index* for writing, resetting its progress.

        Re-sending a part that was already received is allowed (the byte
        range is simply overwritten), but two concurrent requests for the
        same part are rejected.
        """
        upload, record = self._require_receiving(transfer_id)
        assert record.parts is not None
        if not 0 <= index < len(record.parts):
            raise PartError(
                400,
                f"Part {index} out of range (0..{len(record.parts) - 1})",
            )
        if index in upload.in_flight:
            raise PartError(409, f"Part {index} is already being uploaded")
        upload.in_flight.add(index)
        part = record.parts[index]
        self._set_part_bytes(record, part, 0)
        return upload, part

    def update_part(self, transfer_id: str, index: int, bytes_received: int) -> None:
        """Record progress for part *index* and refresh the aggregate total."""
        record = self._records.get(transfer_id)
        if record is None or record.parts is None:
            return
        self._set_part_bytes(record, record.parts[index], bytes_received)

    def end_part(self, transfer_id: str, index: int) -> None:
        """Release the write claim on part *index*."""
        upload = self._multipart.get(transfer_id)
        if upload is not None:
            upload.in_flight.discard(index)

    def complete_multipart(self, transfer_id: str) -> MultipartUpload:
        """Verify every part is fully received and mark the transfer RECEIVED."""
        upload, record = self._require_receiving(transfer_id)
        assert record.parts is not None
        if upload.in_flight:
            raise PartError(409, f"Parts still uploading: {sorted(upload.in_flight)}")
        missing = [p.index for p in record.parts if not p.complete]
        if missing:
            raise PartError(409, f"Missing or incomplete parts: {missing}")
        record.state = TransferState.RECEIVED
        del self._multipart[transfer_id]
        return upload

    def abort_multipart(self, transfer_id: str, reason: str) -> MultipartUpload:
        """Mark a multipart upload as failed and drop its bookkeeping."""
        upload, record = self._require_receiving(transfer_id)
        record.state = TransferState.ERROR
        record.error = reason
        del self._multipart[transfer_id]
        return upload

    def _require_receiving(
        self, transfer_id: str
    ) -> tuple[MultipartUpload, TransferRecord]:
        record = self.get(transfer_id)
        upload = self._multipart.get(transfer_id)
        if record is None or record.parts is None:
            raise PartError(404, "Multipart upload not found")
        if upload is None or record.state != TransferState.RECEIVING:
            raise PartError(
                409, f"Transfer is not accepting parts (state={record.state.value})"
            )
        return upload, record

    @staticmethod
    def _set_part_bytes(record: TransferRecord, part: PartStatus, value: int) -> None:
        record.bytes_received += value - part.bytes_received
        part.bytes_received = value

    def _evict(self) -> None:
        """Remove the oldest terminal record(s) until size <= maxsize."""
        while len(self._records) > self._maxsize:
            # Find oldest terminal record (iteration is oldest-first).
            victim: str | None = None
            for tid, record in self._records.items():
                if record.state in self._TERMINAL:
                    victim = tid
                    break
            if victim is None:
                # All records are in-progress; allow temporary oversize.
                break
            del self._records[victim]
            self._multipart.pop(victim, None)
