from __future__ import annotations

import threading
import time

from mstransfer.server.models import TransferRecord, TransferState


class TransferRegistry:
    """Thread-safe in-memory registry of transfer records."""

    def __init__(self) -> None:
        self._records: dict[str, TransferRecord] = {}
        self._lock = threading.Lock()

    def create(self, transfer_id: str, filename: str) -> TransferRecord:
        record = TransferRecord(transfer_id=transfer_id, filename=filename)
        with self._lock:
            self._records[transfer_id] = record
        return record

    def get(self, transfer_id: str) -> TransferRecord | None:
        with self._lock:
            return self._records.get(transfer_id)

    def update(self, transfer_id: str, **kwargs: object) -> TransferRecord | None:
        with self._lock:
            record = self._records.get(transfer_id)
            if record is None:
                return None
            for key, value in kwargs.items():
                setattr(record, key, value)
            return record

    def cleanup(self, max_age_seconds: float = 300) -> int:
        """Remove old completed/errored records. Returns count removed."""
        now = time.time()
        terminal = {TransferState.DONE, TransferState.ERROR}
        to_remove: list[str] = []
        with self._lock:
            for tid, record in self._records.items():
                if record.state in terminal:
                    age = now - record.created_at.timestamp()
                    if age > max_age_seconds:
                        to_remove.append(tid)
            for tid in to_remove:
                del self._records[tid]
        return len(to_remove)
