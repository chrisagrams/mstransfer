from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

from starlette.datastructures import State

from mstransfer.server.models import StoreFormat, TransferRecord, TransferState

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


class TransferRegistry:
    """In-memory LRU registry of transfer records.

    When the number of records exceeds *maxsize*, the least-recently-used
    **terminal** (DONE / ERROR) record is evicted.  In-progress transfers
    are never evicted; the registry may temporarily exceed *maxsize* if all
    records are active.
    """

    _TERMINAL = frozenset({TransferState.DONE, TransferState.ERROR})

    def __init__(self, maxsize: int = 1024) -> None:
        self._records: OrderedDict[str, TransferRecord] = OrderedDict()
        self._maxsize = maxsize

    def create(self, transfer_id: str, filename: str) -> TransferRecord:
        record = TransferRecord(transfer_id=transfer_id, filename=filename)
        self._records[transfer_id] = record
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
