from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

__all__ = ["FileResult"]

T = TypeVar("T")


@dataclass
class FileResult(Generic[T]):
    """Result of transferring a single file in a batch.

    Uploads return ``FileResult[UploadResponse]`` and downloads return
    ``FileResult[DownloadResult]``, one per input, in input order.

    Exactly one of *response* and *error* is set: *response* on success,
    *error* the exception that failed the transfer.  An exception raised by
    the batch's completion progress callback is logged and does not count
    as a failure.
    """

    filename: str
    response: T | None = field(default=None)
    error: Exception | None = field(default=None)
