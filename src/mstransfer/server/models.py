from __future__ import annotations

from collections.abc import (
    Sequence,  # noqa: TC003 (Pydantic resolves annotations at runtime)
)
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path  # noqa: TC003 (dataclass field type)
from typing import Literal

from pydantic import BaseModel, Field, SerializeAsAny

from mstransfer.store import MSFormat  # noqa: TC001 (Pydantic resolves annotations at runtime)

StoreFormat = Literal["msz", "mzml"]


class TransferState(StrEnum):
    """Enumeration of possible states for a file transfer."""

    RECEIVING = "receiving"
    RECEIVED = "received"
    DECOMPRESSING = "decompressing"
    DONE = "done"
    ERROR = "error"


class TransferRecord(BaseModel):
    """Data model for a file transfer record in the registry."""

    transfer_id: str
    filename: str
    state: TransferState = TransferState.RECEIVING
    bytes_received: int = 0
    stored_as: str = ""
    error: str | None = None
    created_at: datetime = Field(default_factory=datetime.now)

    model_config = {"from_attributes": True}


class UploadResponse(BaseModel):
    """Response model for a successful upload."""

    transfer_id: str
    filename: str
    stored_as: str
    state: TransferState
    bytes_received: int


class ErrorResponse(BaseModel):
    """Body of the errors the routes raise via `HTTPException`.

    FastAPI's own 422 validation errors use a list `detail` instead.
    """

    detail: str = Field(min_length=1)


class FileInfo(BaseModel):
    """Metadata for a single stored file.

    Subclass this to add provider-specific fields (e.g. checksum, URL,
    upload timestamp).  Extra fields are preserved in the API response
    thanks to `SerializeAsAny` on `FileListResponse`.
    """

    name: str
    size_bytes: int
    format: MSFormat


@dataclass(frozen=True)
class ServedFile:
    """A file to serve under a name other than its on-disk name.

    Return this from `FileProvider.get_file` when the bytes are stored at
    a path whose name is not the one the client asked for (a
    content-addressed blob, a cache entry keyed by ID, a temp file).

    Attributes:
        path: Where the bytes are stored.  Range requests and the `ETag`
            and `Last-Modified` validators come from this file.
        name: The name to serve the file as.  It is sent in
            `Content-Disposition` and its suffix picks the media type.
    """

    path: Path
    name: str


class FileListResponse(BaseModel):
    """Response model for the file listing endpoint."""

    files: Sequence[SerializeAsAny[FileInfo]]


class HealthResponse(BaseModel):
    """Response model for health check endpoint."""

    status: str
    version: str
    store_as: StoreFormat
