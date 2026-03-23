from __future__ import annotations

from collections.abc import (
    Sequence,  # noqa: TC003 (Pydantic resolves annotations at runtime)
)
from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, SerializeAsAny

StoreFormat = Literal["msz", "mzml"]


class TransferState(str, Enum):
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


class FileInfo(BaseModel):
    """Metadata for a single stored file.

    Subclass this to add provider-specific fields (e.g. checksum, URL,
    upload timestamp).  Extra fields are preserved in the API response
    thanks to :pydantic:`SerializeAsAny` on :class:`FileListResponse`.
    """

    name: str
    size_bytes: int
    format: StoreFormat


class FileListResponse(BaseModel):
    """Response model for the file listing endpoint."""

    files: Sequence[SerializeAsAny[FileInfo]]


class HealthResponse(BaseModel):
    """Response model for health check endpoint."""

    status: str
    version: str
    store_as: StoreFormat
