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


class PartStatus(BaseModel):
    """Progress of a single part of a multipart upload."""

    index: int
    offset: int
    size: int
    bytes_received: int = 0

    @property
    def complete(self) -> bool:
        return self.bytes_received == self.size


class TransferRecord(BaseModel):
    """Data model for a file transfer record in the registry."""

    transfer_id: str
    filename: str
    state: TransferState = TransferState.RECEIVING
    bytes_received: int = 0
    stored_as: str = ""
    error: str | None = None
    created_at: datetime = Field(default_factory=datetime.now)
    # Only populated for multipart uploads (see ``POST /v1/uploads``).
    total_bytes: int | None = None
    parts: list[PartStatus] | None = None

    model_config = {"from_attributes": True}


class UploadResponse(BaseModel):
    """Response model for a successful upload."""

    transfer_id: str
    filename: str
    stored_as: str
    state: TransferState
    bytes_received: int


# Upper bound on parts per multipart upload, to keep per-transfer state small.
MAX_UPLOAD_PARTS = 10_000


class MultipartInitRequest(BaseModel):
    """Request body for initiating a multipart upload."""

    transfer_id: str = Field(min_length=1)
    filename: str = Field(min_length=1)
    total_size: int = Field(gt=0)
    part_size: int = Field(gt=0)


class MultipartInitResponse(BaseModel):
    """Response for a newly initiated multipart upload.

    Part *n* covers bytes ``[n * part_size, min((n + 1) * part_size,
    total_size))`` of the file.
    """

    transfer_id: str
    filename: str
    total_size: int
    part_size: int
    part_count: int


class PartUploadResponse(BaseModel):
    """Response for a successfully received part."""

    transfer_id: str
    index: int
    offset: int
    size: int
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
