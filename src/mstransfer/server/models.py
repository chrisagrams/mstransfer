from __future__ import annotations

from collections.abc import (
    Sequence,  # noqa: TC003 (Pydantic resolves annotations at runtime)
)
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SerializeAsAny

StoreFormat = Literal["msz", "mzml"]
FileFormat = Literal["msz", "mzml", "mszx"]


class TransferState(StrEnum):
    """Enumeration of possible states for a file transfer."""

    RECEIVING = "receiving"
    RECEIVED = "received"
    DECOMPRESSING = "decompressing"
    DONE = "done"
    ERROR = "error"


class SourceFormat(StrEnum):
    """Format of an uploaded payload, as declared by the client."""

    MZML = "mzml"
    MSZ = "msz"
    MSZX = "mszx"

    @classmethod
    def from_upload(cls, header: str | None, filename: str) -> SourceFormat:
        """Parse ``X-Source-Format``, falling back to the filename extension.

        The header is matched case-insensitively.  When it is absent, a
        ``.mszx`` filename means an archive; anything else is treated as an
        MSZ stream, which is what clients sent before the header existed.
        Raises :class:`ValueError` for an unrecognised header value.
        """
        if header is None:
            is_mszx = Path(filename).suffix.lower() == ".mszx"
            return cls.MSZX if is_mszx else cls.MSZ
        return cls(header.strip().lower())

    @property
    def stored_suffix(self) -> str:
        """mzML arrives as a compressed MSZ stream; archives keep their suffix."""
        return ".mszx" if self is SourceFormat.MSZX else ".msz"

    @property
    def is_archive(self) -> bool:
        """Whether the payload is an .mszx archive, stored without conversion."""
        return self is SourceFormat.MSZX


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
    format: FileFormat


class FileListResponse(BaseModel):
    """Response model for the file listing endpoint."""

    files: Sequence[SerializeAsAny[FileInfo]]


class HealthResponse(BaseModel):
    """Response model for health check endpoint."""

    status: str
    version: str
    store_as: StoreFormat
