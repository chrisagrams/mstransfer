from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class TransferState(str, Enum):
    RECEIVING = "receiving"
    RECEIVED = "received"
    DECOMPRESSING = "decompressing"
    DONE = "done"
    ERROR = "error"


class TransferRecord(BaseModel):
    transfer_id: str
    filename: str
    state: TransferState = TransferState.RECEIVING
    bytes_received: int = 0
    stored_as: str = ""
    error: str | None = None
    created_at: datetime = Field(default_factory=datetime.now)

    model_config = {"from_attributes": True}


class UploadResponse(BaseModel):
    transfer_id: str
    filename: str
    stored_as: str
    state: TransferState
    bytes_received: int


class HealthResponse(BaseModel):
    status: str
    version: str
    store_as: str
