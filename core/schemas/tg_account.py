from enum import Enum
from datetime import datetime
from pydantic import BaseModel, ConfigDict, UUID4


class TGAccountStatus(str, Enum):
    working = "WORKING"
    muted = "MUTED"
    deleted = "DELETED"


class TGAccountBase(BaseModel):
    channel_guid: UUID4 | None
    telegram_id: int
    last_datetime_pause: datetime | None
    pause_in_seconds: int | None
    phone_number: int
    string_session: str
    status: TGAccountStatus


class TGAccountCreate(TGAccountBase):
    pass


class TGAccountUpdate(TGAccountBase):
    channel_guid: UUID4 | None = None
    telegram_id: int | None = None
    last_datetime_pause: datetime | None = None
    pause_in_seconds: int | None = None
    phone_number: int | None = None
    string_session: str | None = None
    status: TGAccountStatus | None = None


class TGAccountInDB(TGAccountBase):
    model_config = ConfigDict(from_attributes=True)

    guid: UUID4
