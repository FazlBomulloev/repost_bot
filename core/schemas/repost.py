from datetime import date
from uuid import UUID

from pydantic import BaseModel, ConfigDict, UUID4


class RepostBase(BaseModel):
    channel_guid: UUID
    repost_message_id: int
    created_at: date


class RepostCreate(RepostBase):
    pass


class RepostUpdate(RepostBase):
    channel_guid: UUID | None = None
    repost_message_id: int | None = None
    created_at: date | None = None


class RepostInDB(RepostBase):
    model_config = ConfigDict(from_attributes=True)

    guid: UUID4
