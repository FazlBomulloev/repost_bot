from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, UUID4


class GroupBase(BaseModel):
    channel_guid: UUID
    url: str


class GroupCreate(GroupBase):
    pass


class GroupUpdate(GroupBase):
    channel_guid: UUID | None = None
    url: str | None = None


class GroupInDB(GroupBase):
    model_config = ConfigDict(from_attributes=True)

    guid: UUID4
