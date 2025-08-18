from datetime import datetime
from pydantic import BaseModel, ConfigDict, UUID4


class ChannelBase(BaseModel):
    url: str
    telegram_channel_id: int


class ChannelCreate(ChannelBase):
    pass


class ChannelUpdate(ChannelBase):
    url: str | None = None
    telegram_channel_id: int | None = None


class ChannelInDB(ChannelBase):
    model_config = ConfigDict(from_attributes=True)

    guid: UUID4
