from datetime import datetime
from typing import List
from uuid import UUID

from sqlalchemy import Enum, select, update, delete
from sqlalchemy.orm import Mapped, mapped_column

from core.models.base import Base, async_session_maker
from core.schemas import channel as channel_schemas


class Channel(Base):
    __tablename__ = "channels"

    url: Mapped[str]
    telegram_channel_id: Mapped[int]


async def create_channel(channel_in: channel_schemas.ChannelCreate) -> Channel:
    async with async_session_maker() as session:
        channel = Channel(**channel_in.model_dump())
        session.add(channel)
        await session.commit()
        return channel


async def delete_channel_by_guid(guid: int) -> None:
    async with async_session_maker() as session:
        query = delete(Channel).where(Channel.guid == UUID(guid, version=4))
        await session.execute(query)
        await session.commit()


async def get_channels() -> List[Channel]:
    async with async_session_maker() as session:
        result = await session.execute(select(Channel))
        return result.scalars().all()


async def get_channel_by_guid(guid: str) -> Channel:
    async with async_session_maker() as session:
        query = select(Channel).where(Channel.guid == UUID(guid, version=4))
        result = await session.execute(query)
        return result.scalars().first()


async def get_channel_by_telegram_channel_id(telegram_channel_id: int) -> Channel:
    async with async_session_maker() as session:
        query = select(Channel).where(Channel.telegram_channel_id == telegram_channel_id)
        result = await session.execute(query)
        return result.scalars().first()
