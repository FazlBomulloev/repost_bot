from datetime import datetime, date
from typing import List
from uuid import UUID

from sqlalchemy import Enum, select, update, delete
from sqlalchemy.orm import Mapped, mapped_column

from core.models.base import Base, async_session_maker
from core.schemas import repost as repost_schemas


class Repost(Base):
    __tablename__ = "reposts"

    channel_guid: Mapped[UUID]
    repost_message_id: Mapped[int]
    created_at: Mapped[date]


async def create_repost(repost_in: repost_schemas.RepostCreate) -> Repost:
    async with async_session_maker() as session:
        group = Repost(repost_in.model_dump())
        session.add(group)
        await session.commit()
        return group


async def get_repost_for_day(channel_guid: str) -> List[Repost]:
    """Получение репостов за сегодня (для совместимости)"""
    async with async_session_maker() as session:
        query = select(Repost).where(
            Repost.channel_guid == UUID(channel_guid, version=4),
            Repost.created_at == datetime.now().date()
        )
        result = await session.execute(query)
        return result.scalars().all()


async def get_reposts_by_date(channel_guid: str, date: date) -> List[Repost]:
    """Получение репостов по конкретной дате"""
    async with async_session_maker() as session:
        query = select(Repost).where(
            Repost.channel_guid == UUID(channel_guid, version=4),
            Repost.created_at == date
        )
        result = await session.execute(query)
        return result.scalars().all()


async def get_reposts_by_date_range(channel_guid: str, start_date: date, end_date: date) -> List[Repost]:
    """Получение репостов за период"""
    async with async_session_maker() as session:
        query = select(Repost).where(
            Repost.channel_guid == UUID(channel_guid, version=4),
            Repost.created_at >= start_date,
            Repost.created_at <= end_date
        )
        result = await session.execute(query)
        return result.scalars().all()


async def get_total_reposts_count() -> int:
    """Общее количество репостов"""
    async with async_session_maker() as session:
        query = select(Repost)
        result = await session.execute(query)
        return len(result.scalars().all())


async def get_reposts_count_by_channel(channel_guid: str) -> int:
    """Количество репостов конкретного канала"""
    async with async_session_maker() as session:
        query = select(Repost).where(
            Repost.channel_guid == UUID(channel_guid, version=4)
        )
        result = await session.execute(query)
        return len(result.scalars().all())