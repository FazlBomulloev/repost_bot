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
        group = Repost(**repost_in.model_dump())
        session.add(group)
        await session.commit()
        return group


async def get_repost_for_day(channel_guid: str) -> List[Repost]:
    async with async_session_maker() as session:
        query = select(Repost).where(
            Repost.channel_guid == UUID(channel_guid, version=4),
            Repost.created_at == datetime.now().date()
        )
        result = await session.execute(query)
        return result.scalars().all()
