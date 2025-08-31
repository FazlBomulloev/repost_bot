from datetime import datetime
from typing import List
from uuid import UUID

from sqlalchemy import Enum, select, update, delete
from sqlalchemy.orm import Mapped, mapped_column

from core.models.base import Base, async_session_maker
from core.schemas import group as group_schemas


class Group(Base):
    __tablename__ = "groups"

    channel_guid: Mapped[UUID]
    url: Mapped[str]


async def create_group(group_in: group_schemas.GroupCreate) -> Group:
    async with async_session_maker() as session:
        group = Group(group_in.model_dump())
        session.add(group)
        await session.commit()
        return group


async def delete_group_by_url(channel_guid: str, url: int) -> None:
    async with async_session_maker() as session:
        query = delete(Group).where(Group.channel_guid == UUID(str(channel_guid), version=4), Group.url == url)
        await session.execute(query)
        await session.commit()


async def get_all_groups_by_channel_guid(channel_guid: str) -> List[Group]:
    async with async_session_maker() as session:
        result = await session.execute(select(Group).where(Group.channel_guid == UUID(channel_guid, version=4)))
        return result.scalars().all()

