from uuid import UUID
from datetime import datetime
from typing import List

from sqlalchemy import Enum, select, update, delete
from sqlalchemy.orm import Mapped, mapped_column

from core.models.base import Base, async_session_maker
from core.schemas import tg_account as tg_account_schemas


class TGAccount(Base):
    __tablename__ = "tg_accounts"

    channel_guid: Mapped[UUID] = mapped_column(nullable=True)
    telegram_id: Mapped[int]
    last_datetime_pause: Mapped[datetime] = mapped_column(nullable=True)
    pause_in_seconds: Mapped[int] = mapped_column(nullable=True)
    phone_number: Mapped[int] = mapped_column(unique=True)
    string_session: Mapped[str]
    status: Mapped[str] = mapped_column(
        Enum("WORKING", "MUTED", "DELETED", name="tg_account_status", create_type=False)
    )


async def create_tg_account(tg_account_in: tg_account_schemas.TGAccountCreate) -> TGAccount:
    async with async_session_maker() as session:
        tg_account = TGAccount(**tg_account_in.model_dump())
        session.add(tg_account)
        await session.commit()
        return tg_account



async def get_tg_accounts_by_channel_guid(channel_guid: int | None) -> List[TGAccount]:
    async with async_session_maker() as session:
        if channel_guid is None:
            query = select(TGAccount).where(
                TGAccount.channel_guid.is_(None),
                TGAccount.status != "DELETED"
            )
        else:
            query = select(TGAccount).where(
                TGAccount.channel_guid == UUID(channel_guid, version=4),
                TGAccount.status != "DELETED"
            )
        result = await session.execute(query)
        return result.scalars().all()


async def get_tg_accounts() -> List[TGAccount]:
    async with async_session_maker() as session:
        query = select(TGAccount)
        result = await session.execute(query)
        return result.scalars().all()



async def delete_tg_account(tg_account: TGAccount) -> None:
    async with async_session_maker() as session:
        await session.delete(tg_account)
        await session.commit()


async def update_tg_account(tg_account: TGAccount, tg_account_update: tg_account_schemas.TGAccountUpdate) -> TGAccount:
    async with async_session_maker() as session:
        query = update(TGAccount).where(TGAccount.guid == tg_account.guid).values(**tg_account_update.model_dump(exclude_unset=True))
        await session.execute(query)
        await session.commit()

    return tg_account


async def set_channel_guid_where_channel_guid_is_none(channel_guid: str, count_accounts: int) -> None:
    async with async_session_maker() as session:
        query = select(TGAccount).limit(count_accounts).where(TGAccount.channel_guid.is_(None))
        result = await session.execute(query)
        guids = [i.guid for i in result.scalars().all()]
        print(guids)
        query = update(TGAccount).where(TGAccount.guid.in_(guids)).values(channel_guid=UUID(str(channel_guid), version=4))
        await session.execute(query)
        await session.commit()


async def set_delete_status_tg_account_by_phone_number(phone_number: str) -> None:
    async with async_session_maker() as session:
        query = update(TGAccount).where(TGAccount.phone_number == phone_number).values(status="DELETED")
        await session.execute(query)
        await session.commit()


async def set_new_channel_guid_where_channel_guid(channel_guid: str, new_channel_guid: str, count_accounts: int) -> None:
    async with async_session_maker() as session:
        if isinstance(channel_guid, str):
            query = select(TGAccount).where(TGAccount.channel_guid == UUID(channel_guid, version=4))
        else:
            query = select(TGAccount).where(TGAccount.channel_guid == channel_guid)
        result = await session.execute(query)
        guids = []
        for i in result.scalars().all():
            if i.status == "WORKING":
                guids.append(i.guid)
            if len(guids) == count_accounts:
                break
        query = update(TGAccount).where(TGAccount.guid.in_(guids)).values(channel_guid=new_channel_guid)
        await session.execute(query)
        await session.commit()


async def has_pause_paused(tg_account: TGAccount) -> bool:
    current_time = datetime.now()
    elapsed_time = current_time - tg_account.last_datetime_pause
    if elapsed_time.total_seconds() >= tg_account.pause_in_seconds:
        async with async_session_maker() as session:
            query = update(TGAccount).where(TGAccount.guid == tg_account.guid).values(
                last_datetime_pause=None,
                pause_in_seconds=None,
                status="WORKING"
            )
            await session.execute(query)
            await session.commit()
        return True

    return False


async def add_pause(tg_account: TGAccount, pause_in_seconds: int) -> None:
    async with async_session_maker() as session:
        query = update(TGAccount).where(TGAccount.guid == tg_account.guid).values(
            last_datetime_pause=datetime.now(),
            pause_in_seconds=pause_in_seconds,
        )
        await session.execute(query)
        await session.commit()


async def get_tg_accounts_by_status(status: str) -> List[TGAccount]:
    async with async_session_maker() as session:
        query = select(TGAccount).where(TGAccount.status == status)
        result = await session.execute(query)
        return result.scalars().all()


async def get_tg_accounts_by_status_in_channel(status: str) -> List[TGAccount]:
    async with async_session_maker() as session:
        query = select(TGAccount).where(TGAccount.status == status, TGAccount.channel_guid.is_not(None))
        result = await session.execute(query)
        return result.scalars().all()


async def get_tg_accounts_without_channel() -> List[TGAccount]:
    async with async_session_maker() as session:
        query = select(TGAccount).where(TGAccount.channel_guid.is_(None))
        result = await session.execute(query)
        return result.scalars().all()


async def get_tg_accounts_by_channel_guid_and_status(channel_guid: str, status: str) -> List[TGAccount]:
    async with async_session_maker() as session:
        query = select(TGAccount).where(TGAccount.channel_guid == UUID(channel_guid, version=4), TGAccount.status == status)
        result = await session.execute(query)
        return result.scalars().all()


async def get_tg_account_by_phone_number(phone_number: int) -> TGAccount:
    async with async_session_maker() as session:
        query = select(TGAccount).where(TGAccount.phone_number == phone_number)
        result = await session.execute(query)
        return result.scalars().first()
