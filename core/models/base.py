from uuid import UUID, uuid4
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, sessionmaker

from core.settings import settings


class Base(DeclarativeBase):
    __abstract__ = True

    guid: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)


engine = create_async_engine(settings.database_url.get_secret_value())
async_session_maker = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session
