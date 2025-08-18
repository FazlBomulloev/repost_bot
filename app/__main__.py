import asyncio
import logging

from aiogram import Dispatcher

from core.settings import bot
from app.handlers import setup_routes


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)


async def main() -> None:
    dp = Dispatcher()
    setup_routes(dp=dp)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())