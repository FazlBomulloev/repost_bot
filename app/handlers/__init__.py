from aiogram import Dispatcher

from . import menu, accounts, channel, settings, stats


def setup_routes(dp: Dispatcher) -> None:
    dp.include_router(menu.router)
    dp.include_router(accounts.router)
    dp.include_router(channel.router)
    dp.include_router(settings.router)
    dp.include_router(stats.router)
