import asyncio
import random
import subprocess
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger
from telethon import TelegramClient
from telethon.errors import UserAlreadyParticipantError, FloodWaitError
from telethon.errors.rpcerrorlist import FloodWaitError as FloodWaitError2
from telethon.events import NewMessage
from telethon.tl.functions.channels import JoinChannelRequest

from core.models import tg_account as tg_account_db, channel as channel_db
from auto_reposting import telegram_utils, telegram_utils2
from core.settings import json_settings

log_file_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"


def is_within_work_time(current_time, start, end):
    if start < end:
        return start <= current_time < end
    else:
        return current_time >= start or current_time < end


async def check_subscribe_in_channels(client: TelegramClient) -> None:
    channels_where_subscribed = []
    for channel in await channel_db.get_channels():
        print(channels_where_subscribed)
        if channel.telegram_channel_id not in channels_where_subscribed:
            try:
                tg_channel = await client.get_entity(channel.url)
                logger.info(await client(JoinChannelRequest(tg_channel)))
            except UserAlreadyParticipantError:
                channels_where_subscribed.append(channel.telegram_channel_id)
            except Exception as e:
                logger.exception(e)
                continue
            channels_where_subscribed.append(channel.telegram_channel_id)

        await asyncio.sleep(random.randint(1, 2))


async def main() -> None:
    # scheduler = AsyncIOScheduler()
    logger.info("Запущен бот для отслеживания постов с каналов...")
    tg_accounts = await tg_account_db.get_tg_accounts()
    random_telegram_client = None

    counter_tg_accounts = 0
    stop_iter = False
    random_tg_account = None
    while not stop_iter:
        if counter_tg_accounts + 1 >= len(tg_accounts):
            logger.info("Нету аккаунтов")
            return

        random_tg_account: tg_account_db.TGAccount = random.choice(tg_accounts)
        random_telegram_client = await telegram_utils2.create_tg_client(tg_account=random_tg_account)
        if random_telegram_client is not None:
            stop_iter = True

    logger.info(f"Авторизован аккаунт: +{random_tg_account.phone_number}")
    await random_telegram_client.connect()

    # scheduler.add_job(check_subscribe_in_channels, trigger=IntervalTrigger(hours=1), args=[random_telegram_client])
    # scheduler.start()

    @random_telegram_client.on(NewMessage)
    async def new_message(event: NewMessage.Event) -> None:
        try:
            message_id = event.original_update.message.id
            channel_id = event.original_update.message.peer_id.channel_id
            if channel_id not in [channel.telegram_channel_id for channel in await channel_db.get_channels()]:
                return
        except Exception as e:
            logger.error(e)
            return
        current_time = datetime.now().time()
        start_time = datetime.strptime(await json_settings.async_get_attribute("start_time"), "%H:%M").time()
        end_time = datetime.strptime(await json_settings.async_get_attribute("end_time"), "%H:%M").time()

        if not is_within_work_time(current_time, start_time, end_time):
            logger.info("Не рабочее время!")
            return

        logger.info(f"Получено новое сообщение CHANNEL ID: {channel_id} MESSAGE ID: {message_id}")

        command = [
            'venv/bin/python3',
            'process_post3.py',
            '--telegram_message_id',
            str(message_id),
            '--telegram_channel_id',
            str(channel_id),
            '--log_filename',
            log_file_name
        ]
        subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    await check_subscribe_in_channels(client=random_telegram_client)
    await random_telegram_client.run_until_disconnected()
    # while True:
    #     print(random_telegram_client.is_connected())
    #     await random_telegram_client.disconnected


if __name__ == "__main__":
    logger.add(f"logs/{log_file_name}", rotation="1 day", retention="10 days", compression="zip")
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logger.exception(f"ЗАВЕРШИЛСЯ С ОШИБКОЙ: {e.__class__.__name__}: {e}")
