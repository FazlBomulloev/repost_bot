import asyncio
import random
import subprocess
from datetime import datetime

from loguru import logger
from telethon import TelegramClient
from telethon.errors import UserAlreadyParticipantError
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
    try:
        for channel in await channel_db.get_channels():
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
    except Exception as e:
        logger.error(f"Ошибка при подписке на каналы: {e}")


async def get_working_client() -> tuple[TelegramClient, tg_account_db.TGAccount]:
    """Получает рабочий клиент с правильной обработкой ошибок"""
    tg_accounts = await tg_account_db.get_tg_accounts_by_status("WORKING")
    
    if not tg_accounts:
        logger.error("Нет рабочих аккаунтов в базе")
        return None, None
    
    # Перемешиваем аккаунты для равномерного распределения нагрузки
    random.shuffle(tg_accounts)
    
    max_attempts = min(len(tg_accounts), 10)  # Ограничиваем количество попыток
    
    for attempt in range(max_attempts):
        account = tg_accounts[attempt]
        
        # Проверяем паузу аккаунта
        if account.last_datetime_pause and account.pause_in_seconds:
            if not await tg_account_db.has_pause_paused(account):
                logger.info(f"Аккаунт +{account.phone_number} на паузе, пропускаем")
                continue
        
        try:
            client = await telegram_utils2.create_tg_client(account)
            if client is not None:
                logger.info(f"Успешно авторизован аккаунт: +{account.phone_number}")
                return client, account
            else:
                logger.warning(f"Не удалось авторизовать аккаунт +{account.phone_number}")
                
        except Exception as e:
            logger.error(f"Ошибка при создании клиента для +{account.phone_number}: {e}")
            continue
    
    logger.error("Не удалось найти рабочий аккаунт после всех попыток")
    return None, None


async def main() -> None:
    logger.info("Запущен бот для отслеживания постов с каналов...")
    
    # Получаем рабочий клиент
    random_telegram_client, random_tg_account = await get_working_client()
    
    if random_telegram_client is None:
        logger.error("Нет доступных аккаунтов для работы. Ожидание 5 минут...")
        await asyncio.sleep(300)
        return
    
    try:
        await random_telegram_client.connect()
        
        @random_telegram_client.on(NewMessage)
        async def new_message(event: NewMessage.Event) -> None:
            try:
                message_id = event.original_update.message.id
                channel_id = event.original_update.message.peer_id.channel_id
                
                # Проверяем, что канал в нашем списке
                channels = await channel_db.get_channels()
                if channel_id not in [channel.telegram_channel_id for channel in channels]:
                    return
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке события: {e}")
                return
            
            # Проверяем рабочее время
            try:
                current_time = datetime.now().time()
                start_time = datetime.strptime(await json_settings.async_get_attribute("start_time"), "%H:%M").time()
                end_time = datetime.strptime(await json_settings.async_get_attribute("end_time"), "%H:%M").time()

                if not is_within_work_time(current_time, start_time, end_time):
                    logger.info("Не рабочее время!")
                    return
            except Exception as e:
                logger.error(f"Ошибка при проверке рабочего времени: {e}")
                return

            logger.info(f"Получено новое сообщение CHANNEL ID: {channel_id} MESSAGE ID: {message_id}")

            # Запускаем обработку в отдельном процессе
            try:
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
            except Exception as e:
                logger.error(f"Ошибка при запуске процесса обработки: {e}")

        # Подписываемся на каналы
        await check_subscribe_in_channels(client=random_telegram_client)
        
        # Основной цикл работы
        await random_telegram_client.run_until_disconnected()
        
    except Exception as e:
        logger.error(f"Ошибка в основном цикле: {e}")
    finally:
        # Обязательно закрываем соединение
        if random_telegram_client and random_telegram_client.is_connected():
            try:
                await random_telegram_client.disconnect()
                logger.info("Клиент отключен")
            except Exception as e:
                logger.error(f"Ошибка при отключении клиента: {e}")


if __name__ == "__main__":
    logger.add(f"logs/{log_file_name}", rotation="1 day", retention="10 days", compression="zip")
    
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки")
            break
        except Exception as e:
            logger.exception(f"ЗАВЕРШИЛСЯ С ОШИБКОЙ: {e.__class__.__name__}: {e}")
        
        # Ждем перед перезапуском
        logger.info("Перезапуск через 30 секунд...")
        asyncio.run(asyncio.sleep(30))