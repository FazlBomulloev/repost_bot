import asyncio
import random
from datetime import datetime
from typing import Optional, Tuple, List

from aiogram import Dispatcher
from app.handlers import setup_routes
from loguru import logger
from telethon import TelegramClient, errors
from telethon.errors import UserAlreadyParticipantError, FloodWaitError, FloodError
from telethon.errors.rpcerrorlist import FloodWaitError as FloodWaitError2
from telethon.events import NewMessage
from telethon.tl.functions.channels import JoinChannelRequest

from core.models import tg_account as tg_account_db, channel as channel_db
from auto_reposting import telegram_utils, telegram_utils2
from auto_pause_restorer import start_pause_restorer, stop_pause_restorer, pause_restorer
from core.settings import json_settings, bot
from auto_reposting.channel_processor import channel_processor
from core.schemas import tg_account as tg_account_schemas

log_file_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"


class ListenerAccountManager:
    """Менеджер для управления аккаунтами-слушателями с автоматическим переключением"""
    
    def __init__(self):
        self.current_client: Optional[TelegramClient] = None
        self.current_account: Optional[tg_account_db.TGAccount] = None
        self.current_account_index: int = 0
        self.available_accounts: List[tg_account_db.TGAccount] = []
        self.max_retry_attempts = 3
        
    async def get_available_accounts(self) -> List[tg_account_db.TGAccount]:
        """Получает список доступных аккаунтов для прослушивания"""
        all_accounts = await tg_account_db.get_tg_accounts_by_status("WORKING")
        
        # Фильтруем аккаунты: убираем те что на паузе
        available = []
        for account in all_accounts:
            if account.last_datetime_pause and account.pause_in_seconds:
                if not await tg_account_db.has_pause_paused(account):
                    continue  # Аккаунт на паузе
            available.append(account)
            
        return available
    
    async def switch_to_next_account(self) -> Tuple[Optional[TelegramClient], Optional[tg_account_db.TGAccount]]:
        """Переключается на следующий доступный аккаунт"""
        logger.info("🔄 Переключаюсь на следующий аккаунт...")
        
        # Закрываем текущий клиент если есть
        if self.current_client:
            try:
                await self.current_client.disconnect()
                logger.info(f"🔌 Отключен аккаунт +{self.current_account.phone_number if self.current_account else 'Unknown'}")
            except Exception as e:
                logger.error(f"Ошибка при отключении клиента: {e}")
            self.current_client = None
            self.current_account = None
        
        # Обновляем список доступных аккаунтов
        self.available_accounts = await self.get_available_accounts()
        
        if not self.available_accounts:
            logger.error("❌ Нет доступных аккаунтов для прослушивания!")
            return None, None
        
        # Пробуем аккаунты начиная с текущего индекса
        attempts = 0
        while attempts < len(self.available_accounts):
            if self.current_account_index >= len(self.available_accounts):
                self.current_account_index = 0  # Возвращаемся к началу списка
                
            account = self.available_accounts[self.current_account_index]
            
            try:
                client = await telegram_utils2.create_tg_client(account)
                
                if client is None:
                    logger.warning(f"⚠️ Не удалось создать клиент для +{account.phone_number}")
                    self.current_account_index += 1
                    attempts += 1
                    continue
                
                # Проверяем авторизацию
                async with client:
                    try:
                        await client.get_me()
                        self.current_client = client
                        self.current_account = account
                        logger.success(f"✅ Активирован аккаунт-слушатель: +{account.phone_number}")
                        return client, account
                    except (errors.UnauthorizedError, errors.PhoneNumberInvalidError, errors.AuthKeyDuplicatedError):
                        logger.warning(f"🗑️ Аккаунт +{account.phone_number} потерял авторизацию, удаляю из БД")
                        await tg_account_db.update_tg_account(
                            tg_account=account,
                            tg_account_update=tg_account_schemas.TGAccountUpdate(
                                status=tg_account_schemas.TGAccountStatus.deleted
                            )
                        )
                        await client.disconnect()
                        
            except Exception as e:
                logger.error(f"❌ Ошибка при подключении к +{account.phone_number}: {e}")
                
            self.current_account_index += 1
            attempts += 1
        
        logger.error("❌ Не удалось найти рабочий аккаунт после всех попыток!")
        return None, None
    
    async def handle_client_error(self, error: Exception) -> Tuple[Optional[TelegramClient], Optional[tg_account_db.TGAccount]]:
        """Обрабатывает ЛЮБУЮ ошибку текущего клиента и переключается на следующий"""
        if self.current_account:
            logger.warning(f"❌ Ошибка у аккаунта +{self.current_account.phone_number}: {error}")
            logger.info(f"🔄 Переключаюсь на следующий аккаунт из-за ошибки")
            
            # Только при критических ошибках авторизации помечаем как удаленный
            if isinstance(error, (errors.UnauthorizedError, errors.PhoneNumberInvalidError, errors.AuthKeyDuplicatedError)):
                logger.warning(f"🗑️ Помечаю аккаунт +{self.current_account.phone_number} как удаленный")
                await tg_account_db.update_tg_account(
                    tg_account=self.current_account,
                    tg_account_update=tg_account_schemas.TGAccountUpdate(
                        status=tg_account_schemas.TGAccountStatus.deleted
                    )
                )
            # Для всех остальных ошибок (включая FROZEN_METHOD_INVALID, FloodWait) - просто переключаемся
        
        # Переключаемся на следующий аккаунт
        self.current_account_index += 1
        return await self.switch_to_next_account()


def is_within_work_time(current_time, start, end):
    if start < end:
        return start <= current_time < end
    else:
        return current_time >= start or current_time < end


async def check_subscribe_in_channels_simple(client: TelegramClient, account: tg_account_db.TGAccount) -> None:
    """Простая подписка на каналы - любая ошибка приводит к переключению аккаунта"""
    try:
        for channel in await channel_db.get_channels():
            try:
                tg_channel = await client.get_entity(channel.url)
                await client(JoinChannelRequest(tg_channel))
                logger.info(f"✅ Подписался на канал {channel.url}")
                
            except UserAlreadyParticipantError:
                logger.debug(f"👤 Уже подписан на {channel.url}")
                
            except Exception as e:
                logger.warning(f"⚠️ Ошибка подписки на {channel.url}: {e}")
                # Любая ошибка подписки - переключаем аккаунт
                raise Exception(f"Subscription error for account +{account.phone_number}: {e}")
                    
            await asyncio.sleep(random.randint(1, 2))  # Короткая пауза между подписками
    
    except Exception as e:
        logger.error(f"❌ Ошибка при подписке на каналы для +{account.phone_number}: {e}")
        raise  # Прокидываем ошибку наверх для переключения аккаунта


async def setup_fresh_dispatcher() -> Dispatcher:
    """Создает новый диспетчер с чистыми роутерами"""
    dp = Dispatcher()
    
    try:
        # Импортируем роутеры заново для избежания проблем с переподключением
        import importlib
        from app.handlers import menu, accounts, channel, settings, stats
        
        # Перезагружаем модули
        importlib.reload(menu)
        importlib.reload(accounts) 
        importlib.reload(channel)
        importlib.reload(settings)
        importlib.reload(stats)
        
        # Подключаем роутеры к новому диспетчеру
        dp.include_router(menu.router)
        dp.include_router(accounts.router)
        dp.include_router(channel.router) 
        dp.include_router(settings.router)
        dp.include_router(stats.router)
        
        logger.info("✅ Роутеры успешно настроены")
        return dp
    except Exception as e:
        logger.error(f"❌ Ошибка настройки роутеров: {e}")
        raise


async def main() -> None:
    logger.info("🚀 Запущен объединенный бот (управление + репостинг) с резервными аккаунтами...")

    # Создаем новый диспетчер для избежания ошибки router is attached
    dp = await setup_fresh_dispatcher()
    
    # Создаем менеджер аккаунтов-слушателей
    listener_manager = ListenerAccountManager()
    
    logger.info("🤖 Запуск Telegram бота для управления...")
    bot_task = asyncio.create_task(dp.start_polling(bot))
    logger.success("✅ Telegram бот запущен")

    # Запускаем процессор каналов
    logger.info("🚀 Запуск процессора каналов...")
    await channel_processor.start()
    logger.success("✅ Процессор каналов запущен")

    # Запускаем автовосстановление пауз
    pause_restorer_task = None
    try:
        logger.info("🔄 Запуск автовосстановления пауз...")
        pause_restorer_task = asyncio.create_task(start_pause_restorer())
        logger.success("✅ Автовосстановление пауз запущено")
    except Exception as e:
        logger.error(f"Ошибка при запуске автовосстановления: {e}")

    # Основной цикл с автоматическим переключением аккаунтов
    while True:
        try:
            # Получаем рабочий клиент-слушатель
            random_telegram_client, random_tg_account = await listener_manager.switch_to_next_account()

            if random_telegram_client is None:
                logger.error("❌ Нет доступных аккаунтов для прослушивания. Ожидание 5 минут...")
                await asyncio.sleep(300)
                continue

            try:
                await random_telegram_client.connect()
                logger.success(f"🎧 Слушаю через аккаунт: +{random_tg_account.phone_number}")

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

                    logger.info(f"📨 Новое сообщение CHANNEL ID: {channel_id} MESSAGE ID: {message_id}")

                    # Добавляем в очередь процессора
                    try:
                        success = await channel_processor.add_message(channel_id, message_id)

                        if success:
                            logger.info(f"✅ Сообщение {message_id} добавлено в очередь")
                            stats = channel_processor.get_stats()
                            logger.info(f"📊 Очередь: {stats['total_queue_size']}, Обработано всего: {stats['total_processed']}")
                        else:
                            logger.warning(f"⚠️ Сообщение {message_id} не добавлено")

                    except Exception as e:
                        logger.error(f"Ошибка при добавлении сообщения в очередь: {e}")

                # 🔧 ПРОСТАЯ подписка на каналы - любая ошибка = переключение аккаунта
                try:
                    await check_subscribe_in_channels_simple(client=random_telegram_client, account=random_tg_account)
                    logger.success(f"✅ Успешная подписка на каналы для +{random_tg_account.phone_number}")
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка при подписке на каналы для +{random_tg_account.phone_number}: {e}")
                    logger.info("🔄 Переключаюсь на следующий аккаунт")
                    await listener_manager.handle_client_error(e)
                    continue

                # Работаем до отключения или ошибки
                await random_telegram_client.run_until_disconnected()
                
            except Exception as e:
                logger.warning(f"❌ ЛЮБАЯ ошибка с аккаунтом +{random_tg_account.phone_number}: {e}")
                logger.info("🔄 Переключаюсь на следующий аккаунт")
                await listener_manager.handle_client_error(e)
                await asyncio.sleep(5)  # Короткая пауза перед следующим аккаунтом
                continue

        except KeyboardInterrupt:
            logger.info("🛑 Получен сигнал остановки")
            break
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в основном цикле: {e}")
            await asyncio.sleep(30)  # Пауза перед повтором
            continue

    # Cleanup при завершении работы
    logger.info("🧹 Очистка ресурсов...")
    
    # Останавливаем Telegram бота
    if 'bot_task' in locals() and not bot_task.done():
        logger.info("🛑 Остановка Telegram бота...")
        bot_task.cancel()
        try:
            await bot_task
        except asyncio.CancelledError:
            pass

    # Останавливаем процессор сообщений
    logger.info("🛑 Остановка процессора сообщений...")
    await channel_processor.stop()

    # Останавливаем автовосстановление
    if pause_restorer_task:
        logger.info("🛑 Остановка автовосстановления пауз...")
        stop_pause_restorer()
        try:
            await asyncio.wait_for(pause_restorer_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Таймаут при остановке автовосстановления")
            pause_restorer_task.cancel()

    # Закрываем слушающий клиент
    if listener_manager.current_client and listener_manager.current_client.is_connected():
        try:
            await listener_manager.current_client.disconnect()
            logger.info("🔌 Клиент-слушатель отключен")
        except Exception as e:
            logger.error(f"Ошибка при отключении клиента-слушателя: {e}")

    # Останавливаем диспетчер
    try:
        await dp.stop_polling()
        logger.info("✅ Диспетчер остановлен")
    except Exception as e:
        logger.debug(f"Ошибка при остановке диспетчера: {e}")


if __name__ == "__main__":
    logger.add(f"logs/{log_file_name}", rotation="1 day", retention="10 days", compression="zip")

    while True:
        try:
            # Очищаем ресурсы перед каждым запуском
            import gc
            gc.collect()
            
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("🛑 Получен сигнал остановки")
            break
        except Exception as e:
            logger.exception(f"💥 ЗАВЕРШИЛСЯ С ОШИБКОЙ: {e.__class__.__name__}: {e}")

        # Ждем перед перезапуском
        logger.info("🔄 Перезапуск через 30 секунд...")
        asyncio.run(asyncio.sleep(30))
