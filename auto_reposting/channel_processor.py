import asyncio
from datetime import datetime
from typing import Dict, Set, Optional, List
from dataclasses import dataclass
from loguru import logger
import random

from core.models import channel as channel_db, tg_account as tg_account_db
from auto_reposting import telegram_utils2
from core.settings import json_settings


@dataclass
class ChannelTask:
    channel_id: int
    message_id: int
    timestamp: datetime


class ChannelWorker:
    """Воркер с ПОСТОЯННЫМ состоянием ротации аккаунтов"""
    
    def __init__(self, channel_guid: str, channel_url: str, worker_id: int):
        self.channel_guid = channel_guid
        self.channel_url = channel_url
        self.worker_id = worker_id
        self.task_queue = asyncio.Queue()
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        self.current_task = None
        
        # 🎯 ПОСТОЯННОЕ СОСТОЯНИЕ РОТАЦИИ (НЕ ОБНУЛЯЕТСЯ!)
        self.current_account_index = 0
        self.current_account_reposts = 0
        self.current_account = None
        self.available_accounts = []
        self.last_accounts_refresh = None
        
        # Кэш групп для каждого аккаунта
        self.account_groups_cache: Dict[int, Set[str]] = {}  
        
        self.logger = logger.bind(worker_id=worker_id, channel=channel_url)
        
    async def start(self):
        """Запуск воркера"""
        self.running = True
        self.logger.info(f"🚀 Воркер {self.worker_id} запущен для канала {self.channel_url}")
        
        while self.running:
            try:
                task = await asyncio.wait_for(
                    self.task_queue.get(), 
                    timeout=10.0
                )
                
                self.current_task = task
                await self._process_channel_task_with_rotation(task)
                self.task_queue.task_done()
                self.processed_count += 1
                self.current_task = None
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Критическая ошибка в воркере: {e}")
                self.error_count += 1
                self.current_task = None
                await asyncio.sleep(1)
    
    async def refresh_available_accounts(self):
        """Обновляет список доступных аккаунтов (только если прошло время)"""
        now = datetime.now()
        
        # Обновляем список аккаунтов каждые 5 минут или при первом запуске
        if (self.last_accounts_refresh is None or 
            (now - self.last_accounts_refresh).total_seconds() > 300):
            
            self.available_accounts = await tg_account_db.get_working_accounts_by_channel(self.channel_guid)
            self.last_accounts_refresh = now
            
            self.logger.info(f"🔄 Обновлен список аккаунтов: {len(self.available_accounts)} доступно")
            
            # Если текущий индекс больше чем аккаунтов - сбрасываем
            if self.current_account_index >= len(self.available_accounts):
                self.current_account_index = 0
                self.current_account_reposts = 0
                self.logger.info("🔄 Сброшен индекс аккаунтов")
    
    async def get_current_working_account(self):
        await self.refresh_available_accounts()
        
        if not self.available_accounts:
            self.logger.error("❌ Нет доступных аккаунтов")
            return None
        
        # Получаем настройки
        try:
            number_reposts_before_pause = await json_settings.async_get_attribute("number_reposts_before_pause")
            pause_after_rate_reposts = await json_settings.async_get_attribute("pause_after_rate_reposts")
        except:
            number_reposts_before_pause = 15
            pause_after_rate_reposts = 3600
        
        # 🔄 ПРОВЕРЯЕМ НУЖНО ЛИ ПЕРЕКЛЮЧИТЬ АККАУНТ
        if self.current_account_reposts >= number_reposts_before_pause:
            # Ставим текущий аккаунт на паузу
            if (self.current_account_index < len(self.available_accounts) and 
                self.current_account_reposts > 0):
                
                old_account = self.available_accounts[self.current_account_index]
                await tg_account_db.add_pause(old_account, pause_after_rate_reposts)
                
                self.logger.warning(f"⏸️ Аккаунт +{old_account.phone_number} достиг лимита ({self.current_account_reposts}), пауза {pause_after_rate_reposts//60} мин")
            
            # Переключаемся на следующий аккаунт
            self.current_account_index += 1
            self.current_account_reposts = 0
        
        # 🆕 ПРОВЕРЯЕМ ТЕКУЩИЙ АККАУНТ НА FROZEN_METHOD_INVALID
        max_attempts = len(self.available_accounts)
        attempts = 0
        
        while attempts < max_attempts:
            # Если индекс выходит за границы - начинаем сначала
            if self.current_account_index >= len(self.available_accounts):
                self.current_account_index = 0
                
            candidate_account = self.available_accounts[self.current_account_index]
            
            # Простая проверка - пробуем создать клиент
            test_client = await telegram_utils2.create_tg_client(candidate_account)
            if test_client:
                try:
                    # Быстрая проверка доступности методов
                    async with test_client:
                        await test_client.get_me()
                    
                    self.current_account = candidate_account
                    self.logger.info(f"✅ Выбран аккаунт +{candidate_account.phone_number} (попытка {attempts + 1})")
                    return candidate_account
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Аккаунт +{candidate_account.phone_number} недоступен: {e}")
                    # Переходим к следующему
                    self.current_account_index += 1
                    attempts += 1
                    continue
            else:
                self.logger.warning(f"⚠️ Не удалось создать клиент для +{candidate_account.phone_number}")
                self.current_account_index += 1
                attempts += 1
                continue
        
        # Если никого не нашли - ждем и пробуем снова
        self.logger.error("❌ Все аккаунты недоступны, жду 5 минут")
        await asyncio.sleep(300)
        await self.refresh_available_accounts()
        self.current_account_index = 0
        
        return None


    async def handle_account_error(self, error_message: str):
        if self.current_account:
            self.logger.warning(f"🔄 Ошибка у аккаунта +{self.current_account.phone_number}: {error_message}")
            
            # Если FROZEN_METHOD_INVALID - переключаемся немедленно
            if "FROZEN_METHOD_INVALID" in error_message:
                self.logger.warning(f"🧊 У аккаунта +{self.current_account.phone_number} заморожены методы, переключаюсь")
                self.current_account_index += 1
                self.current_account_reposts = 0  # Сбрасываем счетчик только при принудительном переключении
                return await self.get_current_working_account()
        
        return self.current_account

    async def increment_account_reposts(self):
        """Увеличивает счетчик репостов текущего аккаунта"""
        self.current_account_reposts += 1
        
        if self.current_account:
            self.logger.debug(f"📊 Репостов у +{self.current_account.phone_number}: {self.current_account_reposts}")
    
    async def _check_stop_links_in_message(self, telegram_client, channel, message_id, tg_accounts, task_logger) -> bool:
        """Проверяет стоп-ссылки в сообщении"""
        try:
            stop_links = await json_settings.async_get_attribute("stop_links")
            if not stop_links:
                return False

            async with telegram_client:
                message = await telegram_client.get_messages(channel.telegram_channel_id, ids=message_id)
                
                if not message or not message.message:
                    return False

                for stop_link in stop_links:
                    if stop_link in message.message:
                        task_logger.info(f"🚫 В сообщении найдена стоп-ссылка: {stop_link}")
                        
                        # Ставим реакции
                        await telegram_utils2.send_reaction_with_accounts_on_message(
                            tg_accounts=tg_accounts,
                            message=message,
                            channel_url=channel.url,
                            emoji_reaction=await json_settings.async_get_attribute("reaction")
                        )
                        
                        task_logger.success("❤️ Реакции поставлены на сообщение со стоп-ссылкой")
                        return True

            return False
            
        except Exception as e:
            task_logger.error(f"Ошибка при проверке стоп-ссылок: {e}")
            return False
    
    async def _process_channel_task_with_rotation(self, task: ChannelTask):
        start_time = datetime.now()
        task_logger = self.logger.bind(msg_id=task.message_id)
        
        try:
            # Получаем данные канала
            channel = await channel_db.get_channel_by_telegram_channel_id(task.channel_id)
            if not channel:
                task_logger.error(f"Канал {task.channel_id} не найден в БД")
                return
            
            # Получаем группы канала
            from core.models import group as group_db
            all_groups = await group_db.get_all_groups_by_channel_guid(self.channel_guid)
            if not all_groups:
                task_logger.warning("Нет групп для репостинга")
                return
            
            # Ограничиваем количество групп
            try:
                max_groups = await json_settings.async_get_attribute("max_groups_per_post")
                delay_between_groups = await json_settings.async_get_attribute("delay_between_groups")
                check_stop_links = await json_settings.async_get_attribute("check_stop_links")
            except:
                max_groups = 20
                delay_between_groups = 120
                check_stop_links = True
            
            selected_groups = random.sample(all_groups, min(max_groups, len(all_groups)))
            task_logger.info(f"📊 Выбрано {len(selected_groups)} из {len(all_groups)} групп")
            
            # Проверяем стоп-ссылки (пропускаем если есть проблемы)
            if check_stop_links:
                current_account = await self.get_current_working_account()
                if current_account:
                    temp_client = await telegram_utils2.create_tg_client(current_account)
                    if temp_client:
                        try:
                            stop_links_found = await self._check_stop_links_in_message(
                                temp_client, channel, task.message_id, [current_account], task_logger
                            )
                            if stop_links_found:
                                task_logger.info("🛑 Найдены стоп-ссылки, обработка завершена")
                                return
                        except Exception as e:
                            task_logger.warning(f"⚠️ Ошибка проверки стоп-ссылок: {e}")
                        finally:
                            await temp_client.disconnect()
            
            # 🚀 ГЛАВНЫЙ ЦИКЛ С АВТОПЕРЕКЛЮЧЕНИЕМ ПРИ ОШИБКАХ
            successful_reposts = 0
            
            for i, group in enumerate(selected_groups, 1):
                group_logger = task_logger.bind(group_idx=i, total=len(selected_groups))
                
                try:
                    # Получаем текущий рабочий аккаунт
                    working_account = await self.get_current_working_account()
                    if not working_account:
                        group_logger.error("❌ Нет доступных аккаунтов")
                        break
                    
                    group_logger.info(f"🎯 Группа {i}: {group.url} (аккаунт +{working_account.phone_number}, репост #{self.current_account_reposts + 1})")
                    
                    # 🔄 ПРОБУЕМ НЕСКОЛЬКО АККАУНТОВ ДЛЯ ОДНОЙ ГРУППЫ
                    repost_success = False
                    account_attempts = 0
                    max_account_attempts = min(3, len(self.available_accounts))  # Максимум 3 попытки с разными аккаунтами
                    
                    while not repost_success and account_attempts < max_account_attempts:
                        # Создаем клиент
                        telegram_client = await telegram_utils2.create_tg_client(working_account)
                        if not telegram_client:
                            group_logger.warning(f"⚠️ Не удалось создать клиент для +{working_account.phone_number}")
                            # Переключаемся на следующий аккаунт
                            await self.handle_account_error("Client creation failed")
                            working_account = await self.get_current_working_account()
                            account_attempts += 1
                            continue
                        
                        try:
                            # Вступаем в группу
                            join_success = await telegram_utils2.checking_and_joining_if_possible(
                                telegram_client=telegram_client,
                                url=group.url,
                                channel=channel
                            )
                            
                            if not join_success:
                                group_logger.warning(f"⚠️ Не удалось вступить в группу {group.url} с +{working_account.phone_number}")
                                # Проверяем - если это FROZEN_METHOD_INVALID, переключаемся
                                await self.handle_account_error("Join failed - possibly FROZEN_METHOD_INVALID")
                                working_account = await self.get_current_working_account()
                                account_attempts += 1
                            else:
                                # Делаем репост
                                repost_result = await telegram_utils2.repost_in_group_by_message_id(
                                    message_id=task.message_id,
                                    telegram_client=telegram_client,
                                    telegram_channel_id=channel.telegram_channel_id,
                                    channel_url=channel.url,
                                    group_url=group.url
                                )
                                
                                if repost_result:
                                    successful_reposts += 1
                                    await self.increment_account_reposts()
                                    repost_success = True
                                    
                                    group_logger.success(f"✅ Репост успешен с +{working_account.phone_number} (#{self.current_account_reposts})")
                                    
                                    # Записываем в БД
                                    try:
                                        from core.schemas import repost as repost_schemas
                                        from core.models import repost as repost_db
                                        await repost_db.create_repost(
                                            repost_in=repost_schemas.RepostCreate(
                                                channel_guid=channel.guid,
                                                repost_message_id=task.message_id,
                                                created_at=datetime.now().date()
                                            )
                                        )
                                    except Exception as db_error:
                                        group_logger.debug(f"Ошибка записи в БД: {db_error}")
                                else:
                                    group_logger.warning(f"❌ Репост не удался с +{working_account.phone_number}")
                                    account_attempts += 1
                                    
                        except Exception as group_error:
                            error_str = str(group_error)
                            group_logger.error(f"❌ Ошибка при работе с группой {group.url}: {group_error}")
                            
                            # Если FROZEN_METHOD_INVALID - переключаемся на другой аккаунт
                            if "FROZEN_METHOD_INVALID" in error_str:
                                await self.handle_account_error(error_str)
                                working_account = await self.get_current_working_account()
                            
                            account_attempts += 1
                            
                        finally:
                            # ОБЯЗАТЕЛЬНО закрываем клиент
                            try:
                                await telegram_client.disconnect()
                            except:
                                pass
                    
                    if not repost_success:
                        group_logger.warning(f"⚠️ Не удалось сделать репост в {group.url} после {account_attempts} попыток")
                    
                    # Пауза между группами
                    if i < len(selected_groups):
                        await asyncio.sleep(delay_between_groups)
                            
                except Exception as group_error:
                    group_logger.error(f"Критическая ошибка при обработке группы {group.url}: {group_error}")
                    await asyncio.sleep(delay_between_groups // 2)
            
            # Финальная статистика
            processing_time = (datetime.now() - start_time).total_seconds()
            success_rate = (successful_reposts / len(selected_groups) * 100) if selected_groups else 0
            
            task_logger.success(f"🎉 Обработка завершена: {successful_reposts}/{len(selected_groups)} ({success_rate:.1f}%) за {processing_time:.1f}с")
            
            if self.current_account:
                task_logger.info(f"📊 Общих репостов у аккаунта +{self.current_account.phone_number}: {self.current_account_reposts}")
                
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            task_logger.error(f"❌ Критическая ошибка: {e}")
            self.error_count += 1
    
    async def add_task(self, channel_id: int, message_id: int) -> bool:
        """Добавить задачу в очередь канала"""
        try:
            task = ChannelTask(
                channel_id=channel_id,
                message_id=message_id,
                timestamp=datetime.now()
            )
            
            self.task_queue.put_nowait(task)
            
            queue_size = self.task_queue.qsize()
            self.logger.info(f"➕ Сообщение {message_id} добавлено в очередь. Размер: {queue_size}")
            
            return True
            
        except asyncio.QueueFull:
            self.logger.error(f"🚫 Очередь канала {self.channel_url} переполнена!")
            return False
        except Exception as e:
            self.logger.error(f"❌ Ошибка при добавлении задачи: {e}")
            return False
    
    def stop(self):
        """Остановка воркера"""
        self.running = False
        self.logger.info(f"🛑 Воркер {self.worker_id} остановлен. Обработано: {self.processed_count}")
    
    def get_stats(self) -> dict:
        """Статистика воркера"""
        return {
            'worker_id': self.worker_id,
            'channel_guid': self.channel_guid,
            'channel_url': self.channel_url,
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'running': self.running,
            'queue_size': self.task_queue.qsize(),
            'cached_accounts': len(self.account_groups_cache),
            'total_cached_groups': sum(len(groups) for groups in self.account_groups_cache.values()),
            'current_task': {
                'channel_id': self.current_task.channel_id if self.current_task else None,
                'message_id': self.current_task.message_id if self.current_task else None,
                'started_at': self.current_task.timestamp if self.current_task else None
            } if self.current_task else None,
            'rotation': self.get_rotation_stats()
        }
    
    def get_rotation_stats(self) -> dict:
        """Статистика ротации аккаунтов"""
        return {
            'current_account_index': self.current_account_index,
            'current_account_reposts': self.current_account_reposts,
            'current_account_phone': self.current_account.phone_number if self.current_account else None,
            'available_accounts_count': len(self.available_accounts),
            'last_accounts_refresh': self.last_accounts_refresh
        }


class ChannelProcessor:
    """Менеджер воркеров каналов с исправленной ротацией"""
    
    def __init__(self):
        self.channel_workers: Dict[str, ChannelWorker] = {}
        self.worker_tasks: Dict[str, asyncio.Task] = {}
        self.running = False
        self.start_time = datetime.now()
        
        # Защита от дублей
        self.processing_messages: Dict[str, Set[tuple]] = {}
        
        logger.info("🏗️ Инициализирован процессор с исправленной ротацией аккаунтов")
    
    async def _channel_has_accounts(self, channel_guid: str) -> bool:
        """Проверяет, есть ли у канала рабочие аккаунты"""
        try:
            accounts = await tg_account_db.get_working_accounts_by_channel(channel_guid)
            return len(accounts) > 0
        except Exception as e:
            logger.error(f"Ошибка при проверке аккаунтов для канала {channel_guid}: {e}")
            return False
    
    async def start(self):
        """Запуск воркеров для каналов с аккаунтами"""
        self.running = True
        
        channels = await channel_db.get_channels()
        if not channels:
            logger.warning("⚠️ Нет каналов в базе данных!")
            return
        
        logger.info(f"🔄 Проверяю {len(channels)} каналов на наличие аккаунтов...")
        
        active_channels = 0
        for i, channel in enumerate(channels):
            channel_guid = str(channel.guid)
            
            if await self._channel_has_accounts(channel_guid):
                worker = ChannelWorker(
                    channel_guid=channel_guid,
                    channel_url=channel.url,
                    worker_id=i + 1
                )
                
                self.channel_workers[channel_guid] = worker
                self.processing_messages[channel_guid] = set()
                
                # Запускаем воркер
                task = asyncio.create_task(worker.start())
                self.worker_tasks[channel_guid] = task
                
                active_channels += 1
                logger.info(f"✅ Воркер создан для канала {channel.url}")
            else:
                logger.info(f"⏭️ Канал {channel.url} пропущен - нет аккаунтов")
        
        logger.success(f"✅ Запущено {active_channels} воркеров с исправленной ротацией")
    
    async def ensure_worker_for_channel(self, channel_guid: str) -> bool:
        """Создает воркер для канала если нужно"""
        if channel_guid in self.channel_workers:
            return True
            
        if not await self._channel_has_accounts(channel_guid):
            return False
        
        try:
            channel = await channel_db.get_channel_by_guid(channel_guid)
            if not channel:
                return False
            
            worker_id = len(self.channel_workers) + 1
            worker = ChannelWorker(
                channel_guid=channel_guid,
                channel_url=channel.url,
                worker_id=worker_id
            )
            
            self.channel_workers[channel_guid] = worker
            self.processing_messages[channel_guid] = set()
            
            task = asyncio.create_task(worker.start())
            self.worker_tasks[channel_guid] = task
            
            logger.success(f"🎉 Создан новый воркер для канала {channel.url}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка создания воркера для канала {channel_guid}: {e}")
            return False
    
    async def remove_worker_if_no_accounts(self, channel_guid: str) -> bool:
        """Удаляет воркер если у канала нет аккаунтов"""
        if channel_guid not in self.channel_workers:
            return True
            
        if await self._channel_has_accounts(channel_guid):
            return False
        
        try:
            worker = self.channel_workers[channel_guid]
            worker.stop()
            
            if channel_guid in self.worker_tasks:
                task = self.worker_tasks[channel_guid]
                if not task.done():
                    task.cancel()
                del self.worker_tasks[channel_guid]
            
            del self.channel_workers[channel_guid]
            if channel_guid in self.processing_messages:
                del self.processing_messages[channel_guid]
            
            logger.info(f"🗑️ Воркер канала {worker.channel_url} удален - нет аккаунтов")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка удаления воркера канала {channel_guid}: {e}")
            return False
    
    async def add_message(self, channel_id: int, message_id: int) -> bool:
        """Добавляет сообщение в очередь канала"""
        if not self.running:
            logger.error("❌ Процессор не запущен!")
            return False
        
        try:
            channel = await channel_db.get_channel_by_telegram_channel_id(channel_id)
            if not channel:
                logger.warning(f"⚠️ Канал с ID {channel_id} не найден в базе")
                return False
            
            channel_guid = str(channel.guid)
            
            if not await self.ensure_worker_for_channel(channel_guid):
                logger.warning(f"❌ Не удалось создать воркер для канала {channel.url}")
                return False
            
            if channel_guid not in self.channel_workers:
                logger.error(f"❌ Воркер для канала {channel.url} не найден!")
                return False
            
            # Защита от дублей
            message_key = (channel_id, message_id)
            if message_key in self.processing_messages[channel_guid]:
                logger.warning(f"🔄 Сообщение {message_id} канала {channel.url} уже в обработке")
                return False
            
            # Добавляем в очередь
            worker = self.channel_workers[channel_guid]
            success = await worker.add_task(channel_id, message_id)
            
            if success:
                self.processing_messages[channel_guid].add(message_key)
                
                # Планируем удаление из защиты от дублей
                asyncio.create_task(self._cleanup_processed_message(channel_guid, message_key, delay=3600))
                
                logger.info(f"✅ Сообщение {message_id} передано воркеру с ротацией канала {channel.url}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении сообщения: {e}")
            return False
    
    async def _cleanup_processed_message(self, channel_guid: str, message_key: tuple, delay: int):
        """Удаляет сообщение из защиты от дублей"""
        try:
            await asyncio.sleep(delay)
            if channel_guid in self.processing_messages:
                self.processing_messages[channel_guid].discard(message_key)
        except Exception as e:
            logger.error(f"Ошибка очистки дублей: {e}")
    
    def get_stats(self) -> dict:
        """Общая статистика процессора"""
        if not self.channel_workers:
            return {
                'running': self.running,
                'channels_count': 0,
                'total_processed': 0,
                'total_errors': 0,
                'workers': []
            }
        
        total_processed = sum(worker.processed_count for worker in self.channel_workers.values())
        total_errors = sum(worker.error_count for worker in self.channel_workers.values())
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        workers_stats = [worker.get_stats() for worker in self.channel_workers.values()]
        
        active_channels = sum(1 for w in self.channel_workers.values() if w.current_task is not None)
        total_queue_size = sum(w.task_queue.qsize() for w in self.channel_workers.values())
        total_cached_groups = sum(w.get_stats().get('total_cached_groups', 0) for w in self.channel_workers.values())
        
        # Статистика ротации
        total_account_reposts = sum(w.current_account_reposts for w in self.channel_workers.values())
        active_accounts = len([w for w in self.channel_workers.values() if w.current_account])
        
        return {
            'running': self.running,
            'uptime_seconds': uptime,
            'channels_count': len(self.channel_workers),
            'active_channels': active_channels,
            'total_queue_size': total_queue_size,
            'total_cached_groups': total_cached_groups,
            'total_processed': total_processed,
            'total_errors': total_errors,
            'success_rate': (total_processed / (total_processed + total_errors) * 100) if (total_processed + total_errors) > 0 else 0,
            'messages_per_hour': (total_processed / (uptime / 3600)) if uptime > 0 else 0,
            'rotation_stats': {
                'total_account_reposts': total_account_reposts,
                'active_accounts': active_accounts
            },
            'workers': workers_stats
        }
    
    async def stop(self):
        """Остановка всех воркеров"""
        if not self.running:
            return
            
        logger.info("🛑 Останавливаю все воркеры...")
        
        # Останавливаем воркеры
        for worker in self.channel_workers.values():
            worker.stop()
        
        # Ждем очистки очередей
        for channel_guid, worker in self.channel_workers.items():
            try:
                await asyncio.wait_for(worker.task_queue.join(), timeout=60.0)
                logger.debug(f"✅ Очередь канала {worker.channel_url} очищена")
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ Таймаут очистки очереди канала {worker.channel_url}")
        
        # Отменяем задачи
        for task in self.worker_tasks.values():
            if not task.done():
                task.cancel()
        
        if self.worker_tasks:
            try:
                await asyncio.wait(self.worker_tasks.values(), timeout=15.0)
            except asyncio.TimeoutError:
                logger.warning("⚠️ Некоторые воркеры не завершились вовремя")
        
        self.running = False
        
        # Финальная статистика
        stats = self.get_stats()
        logger.success(f"✅ Процессор остановлен. Каналов: {stats['channels_count']}, обработано: {stats['total_processed']}")
        
        # Очистка
        self.channel_workers.clear()
        self.worker_tasks.clear()
        self.processing_messages.clear()


# Глобальный экземпляр процессора
channel_processor = ChannelProcessor()
