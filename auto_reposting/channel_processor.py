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
    """Воркер для последовательной обработки с проверкой норм и кэшированием групп"""
    
    def __init__(self, channel_guid: str, channel_url: str, worker_id: int):
        self.channel_guid = channel_guid
        self.channel_url = channel_url
        self.worker_id = worker_id
        self.task_queue = asyncio.Queue()
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        self.current_task = None
        
        self.account_groups_cache: Dict[int, Set[str]] = {}  
        
        # Под-воркеры для параллельной обработки
        self.sub_workers = {}
        self.active_sub_workers = 0
        self.max_sub_workers = 3
        
        self.logger = logger.bind(worker_id=worker_id, channel=channel_url)
        
    async def start(self):
        """Запуск основного воркера"""
        self.running = True
        self.logger.info(f"🚀 Последовательный воркер {self.worker_id} запущен для канала {self.channel_url}")
        
        while self.running:
            try:
                task = await asyncio.wait_for(
                    self.task_queue.get(), 
                    timeout=10.0
                )
                
                available_accounts = await tg_account_db.get_working_accounts_by_channel(self.channel_guid)
                if not available_accounts:
                    self.logger.warning("⚠️ Нет доступных аккаунтов для обработки задачи")
                    self.task_queue.task_done()
                    continue
                
                # Если есть активные под-воркеры и свободные аккаунты - запускаем новый под-воркер
                if self.active_sub_workers > 0 and len(available_accounts) > self.active_sub_workers + 1:
                    if self.active_sub_workers < self.max_sub_workers:
                        await self._spawn_sub_worker(task)
                        self.task_queue.task_done()
                        continue
                
                # Обрабатываем задачу в основном потоке
                self.current_task = task
                await self._process_channel_task_with_norms(task)
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
    
    async def _spawn_sub_worker(self, task: ChannelTask):
        """Запускает под-воркер для обработки задачи"""
        if self.active_sub_workers >= self.max_sub_workers:
            return False
            
        sub_worker_id = len(self.sub_workers) + 1
        sub_logger = self.logger.bind(sub_worker=sub_worker_id)
        
        sub_logger.info(f"🔥 Запускаю под-воркер {sub_worker_id} для сообщения {task.message_id}")
        
        sub_task = asyncio.create_task(
            self._sub_worker_process(task, sub_worker_id, sub_logger)
        )
        
        self.sub_workers[f"sub_{sub_worker_id}"] = sub_task
        self.active_sub_workers += 1
        
        asyncio.create_task(self._cleanup_sub_worker(f"sub_{sub_worker_id}", sub_task))
        return True
    
    async def _cleanup_sub_worker(self, sub_worker_key: str, sub_task: asyncio.Task):
        """Очищает завершенный под-воркер"""
        try:
            await sub_task
        except Exception as e:
            self.logger.error(f"Ошибка в под-воркере {sub_worker_key}: {e}")
        finally:
            if sub_worker_key in self.sub_workers:
                del self.sub_workers[sub_worker_key]
                self.active_sub_workers -= 1
                self.logger.debug(f"🧹 Под-воркер {sub_worker_key} завершен")
    
    async def _sub_worker_process(self, task: ChannelTask, sub_worker_id: int, sub_logger):
        """Процесс под-воркера"""
        try:
            sub_logger.info(f"🎯 Под-воркер {sub_worker_id} обрабатывает сообщение {task.message_id}")
            await self._process_channel_task_with_norms(task, sub_logger)
            self.processed_count += 1
            sub_logger.success(f"✅ Под-воркер {sub_worker_id} завершил обработку {task.message_id}")
        except Exception as e:
            self.error_count += 1
            sub_logger.error(f"❌ Ошибка в под-воркере {sub_worker_id}: {e}")
    
    async def _process_channel_task_with_norms(self, task: ChannelTask, custom_logger=None):
        """Обработка с учетом норм репостов и ротацией аккаунтов"""
        task_logger = custom_logger or self.logger
        start_time = datetime.now()
        
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
            except:
                max_groups = 15  # Уменьшили с 25 до 15
            
            if len(all_groups) > max_groups:
                selected_groups = random.sample(all_groups, max_groups)
                task_logger.info(f"📊 Выбрано {max_groups} из {len(all_groups)} групп")
            else:
                selected_groups = all_groups
            
            # Получаем настройки
            try:
                number_reposts_before_pause = await json_settings.async_get_attribute("number_reposts_before_pause")
                pause_after_rate_reposts = await json_settings.async_get_attribute("pause_after_rate_reposts")
                delay_between_groups = await json_settings.async_get_attribute("delay_between_groups")
                check_stop_links = await json_settings.async_get_attribute("check_stop_links")
            except:
                number_reposts_before_pause = 15
                pause_after_rate_reposts = 3600
                delay_between_groups = 15  # Увеличили с 5 до 15 секунд
                check_stop_links = True
            
            task_logger.info(f"📨 Обработка {len(selected_groups)} групп с нормой {number_reposts_before_pause} репостов")
            task_logger.info(f"⏱️ Задержка между группами: {delay_between_groups}с")
            
            # 🎯 ОСНОВНОЙ ЦИКЛ С РОТАЦИЕЙ АККАУНТОВ
            successful_reposts = 0
            current_account = None
            telegram_client = None
            counter_reposts_current_account = 0
            
            # Получаем рабочие аккаунты
            working_accounts = await tg_account_db.get_working_accounts_by_channel(self.channel_guid)
            if not working_accounts:
                task_logger.error("Нет рабочих аккаунтов")
                return
            
            account_index = 0
            processed_groups = 0
            
            # Проверяем стоп-ссылки один раз в начале
            if check_stop_links and working_accounts:
                temp_client = await telegram_utils2.create_tg_client(working_accounts[0])
                if temp_client:
                    try:
                        stop_links_found = await self._check_stop_links_in_message(
                            temp_client, channel, task.message_id, working_accounts[:3], task_logger
                        )
                        if stop_links_found:
                            task_logger.info("🛑 Найдены стоп-ссылки, обработка завершена")
                            return
                    finally:
                        await temp_client.disconnect()
            
            # Основной цикл обработки групп
            for i, group in enumerate(selected_groups, 1):
                try:
                    # 🔄 ПРОВЕРЯЕМ НУЖНА ЛИ СМЕНА АККАУНТА
                    if (current_account is None or 
                        counter_reposts_current_account >= number_reposts_before_pause):
                        
                        if current_account:
                            # Ставим предыдущий аккаунт на паузу
                            task_logger.info(f"⏸️ Аккаунт +{current_account.phone_number} достиг нормы ({counter_reposts_current_account}), ставлю на паузу")
                            await tg_account_db.add_pause(current_account, pause_after_rate_reposts)
                            
                            # Закрываем клиент
                            if telegram_client:
                                await telegram_client.disconnect()
                                telegram_client = None
                        
                        # Ищем следующий доступный аккаунт
                        next_account = None
                        attempts = 0
                        while attempts < len(working_accounts) and not next_account:
                            if account_index >= len(working_accounts):
                                account_index = 0
                            
                            candidate = working_accounts[account_index]
                            
                            # Проверяем что аккаунт не на паузе
                            if candidate.last_datetime_pause and candidate.pause_in_seconds:
                                if not await tg_account_db.has_pause_paused(candidate):
                                    account_index += 1
                                    attempts += 1
                                    continue
                            
                            next_account = candidate
                            account_index += 1
                            break
                        
                        if not next_account:
                            task_logger.warning("❌ Все аккаунты на паузе или недоступны")
                            break
                        
                        # Создаем клиент для нового аккаунта
                        telegram_client = await telegram_utils2.create_tg_client(next_account)
                        if not telegram_client:
                            task_logger.error(f"❌ Не удалось создать клиент для +{next_account.phone_number}")
                            continue
                        
                        current_account = next_account
                        counter_reposts_current_account = 0
                        task_logger.info(f"🔄 Переключился на аккаунт: +{current_account.phone_number}")
                    
                    # Проверяем кэш групп для текущего аккаунта
                    phone_number = current_account.phone_number
                    if phone_number not in self.account_groups_cache:
                        self.account_groups_cache[phone_number] = set()
                    
                    group_logger = task_logger.bind(group_idx=i, total_groups=len(selected_groups))
                    group_logger.info(f"🎯 Обрабатываю группу {i}/{len(selected_groups)}: {group.url}")
                    
                    # 🚀 ПРОВЕРЯЕМ СОСТОИТ ЛИ УЖЕ В ГРУППЕ
                    already_joined = group.url in self.account_groups_cache[phone_number]
                    if already_joined:
                        group_logger.info(f"✅ Аккаунт уже в группе {group.url}, пропускаю вступление")
                    else:
                        # Пытаемся вступить в группу
                        join_success = await telegram_utils2.checking_and_joining_if_possible(
                            telegram_client=telegram_client,
                            url=group.url,
                            channel=channel
                        )
                        
                        if not join_success:
                            group_logger.warning(f"⚠️ Не удалось вступить в группу {group.url}")
                            await asyncio.sleep(delay_between_groups)
                            continue
                        
                        # Добавляем в кэш успешно вступленную группу
                        self.account_groups_cache[phone_number].add(group.url)
                        group_logger.info(f"➕ Добавил группу {group.url} в кэш аккаунта")
                    
                    # Делаем репост
                    repost_success = await telegram_utils2.repost_in_group_by_message_id(
                        message_id=task.message_id,
                        telegram_client=telegram_client,
                        telegram_channel_id=channel.telegram_channel_id,
                        channel_url=channel.url,
                        group_url=group.url
                    )
                    
                    if repost_success:
                        successful_reposts += 1
                        counter_reposts_current_account += 1  # 🎯 УВЕЛИЧИВАЕМ СЧЕТЧИК ПОСЛЕ КАЖДОЙ ГРУППЫ
                        group_logger.success(f"✅ Репост в {group.url} (репост #{counter_reposts_current_account})")
                        
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
                        group_logger.warning(f"❌ Репост в {group.url} не удался")
                    
                    processed_groups += 1
                    
                    # Пауза между группами (не после последней)
                    if i < len(selected_groups):
                        group_logger.debug(f"⏱️ Пауза {delay_between_groups}с до следующей группы")
                        await asyncio.sleep(delay_between_groups)
                        
                except Exception as group_error:
                    group_logger.error(f"Ошибка при обработке группы {group.url}: {group_error}")
                    await asyncio.sleep(delay_between_groups)
            
            # Закрываем клиент
            if telegram_client:
                try:
                    await telegram_client.disconnect()
                    task_logger.debug("🔌 Клиент отключен")
                except:
                    pass
            
            # Финальная статистика
            processing_time = (datetime.now() - start_time).total_seconds()
            success_rate = (successful_reposts / len(selected_groups) * 100) if selected_groups else 0
            
            task_logger.success(f"🎉 Задача завершена: {successful_reposts}/{len(selected_groups)} ({success_rate:.1f}%) за {processing_time:.1f}с")
            
            if current_account:
                task_logger.info(f"📊 Итого репостов у аккаунта +{current_account.phone_number}: {counter_reposts_current_account}")
                
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            task_logger.error(f"❌ Критическая ошибка при обработке за {processing_time:.1f}с: {e}")
            self.error_count += 1
    
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
            
            if queue_size > 1:
                self.logger.info(f"📋 В очереди {queue_size} задач, активно под-воркеров: {self.active_sub_workers}")
            
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
        
        # Отменяем все под-воркеры
        for sub_worker_key, sub_task in self.sub_workers.items():
            if not sub_task.done():
                sub_task.cancel()
                
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
            'active_sub_workers': self.active_sub_workers,
            'max_sub_workers': self.max_sub_workers,
            'cached_accounts': len(self.account_groups_cache),
            'total_cached_groups': sum(len(groups) for groups in self.account_groups_cache.values()),
            'current_task': {
                'channel_id': self.current_task.channel_id if self.current_task else None,
                'message_id': self.current_task.message_id if self.current_task else None,
                'started_at': self.current_task.timestamp if self.current_task else None
            } if self.current_task else None
        }


class ChannelProcessor:
    """Менеджер последовательных воркеров каналов"""
    
    def __init__(self):
        self.channel_workers: Dict[str, ChannelWorker] = {}
        self.worker_tasks: Dict[str, asyncio.Task] = {}
        self.running = False
        self.start_time = datetime.now()
        
        # Защита от дублей
        self.processing_messages: Dict[str, Set[tuple]] = {}
        
        logger.info("🏗️ Инициализирован процессор с нормами и кэшированием групп")
    
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
                logger.info(f"✅ Воркер с нормами создан для канала {channel.url}")
            else:
                logger.info(f"⏭️ Канал {channel.url} пропущен - нет аккаунтов")
        
        logger.success(f"✅ Запущено {active_channels} воркеров с системой норм и кэшированием")
    
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
            
            logger.success(f"🎉 Создан новый воркер с нормами для канала {channel.url}")
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
                
                logger.info(f"✅ Сообщение {message_id} передано воркеру с нормами канала {channel.url}")
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
        total_sub_workers = sum(w.active_sub_workers for w in self.channel_workers.values())
        total_cached_groups = sum(w.get_stats().get('total_cached_groups', 0) for w in self.channel_workers.values())
        
        return {
            'running': self.running,
            'uptime_seconds': uptime,
            'channels_count': len(self.channel_workers),
            'active_channels': active_channels,
            'total_queue_size': total_queue_size,
            'total_sub_workers': total_sub_workers,
            'total_cached_groups': total_cached_groups,
            'total_processed': total_processed,
            'total_errors': total_errors,
            'success_rate': (total_processed / (total_processed + total_errors) * 100) if (total_processed + total_errors) > 0 else 0,
            'messages_per_hour': (total_processed / (uptime / 3600)) if uptime > 0 else 0,
            'workers': workers_stats
        }
    
    async def stop(self):
        """Остановка всех воркеров"""
        if not self.running:
            return
            
        logger.info("🛑 Останавливаю все воркеры с нормами...")
        
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
        logger.success(f"✅ Процессор с нормами остановлен. Каналов: {stats['channels_count']}, обработано: {stats['total_processed']}")
        
        # Очистка
        self.channel_workers.clear()
        self.worker_tasks.clear()
        self.processing_messages.clear()


# Глобальный экземпляр
channel_processor = ChannelProcessor()