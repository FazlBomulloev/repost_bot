import asyncio
from datetime import datetime
from typing import Dict, Set, Optional
from dataclasses import dataclass
from loguru import logger

from core.models import channel as channel_db, tg_account as tg_account_db


@dataclass
class ChannelTask:
    channel_id: int
    message_id: int
    timestamp: datetime


class ChannelWorker:
    """Воркер, закрепленный за конкретным каналом"""
    
    def __init__(self, channel_guid: str, channel_url: str, worker_id: int):
        self.channel_guid = channel_guid
        self.channel_url = channel_url
        self.worker_id = worker_id
        self.task_queue = asyncio.Queue(maxsize=100)  # Увеличенная очередь
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        self.current_task = None
        self.logger = logger.bind(worker_id=worker_id, channel=channel_url)
        
    async def start(self):
        """Запуск воркера для конкретного канала"""
        self.running = True
        self.logger.info(f"🚀 Воркер {self.worker_id} запущен для канала {self.channel_url}")
        
        # Создаем несколько параллельных обработчиков для ускорения
        workers = []
        num_parallel_workers = 3  # Количество параллельных обработчиков
        
        for i in range(num_parallel_workers):
            worker_task = asyncio.create_task(self._worker_loop(i + 1))
            workers.append(worker_task)
        
        # Ждем завершения всех воркеров
        try:
            await asyncio.gather(*workers)
        except Exception as e:
            self.logger.error(f"Ошибка в воркерах канала: {e}")
        finally:
            # Отменяем все незавершенные задачи
            for task in workers:
                if not task.done():
                    task.cancel()
    
    async def _worker_loop(self, sub_worker_id: int):
        """Цикл обработки для под-воркера"""
        sub_logger = self.logger.bind(sub_worker=sub_worker_id)
        sub_logger.info(f"🔥 Под-воркер {sub_worker_id} запущен")
        
        while self.running:
            try:
                # Получаем задачу с таймаутом
                task = await asyncio.wait_for(
                    self.task_queue.get(), 
                    timeout=10.0
                )
                
                self.current_task = task
                await self._process_channel_task(task, sub_logger)
                self.task_queue.task_done()
                self.processed_count += 1
                self.current_task = None
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                sub_logger.error(f"Критическая ошибка в под-воркере: {e}")
                self.error_count += 1
                self.current_task = None
                await asyncio.sleep(1)
    
    async def _process_channel_task(self, task: ChannelTask, sub_logger):
        """Обработка задачи конкретного канала"""
        start_time = datetime.now()
        sub_logger.info(f"📨 Начинаю обработку сообщения {task.message_id}")
        
        try:
            # Импортируем функцию из process_post3.py
            from process_post3 import new_message_in_channel
            
            # Вызываем существующую логику для конкретного канала
            await new_message_in_channel(
                telegram_channel_id=task.channel_id,
                telegram_message_id=task.message_id
            )
            
            processing_time = (datetime.now() - start_time).total_seconds()
            sub_logger.success(f"✅ Сообщение {task.message_id} обработано за {processing_time:.1f} сек")
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            sub_logger.error(f"❌ Ошибка при обработке {task.message_id} за {processing_time:.1f} сек: {e}")
            self.error_count += 1
            raise
    
    async def add_task(self, channel_id: int, message_id: int) -> bool:
        """Добавить задачу в очередь канала"""
        try:
            task = ChannelTask(
                channel_id=channel_id,
                message_id=message_id,
                timestamp=datetime.now()
            )
            
            self.task_queue.put_nowait(task)
            self.logger.info(f"➕ Сообщение {message_id} добавлено в очередь канала. Размер: {self.task_queue.qsize()}")
            return True
            
        except asyncio.QueueFull:
            self.logger.error(f"🚫 Очередь канала {self.channel_url} переполнена!")
            return False
        except Exception as e:
            self.logger.error(f"❌ Ошибка при добавлении задачи канала: {e}")
            return False
    
    def stop(self):
        """Остановка воркера"""
        self.running = False
        self.logger.info(f"🛑 Воркер {self.worker_id} канала {self.channel_url} остановлен. Обработано: {self.processed_count}")
    
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
            'current_task': {
                'channel_id': self.current_task.channel_id if self.current_task else None,
                'message_id': self.current_task.message_id if self.current_task else None,
                'started_at': self.current_task.timestamp if self.current_task else None
            } if self.current_task else None
        }


class ChannelProcessor:
    """Менеджер воркеров с выделенными воркерами для каждого канала"""
    
    def __init__(self):
        self.channel_workers: Dict[str, ChannelWorker] = {}  # channel_guid -> worker
        self.worker_tasks: Dict[str, asyncio.Task] = {}
        self.running = False
        self.start_time = datetime.now()
        
        # Защита от дублей по каналам
        self.processing_messages: Dict[str, Set[tuple]] = {}  # channel_guid -> set of (channel_id, message_id)
        
        logger.info("🏗️ Инициализирован процессор с выделенными воркерами для каналов")
    
    async def _channel_has_accounts(self, channel_guid: str) -> bool:
        """Проверяет, есть ли у канала аккаунты"""
        try:
            accounts = await tg_account_db.get_working_accounts_by_channel(channel_guid)
            return len(accounts) > 0
        except Exception as e:
            logger.error(f"Ошибка при проверке аккаунтов для канала {channel_guid}: {e}")
            return False
    
    async def start(self):
        """Запуск воркеров только для каналов с аккаунтами"""
        self.running = True
        
        # Получаем все каналы из базы
        channels = await channel_db.get_channels()
        
        if not channels:
            logger.warning("⚠️ Нет каналов в базе данных!")
            return
        
        logger.info(f"🔄 Проверяю {len(channels)} каналов на наличие аккаунтов...")
        
        # Создаем воркеры только для каналов с аккаунтами
        active_channels = 0
        for i, channel in enumerate(channels):
            channel_guid = str(channel.guid)
            
            # Проверяем, есть ли аккаунты у канала
            if await self._channel_has_accounts(channel_guid):
                worker = ChannelWorker(
                    channel_guid=channel_guid,
                    channel_url=channel.url,
                    worker_id=i + 1
                )
                
                self.channel_workers[channel_guid] = worker
                self.processing_messages[channel_guid] = set()
                
                # Запускаем воркер в отдельной задаче
                task = asyncio.create_task(worker.start())
                self.worker_tasks[channel_guid] = task
                
                active_channels += 1
                logger.info(f"✅ Воркер создан для канала {channel.url}")
            else:
                logger.info(f"⏭️ Канал {channel.url} пропущен - нет аккаунтов")
        
        logger.success(f"✅ Запущено {active_channels} воркеров для каналов с аккаунтами")
    
    async def ensure_worker_for_channel(self, channel_guid: str) -> bool:
        """Создает воркер для канала если его нет и есть аккаунты"""
        if channel_guid in self.channel_workers:
            return True  # Воркер уже существует
            
        # Проверяем, есть ли аккаунты
        if not await self._channel_has_accounts(channel_guid):
            logger.info(f"Не создаю воркер для канала {channel_guid} - нет аккаунтов")
            return False
        
        try:
            # Получаем информацию о канале
            channel = await channel_db.get_channel_by_guid(channel_guid)
            if not channel:
                logger.error(f"Канал {channel_guid} не найден в БД")
                return False
            
            # Создаем воркер
            worker_id = len(self.channel_workers) + 1
            worker = ChannelWorker(
                channel_guid=channel_guid,
                channel_url=channel.url,
                worker_id=worker_id
            )
            
            self.channel_workers[channel_guid] = worker
            self.processing_messages[channel_guid] = set()
            
            # Запускаем воркер
            task = asyncio.create_task(worker.start())
            self.worker_tasks[channel_guid] = task
            
            logger.success(f"🎉 Создан новый воркер для канала {channel.url}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при создании воркера для канала {channel_guid}: {e}")
            return False
    
    async def remove_worker_if_no_accounts(self, channel_guid: str) -> bool:
        """Удаляет воркер канала если у него нет аккаунтов"""
        if channel_guid not in self.channel_workers:
            return True  # Воркера и так нет
            
        # Проверяем, есть ли аккаунты
        if await self._channel_has_accounts(channel_guid):
            return False  # Есть аккаунты, воркер нужен
        
        try:
            # Останавливаем воркер
            worker = self.channel_workers[channel_guid]
            worker.stop()
            
            # Отменяем задачу
            if channel_guid in self.worker_tasks:
                task = self.worker_tasks[channel_guid]
                if not task.done():
                    task.cancel()
                del self.worker_tasks[channel_guid]
            
            # Удаляем из словарей
            del self.channel_workers[channel_guid]
            if channel_guid in self.processing_messages:
                del self.processing_messages[channel_guid]
            
            logger.info(f"🗑️ Воркер канала {worker.channel_url} удален - нет аккаунтов")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при удалении воркера канала {channel_guid}: {e}")
            return False
    
    async def add_message(self, channel_id: int, message_id: int) -> bool:
        """
        Добавляет сообщение в очередь соответствующего канала
        """
        if not self.running:
            logger.error("❌ Процессор не запущен!")
            return False
        
        # Найти канал по telegram_channel_id
        try:
            channel = await channel_db.get_channel_by_telegram_channel_id(channel_id)
            if not channel:
                logger.warning(f"⚠️ Канал с ID {channel_id} не найден в базе")
                return False
            
            channel_guid = str(channel.guid)
            
            # Убеждаемся что воркер существует
            if not await self.ensure_worker_for_channel(channel_guid):
                logger.warning(f"❌ Не удалось создать воркер для канала {channel.url}")
                return False
            
            # Найти соответствующий воркер
            if channel_guid not in self.channel_workers:
                logger.error(f"❌ Воркер для канала {channel.url} не найден!")
                return False
            
            # Защита от дублей для конкретного канала
            message_key = (channel_id, message_id)
            if message_key in self.processing_messages[channel_guid]:
                logger.warning(f"🔄 Сообщение {message_id} канала {channel.url} уже в обработке")
                return False
            
            # Добавляем в очередь конкретного воркера
            worker = self.channel_workers[channel_guid]
            success = await worker.add_task(channel_id, message_id)
            
            if success:
                self.processing_messages[channel_guid].add(message_key)
                
                # Планируем удаление из защиты от дублей
                asyncio.create_task(self._cleanup_processed_message(channel_guid, message_key, delay=3600))
                
                logger.info(f"✅ Сообщение {message_id} передано воркеру канала {channel.url}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении сообщения: {e}")
            return False
    
    async def _cleanup_processed_message(self, channel_guid: str, message_key: tuple, delay: int):
        """Удаляет сообщение из защиты от дублей через delay секунд"""
        try:
            await asyncio.sleep(delay)
            if channel_guid in self.processing_messages:
                self.processing_messages[channel_guid].discard(message_key)
                logger.debug(f"🧹 Очищена защита от дублей для канала {channel_guid}: {message_key}")
        except Exception as e:
            logger.error(f"Ошибка при очистке дублей: {e}")
    
    def get_stats(self) -> dict:
        """Получение детальной статистики"""
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
        
        # Статистика по воркерам
        workers_stats = [worker.get_stats() for worker in self.channel_workers.values()]
        
        # Статистика по активности каналов
        active_channels = sum(1 for w in self.channel_workers.values() if w.current_task is not None)
        total_queue_size = sum(w.task_queue.qsize() for w in self.channel_workers.values())
        
        return {
            'running': self.running,
            'uptime_seconds': uptime,
            'channels_count': len(self.channel_workers),
            'active_channels': active_channels,
            'total_queue_size': total_queue_size,
            'total_processed': total_processed,
            'total_errors': total_errors,
            'success_rate': (total_processed / (total_processed + total_errors) * 100) if (total_processed + total_errors) > 0 else 0,
            'messages_per_hour': (total_processed / (uptime / 3600)) if uptime > 0 else 0,
            'workers': workers_stats
        }
    
    def get_channel_stats(self, channel_guid: str) -> Optional[dict]:
        """Статистика конкретного канала"""
        if channel_guid in self.channel_workers:
            return self.channel_workers[channel_guid].get_stats()
        return None
    
    async def stop(self):
        """Остановка всех воркеров каналов"""
        if not self.running:
            return
            
        logger.info("🛑 Останавливаю все воркеры каналов...")
        
        # Останавливаем все воркеры
        for worker in self.channel_workers.values():
            worker.stop()
        
        # Ждем завершения текущих задач
        for channel_guid, worker in self.channel_workers.items():
            try:
                await asyncio.wait_for(worker.task_queue.join(), timeout=30.0)
                logger.debug(f"✅ Очередь канала {worker.channel_url} очищена")
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ Таймаут при ожидании завершения задач канала {worker.channel_url}")
        
        # Отменяем задачи воркеров
        for task in self.worker_tasks.values():
            if not task.done():
                task.cancel()
        
        # Ждем отмены всех задач
        if self.worker_tasks:
            try:
                await asyncio.wait(self.worker_tasks.values(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("⚠️ Некоторые воркеры каналов не завершились вовремя")
        
        self.running = False
        
        # Финальная статистика
        stats = self.get_stats()
        logger.success(f"✅ Процессор каналов остановлен. Каналов: {stats['channels_count']}, обработано: {stats['total_processed']}")
        
        # Очищаем ресурсы
        self.channel_workers.clear()
        self.worker_tasks.clear()
        self.processing_messages.clear()


# Глобальный экземпляр процессора
channel_processor = ChannelProcessor()