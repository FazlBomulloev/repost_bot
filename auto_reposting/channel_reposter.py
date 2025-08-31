import asyncio
from datetime import datetime
from typing import Dict, Set, Optional
from dataclasses import dataclass
from loguru import logger

from core.models import channel as channel_db


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
        self.task_queue = asyncio.Queue(maxsize=50)  # Небольшая очередь на канал
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        self.current_task = None
        self.logger = logger.bind(worker_id=worker_id, channel=channel_url)
        
    async def start(self):
        """Запуск воркера для конкретного канала"""
        self.running = True
        self.logger.info(f"🚀 Воркер {self.worker_id} запущен для канала {self.channel_url}")
        
        while self.running:
            try:
                # Получаем задачу с таймаутом
                task = await asyncio.wait_for(
                    self.task_queue.get(), 
                    timeout=10.0  # Больший таймаут для каналов
                )
                
                self.current_task = task
                await self._process_channel_task(task)
                self.task_queue.task_done()
                self.processed_count += 1
                self.current_task = None
                
            except asyncio.TimeoutError:
                # Нормально, просто ждем новые задачи для этого канала
                continue
            except Exception as e:
                self.logger.error(f"Критическая ошибка в воркере канала: {e}")
                self.error_count += 1
                self.current_task = None
                await asyncio.sleep(1)
    
    async def _process_channel_task(self, task: ChannelTask):
        """Обработка задачи конкретного канала"""
        start_time = datetime.now()
        self.logger.info(f"📨 Начинаю обработку сообщения {task.message_id} канала {self.channel_url}")
        
        try:
            # Импортируем функцию из process_post3.py
            from process_post3 import new_message_in_channel
            
            # Вызываем существующую логику для конкретного канала
            await new_message_in_channel(
                telegram_channel_id=task.channel_id,
                telegram_message_id=task.message_id
            )
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.success(f"✅ Сообщение {task.message_id} канала {self.channel_url} обработано за {processing_time:.1f} сек")
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"❌ Ошибка при обработке {task.message_id} канала {self.channel_url} за {processing_time:.1f} сек: {e}")
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


class ChannelDedicatedProcessor:
    """Менеджер воркеров с выделенными воркерами для каждого канала"""
    
    def __init__(self):
        self.channel_workers: Dict[str, ChannelWorker] = {}  # channel_guid -> worker
        self.worker_tasks: Dict[str, asyncio.Task] = {}
        self.running = False
        self.start_time = datetime.now()
        
        # Защита от дублей по каналам
        self.processing_messages: Dict[str, Set[tuple]] = {}  # channel_guid -> set of (channel_id, message_id)
        
        logger.info("🏗️ Инициализирован процессор с выделенными воркерами для каналов")
    
    async def start(self):
        """Запуск всех воркеров для каждого канала"""
        self.running = True
        
        # Получаем все каналы из базы
        channels = await channel_db.get_channels()
        
        if not channels:
            logger.warning("⚠️ Нет каналов в базе данных!")
            return
        
        logger.info(f"🔄 Создание воркеров для {len(channels)} каналов...")
        
        # Создаем воркер для каждого канала
        for i, channel in enumerate(channels):
            worker = ChannelWorker(
                channel_guid=str(channel.guid),
                channel_url=channel.url,
                worker_id=i + 1
            )
            
            self.channel_workers[str(channel.guid)] = worker
            self.processing_messages[str(channel.guid)] = set()
            
            # Запускаем воркер в отдельной задаче
            task = asyncio.create_task(worker.start())
            self.worker_tasks[str(channel.guid)] = task
        
        logger.success(f"✅ Все {len(channels)} воркеров каналов запущены")
    
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
channel_processor = ChannelDedicatedProcessor()