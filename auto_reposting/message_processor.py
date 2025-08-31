import asyncio
from datetime import datetime
from typing import Dict, Set
from dataclasses import dataclass
from loguru import logger


@dataclass
class MessageTask:
    channel_id: int
    message_id: int
    timestamp: datetime


class MessageWorker:
    """Воркер для обработки сообщений"""
    
    def __init__(self, worker_id: int, task_queue: asyncio.Queue):
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        self.current_task = None
        self.logger = logger.bind(worker_id=worker_id)
        
    async def start(self):
        """Запуск воркера"""
        self.running = True
        self.logger.info(f"🚀 Воркер {self.worker_id} запущен")
        
        while self.running:
            try:
                # Получаем задачу с таймаутом
                task = await asyncio.wait_for(
                    self.task_queue.get(), 
                    timeout=5.0
                )
                
                self.current_task = task
                await self._process_task(task)
                self.task_queue.task_done()
                self.processed_count += 1
                self.current_task = None
                
            except asyncio.TimeoutError:
                # Нормально, просто ждем новые задачи
                continue
            except Exception as e:
                self.logger.error(f"Критическая ошибка в воркере: {e}")
                self.error_count += 1
                self.current_task = None
                await asyncio.sleep(1)
    
    async def _process_task(self, task: MessageTask):
        """Обработка одной задачи"""
        start_time = datetime.now()
        self.logger.info(f"📨 Начинаю обработку сообщения {task.message_id} из канала {task.channel_id}")
        
        try:
            # Импортируем функцию из process_post3.py
            from process_post3 import new_message_in_channel
            
            # Вызываем существующую логику
            await new_message_in_channel(
                telegram_channel_id=task.channel_id,
                telegram_message_id=task.message_id
            )
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.success(f"✅ Сообщение {task.message_id} обработано за {processing_time:.1f} сек")
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"❌ Ошибка при обработке {task.message_id} за {processing_time:.1f} сек: {e}")
            self.error_count += 1
            raise  # Передаем ошибку наверх для статистики
    
    def stop(self):
        """Остановка воркера"""
        self.running = False
        self.logger.info(f"🛑 Воркер {self.worker_id} остановлен. Обработано: {self.processed_count}, ошибок: {self.error_count}")
    
    def get_stats(self) -> dict:
        """Статистика воркера"""
        return {
            'worker_id': self.worker_id,
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'running': self.running,
            'current_task': {
                'channel_id': self.current_task.channel_id if self.current_task else None,
                'message_id': self.current_task.message_id if self.current_task else None,
                'started_at': self.current_task.timestamp if self.current_task else None
            } if self.current_task else None
        }


class MessageProcessor:
    """Менеджер воркеров для обработки сообщений"""
    
    def __init__(self, num_workers: int = 3, max_queue_size: int = 100):
        self.num_workers = num_workers
        self.task_queue = asyncio.Queue(maxsize=max_queue_size)
        self.workers: Dict[int, MessageWorker] = {}
        self.worker_tasks: Dict[int, asyncio.Task] = {}
        self.running = False
        
        # Защита от дублей
        self.processing_messages: Set[tuple] = set()
        self.start_time = datetime.now()
        
        logger.info(f"🏗️ Инициализирован процессор с {num_workers} воркерами, макс. очередь: {max_queue_size}")
    
    async def start(self):
        """Запуск всех воркеров"""
        self.running = True
        
        # Создаем и запускаем воркеров
        for i in range(self.num_workers):
            worker = MessageWorker(i + 1, self.task_queue)
            self.workers[i] = worker
            
            # Запускаем каждого воркера в отдельной задаче
            task = asyncio.create_task(worker.start())
            self.worker_tasks[i] = task
        
        logger.success(f"✅ Все {self.num_workers} воркеров запущены")
    
    async def add_message(self, channel_id: int, message_id: int) -> bool:
        """
        Добавляет сообщение в очередь на обработку
        Возвращает True если добавлено, False если дубль или ошибка
        """
        if not self.running:
            logger.error("❌ Процессор не запущен!")
            return False
            
        message_key = (channel_id, message_id)
        
        # Защита от дублей
        if message_key in self.processing_messages:
            logger.warning(f"🔄 Сообщение {message_id} из канала {channel_id} уже в обработке")
            return False
        
        try:
            task = MessageTask(
                channel_id=channel_id,
                message_id=message_id,
                timestamp=datetime.now()
            )
            
            # Добавляем в очередь (неблокирующе)
            self.task_queue.put_nowait(task)
            self.processing_messages.add(message_key)
            
            logger.info(f"➕ Сообщение {message_id} добавлено в очередь. Размер очереди: {self.task_queue.qsize()}")
            
            # Планируем удаление из защиты от дублей через час
            asyncio.create_task(self._cleanup_processed_message(message_key, delay=3600))
            
            return True
            
        except asyncio.QueueFull:
            logger.error(f"🚫 Очередь переполнена! Размер: {self.task_queue.qsize()}")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении в очередь: {e}")
            return False
    
    async def _cleanup_processed_message(self, message_key: tuple, delay: int):
        """Удаляет сообщение из защиты от дублей через delay секунд"""
        try:
            await asyncio.sleep(delay)
            self.processing_messages.discard(message_key)
            logger.debug(f"🧹 Очищена защита от дублей для {message_key}")
        except Exception as e:
            logger.error(f"Ошибка при очистке дублей: {e}")
    
    def get_stats(self) -> dict:
        """Получение детальной статистики"""
        total_processed = sum(worker.processed_count for worker in self.workers.values())
        total_errors = sum(worker.error_count for worker in self.workers.values())
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        # Статистика по воркерам
        workers_stats = [worker.get_stats() for worker in self.workers.values()]
        
        return {
            'running': self.running,
            'uptime_seconds': uptime,
            'workers_count': len(self.workers),
            'queue_size': self.task_queue.qsize(),
            'processing_messages_count': len(self.processing_messages),
            'total_processed': total_processed,
            'total_errors': total_errors,
            'success_rate': (total_processed / (total_processed + total_errors) * 100) if (total_processed + total_errors) > 0 else 0,
            'messages_per_hour': (total_processed / (uptime / 3600)) if uptime > 0 else 0,
            'workers': workers_stats
        }
    
    def get_simple_stats(self) -> dict:
        """Упрощенная статистика для логов"""
        total_processed = sum(worker.processed_count for worker in self.workers.values())
        return {
            'queue_size': self.task_queue.qsize(),
            'total_processed': total_processed,
            'running_workers': sum(1 for w in self.workers.values() if w.running)
        }
    
    async def wait_for_completion(self, timeout: int = 300):
        """Ждет завершения всех задач в очереди"""
        logger.info(f"⏳ Ожидание завершения всех задач (таймаут: {timeout} сек)")
        try:
            await asyncio.wait_for(self.task_queue.join(), timeout=timeout)
            logger.success("✅ Все задачи завершены")
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ Таймаут при ожидании завершения задач")
    
    async def stop(self):
        """Остановка всех воркеров"""
        if not self.running:
            return
            
        logger.info("🛑 Останавливаю все воркеры...")
        
        # Останавливаем воркеры
        for worker in self.workers.values():
            worker.stop()
        
        # Ждем завершения текущих задач (с таймаутом)
        try:
            await asyncio.wait_for(self.task_queue.join(), timeout=30.0)
            logger.info("✅ Все текущие задачи завершены")
        except asyncio.TimeoutError:
            logger.warning("⚠️ Таймаут при ожидании завершения задач")
        
        # Отменяем задачи воркеров
        for task in self.worker_tasks.values():
            if not task.done():
                task.cancel()
        
        # Ждем отмены всех задач
        if self.worker_tasks:
            try:
                await asyncio.wait(self.worker_tasks.values(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("⚠️ Некоторые воркеры не завершились вовремя")
        
        self.running = False
        
        # Финальная статистика
        stats = self.get_stats()
        logger.success(f"✅ Процессор остановлен. Всего обработано: {stats['total_processed']}, ошибок: {stats['total_errors']}")
        
        # Очищаем ресурсы
        self.workers.clear()
        self.worker_tasks.clear()
        self.processing_messages.clear()


# Функция для получения статистики (можно использовать из бота или API)
def get_global_processor_stats():
    """Получить статистику глобального процессора"""
    # Предполагается что message_processor импортирован в main
    try:
        from auto_reposting.__main__ import message_processor
        return message_processor.get_stats()
    except ImportError:
        return {"error": "Процессор не инициализирован"}