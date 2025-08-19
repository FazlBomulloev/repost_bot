import asyncio
from datetime import datetime, timedelta
from typing import List, Dict
from loguru import logger

from core.models import tg_account as tg_account_db
from core.schemas import tg_account as tg_account_schemas
from core.settings import settings


class PauseRestorer:
    """Класс для автоматического восстановления аккаунтов из пауз"""
    
    def __init__(self):
        self.check_interval = 1200  # 20 минут в секундах
        self.running = False
        self.last_check = None
        self.stats = {
            'total_checks': 0,
            'total_restored': 0,
            'last_restored_count': 0
        }
    
    async def check_and_restore_expired_pauses(self) -> Dict:
        """
        Проверяет и восстанавливает аккаунты с истёкшими паузами
        Возвращает статистику операции
        """
        check_start = datetime.now()
        self.stats['total_checks'] += 1
        
        result = {
            'timestamp': check_start,
            'checked_accounts': 0,
            'restored_accounts': 0,
            'restored_phones': [],
            'still_paused': 0,
            'errors': []
        }
        
        try:
            # Получаем все аккаунты в муте
            muted_accounts = await tg_account_db.get_tg_accounts_by_status("MUTED")
            result['checked_accounts'] = len(muted_accounts)
            
            logger.info(f"🔍 Проверяю {len(muted_accounts)} аккаунтов в муте на истёкшие паузы")
            
            current_time = datetime.now()
            
            for account in muted_accounts:
                try:
                    # Пропускаем аккаунты без установленной паузы
                    if not account.last_datetime_pause or not account.pause_in_seconds:
                        logger.debug(f"Аккаунт +{account.phone_number} в муте без паузы - пропускаем")
                        continue
                    
                    # Вычисляем время окончания паузы
                    pause_end_time = account.last_datetime_pause + timedelta(seconds=account.pause_in_seconds)
                    
                    # Проверяем, истекла ли пауза
                    if current_time >= pause_end_time:
                        logger.info(f"⏰ Восстанавливаю аккаунт +{account.phone_number} (пауза истекла)")
                        
                        # Восстанавливаем аккаунт
                        await tg_account_db.update_tg_account(
                            tg_account=account,
                            tg_account_update=tg_account_schemas.TGAccountUpdate(
                                last_datetime_pause=None,
                                pause_in_seconds=None,
                                status=tg_account_schemas.TGAccountStatus.working
                            )
                        )
                        
                        result['restored_accounts'] += 1
                        result['restored_phones'].append(account.phone_number)
                        self.stats['total_restored'] += 1
                        
                        logger.success(f"✅ Аккаунт +{account.phone_number} восстановлен и готов к работе")
                    else:
                        # Аккаунт всё ещё на паузе
                        result['still_paused'] += 1
                        remaining_seconds = (pause_end_time - current_time).total_seconds()
                        remaining_minutes = remaining_seconds / 60
                        
                        logger.debug(f"⏳ Аккаунт +{account.phone_number}: осталось {remaining_minutes:.1f} минут")
                
                except Exception as e:
                    error_msg = f"Ошибка при обработке аккаунта +{account.phone_number}: {e}"
                    logger.error(error_msg)
                    result['errors'].append(error_msg)
            
            # Обновляем статистику
            self.stats['last_restored_count'] = result['restored_accounts']
            self.last_check = check_start
            
            # Логируем результат
            if result['restored_accounts'] > 0:
                logger.success(f"🎉 Восстановлено аккаунтов: {result['restored_accounts']}")
                logger.info(f"📱 Восстановленные номера: {', '.join([f'+{phone}' for phone in result['restored_phones']])}")
            else:
                logger.info("ℹ️ Нет аккаунтов для восстановления")
            
            if result['still_paused'] > 0:
                logger.info(f"⏳ Аккаунтов всё ещё на паузе: {result['still_paused']}")
            
            return result
            
        except Exception as e:
            error_msg = f"Критическая ошибка при проверке пауз: {e}"
            logger.error(error_msg)
            result['errors'].append(error_msg)
            return result
    
    async def send_notification_if_needed(self, restore_result: Dict) -> None:
        """Отправляет уведомление админу если восстановлены аккаунты"""
        if restore_result['restored_accounts'] == 0:
            return
        
        try:
            from core.settings import bot
            
            # Формируем сообщение
            message = f"""🔄 **Автовосстановление аккаунтов**
            
✅ Восстановлено: {restore_result['restored_accounts']} аккаунтов"""
            
            if restore_result['errors']:
                message += f"\n\n⚠️ **Ошибки:** {len(restore_result['errors'])}"
            
            await bot.send_message(
                chat_id=settings.admin_chat_id,
                text=message,
                parse_mode='Markdown'
            )
            
            logger.success("📤 Уведомление отправлено админу")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления: {e}")
    
    async def run_continuous_check(self) -> None:
        """Запуск непрерывной проверки каждые 20 минут"""
        self.running = True
        logger.info(f"🚀 Запуск автовосстановления пауз (интервал: {self.check_interval/60:.0f} минут)")
        
        while self.running:
            try:
                # Выполняем проверку
                result = await self.check_and_restore_expired_pauses()
                
                # Отправляем уведомление если нужно
                await self.send_notification_if_needed(result)
                
                # Ждём до следующей проверки
                logger.debug(f"⏰ Следующая проверка через {self.check_interval/60:.0f} минут")
                await asyncio.sleep(self.check_interval)
                
            except asyncio.CancelledError:
                logger.info("🛑 Получен сигнал остановки автовосстановления")
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле автовосстановления: {e}")
                # При ошибке ждём меньше времени
                await asyncio.sleep(60)
        
        self.running = False
        logger.info("🏁 Автовосстановление остановлено")
    
    def stop(self) -> None:
        """Остановка автовосстановления"""
        self.running = False
    
    def get_stats(self) -> Dict:
        """Получение статистики работы"""
        return {
            'running': self.running,
            'last_check': self.last_check,
            'check_interval_minutes': self.check_interval / 60,
            **self.stats
        }


# Глобальный экземпляр восстановителя
pause_restorer = PauseRestorer()


async def start_pause_restorer() -> None:
    """Запуск автовосстановления как фоновой задачи"""
    try:
        await pause_restorer.run_continuous_check()
    except Exception as e:
        logger.error(f"Фатальная ошибка в автовосстановлении: {e}")


def stop_pause_restorer() -> None:
    """Остановка автовосстановления"""
    pause_restorer.stop()
