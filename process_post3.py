import argparse
import asyncio
from datetime import datetime
from typing import List

from loguru import logger
from opentele.tl import TelegramClient
from telethon import errors

from auto_reposting import telegram_utils, exc, telegram_utils2

from core.schemas import repost as repost_schemas
from core.models import tg_account as tg_account_db, channel as channel_db, group as group_db, repost as repost_db
from core.settings import json_settings, settings


async def cleanup_clients(clients_to_cleanup: List[TelegramClient]) -> None:
    """ОБЯЗАТЕЛЬНО отключает все переданные клиенты"""
    cleanup_count = 0
    for client in clients_to_cleanup:
        if client:
            try:
                await client.disconnect()
                cleanup_count += 1
                logger.debug("🔌 Клиент отключен")
            except Exception as e:
                logger.debug(f"Ошибка при отключении клиента: {e}")
    
    if cleanup_count > 0:
        logger.info(f"✅ Отключено {cleanup_count} клиентов")


async def check_stop_link_in_message(
        tg_accounts: List[tg_account_db.TGAccount],
        telegram_client: TelegramClient,
        channel_url: str,
        telegram_channel_id: int,
        telegram_message_id: int
) -> bool:
    """Проверяет наличие стоп-ссылок в сообщении и ставит реакции если найдены"""
    try:
        stop_links = await json_settings.async_get_attribute("stop_links")
        if not stop_links:
            return False

        async with telegram_client:
            await telegram_client.get_entity(channel_url)
            message = await telegram_client.get_messages(telegram_channel_id, ids=telegram_message_id)
            
            if not message or not message.message:
                return False

            # Проверяем каждую стоп-ссылку
            for stop_link in stop_links:
                if stop_link in message.message:
                    logger.info(f"🚫 В посте найдена стоп-ссылка: {stop_link}")
                    
                    # Ставим реакции
                    await telegram_utils2.send_reaction_with_accounts_on_message(
                        tg_accounts=tg_accounts,
                        message=message,
                        channel_url=channel_url,
                        emoji_reaction=await json_settings.async_get_attribute("reaction")
                    )
                    
                    logger.success("❤️ Реакции поставлены на сообщение со стоп-ссылкой")
                    return True

        return False
        
    except Exception as e:
        logger.error(f"Ошибка при проверке стоп-ссылок: {e}")
        return False


async def repost_to_group_batch(
        groups_batch: List[group_db.Group],
        channel: channel_db.Channel,
        telegram_message_id: int,
        telegram_client: TelegramClient,
        batch_id: int
) -> tuple[int, List[str]]:
    """🚀 Обрабатывает пакет групп параллельно для ускорения"""
    successful_reposts = 0
    processed_groups = []
    
    logger.info(f"📦 Batch {batch_id}: Начинаю обработку {len(groups_batch)} групп")
    start_time = datetime.now()
    
    # Создаем задачи для параллельной обработки групп в пакете
    tasks = []
    for i, group in enumerate(groups_batch):
        task = asyncio.create_task(
            repost_to_single_group(
                group=group,
                channel=channel,
                telegram_message_id=telegram_message_id,
                telegram_client=telegram_client,
                group_index=i + 1,
                batch_id=batch_id
            )
        )
        tasks.append((task, group))
    
    # Ждем завершения всех задач в пакете с таймаутом
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*[task for task, _ in tasks], return_exceptions=True),
            timeout=300.0  # 5 минут на пакет
        )
    except asyncio.TimeoutError:
        logger.error(f"❌ Batch {batch_id}: Таймаут обработки пакета")
        # Отменяем все незавершенные задачи
        for task, _ in tasks:
            if not task.done():
                task.cancel()
        return 0, []
    
    # Обрабатываем результаты
    for i, (result, (_, group)) in enumerate(zip(results, tasks)):
        try:
            if isinstance(result, Exception):
                logger.error(f"❌ Batch {batch_id}: Ошибка в группе {group.url}: {result}")
            elif result:
                successful_reposts += 1
                processed_groups.append(group.url)
                
                # Записываем успешный репост в базу
                try:
                    await repost_db.create_repost(
                        repost_in=repost_schemas.RepostCreate(
                            channel_guid=channel.guid,
                            repost_message_id=telegram_message_id,
                            created_at=datetime.now().date()
                        )
                    )
                    logger.debug(f"📊 Репост записан в БД для группы {group.url}")
                except Exception as db_error:
                    logger.error(f"Ошибка записи в БД для группы {group.url}: {db_error}")
                    
                logger.success(f"✅ Batch {batch_id}: Репост в {group.url}")
            else:
                logger.warning(f"⚠️ Batch {batch_id}: Репост в {group.url} не удался")
                
        except Exception as e:
            logger.error(f"Ошибка при обработке результата для {group.url}: {e}")
    
    processing_time = (datetime.now() - start_time).total_seconds()
    success_rate = (successful_reposts / len(groups_batch) * 100) if groups_batch else 0
    
    logger.success(f"🎯 Batch {batch_id}: Завершен за {processing_time:.1f}с. Успешно: {successful_reposts}/{len(groups_batch)} ({success_rate:.1f}%)")
    
    return successful_reposts, processed_groups


async def repost_to_single_group(
        group: group_db.Group,
        channel: channel_db.Channel,
        telegram_message_id: int,
        telegram_client: TelegramClient,
        group_index: int = 0,
        batch_id: int = 0
) -> bool:
    """Делает репост в одну группу с повторными попытками и оптимизациями"""
    max_attempts = 2
    group_logger = logger.bind(batch=batch_id, group_idx=group_index)
    
    for attempt in range(max_attempts):
        try:
            group_logger.debug(f"🎯 Попытка {attempt + 1}: {group.url}")
            
            # Пытаемся вступить в группу
            join_success = await telegram_utils2.checking_and_joining_if_possible(
                telegram_client=telegram_client,
                url=group.url,
                channel=channel
            )
            
            if not join_success:
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)  # Короткая пауза перед повтором
                    continue
                else:
                    group_logger.warning(f"❌ Не удалось вступить в группу {group.url}")
                    return False
            
            # Пытаемся сделать репост
            repost_success = await telegram_utils2.repost_in_group_by_message_id(
                message_id=telegram_message_id,
                telegram_client=telegram_client,
                telegram_channel_id=channel.telegram_channel_id,
                channel_url=channel.url,
                group_url=group.url
            )
            
            if repost_success:
                group_logger.success(f"✅ Репост в {group.url}")
                return True
            elif attempt < max_attempts - 1:
                await asyncio.sleep(2)  # Пауза перед повтором репоста
            
        except errors.FloodWaitError as e:
            group_logger.warning(f"⏳ FloodWait в группе {group.url}: {e}")
            return False  # При FloodWait не повторяем
        except (errors.ChatWriteForbiddenError, errors.UserBannedInChannelError) as e:
            group_logger.warning(f"🚫 Бан/ограничения в группе {group.url}: {type(e).__name__}")
            return False  # При банах не повторяем
        except Exception as e:
            group_logger.error(f"❌ Ошибка при репосте в {group.url}, попытка {attempt + 1}: {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(1)
    
    group_logger.warning(f"❌ Не удалось сделать репост в {group.url} после {max_attempts} попыток")
    return False


def calculate_optimal_batch_size(num_groups: int, num_accounts: int) -> int:
    """Вычисляет оптимальный размер пакета на основе количества групп и аккаунтов"""
    if num_groups <= 10:
        return max(2, num_groups // 2)
    elif num_groups <= 50:
        return min(8, max(3, num_groups // num_accounts)) if num_accounts > 0 else 5
    else:
        return min(12, max(5, num_groups // (num_accounts * 2))) if num_accounts > 0 else 8


async def process_group_reposting_fast(
        channel: channel_db.Channel,
        tg_accounts: List[tg_account_db.TGAccount],
        groups: List[group_db.Group],
        telegram_message_id: int
) -> None:
    all_clients_used = []
    
    try:
        # Фильтруем только рабочие аккаунты
        working_accounts = [acc for acc in tg_accounts if acc.status == "WORKING"]
        if not working_accounts:
            await telegram_utils.send_message(
                chat_id=settings.admin_chat_id, 
                text=f"❌ У канала {channel.url} нет рабочих аккаунтов."
            )
            return

        logger.info(f"🚀 БЫСТРЫЙ РЕЖИМ: {len(working_accounts)} аккаунтов ➜ {len(groups)} групп")

        # Получаем настройки с улучшенными значениями по умолчанию
        try:
            number_reposts_before_pause = await json_settings.async_get_attribute("number_reposts_before_pause")
            pause_after_rate_reposts = await json_settings.async_get_attribute("pause_after_rate_reposts")
            pause_between_reposts = await json_settings.async_get_attribute("pause_between_reposts")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка загрузки настроек, использую умолчания: {e}")
            # Оптимизированные значения по умолчанию для быстрого режима
            number_reposts_before_pause = 15
            pause_after_rate_reposts = 3600   
            pause_between_reposts = 25        

        # 🎯 Вычисляем оптимальный размер пакета
        batch_size = calculate_optimal_batch_size(len(groups), len(working_accounts))
        logger.info(f"📦 Оптимальный размер пакета: {batch_size} групп")
        
        # Разбиваем группы на пакеты
        group_batches = [groups[i:i + batch_size] for i in range(0, len(groups), batch_size)]
        logger.info(f"📋 Создано {len(group_batches)} пакетов для обработки")

        account_index = 0
        telegram_client = None
        counter_reposts_current_account = 0
        total_successful_reposts = 0
        start_time = datetime.now()
        
        try:
            # Получаем первый рабочий клиент
            telegram_client, account_index = await telegram_utils2.get_authorized_tg_client_with_check_pause(
                accounts=working_accounts,
                start_index=account_index
            )
            if telegram_client:
                all_clients_used.append(telegram_client)
            else:
                raise exc.NoAccounts("Не удалось получить первый клиент")
                
        except exc.NoAccounts:
            await telegram_utils.send_message(
                chat_id=settings.admin_chat_id, 
                text=f"❌ У канала {channel.url} нет доступных аккаунтов для работы."
            )
            return

        # Проверяем стоп-ссылки только один раз в начале
        try:
            if await check_stop_link_in_message(
                tg_accounts=working_accounts, 
                telegram_client=telegram_client, 
                channel_url=channel.url,
                telegram_channel_id=channel.telegram_channel_id,
                telegram_message_id=telegram_message_id
            ):
                logger.info("🛑 Обработка остановлена из-за стоп-ссылки")
                return
        except Exception as e:
            logger.error(f"Ошибка при проверке стоп-ссылок: {e}")

        # 🚀 ОСНОВНОЙ ЦИКЛ: Обрабатываем пакеты групп параллельно
        for batch_idx, group_batch in enumerate(group_batches, 1):
            batch_start_time = datetime.now()
            logger.info(f"🎯 Пакет {batch_idx}/{len(group_batches)}: Начинаю обработку")

            # Проверяем, нужна ли смена аккаунта
            if counter_reposts_current_account >= number_reposts_before_pause:
                logger.info(f"🔄 Достигнут лимит репостов ({number_reposts_before_pause}), меняю аккаунт")
                
                # Ставим текущий аккаунт на паузу
                current_account = working_accounts[account_index]
                await tg_account_db.add_pause(
                    tg_account=current_account,
                    pause_in_seconds=pause_after_rate_reposts
                )
                
                pause_minutes = pause_after_rate_reposts // 60
                logger.info(f"⏸️ Аккаунт +{current_account.phone_number} на паузе {pause_minutes} мин")

                # Закрываем текущий клиент
                if telegram_client:
                    try:
                        await telegram_client.disconnect()
                        logger.debug("🔌 Старый клиент отключен")
                    except:
                        pass
                    telegram_client = None

                # Получаем следующий аккаунт
                account_index += 1
                try:
                    telegram_client, account_index = await telegram_utils2.get_authorized_tg_client_with_check_pause(
                        accounts=working_accounts,
                        start_index=account_index
                    )
                    if telegram_client:
                        all_clients_used.append(telegram_client)
                    else:
                        raise exc.NoAccounts("Нет следующего клиента")
                        
                    counter_reposts_current_account = 0
                    logger.info(f"🔄 Переключился на аккаунт +{working_accounts[account_index].phone_number}")
                    
                except exc.NoAccounts:
                    logger.warning("🔚 Закончились доступные аккаунты")
                    
                    # Отправляем финальную статистику
                    processing_time = (datetime.now() - start_time).total_seconds()
                    await telegram_utils.send_message(
                        chat_id=settings.admin_chat_id, 
                        text=f"⏹️ Репостинг остановлен - закончились аккаунты\n"
                              f"📊 Канал: {channel.url}\n"
                              f"✅ Успешных репостов: {total_successful_reposts}\n"
                              f"⏱️ Время работы: {processing_time/60:.1f} мин"
                    )
                    return

            # 🚀 Обрабатываем пакет групп параллельно
            try:
                batch_successful, processed_groups = await repost_to_group_batch(
                    groups_batch=group_batch,
                    channel=channel,
                    telegram_message_id=telegram_message_id,
                    telegram_client=telegram_client,
                    batch_id=batch_idx
                )
                
                counter_reposts_current_account += batch_successful
                total_successful_reposts += batch_successful
                
                batch_time = (datetime.now() - batch_start_time).total_seconds()
                logger.info(f"📊 Пакет {batch_idx}: {batch_successful} репостов за {batch_time:.1f}с")
                
                # 🎯 АДАПТИВНАЯ ПАУЗА: зависит от результатов пакета
                if batch_idx < len(group_batches):  # Не ждем после последнего пакета
                    if batch_successful > len(group_batch) * 0.7:  # >70% успеха
                        adaptive_pause = max(pause_between_reposts // 2, 15)  # Короткая пауза
                        logger.debug(f"✅ Хорошие результаты, короткая пауза: {adaptive_pause}с")
                    elif batch_successful > 0:  # Есть успехи
                        adaptive_pause = pause_between_reposts  # Обычная пауза
                        logger.debug(f"⚖️ Средние результаты, обычная пауза: {adaptive_pause}с")
                    else:  # Неудачный пакет
                        adaptive_pause = pause_between_reposts * 2  # Длинная пауза
                        logger.debug(f"❌ Плохие результаты, длинная пауза: {adaptive_pause}с")
                    
                    logger.info(f"⏱️ Пауза {adaptive_pause}с перед пакетом {batch_idx + 1}")
                    await asyncio.sleep(adaptive_pause)
                    
            except Exception as e:
                logger.error(f"❌ Критическая ошибка при обработке пакета {batch_idx}: {e}")
                # При критической ошибке делаем увеличенную паузу
                error_pause = pause_between_reposts * 3
                logger.warning(f"⏸️ Пауза {error_pause}с после ошибки")
                await asyncio.sleep(error_pause)

        # 📊 Финальная статистика и уведомление
        total_time = (datetime.now() - start_time).total_seconds()
        success_rate = (total_successful_reposts / len(groups) * 100) if groups else 0
        speed = (total_successful_reposts / (total_time / 60)) if total_time > 0 else 0
        
        logger.success(f"🏁 ЗАВЕРШЕНО: {total_successful_reposts}/{len(groups)} репостов ({success_rate:.1f}%)")
        logger.info(f"⏱️ Время: {total_time/60:.1f} мин, скорость: {speed:.1f} репостов/мин")
        
        # Отправляем уведомление админу
        try:
            status_emoji = "✅" if success_rate >= 70 else "⚠️" if success_rate >= 30 else "❌"
            await telegram_utils.send_message(
                chat_id=settings.admin_chat_id,
                text=f"{status_emoji} Репостинг завершен\n"
                      f"📺 Канал: {channel.url}\n"
                      f"📊 Результат: {total_successful_reposts}/{len(groups)} ({success_rate:.1f}%)\n"
                      f"⏱️ Время: {total_time/60:.1f} мин\n"
                      f"🚀 Скорость: {speed:.1f} репостов/мин"
            )
        except Exception as notification_error:
            logger.error(f"Ошибка отправки уведомления: {notification_error}")

    finally:
        # ОБЯЗАТЕЛЬНО закрываем ВСЕ использованные клиенты
        logger.info(f"🔌 Закрываю {len(all_clients_used)} использованных клиентов...")
        await cleanup_clients(all_clients_used)
        logger.success("✅ Все клиенты корректно закрыты")


async def new_message_in_channel(telegram_channel_id: int, telegram_message_id: int) -> None:
    """Обрабатывает новое сообщение в канале - УСКОРЕННАЯ и УЛУЧШЕННАЯ версия"""
    processing_start = datetime.now()
    
    try:
        # Получаем канал из базы
        channel = await channel_db.get_channel_by_telegram_channel_id(telegram_channel_id=telegram_channel_id)
        if not channel:
            logger.error(f"❌ Канал с ID {telegram_channel_id} не найден в базе данных")
            return

        logger.info(f"🚀 БЫСТРАЯ ОБРАБОТКА сообщения {telegram_message_id} из канала {channel.url}")

        # Получаем аккаунты канала
        tg_accounts = await tg_account_db.get_tg_accounts_by_channel_guid(channel_guid=str(channel.guid))
        if not tg_accounts:
            logger.warning(f"⚠️ У канала {channel.url} нет привязанных аккаунтов")
            await telegram_utils.send_message(
                chat_id=settings.admin_chat_id, 
                text=f"⚠️ У канала {channel.url} нет привязанных аккаунтов для репостинга."
            )
            return

        # Получаем группы канала
        groups = await group_db.get_all_groups_by_channel_guid(channel_guid=str(channel.guid))
        if not groups:
            logger.warning(f"⚠️ У канала {channel.url} нет привязанных групп")
            await telegram_utils.send_message(
                chat_id=settings.admin_chat_id, 
                text=f"⚠️ У канала {channel.url} нет привязанных групп для репостинга."
            )
            return

        # Фильтруем рабочие аккаунты
        working_accounts = [acc for acc in tg_accounts if acc.status == "WORKING"]
        logger.info(f"📊 Найдено {len(working_accounts)}/{len(tg_accounts)} рабочих аккаунтов и {len(groups)} групп")

        if not working_accounts:
            logger.error(f"❌ У канала {channel.url} нет рабочих аккаунтов")
            await telegram_utils.send_message(
                chat_id=settings.admin_chat_id, 
                text=f"❌ У канала {channel.url} все аккаунты заблокированы или на паузе."
            )
            return

        
        await process_group_reposting_fast(
            channel=channel,
            tg_accounts=tg_accounts,  # Передаем все аккаунты для фильтрации
            groups=groups,
            telegram_message_id=telegram_message_id
        )
        
        # Логируем общее время обработки
        total_processing_time = (datetime.now() - processing_start).total_seconds()
        logger.success(f"🎉 Полная обработка сообщения завершена за {total_processing_time:.1f} секунд")

    except Exception as e:
        processing_time = (datetime.now() - processing_start).total_seconds()
        logger.exception(f"💥 Критическая ошибка при обработке сообщения {telegram_message_id} из канала {telegram_channel_id} за {processing_time:.1f}с: {e}")
        
        try:
            # Отправляем сокращенное уведомление об ошибке
            error_summary = str(e)[:150] + "..." if len(str(e)) > 150 else str(e)
            await telegram_utils.send_message(
                chat_id=settings.admin_chat_id, 
                text=f"💥 Критическая ошибка при обработке сообщения {telegram_message_id}\n"
                      f"⚠️ Ошибка: {error_summary}\n"
                      f"⏱️ Время обработки: {processing_time:.1f}с"
            )
        except Exception as notification_error:
            logger.error(f"Не удалось отправить уведомление об ошибке: {notification_error}")


# Точка входа для subprocess (сохраняем для обратной совместимости)
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--telegram_message_id', type=int, required=True, help='The ID of the message')
    parser.add_argument('--telegram_channel_id', type=int, required=True, help='The ID of the channel')
    parser.add_argument('--log_filename', type=str, required=True, help='The log filename')

    args = parser.parse_args()
    telegram_channel_id = args.telegram_channel_id
    telegram_message_id = args.telegram_message_id

    # Настраиваем логирование для subprocess
    logger.add(
        f"logs/{args.log_filename.replace('.log', '')}/{telegram_channel_id}-{telegram_message_id}.log", 
        rotation="1 day", 
        retention="10 days", 
        compression="zip"
    )
    
    logger.info(f"🚀 БЫСТРАЯ ОБРАБОТКА сообщения ID {telegram_message_id} из канала ID {telegram_channel_id}")

    try:
        asyncio.run(new_message_in_channel(
            telegram_channel_id=telegram_channel_id, 
            telegram_message_id=telegram_message_id
        ))
        logger.info("✅ Subprocess обработка завершена успешно")
    except Exception as e:
        logger.exception(f"💥 SUBPROCESS ЗАВЕРШИЛСЯ С ОШИБКОЙ: {e.__class__.__name__}: {e}")
        exit(1)