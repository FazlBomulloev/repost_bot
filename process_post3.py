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
                    logger.info("В посте найдена стоп ссылка, начинаю ставить реакции!")
                    await telegram_utils2.send_reaction_with_accounts_on_message(
                        tg_accounts=tg_accounts,
                        message=message,
                        channel_url=channel_url,
                        emoji_reaction=await json_settings.async_get_attribute("reaction")
                    )
                    return True

        return False
        
    except Exception as e:
        logger.error(f"Ошибка при проверке стоп-ссылок: {e}")
        return False


async def process_group_reposting(
        channel: channel_db.Channel,
        tg_accounts: List[tg_account_db.TGAccount],
        groups: List[group_db.Group],
        telegram_message_id: int
) -> None:
    # Фильтруем только рабочие аккаунты
    working_accounts = [acc for acc in tg_accounts if acc.status == "WORKING"]
    if not working_accounts:
        await telegram_utils.send_message(
            chat_id=settings.admin_chat_id, 
            text=f"У канала {channel.url} нет рабочих аккаунтов."
        )
        return

    logger.info(f"Найдено {len(working_accounts)} рабочих аккаунтов для канала {channel.url}")

    # Получаем настройки
    try:
        number_reposts_before_pause = await json_settings.async_get_attribute("number_reposts_before_pause")
        pause_after_rate_reposts = await json_settings.async_get_attribute("pause_after_rate_reposts")
        pause_between_reposts = await json_settings.async_get_attribute("pause_between_reposts")
    except Exception as e:
        logger.error(f"Ошибка при получении настроек: {e}")
        # Устанавливаем значения по умолчанию
        number_reposts_before_pause = 10
        pause_after_rate_reposts = 3600
        pause_between_reposts = 60

    account_index = 0
    telegram_client = None
    
    try:
        # Получаем первый рабочий клиент
        telegram_client, account_index = await telegram_utils2.get_authorized_tg_client_with_check_pause(
            accounts=working_accounts,
            start_index=account_index
        )
    except exc.NoAccounts:
        await telegram_utils.send_message(
            chat_id=settings.admin_chat_id, 
            text=f"У канала {channel.url} нет доступных аккаунтов для работы."
        )
        return

    # Проверяем стоп-ссылки
    try:
        if await check_stop_link_in_message(
            tg_accounts=working_accounts, 
            telegram_client=telegram_client, 
            channel_url=channel.url,
            telegram_channel_id=channel.telegram_channel_id,
            telegram_message_id=telegram_message_id
        ):
            return
    except Exception as e:
        logger.error(f"Ошибка при проверке стоп-ссылок: {e}")

    counter_number_reposts_before_pause = 0
    successful_reposts = 0

    for group_index, group in enumerate(groups):
        logger.info(f"Обрабатываю группу {group_index + 1}/{len(groups)}: {group.url}")

        # Проверяем, нужна ли смена аккаунта
        if counter_number_reposts_before_pause >= number_reposts_before_pause:
            logger.info(f"Достигнут лимит репостов ({number_reposts_before_pause}), меняю аккаунт")
            
            # Ставим текущий аккаунт на паузу
            current_account = working_accounts[account_index]
            await tg_account_db.add_pause(
                tg_account=current_account,
                pause_in_seconds=pause_after_rate_reposts
            )
            logger.info(f"Аккаунт +{current_account.phone_number} поставлен на паузу на {pause_after_rate_reposts} секунд")

            # Закрываем текущий клиент
            if telegram_client:
                try:
                    await telegram_client.disconnect()
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
                counter_number_reposts_before_pause = 0
            except exc.NoAccounts:
                logger.warning("Закончились доступные аккаунты")
                await telegram_utils.send_message(
                    chat_id=settings.admin_chat_id, 
                    text=f"У канала {channel.url} закончились аккаунты. Обработано {successful_reposts} репостов."
                )
                return

        # Пытаемся вступить в группу
        max_join_attempts = 3
        join_successful = False
        
        for join_attempt in range(max_join_attempts):
            try:
                join_successful = await telegram_utils2.checking_and_joining_if_possible(
                    telegram_client=telegram_client,
                    url=group.url,
                    channel=channel
                )
                if join_successful:
                    break
                    
            except errors.FloodWaitError:
                logger.warning(f"FloodWait при попытке вступить в группу {group.url}, попытка {join_attempt + 1}")
                if join_attempt < max_join_attempts - 1:
                    # Пытаемся сменить аккаунт
                    if telegram_client:
                        try:
                            await telegram_client.disconnect()
                        except:
                            pass
                        telegram_client = None

                    account_index += 1
                    try:
                        telegram_client, account_index = await telegram_utils2.get_authorized_tg_client_with_check_pause(
                            accounts=working_accounts,
                            start_index=account_index
                        )
                    except exc.NoAccounts:
                        logger.warning("Нет доступных аккаунтов для смены")
                        return
                else:
                    logger.error(f"Не удалось вступить в группу {group.url} после {max_join_attempts} попыток")
                    break
            except Exception as e:
                logger.error(f"Ошибка при вступлении в группу {group.url}: {e}")
                break

        if not join_successful:
            logger.info(f"Пропускаю группу {group.url} - не удалось вступить")
            continue

        # Пытаемся сделать репост
        max_repost_attempts = 3
        repost_successful = False

        for repost_attempt in range(max_repost_attempts):
            try:
                repost_successful = await telegram_utils2.repost_in_group_by_message_id(
                    message_id=telegram_message_id,
                    telegram_client=telegram_client,
                    telegram_channel_id=channel.telegram_channel_id,
                    channel_url=channel.url,
                    group_url=group.url
                )
                
                if repost_successful:
                    # Записываем успешный репост в базу
                    await repost_db.create_repost(
                        repost_in=repost_schemas.RepostCreate(
                            channel_guid=channel.guid,
                            repost_message_id=telegram_message_id,
                            created_at=datetime.now().date()
                        )
                    )
                    logger.info(f"Успешно сделан репост в группу {group.url}")
                    counter_number_reposts_before_pause += 1
                    successful_reposts += 1
                    
                    # Пауза между репостами
                    if pause_between_reposts > 0:
                        await asyncio.sleep(pause_between_reposts)
                    break
                else:
                    logger.warning(f"Репост в группу {group.url} не удался, попытка {repost_attempt + 1}")

            except errors.FloodWaitError:
                logger.warning(f"FloodWait при репосте в группу {group.url}, попытка {repost_attempt + 1}")
                if repost_attempt < max_repost_attempts - 1:
                    # Пытаемся сменить аккаунт
                    try:
                        await telegram_utils.check_ban_in_spambot(telegram_client=telegram_client)
                    except Exception as e:
                        logger.error(f"Ошибка при проверке на спам: {e}")

                    if telegram_client:
                        try:
                            await telegram_client.disconnect()
                        except:
                            pass
                        telegram_client = None

                    account_index += 1
                    try:
                        telegram_client, account_index = await telegram_utils2.get_authorized_tg_client_with_check_pause(
                            accounts=working_accounts,
                            start_index=account_index
                        )
                    except exc.NoAccounts:
                        logger.warning("Нет доступных аккаунтов для смены при FloodWait")
                        return
                else:
                    logger.error(f"Не удалось сделать репост в группу {group.url} после {max_repost_attempts} попыток")
                    break

            except (errors.ChatWriteForbiddenError, errors.UserBannedInChannelError) as e:
                logger.error(f"Репост в группу {group.url} невозможен - бан или ограничения: {e}")
                break

            except Exception as e:
                logger.error(f"Ошибка при репосте в группу {group.url}: {e}")
                if repost_attempt < max_repost_attempts - 1:
                    await asyncio.sleep(5)  # Небольшая пауза перед повтором
                else:
                    logger.error(f"Не удалось сделать репост в группу {group.url} после {max_repost_attempts} попыток")

        if not repost_successful:
            logger.info(f"Не удалось сделать репост в группу {group.url}")

    # Закрываем клиент в конце
    if telegram_client:
        try:
            await telegram_client.disconnect()
        except:
            pass

    logger.info(f"Обработка канала {channel.url} завершена. Успешных репостов: {successful_reposts}")


async def new_message_in_channel(telegram_channel_id: int, telegram_message_id: int) -> None:
    """Обрабатывает новое сообщение в канале"""
    try:
        # Получаем канал из базы
        channel = await channel_db.get_channel_by_telegram_channel_id(telegram_channel_id=telegram_channel_id)
        if not channel:
            logger.error(f"Канал с ID {telegram_channel_id} не найден в базе данных")
            return

        logger.info(f"Обрабатываю сообщение {telegram_message_id} из канала {channel.url}")

        # Получаем аккаунты канала
        tg_accounts = await tg_account_db.get_tg_accounts_by_channel_guid(channel_guid=str(channel.guid))
        if not tg_accounts:
            await telegram_utils.send_message(
                chat_id=settings.admin_chat_id, 
                text=f"У канала {channel.url} нет привязанных аккаунтов."
            )
            return

        # Получаем группы канала
        groups = await group_db.get_all_groups_by_channel_guid(channel_guid=str(channel.guid))
        if not groups:
            await telegram_utils.send_message(
                chat_id=settings.admin_chat_id, 
                text=f"У канала {channel.url} нет привязанных групп."
            )
            return

        logger.info(f"Найдено {len(tg_accounts)} аккаунтов и {len(groups)} групп для канала {channel.url}")

        # Запускаем процесс репостинга
        await process_group_reposting(
            channel=channel,
            tg_accounts=tg_accounts,
            groups=groups,
            telegram_message_id=telegram_message_id
        )

    except Exception as e:
        logger.exception(f"Критическая ошибка при обработке сообщения {telegram_message_id} из канала {telegram_channel_id}: {e}")
        try:
            await telegram_utils.send_message(
                chat_id=settings.admin_chat_id, 
                text=f"Критическая ошибка при обработке сообщения {telegram_message_id}: {str(e)[:200]}"
            )
        except:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--telegram_message_id', type=int, required=True, help='The ID of the message')
    parser.add_argument('--telegram_channel_id', type=int, required=True, help='The ID of the channel')
    parser.add_argument('--log_filename', type=str, required=True, help='The log filename')

    args = parser.parse_args()
    telegram_channel_id = args.telegram_channel_id
    telegram_message_id = args.telegram_message_id

    # Настраиваем логирование
    logger.add(
        f"logs/{args.log_filename.replace('.log', '')}/{telegram_channel_id}-{telegram_message_id}.log", 
        rotation="1 day", 
        retention="10 days", 
        compression="zip"
    )
    
    logger.info(f"Начинаю обработку сообщения ID {telegram_message_id} из канала ID {telegram_channel_id}")

    try:
        asyncio.run(new_message_in_channel(
            telegram_channel_id=telegram_channel_id, 
            telegram_message_id=telegram_message_id
        ))
        logger.info("Обработка завершена успешно")
    except Exception as e:
        logger.exception(f"ЗАВЕРШИЛСЯ С ОШИБКОЙ: {e.__class__.__name__}: {e}")
        exit(1)