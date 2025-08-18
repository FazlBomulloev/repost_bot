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
) -> bool:
    """if exist return True"""
    stop_links = await json_settings.async_get_attribute("stop_links")
    if stop_links is None:
        return False

    async with telegram_client:
        await telegram_client.get_entity(channel_url)
        message = await telegram_client.get_messages(telegram_channel_id, ids=telegram_message_id)

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


async def process_group_reposting(
        channel: channel_db.Channel,
        tg_accounts: List[tg_account_db.TGAccount],
        groups: List[group_db.Group],
        telegram_message_id: int
) -> None:
    counter_telegram_accounts = 0
    try:
        telegram_client = await telegram_utils2.get_authorized_tg_client_with_check_pause(
            accounts=tg_accounts,
            counter_telegram_accounts=counter_telegram_accounts
        )
    except exc.NoAccounts:
        await telegram_utils.send_message(chat_id=settings.admin_chat_id, text=f"У канала {channel.url} нету аккаунтов для работы.")
        return


    if await check_stop_link_in_message(tg_accounts=tg_accounts, telegram_client=telegram_client, channel_url=channel.url):
        return

    counter_number_reposts_before_pause = 0
    for group in groups:
        if counter_number_reposts_before_pause == await json_settings.async_get_attribute("number_reposts_before_pause"):
            counter_number_reposts_before_pause = 0
            account = tg_accounts[counter_telegram_accounts]
            await tg_account_db.add_pause(
                tg_account=account,
                pause_in_seconds=await json_settings.async_get_attribute("pause_after_rate_reposts")
            )
            logger.info(f"Аккаунт c номером {account.phone_number} поставлен на паузу!")
            counter_telegram_accounts += 1

            try:
                telegram_client = await telegram_utils2.get_authorized_tg_client_with_check_pause(
                    accounts=tg_accounts,
                    counter_telegram_accounts=counter_telegram_accounts
                )
            except exc.NoAccounts:
                await telegram_utils.send_message(chat_id=settings.admin_chat_id, text=f"У канала {channel.url} нету аккаунтов для работы.")
                return


        try:
            check_group = await telegram_utils2.checking_and_joining_if_possible(
                telegram_client=telegram_client,
                url=group.url,
                channel=channel
            )
        except errors.FloodWaitError:
            try:
                await telegram_utils.check_ban_in_spambot(telegram_client=telegram_client)
            except Exception as e:
                logger.info(f"Пропускаю группу {group.url} из-за ошибки в спам проверке")
                logger.error(f"Ошибка при проверке на спам {e.__class__.__name__}: {e}, меняю аккаунт")
                counter_telegram_accounts += 1

                try:
                    telegram_client = await telegram_utils2.get_authorized_tg_client_with_check_pause(
                        accounts=tg_accounts,
                        counter_telegram_accounts=counter_telegram_accounts
                    )
                except exc.NoAccounts:
                    await telegram_utils.send_message(chat_id=settings.admin_chat_id, text=f"У канала {channel.url} нету аккаунтов для работы.")
                    return
            continue
        except Exception as e:
            logger.error(f"Пропускаю группу {group.url} > Не смог получить или войти в группу. Ошибка: {e.__class__.__name__}: {e}")
            continue

        if not check_group:
            logger.info(f"Пропускаю группу {group.url} связи не проходением проверки!")
            continue

        try:
            check_repost_message = await telegram_utils.find_repost_message_in_group(
                telegram_client=telegram_client,
                channel_id=telegram_channel_id,
                need_message_id=telegram_message_id,
                url=group.url
            )
        except errors.FloodWaitError:
            try:
                await telegram_utils.check_ban_in_spambot(telegram_client=telegram_client)
            except Exception as e:
                logger.info(f"Пропускаю группу {group.url}")
                logger.error(f"Ошибка при проверке на спам {e.__class__.__name__}: {e}, меняю аккаунт")
                counter_telegram_accounts += 1

                try:
                    telegram_client = await telegram_utils2.get_authorized_tg_client_with_check_pause(
                        accounts=tg_accounts,
                        counter_telegram_accounts=counter_telegram_accounts
                    )
                except exc.NoAccounts:
                    await telegram_utils.send_message(chat_id=settings.admin_chat_id, text=f"У канала {channel.url} нету аккаунтов для работы.")
                    return
            continue
        except Exception as e:
            logger.error(f"Пропускаю группу {group.url} > Не смог получить в группе репост сообщения. Ошибка: {e.__class__.__name__}: {e}")
            continue

        if check_repost_message:
            logger.info(f"Пропускаю группу {group.url} связи с нахождением репост сообщения")
            continue

        logger.info(f"Начинаю обрабатывать группу {group.url}")

        trying_to_repost_counter = 0
        repost_is_complete = False
        while trying_to_repost_counter < 3 and not repost_is_complete:
            logger.info(f"Начинаю {trying_to_repost_counter + 1} попытку репостинга в группу {group.url}")
            try:
                result_reposting = await telegram_utils2.repost_in_group_by_message_id(
                    message_id=telegram_message_id,
                    telegram_client=telegram_client,
                    telegram_channel_id=telegram_channel_id,
                    channel_url=channel.url,
                    group_url=group.url
                )
                if result_reposting:
                    await repost_db.create_repost(
                        repost_in=repost_schemas.RepostCreate(
                            channel_guid=channel.guid,
                            repost_message_id=telegram_message_id,
                            created_at=datetime.now().date()
                        )
                    )
                    logger.info(f"Успешно сделан репост сообщения из канала {channel.url} в группу {group.url}")
                    counter_number_reposts_before_pause += 1
                    repost_is_complete = True

            except errors.FloodWaitError:
                try:
                    await telegram_utils.check_ban_in_spambot(telegram_client=telegram_client)
                except Exception as e:
                    counter_telegram_accounts += 1
                    logger.error(f"Ошибка при проверке на спам {e.__class__.__name__}: {e}, меняю аккаунт")

                    try:
                        telegram_client = await telegram_utils2.get_authorized_tg_client_with_check_pause(
                            accounts=tg_accounts,
                            counter_telegram_accounts=counter_telegram_accounts
                        )
                    except exc.NoAccounts:
                        await telegram_utils.send_message(chat_id=settings.admin_chat_id, text=f"У канала {channel.url} нету аккаунтов для работы.")
                        return
            except (errors.ChatWriteForbiddenError, errors.UserBannedInChannelError) as e:
                logger.error(f"Отправка репост сообщения в группу {group.url} невозможна! Ошибка {e.__class__.__name__}: {e}")
                trying_to_repost_counter = 3
            except Exception as e:
                logger.error(f"Ошибка {e.__class__.__name__}: {e} при отправке репост сообщения в группу {group.url}")

            trying_to_repost_counter += 1

        if not repost_is_complete:
            logger.info(f"Не смог сделать репост сообщения в группу {group.url}")


async def new_message_in_channel(telegram_channel_id: int, telegram_message_id: int) -> None:
    channel = await channel_db.get_channel_by_telegram_channel_id(telegram_channel_id=telegram_channel_id)

    tg_accounts = await tg_account_db.get_tg_accounts_by_channel_guid(channel_guid=str(channel.guid))
    if len(tg_accounts) == 0:
        await telegram_utils.send_message(chat_id=settings.admin_chat_id, text=f"У канала {channel.url} нету аккаунтов для работы.")
        return

    groups = await group_db.get_all_groups_by_channel_guid(channel_guid=str(channel.guid))
    if len(groups) == 0:
        await telegram_utils.send_message(chat_id=settings.admin_chat_id, text=f"У канала {channel.url} нету групп для работы.")
        return

    await process_group_reposting(
        channel=channel,
        tg_accounts=tg_accounts,
        groups=groups,
        telegram_message_id=telegram_message_id
    )



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--telegram_message_id', type=int, required=True, help='The ID of the message')
    parser.add_argument('--telegram_channel_id', type=int, required=True, help='The ID of the channel')
    parser.add_argument('--log_filename', type=str, required=True, help='The ID of the channel')

    args = parser.parse_args()
    telegram_channel_id = args.telegram_channel_id
    telegram_message_id = args.telegram_message_id

    logger.add(f"logs/{args.log_filename.replace('.log', '')}/{telegram_channel_id}-{telegram_message_id}.log", rotation="1 day", retention="10 days", compression="zip")
    logger.info(f"Получено новое сообщение ID {telegram_message_id} с ID канала {telegram_channel_id}")

    try:
        asyncio.run(new_message_in_channel(telegram_channel_id=args.telegram_channel_id, telegram_message_id=args.telegram_message_id))
        logger.info("Завершил работу")
    except Exception as e:
        logger.exception(f"ЗАВЕРШИЛСЯ С ОШИБКОЙ: {e.__class__.__name__}: {e}")
