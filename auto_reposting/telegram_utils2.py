from typing import List, Optional, Tuple

from loguru import logger
from opentele.tl import TelegramClient

from telethon import errors
from telethon.errors import UserAlreadyParticipantError, ReactionInvalidError, FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import  ForwardMessagesRequest, SendReactionRequest
from telethon.tl.types import  Message, ReactionEmoji, InputPeerChannel

from core.models import tg_account as tg_account_db, channel as channel_db, group as group_db
from core.schemas import tg_account as tg_account_schemas
from . import exc, telegram_utils


async def create_tg_client(tg_account: tg_account_db.TGAccount) -> Optional[TelegramClient]:
    """Создает Telegram клиент с правильной обработкой ошибок и освобождением памяти"""
    client = None
    try:
        client = TelegramClient(StringSession(tg_account.string_session))
        await client.start(phone="1")
        
        if not await client.is_user_authorized():
            logger.warning(f"Аккаунт +{tg_account.phone_number} не авторизован")
            await client.disconnect()
            return None

        # Проверяем, что можем получить информацию о себе
        async with client:
            try:
                await client.get_me()
                logger.info(f"Клиент для +{tg_account.phone_number} создан успешно")
                return client
            except (errors.UnauthorizedError, errors.PhoneNumberInvalidError, errors.AuthKeyDuplicatedError):
                logger.warning(f"Аккаунт +{tg_account.phone_number} потерял авторизацию, помечаем как удаленный")
                await tg_account_db.update_tg_account(
                    tg_account=tg_account,
                    tg_account_update=tg_account_schemas.TGAccountUpdate(
                        status=tg_account_schemas.TGAccountStatus.deleted
                    )
                )
                return None
            except errors.FloodWaitError as e:
                logger.warning(f"FloodWait для +{tg_account.phone_number}: {e}")
                return None
            except Exception as e:
                logger.error(f"Неожиданная ошибка при проверке аккаунта +{tg_account.phone_number}: {e}")
                return None
                
    except (errors.UnauthorizedError, errors.PhoneNumberInvalidError, errors.AuthKeyDuplicatedError):
        logger.warning(f"Аккаунт +{tg_account.phone_number} не может быть авторизован, помечаем как удаленный")
        await tg_account_db.update_tg_account(
            tg_account=tg_account,
            tg_account_update=tg_account_schemas.TGAccountUpdate(
                status=tg_account_schemas.TGAccountStatus.deleted
            )
        )
        if client:
            try:
                await client.disconnect()
            except:
                pass
        return None
    except errors.FloodWaitError as e:
        logger.warning(f"FloodWait при создании клиента для +{tg_account.phone_number}: {e}")
        if client:
            try:
                await client.disconnect()
            except:
                pass
        return None
    except Exception as e:
        logger.error(f"Ошибка при создании клиента для +{tg_account.phone_number}: {e}")
        if client:
            try:
                await client.disconnect()
            except:
                pass
        return None


async def get_authorized_tg_client_with_check_pause(
        accounts: List[tg_account_db.TGAccount],
        start_index: int = 0
) -> Tuple[Optional[TelegramClient], int]:
    current_index = start_index
    
    while current_index < len(accounts):
        tg_account = accounts[current_index]
        
        # Проверяем статус аккаунта
        if tg_account.status != "WORKING":
            logger.info(f"Аккаунт +{tg_account.phone_number} имеет статус {tg_account.status}, пропускаем")
            current_index += 1
            continue
        
        # Проверяем паузу
        if tg_account.last_datetime_pause and tg_account.pause_in_seconds:
            if not await tg_account_db.has_pause_paused(tg_account):
                logger.info(f"Аккаунт +{tg_account.phone_number} на паузе, пропускаем")
                current_index += 1
                continue

        # Пытаемся создать клиент
        try:
            tg_client = await create_tg_client(tg_account)
            if tg_client is not None:
                logger.info(f"Авторизован аккаунт: +{tg_account.phone_number}")
                return tg_client, current_index
            else:
                logger.warning(f"Не удалось создать клиент для +{tg_account.phone_number}")
                current_index += 1
                continue
                
        except Exception as e:
            logger.error(f"Ошибка при создании клиента для +{tg_account.phone_number}: {e}")
            current_index += 1
            continue

    # Аккаунты закончились
    raise exc.NoAccounts("Нет доступных аккаунтов!")


async def checking_and_joining_if_possible(telegram_client: TelegramClient, url: str, channel: channel_db.Channel) -> bool:
    """Проверяет группу и присоединяется к ней если возможно"""
    try:
        async with telegram_client:
            try:
                group = await telegram_client.get_entity(url)
            except (errors.UsernameNotOccupiedError, errors.ChannelPrivateError, errors.ChannelInvalidError, ValueError) as e:
                logger.error(f"Группа {url} недоступна: {type(e).__name__} - {e}")
                #await group_db.delete_group_by_url(channel_guid=channel.guid, url=url)
                return False
            except Exception as e:
                logger.error(f"Неожиданная ошибка при получении группы {url}: {e}")
                return False

            try:
                await telegram_client(JoinChannelRequest(group))
                logger.info(f"Успешно присоединился к группе {url}")
                return True
            except UserAlreadyParticipantError:
                logger.info(f"Уже участник группы {url}")
                return True
            except (errors.UsernameNotOccupiedError, errors.ChannelPrivateError, errors.ChannelInvalidError, ValueError) as e:
                logger.error(f"Не могу присоединиться к группе {url}: {type(e).__name__} - {e}")
                #await group_db.delete_group_by_url(channel_guid=channel.guid, url=url)
                return False
            except errors.InviteRequestSentError:
                logger.info(f"Отправлен запрос на вступление в группу {url}")
                return False
            except errors.FloodWaitError as e:
                logger.warning(f"FloodWait при вступлении в группу {url}: {e}")
                raise  # Передаем FloodWait наверх
            except Exception as e:
                logger.error(f"Неожиданная ошибка при вступлении в группу {url}: {e}")
                return False
                
    except Exception as e:
        logger.error(f"Критическая ошибка в checking_and_joining_if_possible: {e}")
        return False


async def repost_in_group_by_message_id(
        message_id: int,
        telegram_client: TelegramClient,
        telegram_channel_id: int,
        channel_url: str,
        group_url: str
) -> bool:
    """Делает репост сообщения в группу"""
    try:
        async with telegram_client:
            telegram_group = await telegram_client.get_entity(group_url)
            await telegram_client.get_entity(channel_url)
            message = await telegram_client.get_messages(telegram_channel_id, ids=message_id)
            
            if not message:
                logger.error(f"Сообщение {message_id} не найдено в канале {telegram_channel_id}")
                return False
            
            await telegram_client(ForwardMessagesRequest(
                from_peer=message.peer_id, 
                id=[message.id], 
                to_peer=telegram_group
            ))
            logger.info(f"Успешно сделан репост в группу {group_url}")
            return True
            
    except Exception as e:
        logger.error(f"Ошибка при репосте в группу {group_url}: {e}")
        return False


async def send_reaction_by_telegram_client(
        telegram_client: TelegramClient,
        message: Message,
        channel_url: str,
        reaction: ReactionEmoji
) -> bool:
    """Ставит реакцию на сообщение"""
    try:
        async with telegram_client:
            channel = await telegram_client.get_entity(channel_url)
            await telegram_client(SendReactionRequest(
                peer=InputPeerChannel(
                    channel_id=channel.id,
                    access_hash=channel.access_hash
                ),
                msg_id=message.id,
                reaction=[reaction]
            ))
            return True
    except ReactionInvalidError:
        logger.info("Недопустимая реакция")
        return False
    except Exception as e:
        logger.error(f"Ошибка при установке реакции: {e}")
        return False


async def send_reaction_with_accounts_on_message(
        tg_accounts: List[tg_account_db.TGAccount],
        message: Message,
        channel_url: str,
        emoji_reaction: str
) -> None:
    """Ставит реакции от нескольких аккаунтов с правильным управлением памятью"""
    
    # Определяем реакцию
    reaction_map = {
        "love": "❤️",
        "ask": "🙏", 
        "like": "👍"
    }
    reaction = ReactionEmoji(emoticon=reaction_map.get(emoji_reaction, "❤️"))

    # Ограничиваем количество аккаунтов для реакций (избегаем перегрузки)
    max_reactions = min(len(tg_accounts), 5)
    selected_accounts = tg_accounts[:max_reactions]

    for tg_account in selected_accounts:
        telegram_client = None
        try:
            telegram_client = await create_tg_client(tg_account)
            
            if telegram_client is None:
                logger.warning(f"Не удалось создать клиент для реакции +{tg_account.phone_number}")
                continue

            success = await send_reaction_by_telegram_client(
                telegram_client=telegram_client,
                message=message,
                channel_url=channel_url,
                reaction=reaction
            )
            
            if success:
                logger.info(f"Реакция поставлена от +{tg_account.phone_number}")
            
        except FloodWaitError:
            logger.warning(f"FloodWait при установке реакции от +{tg_account.phone_number}")
            try:
                await telegram_utils.check_ban_in_spambot(telegram_client=telegram_client)
            except:
                pass
        except Exception as e:
            logger.error(f"Ошибка при установке реакции от +{tg_account.phone_number}: {e}")
        finally:
            # Обязательно закрываем соединение
            if telegram_client:
                try:
                    await telegram_client.disconnect()
                except:
                    pass
                telegram_client = None