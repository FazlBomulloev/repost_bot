from datetime import datetime
from typing import List

from loguru import logger
from opentele.tl import TelegramClient

from telethon import errors
from telethon.errors import UserAlreadyParticipantError, ReactionInvalidError, FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.contacts import UnblockRequest
from telethon.tl.functions.messages import GetHistoryRequest, ForwardMessagesRequest, SendReactionRequest
from telethon.tl.types import PeerChannel, Message, ReactionEmoji, InputPeerChannel

from core.models import tg_account as tg_account_db, channel as channel_db, group as group_db
from core.schemas import tg_account as tg_account_schemas
from core.settings import bot, settings
from . import exc



async def check_ban_in_spambot(telegram_client: TelegramClient) -> None:
    async with telegram_client:
        me = await telegram_client.get_me()
        phone_number = me.phone
        await telegram_client(UnblockRequest('@SpamBot'))

        async with telegram_client.conversation('@SpamBot') as conv:
            await conv.send_message('/start')
            msg = await conv.get_response()

            tg_account = await tg_account_db.get_tg_account_by_phone_number(phone_number=phone_number)

            if "utc" in msg.text.lower():
                await tg_account_db.update_tg_account(
                    tg_account=tg_account,
                    tg_account_update=tg_account_schemas.TGAccountUpdate(
                        status=tg_account_schemas.TGAccountStatus.muted,
                        last_datetime_pause=datetime.now(),
                        pause_in_seconds=432000
                    )
                )
                raise exc.TemporarilyBanned(f"Временный бан https://t.me/+{phone_number}")

            elif "while the account is limited" in msg.text.lower():
                await tg_account_db.update_tg_account(
                    tg_account=tg_account,
                    tg_account_update=tg_account_schemas.TGAccountUpdate(
                        status=tg_account_schemas.TGAccountStatus.deleted
                    )
                )
                raise exc.PermanentlyBanned(f"Навсегда в бане https://t.me/+{phone_number}")


async def create_tg_client(tg_account: tg_account_db.TGAccount) -> TelegramClient:
    client = TelegramClient(StringSession(tg_account.string_session))
    try:
        await client.start(phone="1")
    except (errors.UnauthorizedError, errors.PhoneNumberInvalidError, errors.AuthKeyDuplicatedError):
        await tg_account_db.update_tg_account(
            tg_account=tg_account,
            tg_account_update=tg_account_schemas.TGAccountUpdate(
                status=tg_account_schemas.TGAccountStatus.deleted
            )
        )
        return
    except errors.FloodWaitError:
        return

    except Exception as e:
        logger.exception(e)
        return

    async with client:
        try:
            if not await client.is_user_authorized():
                return

            await client.get_me()
            return client

        except (errors.UnauthorizedError, errors.PhoneNumberInvalidError, errors.AuthKeyDuplicatedError):
            await tg_account_db.update_tg_account(
                tg_account=tg_account,
                tg_account_update=tg_account_schemas.TGAccountUpdate(
                    status=tg_account_schemas.TGAccountStatus.deleted
                )
            )
            return

        except errors.FloodWaitError:
            return

        except Exception as e:
            logger.exception(e)
            return


async def get_authorized_tg_client_with_check_pause(
        accounts: List[tg_account_db.TGAccount],
        counter_telegram_accounts: int
) -> TelegramClient:
    while True:
        if len(accounts) < counter_telegram_accounts:
            raise exc.NoAccounts("Нету больше аккаунтов!")

        try:
            tg_account = accounts[counter_telegram_accounts]
            if tg_account.last_datetime_pause and tg_account.pause_in_seconds:
                if not tg_account_db.has_pause_paused(tg_account=tg_account):
                    counter_telegram_accounts += 1
                    continue

            tg_client = await create_tg_client(tg_account=tg_account)
            if tg_client is None:
                counter_telegram_accounts += 1
                continue
            else:
                logger.info(f"Авторизован аккаунт: +{tg_account.phone_number}")
                return tg_client
        except Exception as e:
            logger.exception(e)
            counter_telegram_accounts += 1
            continue


async def send_message(chat_id: int, text: str) -> None:
    await bot.send_message(chat_id=chat_id, text=text)
    await bot.session.close()


async def checking_and_joining_if_possible(tg_client: TelegramClient, url: str, channel: channel_db.Channel) -> bool:
    is_flood_wait = False
    async with tg_client:
        try:
            group = await tg_client.get_entity(url)
        except (errors.UsernameNotOccupiedError, errors.ChannelPrivateError, errors.ChannelInvalidError, ValueError) as e:
            if isinstance(e, errors.UsernameNotOccupiedError):
                logger.error(f"Сработал UsernameNotOccupiedError - {e}")
            elif isinstance(e, errors.ChannelPrivateError):
                logger.error(f"Сработал ChannelPrivateError - {e}")
            elif isinstance(e, errors.ChannelInvalidError):
                logger.error(f"Сработал ChannelInvalidError - {e}")
            elif isinstance(e, ValueError):
                logger.error(f"Сработал ValueError - {e}")
            else:
                logger.error(f"Неизвестно что сработало - {e}")
            await group_db.delete_group_by_url(channel_guid=channel.guid, url=url)
            return False
        except errors.FloodWaitError:
            is_flood_wait = True
        except Exception as e:
            logger.error(f"Не смог получить сущность группы. Ошибка: {e.__class__.__name__}: {e}")

            await send_message(
                chat_id=settings.admin_chat_id,
                text=f"Неизвестная ошибка!\nПодробнее: {e.__class__.__name__}: {e}"
            )
            return False

    if is_flood_wait:
        try:
            await check_ban_in_spambot(telegram_client=tg_client)
        except Exception as e:
            logger.exception(e)
        raise exc.UserFloodWait("UserFloodWait")


    async with tg_client:
        try:
            await tg_client(JoinChannelRequest(group))
        except UserAlreadyParticipantError:
            return True
        except (errors.UsernameNotOccupiedError, errors.ChannelPrivateError, errors.ChannelInvalidError, ValueError) as e:
            if isinstance(e, errors.UsernameNotOccupiedError):
                logger.error(f"Сработал UsernameNotOccupiedError - {e}")
            elif isinstance(e, errors.ChannelPrivateError):
                logger.error(f"Сработал ChannelPrivateError - {e}")
            elif isinstance(e, errors.ChannelInvalidError):
                logger.error(f"Сработал ChannelInvalidError - {e}")
            elif isinstance(e, ValueError):
                logger.error(f"Сработал ValueError - {e}")
            else:
                logger.error(f"Неизвестно что сработало - {e}")
            await group_db.delete_group_by_url(channel_guid=channel.guid, url=url)
            return False
        except FloodWaitError:
            is_flood_wait = True
        except Exception as e:
            logger.error(f"Не смог вступить в группу. Ошибка: {e.__class__.__name__}: {e}")

            await send_message(
                chat_id=settings.admin_chat_id,
                text=f"Неизвестная ошибка!\nПодробнее: {e.__class__.__name__}: {e}"
            )
            return False

    if is_flood_wait:
        try:
            await check_ban_in_spambot(telegram_client=tg_client)
        except Exception as e:
            logger.exception(e)
        raise exc.UserFloodWait("UserFloodWait")

    return True


async def find_repost_message_in_group(
        telegram_client: TelegramClient,
        channel_id: int,
        need_message_id: int,
        url: str
) -> None:
    async with telegram_client:
        telegram_group = await telegram_client.get_entity(url)
        for message in (await telegram_client(GetHistoryRequest(
                peer=telegram_group,
                limit=1000,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
        ))).messages:
            if message.fwd_from:
                try:
                    if message.fwd_from.from_id.channel_id == channel_id and message.fwd_from.channel_post == need_message_id:
                        return True
                except AttributeError:
                    continue

        return False



async def repost_in_group_by_message_id(
        message_id: int,
        telegram_client: TelegramClient,
        telegram_channel_id: int,
        channel: channel_db.Channel,
        group_url: str
) -> bool:
    """if success repost in group return True"""
    try:
        if await find_repost_message_in_group(
            telegram_client=telegram_client,
            channel_id=telegram_channel_id,
            need_message_id=message_id,
            url=group_url
        ):
            logger.info(f"Найден пост в группе {group_url}")
            return False
    except FloodWaitError:
        try:
            await check_ban_in_spambot(telegram_client=telegram_client)
        except Exception as e:
            logger.exception(e)
        return False
    except Exception as e:
        logger.exception(e)
        return False

    async with telegram_client:
        telegram_group = await telegram_client.get_entity(group_url)
        await telegram_client.get_entity(channel.url)
        message = await telegram_client.get_messages(telegram_channel_id, ids=message_id)

        try:
            await telegram_client(ForwardMessagesRequest(from_peer=message.peer_id, id=[message.id], to_peer=telegram_group))
        except errors.FloodWaitError:
            try:
                await check_ban_in_spambot(telegram_client=telegram_client)
            except Exception as e:
                logger.exception(e)
            return False
        except errors.RPCError as e:
            logger.error(f"Не смог отправить репост в группу. Ошибка: {e.__class__.__name__}: {e}")
            return False

        return True


async def send_reaction_with_accounts_on_message(
        tg_accounts: List[tg_account_db.TGAccount],
        message: Message,
        channel_url: str,
        emoji_reaction: str
) -> None:
    reaction = ReactionEmoji(emoticon="❤️")
    if emoji_reaction == "ask":
        reaction = ReactionEmoji(emoticon="🙏")
    elif emoji_reaction == "like":
        reaction = ReactionEmoji(emoticon="👍")

    telegram_clients = []
    for tg_account in tg_accounts:
        try:
            telegram_account_client = await create_tg_client(tg_account=tg_account)
        except Exception as e:
            logger.exception(f"{e.__class__.__name__}: {e}")
            continue
        telegram_clients.append(telegram_account_client)

    for tg_client in telegram_clients:
        is_flood_wait = False
        async with tg_client:
            channel = await tg_client.get_entity(channel_url)
            try:
                await tg_client(SendReactionRequest(
                    peer=InputPeerChannel(
                        channel_id=channel.id,
                        access_hash=channel.access_hash
                    ),
                    msg_id=message.id,
                    reaction=[reaction])
                )
            except ReactionInvalidError as e:
                logger.info(f"{e.__class__.__name__}: {e}")
            except FloodWaitError:
                is_flood_wait = True
            except Exception as e:
                logger.exception(f"{e.__class__.__name__}: {e}")

        if is_flood_wait:
            try:
                await check_ban_in_spambot(telegram_client=tg_client)
            except Exception as e:
                logger.exception(e)