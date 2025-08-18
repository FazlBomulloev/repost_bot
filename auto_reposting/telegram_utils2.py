from datetime import datetime
from typing import List

from loguru import logger
from opentele.tl import TelegramClient

from telethon import errors
from telethon.errors import UserAlreadyParticipantError, ReactionInvalidError, FloodWaitError, ForbiddenError
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.contacts import UnblockRequest
from telethon.tl.functions.messages import GetHistoryRequest, ForwardMessagesRequest, SendReactionRequest
from telethon.tl.types import PeerChannel, Message, ReactionEmoji, InputPeerChannel

from core.models import tg_account as tg_account_db, channel as channel_db, group as group_db
from core.schemas import tg_account as tg_account_schemas
from core.settings import bot, settings
from . import exc, telegram_utils


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

    if not await client.is_user_authorized():
        return

    async with client:
        try:
            await client.get_me()
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
            logger.error(f"Ошибка в создание telegram клиента {e.__class__.__name__}: {e}")
            return

    return client


async def get_authorized_tg_client_with_check_pause(
        accounts: List[tg_account_db.TGAccount],
        counter_telegram_accounts: int
) -> TelegramClient:
    while True:
        if len(accounts) <= counter_telegram_accounts:
            raise exc.NoAccounts("Нету больше аккаунтов!")

        tg_account = accounts[counter_telegram_accounts]
        if tg_account.last_datetime_pause and tg_account.pause_in_seconds:
            if not await tg_account_db.has_pause_paused(tg_account=tg_account):
                counter_telegram_accounts += 1
                continue

        tg_client = await create_tg_client(tg_account=tg_account)
        if tg_client is not None:
            logger.info(f"Авторизован аккаунт: +{tg_account.phone_number}")
            return tg_client

        counter_telegram_accounts += 1


async def checking_and_joining_if_possible(telegram_client: TelegramClient, url: str, channel: channel_db.Channel) -> bool:
    async with telegram_client:
        try:
            group = await telegram_client.get_entity(url)
        except (errors.UsernameNotOccupiedError, errors.ChannelPrivateError, errors.ChannelInvalidError, ValueError) as e:
            if isinstance(e, errors.UsernameNotOccupiedError):
                logger.error(f"Сработал utils 2 UsernameNotOccupiedError - {e}")
            elif isinstance(e, errors.ChannelPrivateError):
                logger.error(f"Сработал utils 2 ChannelPrivateError - {e}")
            elif isinstance(e, errors.ChannelInvalidError):
                logger.error(f"Сработал utils 2 ChannelInvalidError - {e}")
            elif isinstance(e, ValueError):
                logger.error(f"Сработал utils 2 ValueError - {e}")
            else:
                logger.error(f"Неизвестно utils 2 что сработало - {e}")
            await group_db.delete_group_by_url(channel_guid=channel.guid, url=url)
            return False

        try:
            await telegram_client(JoinChannelRequest(group))
        except UserAlreadyParticipantError:
            return True
        except (errors.UsernameNotOccupiedError, errors.ChannelPrivateError, errors.ChannelInvalidError, ValueError) as e:
            if isinstance(e, errors.UsernameNotOccupiedError):
                logger.error(f"Сработал utils 2 UsernameNotOccupiedError - {e}")
            elif isinstance(e, errors.ChannelPrivateError):
                logger.error(f"Сработал utils 2 ChannelPrivateError - {e}")
            elif isinstance(e, errors.ChannelInvalidError):
                logger.error(f"Сработал utils 2 ChannelInvalidError - {e}")
            elif isinstance(e, ValueError):
                logger.error(f"Сработал utils 2 ValueError - {e}")
            else:
                logger.error(f"Неизвестно utils 2 что сработало - {e}")
            await group_db.delete_group_by_url(channel_guid=channel.guid, url=url)
            return False
        except errors.InviteRequestSentError:
            return False

    return True


async def repost_in_group_by_message_id(
        message_id: int,
        telegram_client: TelegramClient,
        telegram_channel_id: int,
        channel_url: channel_db.Channel,
        group_url: str
) -> bool:
    """if success repost in group return True"""
    async with telegram_client:
        telegram_group = await telegram_client.get_entity(group_url)
        await telegram_client.get_entity(channel_url)
        message = await telegram_client.get_messages(telegram_channel_id, ids=message_id)
        await telegram_client(ForwardMessagesRequest(from_peer=message.peer_id, id=[message.id], to_peer=telegram_group))
        return True


async def send_reaction_by_telegram_client(
        telegram_client: TelegramClient,
        message: Message,
        channel_url: str,
        reaction: str
) -> None:
    async with telegram_client:
        channel = await telegram_client.get_entity(channel_url)
        await telegram_client(SendReactionRequest(
            peer=InputPeerChannel(
                channel_id=channel.id,
                access_hash=channel.access_hash
            ),
            msg_id=message.id,
            reaction=[reaction])
        )


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
            logger.error(f"{e.__class__.__name__}: {e}")
            continue

        if telegram_account_client is None:
            continue

        telegram_clients.append(telegram_account_client)

    for telegram_client in telegram_clients:
        try:
            await send_reaction_by_telegram_client(
                telegram_client=telegram_client,
                message=message,
                channel_url=channel_url,
                reaction=reaction
            )
        except ReactionInvalidError:
            continue
        except FloodWaitError:
            try:
                await telegram_utils.check_ban_in_spambot(telegram_client=telegram_client)
            except Exception as e:
                pass

            continue
        except Exception as e:
            logger.error(f"Ошибка при установки реакции {e.__class__.__name__}: {e}")
            continue
