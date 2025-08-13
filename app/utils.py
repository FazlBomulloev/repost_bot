import asyncio
import logging
import os
import zipfile
from asyncio import Future
from concurrent.futures import ProcessPoolExecutor
from shutil import rmtree

import aiofiles
import aiohttp
from aiofiles.os import makedirs
from aiogram.types import CallbackQuery, Message
from opentele.api import UseCurrentSession, API
from opentele.td import TDesktop
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from telethon import TelegramClient
from telethon.errors import UserDeactivatedBanError
from telethon.sessions import StringSession

from core import settings
from core.models import tg_account as tg_account_db
from core.schemas import tg_account as tg_account_schemas
from core.settings import bot


class TelegramAccount(BaseModel):
    user_id: int
    session: str
    phone_number: str


def assert_message(query: CallbackQuery | Message):
    if isinstance(query, CallbackQuery):
        message = query.message
        assert isinstance(message, Message)
    else:
        message = query
    return message



async def get_telegram_account(path_to_tdata: str) -> TelegramAccount:
    td = TDesktop(path_to_tdata)
    assert td.isLoaded()
    client = await td.ToTelethon(flag=UseCurrentSession, api=API.TelegramIOS.Generate())
    cl_st = await client.start(phone="1")
    async with cl_st:
        me = await client.get_me()

    tg_account = TelegramAccount(user_id=me.id, session=StringSession.save(client.session), phone_number=me.phone)
    return tg_account


def find_tdata_directory(root_directory):
    for root, dirs, files in os.walk(root_directory):
        for dir_name in dirs:
            if dir_name == 'tdata':
                return os.path.join(root, dir_name)
    return None


def _get_telegram_account(path_to_tdata: str) -> TelegramAccount:
    return asyncio.run(get_telegram_account(path_to_tdata))


# async def process_telegram_data(document_file_id: str, unique_document_file_id: str, channel_guid: str | None) -> str:
#     file_info = await bot.get_file(file_id=document_file_id)
#     file = await bot.download_file(file_path=file_info.file_path)
#     root_directory = document_file_id
#     await makedirs(root_directory, exist_ok=True)
#
#     zip_file = f"{root_directory}/{unique_document_file_id}.zip"
#     with open(zip_file, 'wb') as f:
#         f.write(file.getvalue())
#
#     with zipfile.ZipFile(zip_file, 'r') as zip_ref:
#         zip_ref.extractall(root_directory)
#
#     text = "*️⃣ Ошибки которые возникли:\n"
#     for dir in os.listdir(root_directory):
#         tdata_path = find_tdata_directory(root_directory + "/" + dir)
#
#         if not tdata_path:
#             text += "*️⃣ Не нашел папку 'tdata'\n"
#             continue
#
#         tdata_path.replace("\\", "/")
#
#         try:
#             telegram_account = await get_telegram_account(path_to_tdata=tdata_path)
#         except UserDeactivatedBanError as e:
#             text += f"*️⃣ Не смог авторизоваться, ошибка: {e.__class__.__name__}: {e}\n"
#             continue
#         except Exception as e:
#             text += f"*️⃣ Не смог авторизоваться, ошибка: {e.__class__.__name__}: {e}\n"
#             continue
#         finally:
#             rmtree(root_directory + "/" + dir)
#
#         try:
#             await tg_account_db.create_tg_account(
#                 tg_account_in=tg_account_schemas.TGAccountCreate(
#                     channel_guid=channel_guid,
#                     telegram_id=telegram_account.user_id,
#                     last_datetime_pause=None,
#                     pause_in_seconds=None,
#                     phone_number=telegram_account.phone_number,
#                     string_session=telegram_account.session,
#                     status=tg_account_schemas.TGAccountStatus.working
#                 )
#             )
#         except IntegrityError:
#             text += f"*️⃣ Аккаунт https://t.me/+{telegram_account.phone_number} уже есть в базе данных"
#             continue
#
#     return text



async def download_file(session, file_info):
    async with session.get(f"https://api.telegram.org/file/bot{settings.settings.bot_token.get_secret_value()}/{file_info.file_path}") as response:
        return await response.read()


async def process_tdata_file(dir, channel_guid):
    tdata_path = find_tdata_directory(dir)

    if not tdata_path:
        return "*️⃣ Не нашел папку 'tdata'\n"
    tdata_path.replace("\\", "/")

    try:
        telegram_account = await asyncio.wait_for(get_telegram_account(path_to_tdata=tdata_path), timeout=3)
    except UserDeactivatedBanError as e:
        return f"*️⃣ Не смог авторизоваться, ошибка: {e.__class__.__name__}: {e}\n"
    except Exception as e:
        return f"*️⃣ Не смог авторизоваться, ошибка: {e.__class__.__name__}: {e}\n"
    finally:
        rmtree(dir)

    try:
        await tg_account_db.create_tg_account(
            tg_account_in=tg_account_schemas.TGAccountCreate(
                channel_guid=channel_guid,
                telegram_id=telegram_account.user_id,
                last_datetime_pause=None,
                pause_in_seconds=None,
                phone_number=telegram_account.phone_number,
                string_session=telegram_account.session,
                status=tg_account_schemas.TGAccountStatus.working
            )
        )
    except IntegrityError:
        return f"*️⃣ Аккаунт https://t.me/+{telegram_account.phone_number} уже есть в базе данных"

    return ""


async def process_telegram_data(document_file_id: str, unique_document_file_id: str, channel_guid: str | None) -> str:
    bot_file_info = await bot.get_file(file_id=document_file_id)

    async with aiohttp.ClientSession() as session:
        file = await download_file(session, bot_file_info)

    root_directory = document_file_id
    zip_file = f"{root_directory}/{unique_document_file_id}.zip"
    os.makedirs(root_directory, exist_ok=True)

    async with aiofiles.open(zip_file, 'wb') as f:
        await f.write(file)

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(root_directory)

    text = "*️⃣ Ошибки которые возникли:\n"

    tasks = []

    for dir in os.listdir(root_directory):
        if dir != f"{unique_document_file_id}.zip":
            tasks.append(process_tdata_file(f"{root_directory}/{dir}", channel_guid))

    for result in await asyncio.gather(*tasks):
        text += result

    return text
