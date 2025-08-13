import random

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app import utils
from app.keyboards import stats as stats_keyboard, general as general_keyboard
from app.states import Channel as ChannelStates, Group as GroupStates, Account as AccountStates
from auto_reposting import telegram_utils

from core.models import channel as channel_db, repost as repost_db, tg_account as tg_account_db

router = Router()


@router.callback_query(F.data == "stats")
async def stats_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    print(await tg_account_db.get_tg_accounts_without_channel())
    await callback.message.edit_text(
        text="*️⃣ Управление статистикой:\n\n"
             f"*️⃣ Кол-во рабочих аккаунтов: {len(await tg_account_db.get_tg_accounts_by_status_in_channel(status='WORKING'))}\n"
             f"*️⃣ Кол-во удалённых аккаунтов: {len(await tg_account_db.get_tg_accounts_by_status(status='DELETED'))}\n"
             f"*️⃣ Кол-во свободных аккаунтов: {len(await tg_account_db.get_tg_accounts_without_channel())}\n",
        reply_markup=stats_keyboard.menu()
    )



@router.callback_query(F.data == "count_working_accounts_by_channel")
async def count_working_accounts_by_channel(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        text="*️⃣ Выберите канал для вывода статистики:",
        reply_markup=general_keyboard.choice_channel(
            channels=await channel_db.get_channels(),
            channel_callback="work_accs_by_channel_guid_",
            back_callback="stats"
        )
    )


@router.callback_query(F.data.startswith("work_accs_by_channel_guid_"))
async def count_working_accounts_by_channel_guid(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    channel_guid = callback.data.replace("work_accs_by_channel_guid_", "")
    accounts = await tg_account_db.get_tg_accounts_by_channel_guid_and_status(channel_guid=channel_guid, status="WORKING")

    await callback.message.edit_text(
        text=f"*️⃣ Кол-во рабочих аккаунтов: {len(accounts)}\n",
        reply_markup=general_keyboard.back(callback_data="stats")
    )


@router.callback_query(F.data == "count_mute_accounts_by_channel")
async def count_working_accounts_by_channel(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        text="*️⃣ Выберите канал для вывода статистики:",
        reply_markup=general_keyboard.choice_channel(
            channels=await channel_db.get_channels(),
            channel_callback="mute_accs_by_channel_guid_",
            back_callback="stats"
        )
    )


@router.callback_query(F.data.startswith("mute_accs_by_channel_guid_"))
async def count_mute_accounts_by_channel_guid(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    channel_guid = callback.data.replace("mute_accs_by_channel_guid_", "")
    accounts = await tg_account_db.get_tg_accounts_by_channel_guid_and_status(channel_guid=channel_guid, status="MUTED")

    await callback.message.edit_text(
        text=f"*️⃣ Кол-во мут аккаунтов: {len(accounts)}",
        reply_markup=general_keyboard.back(callback_data="stats")
    )


@router.callback_query(F.data == "repost_for_day")
async def repost_for_day_choice_channel(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        text="*️⃣ Выберите канал для вывода статистики:",
        reply_markup=general_keyboard.choice_channel(
            channels=await channel_db.get_channels(),
            channel_callback="repost_for_day_channel_guid_",
            back_callback="stats"
        )
    )


@router.callback_query(F.data.startswith("repost_for_day_channel_guid_"))
async def repost_for_day_choice_channel_guid(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    channel_guid = callback.data.replace("repost_for_day_channel_guid_", "")
    reposts = await repost_db.get_repost_for_day(channel_guid=channel_guid)
    await callback.message.edit_text(
        text=f"*️⃣ Кол-во репостов за день: {len(reposts)}",
        reply_markup=general_keyboard.back(callback_data="stats")
    )