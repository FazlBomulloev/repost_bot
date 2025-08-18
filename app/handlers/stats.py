from datetime import datetime, timedelta

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery

from app.keyboards import stats as stats_keyboard, general as general_keyboard
from core.models import channel as channel_db, repost as repost_db, tg_account as tg_account_db

router = Router()


@router.callback_query(F.data == "stats")
async def stats_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    
    # Получаем общую статистику
    working_accounts_count = len(await tg_account_db.get_tg_accounts_by_status_in_channel(status='WORKING'))
    free_accounts_count = len(await tg_account_db.get_tg_accounts_without_channel())
    
    info_text = (
        "*️⃣ Общая статистика:\n\n"
        f"📊 Кол-во рабочих аккаунтов: {working_accounts_count}\n"
        f"🆓 Кол-во свободных аккаунтов: {free_accounts_count}\n\n"
        "Выберите канал для детальной статистики:"
    )
    
    await callback.message.edit_text(
        text=info_text,
        reply_markup=stats_keyboard.menu(channels=await channel_db.get_channels())
    )


@router.callback_query(F.data.startswith("stats_channel_guid_"))
async def channel_detailed_stats(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    
    channel_guid = callback.data.replace("stats_channel_guid_", "")
    channel = await channel_db.get_channel_by_guid(guid=channel_guid)
    
    # Получаем статистику по репостам
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    reposts_today = await repost_db.get_reposts_by_date(channel_guid=channel_guid, date=today)
    reposts_yesterday = await repost_db.get_reposts_by_date(channel_guid=channel_guid, date=yesterday)
    
    # Получаем количество аккаунтов канала
    channel_accounts = await tg_account_db.get_tg_accounts_by_channel_guid(channel_guid=channel_guid)
    working_accounts = await tg_account_db.get_tg_accounts_by_channel_guid_and_status(
        channel_guid=channel_guid, 
        status="WORKING"
    )
    muted_accounts = await tg_account_db.get_tg_accounts_by_channel_guid_and_status(
        channel_guid=channel_guid, 
        status="MUTED"
    )
    
    stats_text = (
        f"*️⃣ Статистика канала: {channel.url}\n\n"
        f"📈 Репостов сегодня: {len(reposts_today)}\n"
        f"📊 Репостов вчера: {len(reposts_yesterday)}\n\n"
        f"👥 Количество акков у канала: {len(channel_accounts)}\n"
        f"  ├ ✅ Рабочих: {len(working_accounts)}\n"
        f"  └ 🔇 В муте: {len(muted_accounts)}\n"
    )
    
    await callback.message.edit_text(
        text=stats_text,
        reply_markup=general_keyboard.back(callback_data="stats"),
        disable_web_page_preview=True
    )