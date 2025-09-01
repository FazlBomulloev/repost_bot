from datetime import datetime, timedelta

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery

from app.keyboards import stats as stats_keyboard, general as general_keyboard
from core.models import channel as channel_db, repost as repost_db, tg_account as tg_account_db

router = Router()


def format_uptime(seconds: float) -> str:
    """Форматирует время работы в читаемый вид"""
    if seconds < 60:
        return f"{seconds:.0f} сек"
    elif seconds < 3600:
        return f"{seconds/60:.0f} мин"
    else:
        return f"{seconds/3600:.1f} часов"


async def get_processor_stats() -> dict:
    """Получает статистику процессора сообщений"""
    try:
        # Пытаемся импортировать процессор из главного файла
        from auto_reposting.__main__ import message_processor
        return message_processor.get_stats()
    except (ImportError, AttributeError):
        return {
            'running': False,
            'error': 'Процессор не запущен или недоступен'
        }


@router.callback_query(F.data == "stats")
async def stats_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    
    # Получаем общую статистику
    working_accounts_count = len(await tg_account_db.get_tg_accounts_by_status_in_channel(status='WORKING'))
    free_accounts_count = len(await tg_account_db.get_tg_accounts_without_channel())
    muted_accounts_count = len(await tg_account_db.get_tg_accounts_by_status(status='MUTED'))
    
    # Получаем статистику процессора
    processor_stats = await get_processor_stats()
    
    # Формируем сообщение
    info_text = "*️⃣ Общая статистика:\n\n"
    
    # Статистика аккаунтов
    info_text += f"👥 Аккаунты:\n"
    info_text += f"  ├ 🔴 Рабочих: {working_accounts_count}\n"
    info_text += f"  ├ 🆓 Свободных: {free_accounts_count}\n"
    info_text += f"  └ 🔇 В муте: {muted_accounts_count}\n\n"


    info_text += f"\n📊 Выберите канал для детальной статистики:"
    
    await callback.message.edit_text(
        text=info_text,
        reply_markup=stats_keyboard.menu(channels=await channel_db.get_channels())
    )


@router.callback_query(F.data == "processor_detailed_stats")
async def processor_detailed_stats(callback: CallbackQuery, state: FSMContext) -> None:
    """Детальная статистика процессора сообщений"""
    await state.clear()
    
    processor_stats = await get_processor_stats()
    
    if not processor_stats.get('running', False):
        await callback.message.edit_text(
            text="❌ Процессор сообщений не работает\n\n" + 
                 f"Ошибка: {processor_stats.get('error', 'Неизвестная')}",
            reply_markup=general_keyboard.back(callback_data="stats")
        )
        return
    
    # Основная информация
    stats_text = "⚡ Детальная статистика процессора:\n\n"
    
    # Общие метрики
    uptime = processor_stats.get('uptime_seconds', 0)
    stats_text += f"⏱️ Время работы: {format_uptime(uptime)}\n"
    stats_text += f"👷 Воркеров: {processor_stats.get('workers_count', 0)}\n"
    stats_text += f"📋 В очереди: {processor_stats.get('queue_size', 0)}\n"
    stats_text += f"🔄 Обрабатывается: {processor_stats.get('processing_messages_count', 0)}\n\n"
    
    # Статистика обработки
    total_processed = processor_stats.get('total_processed', 0)
    total_errors = processor_stats.get('total_errors', 0)
    success_rate = processor_stats.get('success_rate', 0)
    
    stats_text += f"📊 Статистика обработки:\n"
    stats_text += f"  ├ ✅ Успешно: {total_processed}\n"
    stats_text += f"  ├ ❌ Ошибок: {total_errors}\n"
    stats_text += f"  └ 📈 Успешность: {success_rate:.1f}%\n\n"
    
    # Производительность
    messages_per_hour = processor_stats.get('messages_per_hour', 0)
    if messages_per_hour > 0:
        stats_text += f"🚀 Производительность:\n"
        stats_text += f"  ├ 📈 Сообщений/час: {messages_per_hour:.1f}\n"
        stats_text += f"  └ ⚡ Сообщений/мин: {messages_per_hour/60:.1f}\n\n"
    
    # Статистика воркеров
    workers = processor_stats.get('workers', [])
    if workers:
        stats_text += f"👷 Воркеры:\n"
        for worker in workers:
            worker_id = worker.get('worker_id', 'N/A')
            processed = worker.get('processed_count', 0)
            errors = worker.get('error_count', 0)
            running = "🟢" if worker.get('running', False) else "🔴"
            
            stats_text += f"  ├ {running} Воркер {worker_id}: {processed} обработано"
            if errors > 0:
                stats_text += f" ({errors} ошибок)"
            
            # Текущая задача
            current_task = worker.get('current_task')
            if current_task and current_task.get('message_id'):
                stats_text += f" [работает: {current_task['message_id']}]"
            
            stats_text += "\n"
        
        # Убираем последний символ для красивого отображения
        if stats_text.endswith("  ├"):
            stats_text = stats_text[:-3] + "  └"
    
    await callback.message.edit_text(
        text=stats_text,
        reply_markup=general_keyboard.back(callback_data="stats")
    )


@router.callback_query(F.data.startswith("stats_channel_guid_"))
async def channel_detailed_stats(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    
    channel_guid = callback.data.replace("stats_channel_guid_", "")
    channel = await channel_db.get_channel_by_guid(guid=channel_guid)
    
    # Получаем статистику по репостам
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    week_ago = today - timedelta(days=7)
    
    reposts_today = await repost_db.get_reposts_by_date(channel_guid=channel_guid, date=today)
    reposts_yesterday = await repost_db.get_reposts_by_date(channel_guid=channel_guid, date=yesterday)
    reposts_week = await repost_db.get_reposts_by_date_range(
        channel_guid=channel_guid, 
        start_date=week_ago, 
        end_date=today
    )
    
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
    
    # Получаем количество групп
    from core.models import group as group_db
    channel_groups = await group_db.get_all_groups_by_channel_guid(channel_guid=channel_guid)
    
    stats_text = f"📊 Статистика канала:\n{channel.url}\n\n"
    
    # Статистика репостов
    stats_text += f"📈 Репосты:\n"
    stats_text += f"  ├ 📅 Сегодня: {len(reposts_today)}\n"
    stats_text += f"  ├ 🗓️ Вчера: {len(reposts_yesterday)}\n"
    stats_text += f"  └ 📊 За неделю: {len(reposts_week)}\n\n"
    

    
    # Статистика аккаунтов
    stats_text += f"👥 Аккаунты канала:\n"
    stats_text += f"  ├ 📊 Всего: {len(channel_accounts)}\n"
    stats_text += f"  ├ ✅ Рабочих: {len(working_accounts)}\n"
    stats_text += f"  └ 🔇 В муте: {len(muted_accounts)}\n\n"
    
    # Статистика групп
    stats_text += f"👥 Группы:\n"
    stats_text += f"  └ 📊 Всего: {len(channel_groups)}\n\n"
    
    
    await callback.message.edit_text(
        text=stats_text,
        reply_markup=general_keyboard.back(callback_data="stats"),
        disable_web_page_preview=True
    )