from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardMarkup, InlineKeyboardButton

from core.models import channel as channel_db


def menu(channels: list[channel_db.Channel]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    
    
    
    # Добавляем кнопки для каждого канала
    for channel in channels:
        # Обрезаем длинные URL для красивого отображения
        display_url = channel.url
        if len(display_url) > 35:
            display_url = display_url[:32] + "..."
            
        builder.row(InlineKeyboardButton(
            text=f"📺 {display_url}", 
            callback_data=f"stats_channel_guid_{channel.guid}"
        ))
    
    # Кнопка назад
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu"))
    
    return builder.as_markup()