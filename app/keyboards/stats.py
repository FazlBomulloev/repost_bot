from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardMarkup, InlineKeyboardButton

from core.models import channel as channel_db


def menu(channels: list[channel_db.Channel]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    # Добавляем кнопки для каждого канала
    for channel in channels:
        builder.row(InlineKeyboardButton(
            text=channel.url, 
            callback_data=f"stats_channel_guid_{channel.guid}"
        ))
    
    # Кнопка назад
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu"))
    
    return builder.as_markup()