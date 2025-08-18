from typing import List

from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardMarkup, InlineKeyboardButton
from core.schemas import channel as channel_schemas


def choice_channel(channels: List[channel_schemas.ChannelInDB], channel_callback: str, back_callback: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for channel in channels:
        print(f"{channel_callback}{channel.guid}")
        builder.row(InlineKeyboardButton(text=channel.url, callback_data=f"{channel_callback}{channel.guid}"))

    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data=back_callback))
    return builder.adjust(1).as_markup()


def back(callback_data: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data=callback_data))
    return builder.adjust(1).as_markup()
