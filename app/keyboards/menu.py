from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardMarkup, InlineKeyboardButton


def menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="Каналы", callback_data="channels"),
        InlineKeyboardButton(text="Аккаунты", callback_data="accounts"),
        InlineKeyboardButton(text="Настройки", callback_data="settings"),
        InlineKeyboardButton(text="Статистика", callback_data="stats"),
    )
    return builder.adjust(1).as_markup()
