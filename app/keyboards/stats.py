from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardMarkup, InlineKeyboardButton


def menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Репосты за день", callback_data="repost_for_day"))
    builder.row(InlineKeyboardButton(text="Кол-во рабочих аккаунтов у канала", callback_data="count_working_accounts_by_channel"))
    builder.row(InlineKeyboardButton(text="Кол-во мут аккаунтов у канала", callback_data="count_mute_accounts_by_channel"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu"))
    return builder.as_markup()
