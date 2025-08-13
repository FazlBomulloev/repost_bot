from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardMarkup, InlineKeyboardButton


def menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Вывести список свободных аккаунтов", callback_data="display_list_of_free_accounts"))
    builder.row(InlineKeyboardButton(text="Перенести свободные аккаунты в канал", callback_data="transfer_free_accounts_to_channel"))
    builder.row(InlineKeyboardButton(text="Добавить аккаунты (TData)", callback_data="add_accounts"))
    builder.row(InlineKeyboardButton(text="Удалить аккаунты с базы данных", callback_data="del_accounts"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu"))
    return builder.as_markup()


def back_to_accounts() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="accounts"))
    return builder.as_markup()


def confirm_transfer_free_accounts() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="✅ Да", callback_data="confirm_transfer_free_accounts"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="transfer_free_accounts_to_channel"))
    return builder.as_markup()
