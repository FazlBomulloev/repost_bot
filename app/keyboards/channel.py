from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardMarkup, InlineKeyboardButton

from core.models import channel as channel_db


def menu(channels: list[channel_db.Channel]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for channel in channels:
        print(f"channel_guid_{channel.guid}", len(f"channel_guid_{channel.guid}"))
        builder.row(InlineKeyboardButton(text=channel.url, callback_data=f"channel_guid_{channel.guid}"))

    builder.row(
        InlineKeyboardButton(text="Добавить канал", callback_data="add_channel"),
        # InlineKeyboardButton(text="Удалить канал", callback_data="del_channel"),
    )

    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu"))

    return builder.as_markup()


def channel_menu(channel_guid: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="Вывести список групп", callback_data=f"show_list_groups_chnl_guid_{channel_guid}")
    )
    builder.row(
        InlineKeyboardButton(text="Добавить группы", callback_data=f"add_groups_chnl_guid_{channel_guid}"),
        InlineKeyboardButton(text="Удалить группы", callback_data=f"del_groups_chnl_guid_{channel_guid}"),
    )
    builder.row(
        InlineKeyboardButton(text="Вывести список аккаунтов", callback_data=f"show_list_accs_chnl_guid_{channel_guid}")
    )
    builder.row(
        InlineKeyboardButton(text="Добавить аккаунты", callback_data=f"add_accs_chnl_guid_{channel_guid}"),
        InlineKeyboardButton(text="Удалить аккаунты", callback_data=f"del_accs_chnl_guid_{channel_guid}"),
    )
    builder.row(
        InlineKeyboardButton(
            text="Перенести аккаунты в другой канал",
            callback_data=f"tnsf_accs_to_antr_chl_guid_{channel_guid}"
        )
    )
    builder.row(
        InlineKeyboardButton(text="Удалить канал", callback_data=f"delete_channel_guid_{channel_guid}")
    )
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_channels"))
    return builder.as_markup()


def confirm_transfer_accounts(channel_guid: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="✅ Да", callback_data="confirm_transfer_accounts"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data=f"channel_guid_{channel_guid}"))
    return builder.as_markup()


def confirm_delete_channel(channel_guid: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="✅ Да", callback_data="confirm_delete_channel"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data=f"channel_guid_{channel_guid}"))
    return builder.as_markup()
