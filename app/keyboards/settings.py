from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardMarkup, InlineKeyboardButton


def menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔄 Настройка пауз аккаунтов", callback_data="settings_pauses"))  
    builder.row(InlineKeyboardButton(text="🚫 Настройки стоп ссылок", callback_data="settings_stop_link"))
    builder.row(InlineKeyboardButton(text="🕒 Установить новое время работы", callback_data="set_new_work_time"))
    builder.row(InlineKeyboardButton(text="❤️ Установить новую реакцию", callback_data="set_new_reaction"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu"))
    return builder.as_markup()


def pauses_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Пауза после нормы репостов", callback_data="pause_after_rate_reposts"))
    builder.row(InlineKeyboardButton(text="Кол-во репостов до паузы", callback_data="number_reposts_before_pause"))
    builder.row(InlineKeyboardButton(text="⏱️ Задержка между репостами", callback_data="set_delay_between_reposts"))
    builder.row(InlineKeyboardButton(text="🔄 Задержка между группами", callback_data="set_delay_between_groups"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="settings"))
    return builder.as_markup()

def sequential_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="⏱️ Задержка между репостами", callback_data="set_delay_between_reposts"))
    builder.row(InlineKeyboardButton(text="🔄 Задержка между группами", callback_data="set_delay_between_groups"))
    builder.row(InlineKeyboardButton(text="📊 Максимум групп на пост", callback_data="set_max_groups_per_post"))
    builder.row(InlineKeyboardButton(text="🚫 Проверка стоп-ссылок", callback_data="toggle_check_stop_links"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="settings"))
    return builder.as_markup()


def stop_link_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📋 Вывести все ссылки", callback_data="show_stop_links"))
    builder.row(InlineKeyboardButton(text="➕ Добавить ссылку", callback_data="add_stop_link"))
    builder.row(InlineKeyboardButton(text="➖ Удалить ссылку", callback_data="del_stop_link"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="settings"))
    return builder.as_markup()


def reaction_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="❤️ Сердце", callback_data="reaction_love"))
    builder.row(InlineKeyboardButton(text="🙏 Мольба", callback_data="reaction_ask"))
    builder.row(InlineKeyboardButton(text="👍 Лайк", callback_data="reaction_like"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="settings"))
    return builder.as_markup()


def new_time_work_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🌅 Установить начало рабочего времени", callback_data="set_start_time"))
    builder.row(InlineKeyboardButton(text="🌇 Установить конец рабочего времени", callback_data="set_end_time"))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="settings"))
    return builder.as_markup()