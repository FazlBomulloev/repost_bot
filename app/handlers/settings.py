from datetime import datetime

from aiogram import Router, F
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.keyboards import settings as settings_keyboard, general as general_keyboard
from app.states import Settings as SettingsStates
from core.settings import json_settings

router = Router()


@router.callback_query(F.data == "settings")
# @router.callback_query(F.data == "back_to_settings")
async def settings_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    await callback.message.edit_text(
        text="*️⃣ Управление настройками:",
        reply_markup=settings_keyboard.menu()
    )


@router.callback_query(F.data == "settings_pauses")
async def settings_pauses(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    await callback.message.edit_text(
        text="*️⃣ Выберите нужно действие:",
        reply_markup=settings_keyboard.pauses_menu()
    )


@router.callback_query(F.data == "pause_after_rate_reposts")
@router.callback_query(F.data == "number_reposts_before_pause")
@router.callback_query(F.data == "pause_between_reposts")
async def set_new_pause(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.set_settings_pause)
    await state.update_data(callback_data=callback.data)

    await callback.message.edit_text(
        text="*️⃣ Введите новое значение:",
        reply_markup=general_keyboard.back(callback_data="settings_pauses")
    )


@router.message(SettingsStates.set_settings_pause)
async def settings_pause(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    callback_data = state_data["callback_data"]
    try:
        new_value = int(message.text)
    except ValueError:
        await message.answer(
            text="*️⃣ Принимается только число, повторите попытку:",
            reply_markup=general_keyboard.back(callback_data="settings_pauses")
        )
        return

    await json_settings.async_set_attribute(item=callback_data, value=new_value)
    await message.answer(
        text="*️⃣ Успешно установил новое значение:",
        reply_markup=general_keyboard.back(callback_data="settings_pauses")
    )


@router.callback_query(F.data == "settings_stop_link")
async def settings_stop_link(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        text="*️⃣ Настройки стоп ссылок:",
        reply_markup=settings_keyboard.stop_link_menu()
    )


@router.callback_query(F.data == "add_stop_link")
async def add_stop_link(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.add_stop_link)
    await callback.message.edit_text(
        text="*️⃣ Пришлите ссылку для добавления:",
        reply_markup=general_keyboard.back(callback_data="settings_stop_link")
    )


@router.message(SettingsStates.add_stop_link)
async def add_stop_link_state(message: Message, state: FSMContext) -> None:
    await state.clear()
    stop_links = await json_settings.async_get_attribute("stop_links")
    if stop_links is None:
        stop_links = []
        
    if "\n" in message.text:
        for i in message.text.split("\n"):
            stop_links.append(i)
    else:
        stop_links.append(message.text)
    await json_settings.async_set_attribute("stop_links", stop_links)
    await message.answer(
        text="*️⃣ Успешно обновил данные!",
        reply_markup=general_keyboard.back(callback_data="settings_stop_link")
    )


@router.callback_query(F.data == "del_stop_link")
async def del_stop_link(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        text="*️⃣ Команда для удаления:\n/del_stop_link <ссылка>",
        reply_markup=general_keyboard.back(callback_data="settings_stop_link"),
        parse_mode=None
    )


@router.callback_query(F.data == "show_stop_links")
async def show_stop_links(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    text = "Стоп ссылки:\n"
    stop_links = await json_settings.async_get_attribute("stop_links")
    if stop_links is None:
        stop_links = []
    for stop_link in stop_links:
        text += f"{stop_link}\n"

    await callback.message.edit_text(
        text=text,
        reply_markup=general_keyboard.back(callback_data="settings_stop_link")
    )


@router.message(Command("del_stop_link"))
async def del_stop_link_state(message: Message, state: FSMContext, command: CommandObject) -> None:
    await state.clear()
    if not command.args:
        await message.answer(
            text="*️⃣ Правильная команда для удаления стоп ссылки:\n/del_stop_link <ссылка>",
            reply_markup=general_keyboard.back(callback_data="settings")
        )
    try:
        stop_links = (await json_settings.async_get_attribute("stop_links")).remove(command.args)
    except ValueError:
        await message.answer(text="*️⃣ Не нашел такой ссылки!", reply_markup=general_keyboard.back(callback_data="settings_stop_link"))
        return

    await json_settings.async_set_attribute("stop_links", stop_links)
    await message.answer(text="*️⃣ Успешно обновил данные!", reply_markup=general_keyboard.back(callback_data="settings_stop_link"))


@router.callback_query(F.data == "set_new_reaction")
async def set_new_reaction(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    await callback.message.edit_text(
        text="*️⃣ Поставьте новую реакцию:",
        reply_markup=settings_keyboard.reaction_menu()
    )


@router.callback_query(F.data == "reaction_love")
@router.callback_query(F.data == "reaction_ask")
@router.callback_query(F.data == "reaction_like")
async def set_reaction(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    await json_settings.async_set_attribute("reaction", callback.data.replace("reaction_", ""))
    await callback.message.edit_text(
        text="*️⃣ Установлена новая реакция!",
        reply_markup=general_keyboard.back(callback_data="set_new_reaction")
    )



@router.callback_query(F.data == "set_new_work_time")
async def set_new_work_time(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    await callback.message.edit_text(
        text="*️⃣ Выберите действие",
        reply_markup=settings_keyboard.new_time_work_menu()
    )


@router.callback_query(F.data == "set_start_time")
@router.callback_query(F.data == "set_end_time")
async def set_time(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.set_time)
    await state.update_data(callback_data=callback.data)
    await callback.message.edit_text(
        text="*️⃣ Введите время в формате ЧЧ:ММ",
        reply_markup=general_keyboard.back(callback_data="set_new_work_time")
    )


@router.message(SettingsStates.set_time)
async def set_time_state(message: Message, state: FSMContext) -> None:
    try:
        user_time = datetime.strptime(message.text, '%H:%M').time().strftime('%H:%M')
    except ValueError:
        await message.answer(
            text="*️⃣ Неправильный формат времени. Пожалуйста, отправь время в формате ЧЧ:ММ.",
            reply_markup=general_keyboard.back(callback_data="set_new_work_time")
        )
        return
    state_data = await state.get_data()
    if state_data["callback_data"] == "set_start_time":
        json_key = "start_time"
    else:
        json_key = "end_time"

    await json_settings.async_set_attribute(json_key, user_time)
    await state.clear()
    await message.answer(
        text="*️⃣ Успешно было установлено новое время.",
        reply_markup=general_keyboard.back(callback_data="set_new_work_time")
    )