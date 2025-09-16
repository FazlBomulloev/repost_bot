from datetime import datetime
from aiogram import Router, F
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.keyboards import settings as settings_keyboard, general as general_keyboard
from app.states import Settings as SettingsStates
from core.settings import json_settings

router = Router()


def format_time_unit(seconds: int) -> str:
    """Форматирует время в читаемый вид с единицами измерения"""
    if seconds < 60:
        return f"{seconds} сек"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} мин"
    elif seconds < 86400:
        hours = seconds // 3600
        return f"{hours} ч"
    else:
        days = seconds // 86400
        return f"{days} дн"


def seconds_to_minutes(seconds: int) -> int:
    """Переводит секунды в минуты"""
    return seconds // 60


def minutes_to_seconds(minutes: int) -> int:
    """Переводит минуты в секунды"""
    return minutes * 60


@router.callback_query(F.data == "settings")
async def settings_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    await callback.message.edit_text(
        text="*️⃣ Управление настройками:",
        reply_markup=settings_keyboard.menu()
    )


@router.callback_query(F.data == "settings_pauses")
async def settings_pauses(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    
    # Получаем текущие значения настроек
    try:
        pause_after_rate = await json_settings.async_get_attribute("pause_after_rate_reposts")
        number_reposts = await json_settings.async_get_attribute("number_reposts_before_pause") 
        pause_between = await json_settings.async_get_attribute("pause_between_reposts")
        
        # Форматируем текст с текущими значениями
        settings_text = f"""*️⃣ Текущие настройки пауз:

🔄 Пауза после нормы репостов: {format_time_unit(pause_after_rate)}
📊 Кол-во репостов до паузы: {number_reposts} шт
⏱️ Пауза между репостами: {format_time_unit(pause_between)}

Выберите настройку для изменения:"""
        
    except Exception as e:
        settings_text = "*️⃣ Ошибка загрузки настроек. Выберите нужное действие:"

    await callback.message.edit_text(
        text=settings_text,
        reply_markup=settings_keyboard.pauses_menu()
    )


# 🎯 НОВЫЕ НАСТРОЙКИ ДЛЯ ПОСЛЕДОВАТЕЛЬНОГО РЕЖИМА
@router.callback_query(F.data == "settings_sequential")
async def settings_sequential(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    
    try:
        delay_between_reposts = await json_settings.async_get_attribute("delay_between_reposts")
        delay_between_groups = await json_settings.async_get_attribute("delay_between_groups")
        max_groups_per_post = await json_settings.async_get_attribute("max_groups_per_post")
        check_stop_links = await json_settings.async_get_attribute("check_stop_links")
        
        settings_text = f"""*️⃣ Настройки последовательного репостинга:

⏱️ Задержка между репостами: {format_time_unit(delay_between_reposts)}
🔄 Задержка между группами: {format_time_unit(delay_between_groups)}
📊 Максимум групп на пост: {max_groups_per_post} шт
🚫 Проверка стоп-ссылок: {'✅ Включена' if check_stop_links else '❌ Отключена'}

Выберите настройку для изменения:"""
        
    except Exception as e:
        settings_text = "*️⃣ Настройки последовательного репостинга:"

    await callback.message.edit_text(
        text=settings_text,
        reply_markup=settings_keyboard.sequential_menu()
    )


@router.callback_query(F.data == "set_delay_between_reposts")
async def set_delay_between_reposts(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.set_sequential_setting)
    await state.update_data(setting_key="delay_between_reposts", input_unit="seconds")

    try:
        current_value = await json_settings.async_get_attribute("delay_between_reposts")
        
        hint_text = f"""*️⃣ Задержка между репостами разных сообщений

Текущее значение: {format_time_unit(current_value)}

⏰ Введите новое значение в секундах:
💡 Рекомендуется: 20-60 секунд для безопасности
"""
            
    except Exception:
        hint_text = "*️⃣ Введите задержку между репостами в секундах:"

    await callback.message.edit_text(
        text=hint_text,
        reply_markup=general_keyboard.back(callback_data="settings_sequential")
    )


@router.callback_query(F.data == "set_delay_between_groups")
async def set_delay_between_groups(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.set_sequential_setting)
    await state.update_data(setting_key="delay_between_groups", input_unit="seconds")

    try:
        current_value = await json_settings.async_get_attribute("delay_between_groups")
        
        hint_text = f"""*️⃣ Задержка между группами в одном репосте

Текущее значение: {format_time_unit(current_value)}

⏰ Введите новое значение в секундах:
💡 Рекомендуется: 3-10 секунд между группами
"""
            
    except Exception:
        hint_text = "*️⃣ Введите задержку между группами в секундах:"

    await callback.message.edit_text(
        text=hint_text,
        reply_markup=general_keyboard.back(callback_data="settings_sequential")
    )


@router.callback_query(F.data == "set_max_groups_per_post")
async def set_max_groups_per_post(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.set_sequential_setting)
    await state.update_data(setting_key="max_groups_per_post", input_unit="count")

    try:
        current_value = await json_settings.async_get_attribute("max_groups_per_post")
        
        hint_text = f"""*️⃣ Максимальное количество групп для репоста

Текущее значение: {current_value} групп

📊 Введите новое количество:
💡 Рекомендуется: 20-30 групп для оптимальной скорости
"""
            
    except Exception:
        hint_text = "*️⃣ Введите максимальное количество групп:"

    await callback.message.edit_text(
        text=hint_text,
        reply_markup=general_keyboard.back(callback_data="settings_sequential")
    )


@router.callback_query(F.data == "toggle_check_stop_links")
async def toggle_check_stop_links(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    
    try:
        current_value = await json_settings.async_get_attribute("check_stop_links")
        new_value = not current_value
        await json_settings.async_set_attribute("check_stop_links", new_value)
        
        status_text = "✅ Включена" if new_value else "❌ Отключена"
        await callback.message.edit_text(
            text=f"🚫 Проверка стоп-ссылок: {status_text}",
            reply_markup=general_keyboard.back(callback_data="settings_sequential")
        )
        
    except Exception as e:
        await callback.message.edit_text(
            text=f"❌ Ошибка изменения настройки: {e}",
            reply_markup=general_keyboard.back(callback_data="settings_sequential")
        )


@router.message(SettingsStates.set_sequential_setting)
async def set_sequential_setting(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    setting_key = state_data["setting_key"]
    input_unit = state_data.get("input_unit", "seconds")
    await state.clear()
    
    try:
        user_input = int(message.text)
        if user_input <= 0:
            raise ValueError("Значение должно быть больше 0")
    except ValueError:
        error_text = "*️⃣ Ошибка!\n\n"
        if input_unit == "seconds":
            error_text += "Введите положительное число секунд"
        elif input_unit == "count":
            error_text += "Введите положительное число"
        else:
            error_text += "Введите положительное число"
            
        await message.answer(
            text=error_text,
            reply_markup=general_keyboard.back(callback_data="settings_sequential")
        )
        return

    # Валидация значений
    if setting_key == "delay_between_reposts" and user_input > 300:
        await message.answer(
            text="⚠️ Слишком большая задержка между репостами (максимум 300 секунд)",
            reply_markup=general_keyboard.back(callback_data="settings_sequential")
        )
        return
    
    if setting_key == "delay_between_groups" and user_input > 60:
        await message.answer(
            text="⚠️ Слишком большая задержка между группами (максимум 60 секунд)",
            reply_markup=general_keyboard.back(callback_data="settings_sequential")
        )
        return
    
    if setting_key == "max_groups_per_post" and (user_input < 5 or user_input > 100):
        await message.answer(
            text="⚠️ Количество групп должно быть от 5 до 100",
            reply_markup=general_keyboard.back(callback_data="settings_sequential")
        )
        return

    await json_settings.async_set_attribute(setting_key, user_input)
    
    # Форматируем сообщение об успехе
    if setting_key == "delay_between_reposts":
        success_text = f"✅ Задержка между репостами: {format_time_unit(user_input)}"
    elif setting_key == "delay_between_groups":
        success_text = f"✅ Задержка между группами: {format_time_unit(user_input)}"
    elif setting_key == "max_groups_per_post":
        success_text = f"✅ Максимум групп на пост: {user_input} шт"
    else:
        success_text = "✅ Значение обновлено!"
    
    await message.answer(
        text=success_text,
        reply_markup=general_keyboard.back(callback_data="settings_sequential")
    )


# ОСТАЛЬНЫЕ СУЩЕСТВУЮЩИЕ НАСТРОЙКИ (без изменений)
@router.callback_query(F.data == "pause_after_rate_reposts")
async def set_pause_after_rate_reposts(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.set_settings_pause)
    await state.update_data(callback_data=callback.data, input_unit="minutes")

    try:
        current_value = await json_settings.async_get_attribute(callback.data)
        current_minutes = seconds_to_minutes(current_value)
        
        hint_text = f"""*️⃣ Пауза после нормы репостов

Текущее значение: {current_minutes} минут

⏰ Введите новое значение в минутах:
"""
            
    except Exception:
        hint_text = "*️⃣ Введите новое значение в минутах:"

    await callback.message.edit_text(
        text=hint_text,
        reply_markup=general_keyboard.back(callback_data="settings_pauses")
    )


@router.callback_query(F.data == "pause_between_reposts")
async def set_pause_between_reposts(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.set_settings_pause)
    await state.update_data(callback_data=callback.data, input_unit="minutes")

    try:
        current_value = await json_settings.async_get_attribute(callback.data)
        current_minutes = seconds_to_minutes(current_value)
        
        hint_text = f"""*️⃣ Пауза между репостами

Текущее значение: {current_minutes} минут

⏰ Введите новое значение в минутах:
"""
            
    except Exception:
        hint_text = "*️⃣ Введите новое значение в минутах:"

    await callback.message.edit_text(
        text=hint_text,
        reply_markup=general_keyboard.back(callback_data="settings_pauses")
    )


@router.callback_query(F.data == "number_reposts_before_pause")
async def set_number_reposts_before_pause(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.set_settings_pause)
    await state.update_data(callback_data=callback.data, input_unit="count")

    try:
        current_value = await json_settings.async_get_attribute(callback.data)
        
        hint_text = f"""*️⃣ Количество репостов до паузы

Текущее значение: {current_value} шт

📊 Введите новое количество:
"""
            
    except Exception:
        hint_text = "*️⃣ Введите новое количество:"

    await callback.message.edit_text(
        text=hint_text,
        reply_markup=general_keyboard.back(callback_data="settings_pauses")
    )


@router.message(SettingsStates.set_settings_pause)
async def settings_pause(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    callback_data = state_data["callback_data"]
    input_unit = state_data.get("input_unit", "seconds")
    await state.clear()
    
    try:
        user_input = int(message.text)
        if user_input <= 0:
            raise ValueError("Значение должно быть больше 0")
    except ValueError:
        error_text = "*️⃣ Ошибка!\n\n"
        if input_unit == "minutes":
            error_text += "Введите положительное число минут"
        elif input_unit == "count":
            error_text += "Введите положительное число репостов"
        else:
            error_text += "Введите положительное число"
            
        await message.answer(
            text=error_text,
            reply_markup=general_keyboard.back(callback_data="settings_pauses")
        )
        return

    # Преобразуем в секунды если нужно
    if input_unit == "minutes":
        final_value = minutes_to_seconds(user_input)
    else:
        final_value = user_input

    await json_settings.async_set_attribute(item=callback_data, value=final_value)
    
    # Форматируем сообщение об успехе
    if callback_data == "pause_after_rate_reposts":
        success_text = f"✅ Пауза после нормы репостов установлена: {user_input} минут"
    elif callback_data == "number_reposts_before_pause":
        success_text = f"✅ Количество репостов до паузы установлено: {user_input} шт"
    elif callback_data == "pause_between_reposts":
        success_text = f"✅ Пауза между репостами установлена: {user_input} минут"
    else:
        success_text = "✅ Значение обновлено!"
    
    await message.answer(
        text=success_text,
        reply_markup=general_keyboard.back(callback_data="settings_pauses")
    )


@router.callback_query(F.data == "settings_stop_link")
async def settings_stop_link(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    
    # Показываем количество стоп-ссылок
    try:
        stop_links = await json_settings.async_get_attribute("stop_links")
        if not stop_links:
            stop_links = []
        count = len(stop_links)
        settings_text = f"*️⃣ Настройки стоп-ссылок\n\nВсего стоп-ссылок: {count} шт\n\nВыберите действие:"
    except Exception:
        settings_text = "*️⃣ Настройки стоп-ссылок:"
    
    await callback.message.edit_text(
        text=settings_text,
        reply_markup=settings_keyboard.stop_link_menu()
    )


@router.callback_query(F.data == "add_stop_link")
async def add_stop_link(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.add_stop_link)
    await callback.message.edit_text(
        text="*️⃣ Пришлите ссылку(и) для добавления:\n\n💡 Можно отправить несколько ссылок с новой строки",
        reply_markup=general_keyboard.back(callback_data="settings_stop_link")
    )


@router.message(SettingsStates.add_stop_link)
async def add_stop_link_state(message: Message, state: FSMContext) -> None:
    await state.clear()
    stop_links = await json_settings.async_get_attribute("stop_links")
    if stop_links is None:
        stop_links = []
    
    added_count = 0
    if "\n" in message.text:
        for i in message.text.split("\n"):
            if i.strip():  # Игнорируем пустые строки
                stop_links.append(i.strip())
                added_count += 1
    else:
        stop_links.append(message.text.strip())
        added_count += 1
        
    await json_settings.async_set_attribute("stop_links", stop_links)
    await message.answer(
        text=f"✅ Добавлено {added_count} шт стоп-ссылок!\nВсего в базе: {len(stop_links)} шт",
        reply_markup=general_keyboard.back(callback_data="settings_stop_link")
    )


@router.callback_query(F.data == "del_stop_link")
async def del_stop_link(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        text="*️⃣ Удаление стоп-ссылки\n\nИспользуйте команду:\n/del_stop_link <ссылка>",
        reply_markup=general_keyboard.back(callback_data="settings_stop_link")
    )


@router.callback_query(F.data == "show_stop_links")
async def show_stop_links(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    stop_links = await json_settings.async_get_attribute("stop_links")
    if not stop_links:
        stop_links = []
    
    if len(stop_links) == 0:
        text = "*️⃣ Стоп-ссылки\n\n❌ Список пуст"
    else:
        text = f"*️⃣ Стоп-ссылки ({len(stop_links)} шт):\n\n"
        for i, stop_link in enumerate(stop_links, 1):
            text += f"{i}. {stop_link}\n"
    
    # Если текст слишком длинный, разбиваем на части
    if len(text) > 4000:
        text = text[:3900] + f"\n\n... и ещё {len(stop_links) - 50} ссылок"

    await callback.message.edit_text(
        text=text,
        reply_markup=general_keyboard.back(callback_data="settings_stop_link")
    )


@router.message(Command("del_stop_link"))
async def del_stop_link_state(message: Message, state: FSMContext, command: CommandObject) -> None:
    await state.clear()
    if not command.args:
        await message.answer(
            text="*️⃣ Правильная команда:\n/del_stop_link <ссылка>",
            reply_markup=general_keyboard.back(callback_data="settings_stop_link")
        )
        return
        
    try:
        stop_links = await json_settings.async_get_attribute("stop_links")
        if not stop_links:
            stop_links = []
            
        if command.args in stop_links:
            stop_links.remove(command.args)
            await json_settings.async_set_attribute("stop_links", stop_links)
            await message.answer(
                text=f"✅ Стоп-ссылка удалена!\nОсталось в базе: {len(stop_links)} шт",
                reply_markup=general_keyboard.back(callback_data="settings_stop_link")
            )
        else:
            await message.answer(
                text="❌ Такой стоп-ссылки нет в базе!",
                reply_markup=general_keyboard.back(callback_data="settings_stop_link")
            )
    except Exception as e:
        await message.answer(
            text=f"❌ Ошибка при удалении: {e}",
            reply_markup=general_keyboard.back(callback_data="settings_stop_link")
        )


@router.callback_query(F.data == "set_new_reaction")
async def set_new_reaction(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    # Показываем текущую реакцию
    try:
        current_reaction = await json_settings.async_get_attribute("reaction")
        reaction_map = {
            "love": "❤️ Сердце",
            "ask": "🙏 Мольба", 
            "like": "👍 Лайк"
        }
        current_text = reaction_map.get(current_reaction, current_reaction)
        settings_text = f"*️⃣ Настройка реакций\n\nТекущая реакция: {current_text}\n\nВыберите новую:"
    except Exception:
        settings_text = "*️⃣ Поставьте новую реакцию:"

    await callback.message.edit_text(
        text=settings_text,
        reply_markup=settings_keyboard.reaction_menu()
    )


@router.callback_query(F.data == "reaction_love")
@router.callback_query(F.data == "reaction_ask")
@router.callback_query(F.data == "reaction_like")
async def set_reaction(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    reaction_type = callback.data.replace("reaction_", "")
    await json_settings.async_set_attribute("reaction", reaction_type)
    
    reaction_map = {
        "love": "❤️ Сердце",
        "ask": "🙏 Мольба", 
        "like": "👍 Лайк"
    }
    
    await callback.message.edit_text(
        text=f"✅ Установлена реакция: {reaction_map[reaction_type]}",
        reply_markup=general_keyboard.back(callback_data="set_new_reaction")
    )


@router.callback_query(F.data == "set_new_work_time")
async def set_new_work_time(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    # Показываем текущее рабочее время
    try:
        start_time = await json_settings.async_get_attribute("start_time")
        end_time = await json_settings.async_get_attribute("end_time")
        settings_text = f"*️⃣ Рабочее время\n\nТекущие настройки:\n🌅 Начало: {start_time}\n🌇 Конец: {end_time}\n\nВыберите действие:"
    except Exception:
        settings_text = "*️⃣ Выберите действие:"

    await callback.message.edit_text(
        text=settings_text,
        reply_markup=settings_keyboard.new_time_work_menu()
    )


@router.callback_query(F.data == "set_start_time")
@router.callback_query(F.data == "set_end_time")
async def set_time(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.set_time)
    await state.update_data(callback_data=callback.data)
    
    # Показываем текущее значение и подсказку
    try:
        if callback.data == "set_start_time":
            current_time = await json_settings.async_get_attribute("start_time")
            hint_text = f"*️⃣ Начало рабочего времени\n\nТекущее значение: {current_time}\n\nВведите новое время в формате ЧЧ:ММ\nПример: 09:00"
        else:
            current_time = await json_settings.async_get_attribute("end_time")
            hint_text = f"*️⃣ Конец рабочего времени\n\nТекущее значение: {current_time}\n\nВведите новое время в формате ЧЧ:ММ\nПример: 18:00"
    except Exception:
        hint_text = "*️⃣ Введите время в формате ЧЧ:ММ"
    
    await callback.message.edit_text(
        text=hint_text,
        reply_markup=general_keyboard.back(callback_data="set_new_work_time")
    )


@router.message(SettingsStates.set_time)
async def set_time_state(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    await state.clear()
    
    try:
        user_time = datetime.strptime(message.text, '%H:%M').time().strftime('%H:%M')
    except ValueError:
        await message.answer(
            text="❌ Неправильный формат времени\n\nИспользуйте формат ЧЧ:ММ\nПример: 09:30",
            reply_markup=general_keyboard.back(callback_data="set_new_work_time")
        )
        return
    
    if state_data["callback_data"] == "set_start_time":
        json_key = "start_time"
        time_type = "начала"
    else:
        json_key = "end_time"
        time_type = "окончания"

    await json_settings.async_set_attribute(json_key, user_time)
    
    await message.answer(
        text=f"✅ Время {time_type} рабочего дня установлено: {user_time}",
        reply_markup=general_keyboard.back(callback_data="set_new_work_time")
    )