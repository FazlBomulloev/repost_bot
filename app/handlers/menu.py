from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery

from app.utils import assert_message
from app.keyboards import menu as menu_keyboard
from core.settings import settings

router = Router()


@router.message(CommandStart())
@router.callback_query(F.data == "back_to_menu")
async def start(callback_or_message: Message | CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    
    # Получаем user_id из сообщения или callback
    if isinstance(callback_or_message, CallbackQuery):
        user_id = callback_or_message.from_user.id
    else:
        user_id = callback_or_message.from_user.id
    
    # Проверяем, является ли пользователь админом
    if not settings.is_admin(user_id):
        # Отправляем сообщение о том, что доступ запрещен
        if isinstance(callback_or_message, CallbackQuery):
            await callback_or_message.message.edit_text(
                text="❌ Доступ запрещен. Вы не являетесь администратором бота."
            )
        else:
            await callback_or_message.answer(
                text="❌ Доступ запрещен. Вы не являетесь администратором бота."
            )
        return

    # Если пользователь админ - показываем меню
    message = assert_message(query=callback_or_message)
    if isinstance(callback_or_message, CallbackQuery):
        await message.edit_text(text="*️⃣ Меню:", reply_markup=menu_keyboard.menu())
    else:
        await message.answer(text="*️⃣ Меню:", reply_markup=menu_keyboard.menu())