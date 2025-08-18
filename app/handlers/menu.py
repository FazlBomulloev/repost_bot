from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery

from app.utils import assert_message
from app.keyboards import menu as menu_keyboard

router = Router()


@router.message(CommandStart())
@router.callback_query(F.data == "back_to_menu")
async def start(callback_or_message: Message | CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    message = assert_message(query=callback_or_message)
    if isinstance(callback_or_message, CallbackQuery):
        await message.edit_text(text="*️⃣ Меню:", reply_markup=menu_keyboard.menu())
    else:
        await message.answer(text="*️⃣ Меню:", reply_markup=menu_keyboard.menu())
