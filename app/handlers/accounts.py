from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app import utils
from app.keyboards import accounts as accounts_keyboard, general as general_keyboard
from app.states import Account as AccountStates
from core.models import tg_account as tg_account_db, channel as channel_db

# Импортируем глобальный процессор каналов
from auto_reposting.channel_processor import channel_processor

router = Router()


async def update_channel_workers_if_needed(channel_guid: str = None):
    """Обновляет воркеры каналов при изменении аккаунтов"""
    try:
        if channel_guid:
            # Проверяем конкретный канал
            await channel_processor.ensure_worker_for_channel(channel_guid)
            await channel_processor.remove_worker_if_no_accounts(channel_guid)
        else:
            # Проверяем все каналы (при массовых операциях)
            channels = await channel_db.get_channels()
            for channel in channels:
                guid = str(channel.guid)
                await channel_processor.ensure_worker_for_channel(guid)
                await channel_processor.remove_worker_if_no_accounts(guid)
    except Exception as e:
        print(f"Ошибка при обновлении воркеров: {e}")


@router.callback_query(F.data == "accounts")
@router.callback_query(F.data == "back_to_accounts")
async def accounts_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    await callback.message.edit_text(
        text="*️⃣ Управление аккаунтами:",
        reply_markup=accounts_keyboard.menu()
    )


@router.callback_query(F.data == "display_list_of_free_accounts")
async def display_list_of_free_accounts(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    text = "*️⃣ Список аккаунтов:\n"
    for telegram_account in await tg_account_db.get_tg_accounts_by_channel_guid(channel_guid=None):
        text += f"https://t.me/+{telegram_account.phone_number}\n"

    message_parties = [text[i:i + 4096] for i in range(0, len(text), 4096)]
    for message_part in message_parties:
        await callback.message.answer(message_part, disable_web_page_preview=True)

    await callback.message.answer(
        text="Выберите действие:",
        reply_markup=accounts_keyboard.back_to_accounts(),
        disable_web_page_preview=True
    )


@router.callback_query(F.data == "transfer_free_accounts_to_channel")
async def transfer_free_accounts_to_channel(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    await callback.message.edit_text(
        text="*️⃣ Выберите канал для переноса свободных аккаунтов:",
        reply_markup=general_keyboard.choice_channel(
            channels=await channel_db.get_channels(),
            channel_callback="tnsf_free_accs_to_chnl_guid_",
            back_callback="back_to_accounts"
        )
    )


@router.callback_query(F.data.startswith("tnsf_free_accs_to_chnl_guid_"))
async def transfer_free_accounts_to_channel_by_guid(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    channel_guid = callback.data.replace("tnsf_free_accs_to_chnl_guid_", "")
    channel = await channel_db.get_channel_by_guid(guid=channel_guid)

    await state.set_state(AccountStates.set_count_transfer_free_accounts)
    await state.update_data(channel=channel)

    await callback.message.edit_text(
        text="*️⃣ Введите кол-во аккаунтов для переноса:\n",
        reply_markup=general_keyboard.back(callback_data="accounts")
    )


@router.message(AccountStates.set_count_transfer_free_accounts)
async def set_count_transfer_free_accounts(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    try:
        count_accounts = int(message.text)
    except ValueError:
        await message.answer(
            text="*️⃣ Значение должно быть числом. Повторите попытку!",
            reply_markup=general_keyboard.back(callback_data=f"channel_guid_{state_data['channel'].guid}")
        )
        return

    await state.set_state(AccountStates.transfer_free_accounts)
    await state.update_data(count_accounts=count_accounts)

    await message.answer(
        text=f"*️⃣ Вы уверены, что хотите перенести свободные аккаунты в канал: {state_data['channel'].url}",
        reply_markup=accounts_keyboard.confirm_transfer_free_accounts(),
        disable_web_page_preview=True
    )


@router.callback_query(AccountStates.transfer_free_accounts, F.data == "confirm_transfer_free_accounts")
async def confirm_transfer_free_accounts(callback: CallbackQuery, state: FSMContext) -> None:
    state_data = await state.get_data()
    channel = state_data["channel"]
    await state.clear()

    await tg_account_db.set_channel_guid_where_channel_guid_is_none(
        channel_guid=channel.guid,
        count_accounts=state_data["count_accounts"]
    )
    
    # 🎉 НОВОЕ: Обновляем воркеры после переноса аккаунтов
    await update_channel_workers_if_needed(str(channel.guid))
    
    await callback.message.edit_text(
        text=f"✅ Успешно перенес аккаунты в канал: {channel.url}\n🔄 Воркер канала обновлен!",
        reply_markup=general_keyboard.back(callback_data="back_to_accounts"),
        disable_web_page_preview=True
    )


@router.callback_query(F.data == "add_accounts")
async def add_accounts(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(AccountStates.add_without_channel)

    await callback.message.edit_text(
        text="*️⃣ Пришлите zip-файлы для добавления аккаунтов в базу данных, как только закончите пришлите команду /stop :",
        reply_markup=accounts_keyboard.back_to_accounts()
    )


@router.message(F.document, AccountStates.add_without_channel)
async def add_accounts_state(message: Message, state: FSMContext) -> None:
    result = await utils.process_telegram_data(
        document_file_id=message.document.file_id,
        unique_document_file_id=message.document.file_unique_id,
        channel_guid=None
    )
    if result is not None:
        message_parties = [result[i:i + 4096] for i in range(0, len(result), 4096)]
        for text in message_parties:
            await message.answer(text)


@router.message(Command("stop"), AccountStates.add_without_channel)
async def stop_adding_accounts(message: Message, state: FSMContext) -> None:
    await state.clear()

    # 🎉 НОВОЕ: Обновляем воркеры после добавления аккаунтов
    await update_channel_workers_if_needed()

    await message.answer(
        text="*️⃣ Успешно закончил прием аккаунтов!\n🔄 Воркеры каналов обновлены!",
        reply_markup=accounts_keyboard.back_to_accounts()
    )


@router.message(~F.document, AccountStates.add_without_channel)
async def is_not_document(message: Message) -> None:
    await message.reply(text="*️⃣ Отправьте файл!")


@router.message(~F.text, AccountStates.del_without_channel)
async def is_not_text_message(message: Message, state: FSMContext) -> None:
    await message.reply(text="*️⃣ Пришлите сообщение в виде текста!")


@router.callback_query(F.data == "del_accounts")
async def del_accounts(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(AccountStates.del_without_channel)

    await callback.message.edit_text(
        text="*️⃣ Пришлите список аккаунтов для удаления:",
        reply_markup=accounts_keyboard.back_to_accounts()
    )


@router.message(F.text, AccountStates.del_without_channel)
async def del_accounts_state(message: Message, state: FSMContext) -> None:
    await state.clear()
    
    # Собираем каналы, которые могут потерять аккаунты
    affected_channels = set()
    
    for i in message.text.split("\n"):
        phone_number = i.split("/")[-1].replace("+", "")
        
        # Получаем аккаунт до удаления чтобы знать его канал
        account = await tg_account_db.get_tg_account_by_phone_number(phone_number=int(phone_number))
        if account and account.channel_guid:
            affected_channels.add(str(account.channel_guid))
        
        await tg_account_db.set_delete_status_tg_account_by_phone_number(phone_number=phone_number)
    
    # 🎉 НОВОЕ: Обновляем воркеры для затронутых каналов
    for channel_guid in affected_channels:
        await update_channel_workers_if_needed(channel_guid)

    await message.answer(
        text="*️⃣ Успешно удалил аккаунты!\n🔄 Воркеры затронутых каналов обновлены!",
        reply_markup=accounts_keyboard.back_to_accounts()
    )