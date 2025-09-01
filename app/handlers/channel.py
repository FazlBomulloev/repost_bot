import asyncio
import random

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app import utils
from app.keyboards import channel as channel_keyboard, general as general_keyboard
from app.states import Channel as ChannelStates, Group as GroupStates, Account as AccountStates
from auto_reposting import telegram_utils

from core.models import channel as channel_db, group as group_db, tg_account as tg_account_db
from core.schemas import group as group_schemas
from core.schemas import channel as channel_schemas

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


@router.callback_query(F.data == "channels")
@router.callback_query(F.data == "back_to_channels")
async def channels_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    await callback.message.edit_text(
        text="*️⃣ Управление каналами:",
        reply_markup=channel_keyboard.menu(channels=await channel_db.get_channels())
    )


@router.callback_query(F.data == "add_channel")
async def add_channel(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(ChannelStates.add)
    await callback.message.edit_text(
        text="*️⃣ Введите ссылку канала:",
        reply_markup=general_keyboard.back(callback_data="back_to_channels")
    )


@router.message(ChannelStates.add)
async def add_channel_state(message: Message, state: FSMContext) -> None:
    await state.clear()

    tg_accounts = await tg_account_db.get_tg_accounts()

    counter_tg_accounts = 0
    stop_iter = False
    random_tg_account = None
    random_telegram_client = None
    while not stop_iter:
        if counter_tg_accounts + 1 >= len(tg_accounts):
            await message.answer(
                text="*️⃣ Нету аккаунтов для получения данных по каналу!",
                reply_markup=general_keyboard.back(callback_data="back_to_channels")
            )
            return

        random_tg_account: tg_account_db.TGAccount = random.choice(tg_accounts)
        random_telegram_client = await telegram_utils.create_tg_client(tg_account=random_tg_account)
        if random_telegram_client is not None:
            stop_iter = True

    url = message.text
    async with random_telegram_client:
        try:
            channel = await random_telegram_client.get_entity(url)
        except Exception as e:
            await message.answer(
                text=f"*️⃣ Возникла ошибка с каналом, подробнее: {e.__class__.__name__}: {e}",
                reply_markup=general_keyboard.back(callback_data="back_to_channels")
            )
            return

    new_channel = await channel_db.create_channel(channel_in=channel_schemas.ChannelCreate(url=url, telegram_channel_id=channel.id))

    await message.answer(
        text=f"*️⃣ Успешно добавил ссылку: {url}",
        reply_markup=general_keyboard.back(callback_data="back_to_channels"),
        disable_web_page_preview=True
    )


@router.callback_query(F.data.startswith("channel_guid_"))
async def get_channel_by_guid(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()

    channel = await channel_db.get_channel_by_guid(guid=callback.data.replace("channel_guid_", ""))
    await callback.message.edit_text(
        text=f"*️⃣ Канал: {channel.url}",
        reply_markup=channel_keyboard.channel_menu(channel_guid=channel.guid),
        disable_web_page_preview=True
    )


@router.callback_query(F.data.startswith("show_list_groups_chnl_guid_"))
async def show_list_groups(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    channel_guid = callback.data.replace("show_list_groups_chnl_guid_", "")

    text = f"*️⃣ Список групп:\n"
    for group in await group_db.get_all_groups_by_channel_guid(channel_guid=channel_guid):
        text += f"{group.url}\n"

    message_parties = [text[i:i + 4096] for i in range(0, len(text), 4096)]
    for message_part in message_parties:
        await callback.message.answer(message_part, disable_web_page_preview=True)
        await asyncio.sleep(1)

    await callback.message.answer(
        text="Выберите действие:",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{channel_guid}"),
        disable_web_page_preview=True
    )


@router.callback_query(F.data.startswith("add_groups_chnl_guid_"))
async def add_groups(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(GroupStates.add)
    channel_guid = callback.data.replace("add_groups_chnl_guid_", "")
    await state.update_data(channel_guid=channel_guid)
    await callback.message.edit_text(
        text=f"*️⃣ Пришлите список групп для добавления:",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{channel_guid}"),
        disable_web_page_preview=True
    )


@router.message(F.text, GroupStates.add)
async def add_groups_state(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    channel_guid = state_data["channel_guid"]
    await state.clear()

    for i in message.text.split("\n"):
        i = i.strip()
        links = [j.strip() for j in i.split(" ")]
        if not links:
            links = [i]

        for link in links:
            if not link or link == "" or link == " ":
                continue

            if "@" in link:
                link = f"https://t.me/{link.replace('@', '')}"

            await group_db.create_group(
                group_in=group_schemas.GroupCreate(channel_guid=channel_guid, url=link)
            )

    await message.answer(
        text=f"*️⃣ Успешно добавил группы!",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{channel_guid}"),
        disable_web_page_preview=True
    )


@router.callback_query(F.data.startswith("del_groups_chnl_guid_"))
async def del_groups(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(GroupStates.delete)
    channel_guid = callback.data.replace("del_groups_chnl_guid_", "")
    await state.update_data(channel_guid=channel_guid)
    await callback.message.edit_text(
        text=f"*️⃣ Пришлите список групп для удаления:",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{channel_guid}"),
        disable_web_page_preview=True
    )


@router.message(F.text, GroupStates.delete)
async def del_groups_state(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    channel_guid = state_data["channel_guid"]

    await state.clear()
    for i in message.text.split("\n"):
        await group_db.delete_group_by_url(channel_guid=channel_guid, url=i)

    await message.answer(
        text=f"*️⃣ Успешно удалил группы!",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{channel_guid}"),
        disable_web_page_preview=True
    )


@router.callback_query(F.data.startswith("show_list_accs_chnl_guid_"))
async def show_list_accounts(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    channel_guid = callback.data.replace("show_list_accs_chnl_guid_", "")
    text = "*️⃣ Список аккаунтов:\n"
    for telegram_account in await tg_account_db.get_tg_accounts_by_channel_guid(channel_guid=channel_guid):
        text += f"https://t.me/+{telegram_account.phone_number}\n"

    await callback.message.edit_text(
        text=text,
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{channel_guid}"),
        disable_web_page_preview=True
    )


@router.callback_query(F.data.startswith("add_accs_chnl_guid_"))
async def add_accounts_with_channel(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(AccountStates.add_with_channel)
    channel_guid = callback.data.replace("add_accs_chnl_guid_", "")
    await state.update_data(channel_guid=channel_guid)
    await callback.message.edit_text(
        text="*️⃣ Пришлите zip-файлы для добавления аккаунтов в канал, как только закончите пришлите команду /stop :",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{channel_guid}"),
    )


@router.message(~F.document, AccountStates.add_with_channel)
async def is_not_document(message: Message) -> None:
    await message.reply(text="*️⃣ Отправьте файл!")


@router.message(~F.text, AccountStates.del_with_channel)
async def is_not_text_message(message: Message, state: FSMContext) -> None:
    await message.reply(text="*️⃣ Пришлите сообщение в виде текста!")


@router.message(F.document, AccountStates.add_with_channel)
async def add_accounts_with_channel_state(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    channel_guid = state_data["channel_guid"]
    result = await utils.process_telegram_data(
        document_file_id=message.document.file_id,
        unique_document_file_id=message.document.file_unique_id,
        channel_guid=channel_guid
    )
    if result is not None:
        message_parties = [result[i:i + 4096] for i in range(0, len(result), 4096)]
        for text in message_parties:
            await message.answer(text)


@router.message(Command("stop"), AccountStates.add_with_channel)
async def stop_adding_accounts(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    channel_guid = state_data["channel_guid"]
    await state.clear()
    
    await update_channel_workers_if_needed(channel_guid)

    await message.answer(
        text="*️⃣ Успешно закончил прием аккаунтов!",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{channel_guid}")
    )


@router.callback_query(F.data.startswith("del_accs_chnl_guid_"))
async def del_accounts(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(AccountStates.del_with_channel)
    channel_guid = callback.data.replace("del_accs_chnl_guid_", "")
    await state.update_data(channel_guid=channel_guid)
    await callback.message.edit_text(
        text="*️⃣ Пришлите список аккаунтов для удаления:",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{channel_guid}")
    )


@router.message(F.text, AccountStates.del_with_channel)
async def del_accounts_state(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    channel_guid = state_data["channel_guid"]
    await state.clear()

    for i in message.text.split("\n"):
        phone_number = i.split("/")[-1].replace("+", "")
        await tg_account_db.set_delete_status_tg_account_by_phone_number(phone_number=phone_number)

    await update_channel_workers_if_needed(channel_guid)

    await message.answer(
        text="*️⃣ Успешно удалил аккаунты!",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{channel_guid}")
    )


@router.callback_query(F.data.startswith("tnsf_accs_to_antr_chl_guid_"))
async def transfer_accounts_to_another_channel(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(AccountStates.transfer_accounts)
    channel_guid = callback.data.replace("tnsf_accs_to_antr_chl_guid_", "")
    channel = await channel_db.get_channel_by_guid(guid=channel_guid)
    await state.update_data(channel=channel)

    channels = await channel_db.get_channels()
    for c, chann_db in enumerate(channels):
        if str(chann_db.guid) == str(channel_guid):
            channels.pop(c)

    await callback.message.edit_text(
        text=f"*️⃣ Выберите канал для переноса аккаунтов с канала: {channel.url}",
        reply_markup=general_keyboard.choice_channel(
            channels=channels,
            channel_callback="tnsf_accs_to_channel_guid_",
            back_callback=f"channel_guid_{channel_guid}"
        ),
        disable_web_page_preview=True
    )


@router.callback_query(AccountStates.transfer_accounts, F.data.startswith("tnsf_accs_to_channel_guid_"))
async def transfer_accounts_to_channel(callback: CallbackQuery, state: FSMContext) -> None:
    state_data = await state.get_data()
    channel_guid = callback.data.replace("tnsf_accs_to_channel_guid_", "")
    to_channel = await channel_db.get_channel_by_guid(guid=channel_guid)
    await state.update_data(to_channel=to_channel)
    await state.set_state(AccountStates.set_count_transfer_accounts)

    await callback.message.edit_text(
        text="*️⃣ Введите кол-во аккаунтов для переноса:",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{state_data['channel'].guid}")
    )


@router.message(AccountStates.set_count_transfer_accounts)
async def set_count_transfer_accounts(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    try:
        count_accounts = int(message.text)
    except ValueError:
        await message.answer(
            text="*️⃣ Значение должно быть числом. Повторите попытку!",
            reply_markup=general_keyboard.back(callback_data=f"channel_guid_{state_data['channel'].guid}")
        )
        return

    await state.set_state(AccountStates.transfer_accounts)
    await state.update_data(count_accounts=count_accounts)

    await message.answer(
        text=f"*️⃣ Вы уверены что хотите перенести аккаунты с канала: {state_data['channel'].url} в канал: {state_data['to_channel'].url}",
        reply_markup=channel_keyboard.confirm_transfer_accounts(channel_guid=state_data['channel'].guid),
        disable_web_page_preview=True
    )


@router.callback_query(AccountStates.transfer_accounts, F.data == "confirm_transfer_accounts")
async def confirm_transfer_accounts(callback: CallbackQuery, state: FSMContext) -> None:
    state_data = await state.get_data()
    from_channel_guid = str(state_data["channel"].guid)
    to_channel_guid = str(state_data["to_channel"].guid)

    await tg_account_db.set_new_channel_guid_where_channel_guid(
        channel_guid=from_channel_guid,
        new_channel_guid=state_data["to_channel"].guid,
        count_accounts=state_data["count_accounts"]
    )
    
    await state.clear()

    await update_channel_workers_if_needed(from_channel_guid)
    await update_channel_workers_if_needed(to_channel_guid)

    await callback.message.edit_text(
        text=f"*️⃣ Успешно перенес аккаунты из канала: {state_data['channel'].url} в канал: {state_data['to_channel'].url}",
        reply_markup=general_keyboard.back(callback_data=f"channel_guid_{from_channel_guid}"),
        disable_web_page_preview=True
    )


@router.callback_query(F.data.startswith("delete_channel_guid_"))
async def delete_channel(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(AccountStates.delete_channel)
    channel_guid = callback.data.replace("delete_channel_guid_", "")
    await state.update_data(channel_guid=channel_guid)
    await callback.message.edit_text(
        text=f"*️⃣ Уверены что хотите удалить канал?",
        reply_markup=channel_keyboard.confirm_delete_channel(channel_guid=channel_guid),
        disable_web_page_preview=True
    )


@router.callback_query(F.data == "confirm_delete_channel", AccountStates.delete_channel)
async def confirm_delete_channel(callback: CallbackQuery, state: FSMContext) -> None:
    state_data = await state.get_data()
    channel_guid = state_data["channel_guid"]
    count_accounts = len(await tg_account_db.get_tg_accounts_by_channel_guid(channel_guid=channel_guid))
    await tg_account_db.set_new_channel_guid_where_channel_guid(
        channel_guid=channel_guid,
        new_channel_guid=None,
        count_accounts=count_accounts
    )
    await channel_db.delete_channel_by_guid(guid=channel_guid)
    await state.clear()
    
    await update_channel_workers_if_needed(channel_guid)
    
    await callback.message.edit_text(
        text="*️⃣ Успешно удалил канал!",
        reply_markup=general_keyboard.back(callback_data="channels")
    )