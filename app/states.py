from aiogram.fsm.state import State, StatesGroup



class Account(StatesGroup):
    add_with_channel = State()
    del_with_channel = State()
    add_without_channel = State()
    del_without_channel = State()
    transfer_free_accounts = State()
    transfer_accounts = State()
    set_count_transfer_free_accounts = State()
    set_count_transfer_accounts = State()
    delete_channel = State()


class Channel(StatesGroup):
    add = State()
    delete = State()


class Group(StatesGroup):
    add = State()
    delete = State()


class Settings(StatesGroup):
    set_settings_pause = State()
    add_stop_link = State()
    set_time = State()
