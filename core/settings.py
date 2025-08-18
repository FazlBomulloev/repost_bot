import json
import aiofiles

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from pydantic import SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    admin_chat_id: int = 7367116153
    bot_token: SecretStr
    database_url: SecretStr
    json_settings_file: str = "json_settings.json"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class JSONSettings:
    def __init__(self, json_settings_file: str) -> None:
        self.json_settings_file = json_settings_file
        self.config = {}

    async def load_config(self) -> None:
        async with aiofiles.open(self.json_settings_file, 'r') as file:
            content = await file.read()
            self.config = json.loads(content)


    async def save_config(self) -> None:
        async with aiofiles.open(self.json_settings_file, 'w') as file:
            json_str = json.dumps(self.config, indent=4, ensure_ascii=True)
            await file.write(json_str)

    async def async_get_attribute(self, item):
        await self.load_config()
        return self.config[item]


    async def async_set_attribute(self, item, value):
        await self.load_config()
        self.config[item] = value
        await self.save_config()


settings = Settings()
json_settings = JSONSettings(json_settings_file=settings.json_settings_file)
bot = Bot(token=settings.bot_token.get_secret_value(), default=DefaultBotProperties(parse_mode="HTML"))
