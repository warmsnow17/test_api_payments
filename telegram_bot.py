import json

import os

from aiogram import Bot, Dispatcher, types
from dotenv import load_dotenv
from main import get_data
load_dotenv()


API_TOKEN = os.getenv('BOT_TOKEN')

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)


@dp.message_handler(commands=['start'])
async def start_cmd_handler(message: types.Message):
    await bot.send_message(chat_id=message.chat.id, text="Привет! Отправьте мне JSON с входными данными")


@dp.message_handler(lambda message: message.text.startswith('{') and message.text.endswith('}'))
async def json_message_handler(message: types.Message):
    try:
        input_data = json.loads(message.text)
        result = await get_data(input_data["dt_from"], input_data["dt_upto"], input_data["group_type"])
        await bot.send_message(chat_id=message.chat.id, text=json.dumps(result, indent=2))
    except Exception as e:
        await bot.send_message(chat_id=message.chat.id, text=f"Ошибка: {str(e)}")

if __name__ == '__main__':
    from aiogram import executor
    executor.start_polling(dp)
