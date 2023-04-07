import logging
import json

from dotenv import load_dotenv, dotenv_values
from aiogram import Bot, Dispatcher, executor, types

from utils import get_data


load_dotenv()

API_TOKEN = dotenv_values('.env')['TOKEN']

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

logging.basicConfig(level=logging.INFO)


@dp.message_handler()
async def aggregation(message: types.Message):
    input_data = json.loads(message.text)
    data = get_data(input_data)
    await message.answer(data)

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
