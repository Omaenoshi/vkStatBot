import asyncio
import logging
import sys
import json

import aio_pika
from aiogram import Bot, Dispatcher, exceptions
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.markdown import hbold


class Request:
    def __init__(self, telegram_user_id, vk_user_id):
        self.telegram_user_id = telegram_user_id
        self.vk_user_id = vk_user_id

    def to_json(self):
        return json.dumps(self.__dict__)


class Response:
    def __init__(self, telegram_id, result):
        self.telegram_id = telegram_id
        self.result = result

    def to_json(self):
        return json.dumps(self.__dict__)


TOKEN = "6691609318:AAHvmM6XyO8tZAPf4hsj4Oare_jti173niQ"
RABBITMQ_HOST = 'localhost'

dp = Dispatcher()


async def setup_rabbitmq():
    connection = await aio_pika.connect_robust(f"amqp://{RABBITMQ_HOST}/")
    channel = await connection.channel()

    # Создание Exchange типа DIRECT
    exchange = await channel.declare_exchange("telegram_vk", aio_pika.ExchangeType.DIRECT)

    # Создание Queue
    queue = await channel.declare_queue("telegram_request", durable=True)

    # Создание Binding с routing_key
    await queue.bind(exchange, routing_key="request")

    # Создание Queue для ответов
    response_queue = await channel.declare_queue("telegram_response", durable=True)
    await response_queue.bind(exchange, routing_key="response")

    # Начало прослушивания очереди с ответами
    await response_queue.consume(on_message)

    try:
        # Wait until terminate
        await asyncio.Future()
    finally:
        await connection.close()


async def on_message(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():
        try:
            # Получение объекта Response из очереди
            response_data = json.loads(message.body)
            response = Response(**response_data)

            # Отправка сообщения по telegram_id
            await send_result(response)
        except Exception as e:
            logging.exception(f"Ошибка при обработке сообщения: {e}")


async def send_result(response: Response) -> None:
    bot = Bot(TOKEN)
    # Отправка результата по telegram_id
    await bot.send_message(response.telegram_id, f"Результат: {response.result}")


async def send_to_queue(request: Request) -> None:
    connection = await aio_pika.connect_robust(f"amqp://{RABBITMQ_HOST}/")
    channel = await connection.channel()

    exchange = await channel.get_exchange("telegram_vk")

    # Отправка сообщения в Exchange
    await exchange.publish(
        aio_pika.Message(body=request.to_json().encode()),
        routing_key="request",
    )

    await connection.close()


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Hello, {hbold(message.from_user.full_name)}! Enter the vk user's id:")


@dp.message(lambda message: message.text.isdigit())
async def get_user_id(message: Message):
    user_id = int(message.text)

    # Создание объекта Request
    request = Request(message.from_user.id, user_id)

    await send_to_queue(request)
    await message.answer(f"ID пользователя {user_id} успешно отправлен в очередь.")


async def main() -> None:
    # Initialize Bot instance with a default parse mode which will be passed to all API calls
    bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
    # And the run events dispatching
    await asyncio.gather(setup_rabbitmq(), dp.start_polling(bot))
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
