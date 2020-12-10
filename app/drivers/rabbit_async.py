import asyncio
import os
import aio_pika
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT')
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')


class RabbitAsync:
    def __init__(
            self,
            event_loop=asyncio.get_event_loop(),
            user=RABBITMQ_USER,
            password=RABBITMQ_PASSWORD,
            host=RABBITMQ_HOST,
            port=int(RABBITMQ_PORT)
    ):
        self._event_loop = event_loop

        self._connections_pool = None
        self._channels_pool = None
        self._queue_params = {}

        self._user = user
        self._password = password
        self._host = host
        self._port = port

    async def _create_connection(self):
        return await aio_pika.connect_robust(
            url="amqp://{user}:{password}@{host}:{port}/".format(
                user=self._user,
                password=self._password,
                host=self._host,
                port=self._port
            ),
            loop=self._event_loop
        )

    async def _create_channel(self):
        if self._connections_pool is None:
            raise Exception('self._connections_pool must be exist')

        async with self._connections_pool.acquire() as connection:
            return await connection.channel(publisher_confirms=False)

    def _set_connections_pool(self):
        self._connections_pool = Pool(self._create_connection, max_size=10, loop=self._event_loop)

    def _set_channels_pool(self):
        self._channels_pool = Pool(self._create_channel, max_size=1000, loop=self._event_loop)

    def set_queue_params(self, queue_name, **kwargs):
        self._queue_params[queue_name] = kwargs

    def get_queue_params(self, queue_name):
        return self._queue_params.get(queue_name) if queue_name in self._queue_params else {}

    async def _get_queue(self, channel, queue_name):
        return await channel.declare_queue(name=queue_name, durable=True, **self.get_queue_params(queue_name))

    async def publish(self, queue_name, msg):
        if self._connections_pool is None:
            self._set_connections_pool()

        if self._channels_pool is None:
            self._set_channels_pool()

        async with self._channels_pool.acquire() as channel:
            queue = await self._get_queue(channel, queue_name)

            messages = msg if type(msg) is list else [msg]

            for m in messages:
                await channel.default_exchange.publish(
                    aio_pika.Message(body=m.encode(), content_type='application/json'),
                    routing_key=queue.name,
                )

    async def consuming(self, queue_name, handler, prefetch_count=1000):
        if self._connections_pool is None:
            self._set_connections_pool()

        if self._channels_pool is None:
            self._set_channels_pool()

        async with self._channels_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=prefetch_count)
            queue = await self._get_queue(channel, queue_name)

            async for message in queue:
                await handler(message)
