import argparse
import asyncio
import aio_pika
import json

from app.drivers.rabbit_async import RabbitAsync

# Arguments
arg_parser = argparse.ArgumentParser(description='Run consuming async')
arg_parser.add_argument('-p', type=int, default=1000, help='Prefetch count')

run_args = arg_parser.parse_args()

QUEUE_NAME = 'test_queue'
PREFETCH_SIZE = run_args.p


async def _message_handler(message: aio_pika.IncomingMessage):
    with message.process(requeue=True):
        msg = json.loads(message.body.decode('UTF-8'))
        print(msg)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    rabbit = RabbitAsync(event_loop=loop)

    loop.run_until_complete(
        rabbit.consuming(QUEUE_NAME, _message_handler, PREFETCH_SIZE)
    )

    loop.run_forever()
