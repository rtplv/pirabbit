import argparse
import asyncio
import json

from app import logger
from app.drivers.rabbit_async import RabbitAsync

# Arguments
arg_parser = argparse.ArgumentParser(description='Run producing async')
arg_parser.add_argument('-b', type=int, default=5000, help='Batch count')
run_args = arg_parser.parse_args()

QUEUE_NAME = 'test_queue'
BATCH_SIZE = run_args.b


async def _produce_messages(task_idx):
    """
    Produce RabbitMQ message
    :param task_idx:int
    :return:Future
    """

    messages = []

    for i in range(20):
        messages.append(json.dumps({
            'task': task_idx,
            'message': i,
        }))

    await rabbit.publish(QUEUE_NAME, messages)

    logger.info('Batch produced. Size - {}'.format(BATCH_SIZE))


async def _safe_produce_messages(*args):
    async with semaphore:
        return await _produce_messages(*args)


async def _run_tasks(tasks):
    logger.info('Execute {} tasks'.format(len(tasks)))
    await asyncio.wait(tasks)


async def _run():
    MAX_TASKS_COUNT = 100

    tasks = []

    for i in range(1000):
        if len(tasks) >= MAX_TASKS_COUNT:
            await _run_tasks(tasks)
            tasks.clear()

        tasks.append(ioloop.create_task(_safe_produce_messages(i)))
    else:
        if len(tasks) > 0:
            await _run_tasks(tasks)


if __name__ == '__main__':
    ioloop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(25)

    rabbit = RabbitAsync(ioloop)

    ioloop.run_until_complete(_run())
