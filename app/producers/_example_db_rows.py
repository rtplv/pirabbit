import argparse
import asyncio
import json

from app import logger, redis, db_pool
from app.drivers.rabbit_async import RabbitAsync

# Arguments
arg_parser = argparse.ArgumentParser(description='Run producing async')
arg_parser.add_argument('-b', type=int, default=5000, help='Batch count')
run_args = arg_parser.parse_args()

QUEUE_NAME = 'Dossier_Indexing_Example'
REDIS_HKEY = 'dossier_indexing_example'
BATCH_SIZE = run_args.b


async def _produce_messages(min_id, max_id):
    """
    Produce RabbitMQ message
    :param min_id:int
    :param max_id:int
    :return:Future
    """
    await rabbit.publish(QUEUE_NAME, json.dumps({'min_id': min_id, 'max_id': max_id}))

    logger.info('Entities item range: {} to {}'.format(min_id, max_id))


async def _safe_produce_messages(*args):
    async with semaphore:
        return await _produce_messages(*args)


async def _run_tasks(tasks):
    logger.info('Execute {} tasks'.format(len(tasks)))
    await asyncio.wait(tasks)


async def _run():
    MAX_TASKS_COUNT = 100

    persons_count = await db_pool.fetchval('''
        SELECT max(id) from dossier.persons
    ''')

    logger.info('Persons count - {}'.format(persons_count))

    cached_min_id = redis.hget(REDIS_HKEY, 'person_min_id')
    cached_max_id = redis.hget(REDIS_HKEY, 'person_max_id')

    min_id = int(cached_min_id) if cached_min_id else 1
    max_id = int(cached_max_id) if cached_max_id else BATCH_SIZE

    logger.info('Initial min_id - {}'.format(min_id))
    logger.info('Initial max_id - {}'.format(max_id))

    tasks = []

    while persons_count >= min_id:
        if len(tasks) >= MAX_TASKS_COUNT:
            await _run_tasks(tasks)
            tasks.clear()

        tasks.append(ioloop.create_task(_safe_produce_messages(min_id, max_id)))

        min_id = int(redis.hget(REDIS_HKEY, 'person_min_id') or min_id) + BATCH_SIZE
        max_id = int(redis.hget(REDIS_HKEY, 'person_max_id') or max_id) + BATCH_SIZE

        redis.hset(REDIS_HKEY, 'person_min_id', min_id)
        redis.hset(REDIS_HKEY, 'person_max_id', max_id)
    else:
        if len(tasks) > 0:
            await _run_tasks(tasks)


if __name__ == '__main__':
    ioloop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(25)

    rabbit = RabbitAsync(ioloop)

    ioloop.run_until_complete(_run())
