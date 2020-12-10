import asyncio
import json
import os

import asyncpg
import logging

import redis
from peewee_async import PooledPostgresqlDatabase, Manager
from dotenv import load_dotenv
from app.config.logger import default_handler

load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('app')
logger.addHandler(default_handler)

# Main storage
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')

# ORM DB Pool
db = PooledPostgresqlDatabase(
    database=DB_DATABASE,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

db_async_manager = Manager(db)
db.set_allow_sync(False)


# Raw queries DB pool
async def _set_json_codec(connection):
    await connection.set_type_codec(
        'json',
        encoder=json.dumps,
        decoder=json.loads,
        schema='pg_catalog'
    )


async def _on_connection_init(connection):
    await _set_json_codec(connection)


async def _init_pool():
    return await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        init=_on_connection_init,
        min_size=1,
        max_size=20,
        max_inactive_connection_lifetime=5
    )


db_pool = asyncio.get_event_loop().run_until_complete(_init_pool())


# Redis
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT')

redis = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT
)