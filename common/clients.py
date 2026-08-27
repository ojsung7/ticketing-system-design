"""Redis / PostgreSQL 연결 관리 (서비스 공통).

서비스마다 필요한 백엔드만 골라 연결한다.
- queue / payment: Redis 만 필요
- booking (+worker): Redis + PostgreSQL 필요
"""

import asyncpg
import redis.asyncio as aioredis

from common.config import settings

_db_pool: asyncpg.Pool | None = None
_redis_client: aioredis.Redis | None = None


async def connect_redis() -> None:
    global _redis_client
    _redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)


async def connect_db() -> None:
    global _db_pool
    _db_pool = await asyncpg.create_pool(dsn=settings.database_url, min_size=2, max_size=20)


async def disconnect() -> None:
    global _db_pool, _redis_client
    if _db_pool is not None:
        await _db_pool.close()
        _db_pool = None
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None


def get_redis() -> aioredis.Redis:
    assert _redis_client is not None, "Redis client 가 초기화되지 않았습니다."
    return _redis_client


def get_db() -> asyncpg.Pool:
    assert _db_pool is not None, "DB pool 이 초기화되지 않았습니다."
    return _db_pool
