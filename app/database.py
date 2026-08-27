"""PostgreSQL(asyncpg) / Redis 연결 관리.

FastAPI lifespan 에서 pool 을 열고 닫는다. 애플리케이션 전역에서
`db_pool`, `redis_client` 를 공유해서 사용한다.
"""

import asyncpg
import redis.asyncio as aioredis

from app.config import settings

db_pool: asyncpg.Pool | None = None
redis_client: aioredis.Redis | None = None


async def connect() -> None:
    global db_pool, redis_client
    db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=20,
    )
    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)


async def disconnect() -> None:
    global db_pool, redis_client
    if db_pool is not None:
        await db_pool.close()
    if redis_client is not None:
        await redis_client.aclose()


def get_db() -> asyncpg.Pool:
    assert db_pool is not None, "DB pool 이 초기화되지 않았습니다."
    return db_pool


def get_redis() -> aioredis.Redis:
    assert redis_client is not None, "Redis client 가 초기화되지 않았습니다."
    return redis_client
