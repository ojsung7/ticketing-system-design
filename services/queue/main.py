"""queue-service — 대기열(Waiting Room) + 진입 토큰 발급.

Redis Sorted Set 으로 대기열을 관리하고, 순번이 앞쪽이면 대기열에서 빼며 TTL JWT 를
발급한다. 이 서비스는 DB 를 소유하지 않는다(Redis 만 사용).
"""

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from common.auth import create_entry_token
from common.clients import connect_redis, disconnect, get_redis
from common.config import settings

QUEUE_KEY = "waiting_queue"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_redis()
    yield
    await disconnect()


app = FastAPI(title="queue-service", lifespan=lifespan)


class QueueRequest(BaseModel):
    user_id: int


@app.get("/health")
async def health():
    try:
        ok = await get_redis().ping()
    except Exception:
        ok = False
    return {"service": "queue", "redis": ok}


@app.post("/queue/enter")
async def enter_queue(req: QueueRequest):
    redis = get_redis()
    member = str(req.user_id)
    await redis.zadd(QUEUE_KEY, {member: time.time()}, nx=True)
    rank = await redis.zrank(QUEUE_KEY, member)
    total = await redis.zcard(QUEUE_KEY)
    return {
        "user_id": req.user_id,
        "rank": rank,
        "total_waiting": total,
        "message": "대기열 진입. /queue/status 로 순번을 폴링하세요.",
    }


@app.get("/queue/status")
async def queue_status(user_id: int):
    redis = get_redis()
    member = str(user_id)
    rank = await redis.zrank(QUEUE_KEY, member)

    if rank is None:
        return {"user_id": user_id, "status": "not_in_queue"}

    if rank < settings.allowed_entry_count:
        await redis.zrem(QUEUE_KEY, member)
        token = create_entry_token(user_id)
        return {
            "user_id": user_id,
            "status": "admitted",
            "entry_token": token,
            "ttl": settings.jwt_ttl,
        }

    return {"user_id": user_id, "status": "waiting", "rank": rank, "ahead": rank}
