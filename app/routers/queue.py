"""대기열(Waiting Room) — Redis Sorted Set.

접속 사용자를 waiting_queue(sorted set, score=진입시각)에 넣고, 순번이 앞쪽
ALLOWED_ENTRY_COUNT 안에 들면 대기열에서 빼면서 TTL JWT 를 발급한다.
이 토큰이 있어야만 예매(reserve/confirm) 페이지에 진입할 수 있다.
"""

import time

from fastapi import APIRouter
from pydantic import BaseModel

from app.auth import create_entry_token
from app.config import settings
from app.database import get_redis

router = APIRouter(prefix="/queue", tags=["queue"])

QUEUE_KEY = "waiting_queue"


class QueueRequest(BaseModel):
    user_id: int


@router.post("/enter")
async def enter_queue(req: QueueRequest):
    """대기열 진입. 이미 있으면 기존 순번 유지."""
    redis = get_redis()
    member = str(req.user_id)
    # NX: 이미 대기열에 있으면 진입 시각을 덮어쓰지 않는다(순번 유지).
    await redis.zadd(QUEUE_KEY, {member: time.time()}, nx=True)
    rank = await redis.zrank(QUEUE_KEY, member)
    total = await redis.zcard(QUEUE_KEY)
    return {
        "user_id": req.user_id,
        "rank": rank,          # 0-indexed (앞에 몇 명 있는지)
        "total_waiting": total,
        "message": "대기열 진입. /queue/status 로 순번을 폴링하세요(2~3초 간격 권장).",
    }


@router.get("/status")
async def queue_status(user_id: int):
    """순번 조회. 입장 가능 범위면 대기열에서 빼고 TTL JWT 를 발급한다."""
    redis = get_redis()
    member = str(user_id)
    rank = await redis.zrank(QUEUE_KEY, member)

    if rank is None:
        return {
            "user_id": user_id,
            "status": "not_in_queue",
            "message": "대기열에 없습니다. /queue/enter 를 먼저 호출하세요.",
        }

    if rank < settings.allowed_entry_count:
        # 입장 허용 -> 대기열에서 제거(뒷사람 순번이 당겨진다) + 토큰 발급
        await redis.zrem(QUEUE_KEY, member)
        token = create_entry_token(user_id)
        return {
            "user_id": user_id,
            "status": "admitted",
            "entry_token": token,
            "ttl": settings.jwt_ttl,
            "message": "입장 허용! 이 토큰을 Authorization: Bearer 로 넣어 예매하세요.",
        }

    return {
        "user_id": user_id,
        "status": "waiting",
        "rank": rank,
        "ahead": rank,
        "message": f"대기 중... 앞에 {rank}명. 잠시 후 다시 조회하세요.",
    }
