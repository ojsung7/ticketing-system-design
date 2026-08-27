"""booking-service — 좌석 조회 + 선점(Redis 원자 락).

이 서비스가 좌석/예매 도메인과 PostgreSQL 을 소유한다.
좌석 선점은 Redis Lua script 로 원자적으로 처리하고(중복 선점 방지), 진입 토큰(JWT)이
있어야만 호출할 수 있다. 실제 예매 확정(DB insert)은 worker 가 이벤트를 소비해 수행한다.
"""

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from common.auth import require_entry_token
from common.clients import connect_db, connect_redis, disconnect, get_db, get_redis
from common.config import settings

# GET 이 nil 일 때만 SET -> 원자적 좌석 선점. Redis 단일 스레드에서 통째로 실행된다.
SEAT_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == false then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    return 1
else
    return 0
end
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_redis()
    await connect_db()
    yield
    await disconnect()


app = FastAPI(title="booking-service", lifespan=lifespan)


class ReserveRequest(BaseModel):
    user_id: int


@app.get("/health")
async def health():
    db_ok = redis_ok = False
    try:
        await get_db().fetchval("SELECT 1")
        db_ok = True
    except Exception:
        pass
    try:
        redis_ok = await get_redis().ping()
    except Exception:
        pass
    return {"service": "booking", "db": db_ok, "redis": redis_ok}


@app.get("/performances")
async def list_performances():
    rows = await get_db().fetch("SELECT id, name, show_time FROM performances ORDER BY id")
    return [dict(r) for r in rows]


@app.get("/performances/{performance_id}/seats")
async def list_seats(performance_id: int):
    rows = await get_db().fetch(
        "SELECT id, seat_number, status FROM seats WHERE performance_id = $1 ORDER BY id",
        performance_id,
    )
    if not rows:
        raise HTTPException(status_code=404, detail="공연 또는 좌석이 없습니다.")
    return [dict(r) for r in rows]


@app.post("/seats/{seat_id}/reserve")
async def reserve_seat(
    seat_id: int,
    req: ReserveRequest,
    token_user_id: int = Depends(require_entry_token),
):
    if token_user_id != req.user_id:
        raise HTTPException(status_code=403, detail="토큰의 사용자와 요청 사용자가 다릅니다.")
    redis = get_redis()
    pool = get_db()

    seat = await pool.fetchrow("SELECT id, status FROM seats WHERE id = $1", seat_id)
    if seat is None:
        raise HTTPException(status_code=404, detail="좌석이 없습니다.")
    if seat["status"] != "available":
        raise HTTPException(status_code=409, detail="이미 판매된 좌석입니다.")

    acquired = await redis.eval(
        SEAT_LOCK_SCRIPT,
        1,
        f"seat:{seat_id}",
        str(req.user_id),
        str(settings.seat_lock_ttl),
    )
    if acquired == 0:
        raise HTTPException(status_code=409, detail="이미 선점된 좌석입니다.")

    return {
        "seat_id": seat_id,
        "user_id": req.user_id,
        "status": "held",
        "message": f"좌석 선점 성공. {settings.seat_lock_ttl}초 내 결제를 완료하세요.",
    }


@app.get("/seats/{seat_id}/bookings")
async def seat_bookings(seat_id: int):
    rows = await get_db().fetch(
        "SELECT id, user_id, booked_at FROM bookings WHERE seat_id = $1 ORDER BY id",
        seat_id,
    )
    return {"seat_id": seat_id, "count": len(rows), "bookings": [dict(r) for r in rows]}
