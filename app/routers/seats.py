"""좌석/공연 조회 + 선점 API.

좌석 선점은 Redis + Lua script 로 원자적으로 처리한다. Redis 는 단일 스레드로
명령을 실행하므로 Lua script 안의 GET+SET 이 통째로 원자적으로 실행되어,
동시 요청 중 정확히 한 명만 좌석을 선점하게 된다(check-then-act race 제거).
선점에는 TTL 을 걸어 결제가 완료되지 않으면 자동으로 해제되게 한다.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import settings
from app.database import get_db, get_redis

router = APIRouter(tags=["seats"])

# GET 이 nil(미선점)일 때만 SET 하고 1 을 반환. 이미 값이 있으면 0.
# GET+SET 이 하나의 Lua script 로 묶여 Redis 단일 스레드에서 원자적으로 실행된다.
SEAT_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == false then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    return 1
else
    return 0
end
"""


class ReserveRequest(BaseModel):
    user_id: int


@router.get("/performances")
async def list_performances():
    pool = get_db()
    rows = await pool.fetch("SELECT id, name, show_time FROM performances ORDER BY id")
    return [dict(r) for r in rows]


@router.get("/performances/{performance_id}/seats")
async def list_seats(performance_id: int):
    pool = get_db()
    rows = await pool.fetch(
        """
        SELECT id, seat_number, status
        FROM seats
        WHERE performance_id = $1
        ORDER BY id
        """,
        performance_id,
    )
    if not rows:
        raise HTTPException(status_code=404, detail="공연 또는 좌석이 없습니다.")
    return [dict(r) for r in rows]


@router.post("/seats/{seat_id}/reserve")
async def reserve_seat(seat_id: int, req: ReserveRequest):
    """좌석 선점 — Redis Lua script 원자적 락 (race condition 해결).

    1) Lua script 로 `seat:{id}` 키를 원자적으로 선점(GET+SET). 이미 선점돼 있으면 409.
    2) 선점 성공한 요청만 DB 에 예매 기록/좌석 상태 반영.
    선점 키에는 TTL(SEAT_LOCK_TTL)을 걸어 결제 미완료 시 자동 해제된다.
    """
    redis = get_redis()
    pool = get_db()

    seat = await pool.fetchrow("SELECT id, status FROM seats WHERE id = $1", seat_id)
    if seat is None:
        raise HTTPException(status_code=404, detail="좌석이 없습니다.")
    if seat["status"] != "available":
        raise HTTPException(status_code=409, detail="이미 판매된 좌석입니다.")

    # --- 핵심: 원자적 선점. 동시 요청 중 정확히 하나만 1 을 돌려받는다. ---
    acquired = await redis.eval(
        SEAT_LOCK_SCRIPT,
        1,                       # KEYS 개수
        f"seat:{seat_id}",       # KEYS[1]
        str(req.user_id),        # ARGV[1]
        str(settings.seat_lock_ttl),  # ARGV[2] = TTL(초)
    )
    if acquired == 0:
        raise HTTPException(status_code=409, detail="이미 선점된 좌석입니다.")

    # 선점 성공. DB 에는 아직 아무것도 쓰지 않는다(hold 상태).
    # 결제까지 끝나면 confirm 에서 큐로 넘겨 Worker 가 DB 에 최종 반영한다.
    return {
        "seat_id": seat_id,
        "user_id": req.user_id,
        "status": "held",
        "message": f"좌석 선점 성공. {settings.seat_lock_ttl}초 내 결제를 완료하세요.",
    }


@router.post("/seats/{seat_id}/confirm", status_code=202)
async def confirm_seat(seat_id: int, req: ReserveRequest):
    """결제 확정 — 큐에 적재만 하고 즉시 응답 (비동기 확정 / 백프레셔).

    API 서버는 DB 에 직접 쓰지 않는다. Redis Stream 에 확정 이벤트를 넣고 바로 응답하며,
    실제 DB 반영은 Worker(app/worker.py)가 자기 속도로 소비해서 처리한다.
    트래픽이 순간적으로 튀어도 큐에 쌓였다가 서서히 처리된다.
    """
    redis = get_redis()

    # 이 좌석의 현재 선점자가 요청자 본인인지 확인 (TTL 만료/타인 선점 방어)
    holder = await redis.get(f"seat:{seat_id}")
    if holder is None:
        raise HTTPException(status_code=410, detail="선점이 만료되었습니다. 다시 시도하세요.")
    if holder != str(req.user_id):
        raise HTTPException(status_code=409, detail="다른 사용자가 선점한 좌석입니다.")

    await redis.xadd(
        "booking_confirm_stream",
        {"seat_id": str(seat_id), "user_id": str(req.user_id)},
    )
    return {"seat_id": seat_id, "user_id": req.user_id, "status": "confirming"}


@router.get("/seats/{seat_id}/bookings")
async def seat_bookings(seat_id: int):
    """검증용 — 해당 좌석에 실제로 몇 건의 예매가 쌓였는지 조회.

    race condition 이 발생하면 count 가 2 이상으로 찍힌다(중복 예매).
    """
    pool = get_db()
    rows = await pool.fetch(
        "SELECT id, user_id, booked_at FROM bookings WHERE seat_id = $1 ORDER BY id",
        seat_id,
    )
    return {"seat_id": seat_id, "count": len(rows), "bookings": [dict(r) for r in rows]}
