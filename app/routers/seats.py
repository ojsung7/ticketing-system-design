"""좌석/공연 조회 + 선점 API.

[학습 목적 주의]
이 파일의 좌석 선점(reserve)은 **의도적으로** Redis 락 없이 DB 조회+insert 로만
구현되어 있다. 동시 요청이 몰리면 "빈 좌석"을 두 요청이 동시에 보고 둘 다
예매를 시도하는 race condition 이 발생한다. Phase 1 의 다음 커밋에서 이 문제를
Redis Lua script 원자적 락으로 해결한다.
"""

import asyncio

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.database import get_db

router = APIRouter(tags=["seats"])


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
    """좌석 선점 — DB 조회+삽입만 사용하는 순진한 버전 (race condition 존재!).

    문제: '빈 좌석인지 확인'과 '예매 기록/좌석 상태 변경'이 원자적으로 묶여 있지 않다.
    동시 요청 A, B 가 거의 같은 시각에 들어오면 둘 다 status='available' 을 보고
    통과한 뒤 둘 다 예매를 진행 -> 같은 좌석이 두 번 예매되는 중복 예매가 발생한다.
    (아래 asyncio.sleep 은 실제 부하 상황에서 벌어지는 경합 구간을 재현하기 쉽게
     넓혀 둔 것으로, 실서비스에서는 네트워크/DB 지연이 그 역할을 한다.)
    """
    pool = get_db()

    # 1) 좌석이 비어 있는지 확인 (check)
    seat = await pool.fetchrow(
        "SELECT id, status FROM seats WHERE id = $1", seat_id
    )
    if seat is None:
        raise HTTPException(status_code=404, detail="좌석이 없습니다.")
    if seat["status"] != "available":
        raise HTTPException(status_code=409, detail="이미 선점된 좌석입니다.")

    # --- 경합 구간(check 와 act 사이). 이 틈에 다른 요청이 끼어든다. ---
    await asyncio.sleep(0.05)

    # 2) 예매 기록 + 좌석 상태 변경 (act) — 원자성 보장 없음
    await pool.execute(
        "INSERT INTO bookings (seat_id, user_id) VALUES ($1, $2)",
        seat_id,
        req.user_id,
    )
    await pool.execute(
        "UPDATE seats SET status = 'reserved' WHERE id = $1", seat_id
    )
    return {"seat_id": seat_id, "user_id": req.user_id, "status": "reserved"}
