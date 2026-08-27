"""좌석/공연 조회 API (읽기 전용).

좌석 '선점(reserve)' API 는 Phase 1 의 학습 흐름(문제 재현 -> 해결)을 위해
별도 커밋에서 단계적으로 추가된다.
"""

from fastapi import APIRouter, HTTPException

from app.database import get_db

router = APIRouter(tags=["seats"])


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
