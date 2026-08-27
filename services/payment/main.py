"""payment-service — 결제 확정 -> 이벤트 발행.

결제가 끝나면 booking_confirm_stream(Redis Stream)에 확정 이벤트를 발행하고 즉시 응답한다.
payment-service 는 booking DB 를 직접 건드리지 않는다. 실제 예매 확정은 booking-service
의 worker 가 이벤트를 소비해 수행한다(서비스 간 이벤트 기반 통신 + 데이터 소유권 분리).

(실제 PG 연동은 이 프로젝트 범위 밖. 여기서는 '결제 성공'을 가정하고 확정 이벤트만 낸다.)
"""

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from common.auth import require_entry_token
from common.clients import connect_redis, disconnect, get_redis

STREAM = "booking_confirm_stream"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_redis()
    yield
    await disconnect()


app = FastAPI(title="payment-service", lifespan=lifespan)


class ConfirmRequest(BaseModel):
    user_id: int
    seat_id: int


@app.get("/health")
async def health():
    try:
        ok = await get_redis().ping()
    except Exception:
        ok = False
    return {"service": "payment", "redis": ok}


@app.post("/payments/confirm", status_code=202)
async def confirm_payment(
    req: ConfirmRequest,
    token_user_id: int = Depends(require_entry_token),
):
    if token_user_id != req.user_id:
        raise HTTPException(status_code=403, detail="토큰의 사용자와 요청 사용자가 다릅니다.")
    redis = get_redis()

    # 좌석 선점자가 요청자 본인인지 확인(선점 만료/타인 선점 방어).
    holder = await redis.get(f"seat:{req.seat_id}")
    if holder is None:
        raise HTTPException(status_code=410, detail="선점이 만료되었습니다. 다시 시도하세요.")
    if holder != str(req.user_id):
        raise HTTPException(status_code=409, detail="다른 사용자가 선점한 좌석입니다.")

    # 결제 성공 가정 -> 확정 이벤트 발행 후 즉시 응답(비동기 확정).
    await redis.xadd(
        STREAM, {"seat_id": str(req.seat_id), "user_id": str(req.user_id)}
    )
    return {"seat_id": req.seat_id, "user_id": req.user_id, "status": "confirming"}
