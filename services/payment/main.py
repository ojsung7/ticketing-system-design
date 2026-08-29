"""payment-service — 결제 확정 -> Kafka 이벤트 발행.

Phase 3: Redis Stream 대신 Kafka 토픽(booking-confirm)에 확정 이벤트를 발행한다.
파티션 키를 performance_id 로 잡아 공연별로 처리량을 분산하고, 같은 공연의 이벤트는
같은 파티션으로 가서 순서가 보장된다.

payment 는 booking DB 를 직접 건드리지 않는다. 실제 예매 확정은 booking-service 의
worker(컨슈머 그룹)가 토픽을 소비해 수행한다.
"""

import json
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from common.auth import require_entry_token
from common.clients import connect_redis, disconnect, get_redis
from common.config import settings

producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    await connect_redis()
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        enable_idempotence=True,  # 중복 발행 방지
    )
    await producer.start()
    yield
    await producer.stop()
    await disconnect()


app = FastAPI(title="payment-service", lifespan=lifespan)


class ConfirmRequest(BaseModel):
    user_id: int
    seat_id: int
    performance_id: int = 1  # 파티션 키


@app.get("/health")
async def health():
    try:
        ok = await get_redis().ping()
    except Exception:
        ok = False
    return {"service": "payment", "redis": ok, "kafka": producer is not None}


@app.post("/payments/confirm", status_code=202)
async def confirm_payment(
    req: ConfirmRequest,
    token_user_id: int = Depends(require_entry_token),
):
    if token_user_id != req.user_id:
        raise HTTPException(status_code=403, detail="토큰의 사용자와 요청 사용자가 다릅니다.")
    redis = get_redis()

    holder = await redis.get(f"seat:{req.seat_id}")
    if holder is None:
        raise HTTPException(status_code=410, detail="선점이 만료되었습니다. 다시 시도하세요.")
    if holder != str(req.user_id):
        raise HTTPException(status_code=409, detail="다른 사용자가 선점한 좌석입니다.")

    # 결제 성공 가정 -> Kafka 로 확정 이벤트 발행 (key=performance_id 로 파티셔닝).
    assert producer is not None
    value = json.dumps(
        {"seat_id": req.seat_id, "user_id": req.user_id, "performance_id": req.performance_id}
    ).encode()
    await producer.send_and_wait(
        settings.confirm_topic,
        value=value,
        key=str(req.performance_id).encode(),
    )
    return {"seat_id": req.seat_id, "user_id": req.user_id, "status": "confirming"}
