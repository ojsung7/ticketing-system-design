"""booking-service 의 확정 Worker — Kafka 컨슈머 그룹으로 booking-confirm 소비.

Phase 3: Redis Stream(xread) 대신 Kafka 컨슈머 그룹으로 이벤트를 소비한다.
- group_id 를 공유하는 워커를 여러 개 띄우면 파티션이 워커들에 분배되어 수평 확장된다.
- 파티션은 payment 가 performance_id 를 키로 발행하므로 공연별로 나뉜다.
- 한 워커가 죽으면 그 파티션이 살아있는 워커로 리밸런싱된다.

멱등성: bookings(seat_id) unique 인덱스 + ON CONFLICT DO NOTHING (재처리/재밸런싱 안전).
오프셋은 DB 반영이 끝난 뒤에 커밋한다(at-least-once + 멱등 = 사실상 정확히 한 번 반영).
"""

import asyncio
import json
import os
import socket

from aiokafka import AIOKafkaConsumer

from common.clients import connect_db, connect_redis, disconnect, get_db, get_redis
from common.config import settings

WORKER_ID = os.getenv("HOSTNAME", socket.gethostname())


async def process(seat_id: int, user_id: int, performance_id: int, partition: int) -> None:
    pool = get_db()
    redis = get_redis()
    await pool.execute(
        """
        INSERT INTO bookings (seat_id, user_id) VALUES ($1, $2)
        ON CONFLICT (seat_id) DO NOTHING
        """,
        seat_id,
        user_id,
    )
    await pool.execute("UPDATE seats SET status = 'sold' WHERE id = $1", seat_id)
    await redis.delete(f"seat:{seat_id}")
    print(
        f"[worker {WORKER_ID}] p{partition} 확정 seat={seat_id} "
        f"user={user_id} perf={performance_id}",
        flush=True,
    )


async def main() -> None:
    await connect_redis()
    await connect_db()

    consumer = AIOKafkaConsumer(
        settings.confirm_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=settings.confirm_group,
        enable_auto_commit=False,      # DB 반영 후 수동 커밋 (at-least-once)
        auto_offset_reset="earliest",
    )
    # 브로커/토픽 준비 전이면 재시도
    for attempt in range(30):
        try:
            await consumer.start()
            break
        except Exception as e:  # noqa: BLE001
            print(f"[worker {WORKER_ID}] kafka 연결 재시도 {attempt+1}: {e}", flush=True)
            await asyncio.sleep(2)
    else:
        raise RuntimeError("Kafka 연결 실패")

    print(
        f"[worker {WORKER_ID}] 시작. topic={settings.confirm_topic} "
        f"group={settings.confirm_group} 소비 대기...",
        flush=True,
    )
    try:
        async for msg in consumer:
            data = json.loads(msg.value)
            await process(
                int(data["seat_id"]),
                int(data["user_id"]),
                int(data.get("performance_id", 0)),
                msg.partition,
            )
            await consumer.commit()
    finally:
        await consumer.stop()
        await disconnect()


if __name__ == "__main__":
    asyncio.run(main())
