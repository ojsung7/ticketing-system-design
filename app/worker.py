"""결제 확정 Worker — Redis Stream 소비 -> PostgreSQL 최종 반영.

API 서버는 confirm 시 Redis Stream(booking_confirm_stream)에 이벤트만 넣는다.
이 Worker 가 별도 프로세스로 그 이벤트를 소비해서 bookings insert + 좌석 sold 처리한다.
DB 는 항상 Worker 가 감당 가능한 속도로만 쓰기를 받는다(백프레셔).

멱등성: bookings(seat_id) unique 인덱스 + ON CONFLICT DO NOTHING 으로,
같은 이벤트가 재처리돼도 중복 예매가 생기지 않는다.

Phase 1 은 단순 XREAD 로 구현한다. 컨슈머 그룹(재처리/리밸런싱)은 Phase 3(Kafka)에서 다룬다.
"""

import asyncio

from app.database import connect, disconnect, get_db, get_redis

STREAM = "booking_confirm_stream"


async def process(seat_id: int, user_id: int) -> None:
    pool = get_db()
    redis = get_redis()
    # 최종 예매 반영 (멱등)
    await pool.execute(
        """
        INSERT INTO bookings (seat_id, user_id) VALUES ($1, $2)
        ON CONFLICT (seat_id) DO NOTHING
        """,
        seat_id,
        user_id,
    )
    await pool.execute("UPDATE seats SET status = 'sold' WHERE id = $1", seat_id)
    # 확정 완료 -> 선점 락은 이제 불필요(DB 의 sold 가 source of truth)
    await redis.delete(f"seat:{seat_id}")
    print(f"[worker] 확정 완료 seat={seat_id} user={user_id}", flush=True)


async def main() -> None:
    await connect()
    redis = get_redis()
    print("[worker] 시작. booking_confirm_stream 소비 대기...", flush=True)

    last_id = "0"  # 0 부터 읽어 미처리 이벤트를 놓치지 않는다(멱등하므로 재처리 안전)
    try:
        while True:
            resp = await redis.xread({STREAM: last_id}, block=5000, count=20)
            if not resp:
                continue
            for _stream, entries in resp:
                for entry_id, data in entries:
                    await process(int(data["seat_id"]), int(data["user_id"]))
                    last_id = entry_id
    finally:
        await disconnect()


if __name__ == "__main__":
    asyncio.run(main())
