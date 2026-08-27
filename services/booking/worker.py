"""booking-service 의 확정 Worker — booking_confirm_stream 소비 -> DB 반영.

payment-service 가 결제 확정 시 Redis Stream 에 이벤트를 발행하면, 이 Worker 가
소비해서 bookings insert + 좌석 sold 처리한다. booking 도메인의 DB 쓰기는 모두
이 Worker 를 통해서만 일어난다(백프레셔 + 데이터 소유권 일원화).

멱등성: bookings(seat_id) unique 인덱스 + ON CONFLICT DO NOTHING.
"""

import asyncio

from common.clients import connect_db, connect_redis, disconnect, get_db, get_redis

STREAM = "booking_confirm_stream"


async def process(seat_id: int, user_id: int) -> None:
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
    print(f"[booking-worker] 확정 완료 seat={seat_id} user={user_id}", flush=True)


async def main() -> None:
    await connect_redis()
    await connect_db()
    redis = get_redis()
    print("[booking-worker] 시작. booking_confirm_stream 소비 대기...", flush=True)

    last_id = "0"
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
