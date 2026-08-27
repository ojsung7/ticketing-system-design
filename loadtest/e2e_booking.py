"""예매 전체 흐름 e2e 검증(MSA): 대기열 -> 선점 -> 결제확정 -> Worker 반영.

여러 서비스에 걸쳐 흐른다:
  queue(:8001) -> booking(:8002) reserve -> payment(:8003) confirm
  -> (Redis Stream 이벤트) -> booking-worker -> DB 반영
payment 는 booking DB 를 직접 건드리지 않고 확정 이벤트만 발행한다.

사용법:
    python loadtest/e2e_booking.py --seat 5 --user 777
"""

import argparse
import asyncio

import httpx

QUEUE_URL = "http://localhost:8001"
BOOKING_URL = "http://localhost:8002"
PAYMENT_URL = "http://localhost:8003"


async def main(seat_id: int, user_id: int):
    async with httpx.AsyncClient(timeout=10.0) as client:
        # 0) queue-service: 대기열 통과 -> 진입 토큰 획득
        await client.post(f"{QUEUE_URL}/queue/enter", json={"user_id": user_id})
        st = (
            await client.get(f"{QUEUE_URL}/queue/status", params={"user_id": user_id})
        ).json()
        token = st.get("entry_token")
        print(f"0) queue(:8001) -> {st['status']}, token={'발급됨' if token else '없음'}")
        headers = {"Authorization": f"Bearer {token}"}

        # 1) booking-service: 좌석 선점
        r = await client.post(
            f"{BOOKING_URL}/seats/{seat_id}/reserve",
            json={"user_id": user_id},
            headers=headers,
        )
        print(f"1) booking(:8002) reserve -> {r.status_code} {r.json()}")
        if r.status_code != 200:
            return

        # 2) payment-service: 결제 확정 (이벤트 발행만, 즉시 202)
        r = await client.post(
            f"{PAYMENT_URL}/payments/confirm",
            json={"user_id": user_id, "seat_id": seat_id},
            headers=headers,
        )
        print(f"2) payment(:8003) confirm -> {r.status_code} {r.json()}  (DB 미접근, 이벤트 발행)")
        if r.status_code != 202:
            return

        # 3) booking-worker 가 이벤트를 소비해 DB 에 반영할 때까지 폴링
        print("3) booking-worker 비동기 반영 대기...")
        for i in range(20):
            await asyncio.sleep(0.25)
            b = (await client.get(f"{BOOKING_URL}/seats/{seat_id}/bookings")).json()
            if b["count"] >= 1:
                s = (await client.get(f"{BOOKING_URL}/performances/1/seats")).json()
                status = next((x["status"] for x in s if x["id"] == seat_id), "?")
                print(
                    f"   ✅ {(i + 1) * 0.25:.2f}s 후 DB 반영 완료 "
                    f"— bookings={b['count']}, seat.status={status}"
                )
                return
        print("   ❌ 시간 내 DB 반영 안 됨 (Worker 가 떠 있는지 확인)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seat", type=int, default=5)
    parser.add_argument("--user", type=int, default=777)
    args = parser.parse_args()
    asyncio.run(main(args.seat, args.user))
