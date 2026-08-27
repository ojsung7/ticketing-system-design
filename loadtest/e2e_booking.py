"""예매 전체 흐름 e2e 검증: 선점(reserve) -> 결제확정(confirm) -> Worker 반영.

confirm 은 큐에 넣기만 하고 즉시 202 를 돌려주므로, DB 반영은 잠깐 뒤에
Worker 에 의해 비동기로 일어난다. 이 스크립트는 그 비동기 확정이 실제로
DB 까지 반영되는지 폴링으로 확인한다.

사용법:
    python loadtest/e2e_booking.py --seat 5 --user 777
"""

import argparse
import asyncio

import httpx

BASE_URL = "http://localhost:8000"


async def main(seat_id: int, user_id: int):
    async with httpx.AsyncClient(timeout=10.0) as client:
        # 0) 대기열 통과 -> 진입 토큰 획득
        await client.post(f"{BASE_URL}/queue/enter", json={"user_id": user_id})
        st = (
            await client.get(f"{BASE_URL}/queue/status", params={"user_id": user_id})
        ).json()
        token = st.get("entry_token")
        print(f"0) queue -> {st['status']}, token={'발급됨' if token else '없음'}")
        headers = {"Authorization": f"Bearer {token}"}

        # 1) 선점
        r = await client.post(
            f"{BASE_URL}/seats/{seat_id}/reserve",
            json={"user_id": user_id},
            headers=headers,
        )
        print(f"1) reserve -> {r.status_code} {r.json()}")
        if r.status_code != 200:
            return

        # 2) 결제 확정 (큐 적재만, 즉시 202)
        r = await client.post(
            f"{BASE_URL}/seats/{seat_id}/confirm",
            json={"user_id": user_id},
            headers=headers,
        )
        print(f"2) confirm -> {r.status_code} {r.json()}  (여기서 API 는 DB 를 안 건드림)")
        if r.status_code != 202:
            return

        # 3) Worker 가 DB 에 반영할 때까지 폴링
        print("3) Worker 비동기 반영 대기...")
        for i in range(20):
            await asyncio.sleep(0.25)
            b = (await client.get(f"{BASE_URL}/seats/{seat_id}/bookings")).json()
            if b["count"] >= 1:
                s = (await client.get(f"{BASE_URL}/performances/1/seats")).json()
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
