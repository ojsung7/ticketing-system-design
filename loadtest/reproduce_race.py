"""좌석 선점 race condition 재현/검증 스크립트.

같은 좌석 하나에 대해 서로 다른 user_id 로 동시 예매 요청을 N개 쏜다.
정상이라면 딱 1건만 성공(200)하고 나머지는 409 여야 한다.
락이 없으면 여러 요청이 동시에 'available' 을 보고 통과 -> 여러 건이 200 성공하고
bookings 테이블에 중복 row 가 쌓인다.

사용법:
    # 스택 기동 후
    python loadtest/reproduce_race.py --seat 1 --concurrency 30

    # 다른 좌석으로 반복 테스트하려면 seat 번호를 바꾸면 된다
    #   (한 번 예매된 좌석은 reserved 라 재현이 안 되므로 매번 새 좌석 사용)
"""

import argparse
import asyncio

import httpx

BASE_URL = "http://localhost:8000"


async def get_entry_token(client: httpx.AsyncClient, user_id: int) -> str | None:
    """대기열 진입 -> 순번 확인 -> 진입 토큰 획득."""
    await client.post(f"{BASE_URL}/queue/enter", json={"user_id": user_id})
    r = await client.get(f"{BASE_URL}/queue/status", params={"user_id": user_id})
    return r.json().get("entry_token")


async def reserve(client: httpx.AsyncClient, seat_id: int, user_id: int):
    try:
        token = await get_entry_token(client, user_id)
        resp = await client.post(
            f"{BASE_URL}/seats/{seat_id}/reserve",
            json={"user_id": user_id},
            headers={"Authorization": f"Bearer {token}"},
            timeout=10.0,
        )
        return resp.status_code
    except Exception as e:  # noqa: BLE001
        return f"ERR:{type(e).__name__}"


async def main(seat_id: int, concurrency: int):
    async with httpx.AsyncClient() as client:
        tasks = [reserve(client, seat_id, uid) for uid in range(1, concurrency + 1)]
        results = await asyncio.gather(*tasks)

        # 검증용 조회
        r = await client.get(f"{BASE_URL}/seats/{seat_id}/bookings")
        booked = r.json()

    ok = sum(1 for s in results if s == 200)
    conflict = sum(1 for s in results if s == 409)
    other = [s for s in results if s not in (200, 409)]

    print(f"\n=== 좌석 {seat_id} 동시 선점 결과 (동시성 {concurrency}) ===")
    print(f"  200 선점 성공   : {ok}")
    print(f"  409 이미 선점   : {conflict}")
    if other:
        print(f"  기타 응답       : {other}")
    # reserve 는 이제 '선점(hold)'만 하므로 bookings 는 confirm+worker 후에 쌓인다.
    print(f"  (참고) bookings : {booked['count']}  ← confirm 전이면 0 이 정상")

    if ok > 1:
        print("\n  ❌ RACE CONDITION! 한 좌석을 여러 요청이 동시에 선점했다.")
    else:
        print("\n  ✅ 정상: 동시 요청 중 정확히 1건만 좌석을 선점했다.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seat", type=int, default=1, help="테스트할 좌석 id")
    parser.add_argument("--concurrency", type=int, default=30, help="동시 요청 수")
    args = parser.parse_args()
    asyncio.run(main(args.seat, args.concurrency))
