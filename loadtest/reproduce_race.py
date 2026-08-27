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


async def reserve(client: httpx.AsyncClient, seat_id: int, user_id: int):
    try:
        resp = await client.post(
            f"{BASE_URL}/seats/{seat_id}/reserve",
            json={"user_id": user_id},
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

    print(f"\n=== 좌석 {seat_id} 동시 예매 결과 (동시성 {concurrency}) ===")
    print(f"  200 성공        : {ok}")
    print(f"  409 이미 선점   : {conflict}")
    if other:
        print(f"  기타 응답       : {other}")
    print(f"  실제 bookings 수 : {booked['count']}")

    if booked["count"] > 1 or ok > 1:
        print("\n  ❌ RACE CONDITION 발생! 한 좌석이 중복 예매되었다.")
    else:
        print("\n  ✅ 정상: 한 좌석은 정확히 1건만 예매되었다.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seat", type=int, default=1, help="테스트할 좌석 id")
    parser.add_argument("--concurrency", type=int, default=30, help="동시 요청 수")
    args = parser.parse_args()
    asyncio.run(main(args.seat, args.concurrency))
