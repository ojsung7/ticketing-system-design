"""Kafka 파티셔닝 + 컨슈머 그룹 시연.

공연(performance) 3개에 대해 각각 예매->결제확정을 수행한다. payment 는
performance_id 를 파티션 키로 발행하므로, 같은 공연의 확정 이벤트는 항상 같은
파티션으로 가고 서로 다른 공연은 파티션이 나뉜다. 컨슈머 그룹의 워커를 여러 개
띄우면(docker-compose up -d --scale booking-worker=3) 파티션이 워커들에 분배된다.

실행:
    docker-compose up -d --scale booking-worker=3
    python loadtest/kafka_partition_demo.py
    docker-compose logs booking-worker   # 어느 워커가 어느 파티션을 처리했는지 확인

각 공연의 시작 좌석 id (init.sql 시드 기준):
    performance 1 -> seat id 1..1000 (A*)
    performance 2 -> seat id 1001..1200 (B*)
    performance 3 -> seat id 1201..1400 (C*)
"""

import asyncio

import httpx

QUEUE_URL = "http://localhost:8001"
BOOKING_URL = "http://localhost:8002"
PAYMENT_URL = "http://localhost:8003"

# (performance_id, seat_id) — 공연별로 한 좌석씩
CASES = [
    (1, 10),
    (2, 1010),
    (3, 1210),
]


async def book(client: httpx.AsyncClient, performance_id: int, seat_id: int, user_id: int):
    await client.post(f"{QUEUE_URL}/queue/enter", json={"user_id": user_id})
    token = (
        await client.get(f"{QUEUE_URL}/queue/status", params={"user_id": user_id})
    ).json().get("entry_token")
    headers = {"Authorization": f"Bearer {token}"}

    r = await client.post(
        f"{BOOKING_URL}/seats/{seat_id}/reserve",
        json={"user_id": user_id},
        headers=headers,
    )
    r = await client.post(
        f"{PAYMENT_URL}/payments/confirm",
        json={"user_id": user_id, "seat_id": seat_id, "performance_id": performance_id},
        headers=headers,
    )
    print(f"  perf={performance_id} seat={seat_id} confirm -> {r.status_code}")


async def main():
    async with httpx.AsyncClient(timeout=10.0) as client:
        print("공연별 결제 확정 이벤트 발행 (key=performance_id):")
        await asyncio.gather(
            *[book(client, pid, sid, 90000 + i) for i, (pid, sid) in enumerate(CASES)]
        )
    print("\n→ 이제 `docker-compose logs booking-worker` 로 파티션/워커 분배를 확인하세요.")


if __name__ == "__main__":
    asyncio.run(main())
