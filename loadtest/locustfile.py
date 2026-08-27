"""대기열 '없음 vs 있음' 부하 비교 (Locust).

두 시나리오는 **완전히 동일한 코드 경로**(queue/enter -> queue/status -> reserve)를 탄다.
차이는 서버의 ALLOWED_ENTRY_COUNT(입장 인원 제한) 하나뿐이다.

- 대기열 없음: 서버를 ALLOWED_ENTRY_COUNT=10000000 로 기동 -> 모두 즉시 입장,
  N명이 동시에 backend(reserve)를 폭격한다.
- 대기열 있음: 서버를 ALLOWED_ENTRY_COUNT=50 으로 기동 -> 앞 50명씩만 통과,
  나머지는 싼 Redis 폴링(zrank)으로 대기 -> backend 동시성이 낮게 유지된다.

측정 포인트는 'reserve' 엔드포인트의 처리량/지연/에러율이다.
좌석이 매진(409)되거나 이미 선점(409)된 경우는 정상적인 비즈니스 응답이므로
실패로 세지 않는다(catch_response 로 성공 처리).

실행 예:
    # 서버를 원하는 ALLOWED_ENTRY_COUNT 로 먼저 기동한 뒤
    locust -f loadtest/locustfile.py --headless -u 500 -r 100 -t 30s \
           --host http://localhost:8001 --only-summary

MSA 구성: queue-service(:8001)에서 대기열/토큰, booking-service(:8002)에서 선점.
서비스별 포트가 달라 요청은 절대 URL 로 보낸다.
"""

import random

from locust import HttpUser, between, task

NUM_SEATS = 1000
QUEUE_URL = "http://localhost:8001"
BOOKING_URL = "http://localhost:8002"


class TicketingUser(HttpUser):
    wait_time = between(0.0, 0.05)

    def on_start(self):
        self.user_id = random.randint(1, 100_000_000)

    def _get_token(self) -> str | None:
        """대기열 진입 후, 입장 토큰을 받을 때까지 status 를 폴링."""
        self.client.post(
            f"{QUEUE_URL}/queue/enter", json={"user_id": self.user_id}, name="queue/enter"
        )
        for _ in range(50):  # 최대 50회 폴링
            r = self.client.get(
                f"{QUEUE_URL}/queue/status?user_id={self.user_id}", name="queue/status"
            )
            data = r.json()
            if data.get("status") == "admitted":
                return data["entry_token"]
            # waiting -> 잠깐 쉬고 재조회 (실제 클라이언트 폴링 흉내)
            import time as _t

            _t.sleep(0.1)
        return None

    @task
    def book_seat(self):
        token = self._get_token()
        if not token:
            return
        seat_id = random.randint(1, NUM_SEATS)
        with self.client.post(
            f"{BOOKING_URL}/seats/{seat_id}/reserve",
            json={"user_id": self.user_id},
            headers={"Authorization": f"Bearer {token}"},
            name="reserve",
            catch_response=True,
        ) as resp:
            # 200(선점 성공) / 409(이미 선점·매진) 는 모두 정상 동작.
            if resp.status_code in (200, 409):
                resp.success()
            else:
                resp.failure(f"unexpected {resp.status_code}")
