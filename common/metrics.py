"""Prometheus 계측 공통 헬퍼.

각 FastAPI 서비스에 /metrics 엔드포인트를 붙이고, 요청 수·지연·에러율 같은 기본
HTTP 메트릭을 자동 수집한다. 도메인 지표(대기열 적체, 확정 처리량)는 아래 커스텀
메트릭으로 별도 노출한다.
"""

from prometheus_client import Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator

# ── 도메인 커스텀 메트릭 ──────────────────────────────────
# 대기열 적체(현재 대기 인원). queue-service 가 갱신.
waiting_queue_size = Gauge(
    "ticketing_waiting_queue_size", "현재 대기열에 남아 있는 사용자 수"
)

# 좌석 선점 결과 카운터. booking-service 가 갱신.
seat_reserve_total = Counter(
    "ticketing_seat_reserve_total", "좌석 선점 요청 결과", ["result"]  # result=success|conflict
)

# 결제 확정 이벤트 발행/소비 카운터.
payment_confirm_published_total = Counter(
    "ticketing_payment_confirm_published_total", "발행된 결제 확정 이벤트 수"
)
booking_confirmed_total = Counter(
    "ticketing_booking_confirmed_total", "Worker 가 DB 에 반영한 예매 확정 수"
)


def setup_metrics(app) -> None:
    """FastAPI 앱에 기본 HTTP 메트릭 + /metrics 엔드포인트를 추가한다."""
    Instrumentator().instrument(app).expose(app, endpoint="/metrics")
