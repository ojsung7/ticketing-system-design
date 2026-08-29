"""서비스 공통 설정.

세 서비스(queue/booking/payment)가 같은 환경변수 규약을 공유한다.
JWT_SECRET 은 서비스 간 진입 토큰 검증을 위해 반드시 동일한 값을 써야 한다.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql://ticketing:ticketing@localhost:5432/ticketing"
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap: str = "localhost:9092"

    # 결제 확정 이벤트 토픽 (파티션 키 = performance_id)
    confirm_topic: str = "booking-confirm"
    confirm_group: str = "booking-confirmers"

    seat_lock_ttl: int = 300
    allowed_entry_count: int = 100

    jwt_secret: str = "change-me-in-production"
    jwt_ttl: int = 300

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
