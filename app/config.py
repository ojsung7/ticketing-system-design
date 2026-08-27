from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """환경변수 기반 설정. .env 또는 docker-compose 의 environment 로 주입된다."""

    database_url: str = "postgresql://ticketing:ticketing@localhost:5432/ticketing"
    redis_url: str = "redis://localhost:6379/0"

    seat_lock_ttl: int = 300
    allowed_entry_count: int = 100

    jwt_secret: str = "change-me-in-production"
    jwt_ttl: int = 300

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
