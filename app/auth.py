"""예매 페이지 진입 토큰(JWT).

대기열을 통과한 사용자에게만 TTL 이 걸린 JWT 를 발급한다. 이 토큰이 있어야만
좌석 선점/결제 확정 API 에 진입할 수 있어, 대기열이 실제 트래픽 게이트 역할을 한다.
"""

from datetime import datetime, timedelta, timezone

from fastapi import Header, HTTPException
from jose import JWTError, jwt

from app.config import settings

ALGORITHM = "HS256"


def create_entry_token(user_id: int) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "user_id": user_id,
        "iat": now,
        "exp": now + timedelta(seconds=settings.jwt_ttl),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=ALGORITHM)


async def require_entry_token(authorization: str | None = Header(default=None)) -> int:
    """Authorization: Bearer <token> 헤더를 검증하고 user_id 를 돌려준다."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="예매 진입 토큰이 필요합니다. 먼저 대기열(/queue)을 통과하세요.",
        )
    token = authorization.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="토큰이 만료되었거나 유효하지 않습니다.")
    return int(payload["user_id"])
