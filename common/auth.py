"""예매 진입 토큰(JWT) 발급/검증 — 서비스 공통.

queue-service 가 발급하고, booking/payment 서비스가 같은 JWT_SECRET 으로 검증한다.
이렇게 공유 시크릿 기반 토큰으로 서비스 간 신뢰를 세운다(별도 세션 서버 없이).
"""

from datetime import datetime, timedelta, timezone

from fastapi import Header, HTTPException
from jose import JWTError, jwt

from common.config import settings

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
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="예매 진입 토큰이 필요합니다. 먼저 대기열(queue-service)을 통과하세요.",
        )
    token = authorization.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="토큰이 만료되었거나 유효하지 않습니다.")
    return int(payload["user_id"])
