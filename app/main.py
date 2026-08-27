from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import connect, disconnect, get_db, get_redis
from app.routers import queue, seats


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect()
    yield
    await disconnect()


app = FastAPI(title="Ticketing System (Phase 1)", lifespan=lifespan)

app.include_router(queue.router)
app.include_router(seats.router)


@app.get("/health")
async def health():
    """DB / Redis 연결 상태 확인용 헬스체크."""
    db_ok = redis_ok = False
    try:
        await get_db().fetchval("SELECT 1")
        db_ok = True
    except Exception:
        pass
    try:
        redis_ok = await get_redis().ping()
    except Exception:
        pass
    return {"status": "ok", "db": db_ok, "redis": redis_ok}
