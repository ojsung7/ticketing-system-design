# 🎟️ Ticketing System Design

대용량 동시 접속(콘서트·기차 예매 스타일) 상황을 가정한 **트래픽 스파이크 대응 예매 시스템**.
개인 학습 + 포트폴리오 목적의 미니 프로젝트로, "문제 인식 → 해결 과정"을 커밋 히스토리와
README에 그대로 남기는 것을 목표로 한다.

## 개요

- **목적**: 순간적으로 몰리는 트래픽을 정직하게 다 처리하려 하지 않고, 단계별로 걸러내고
  지연시켜 뒷단(DB)이 감당 가능한 속도로만 요청을 받게 만드는 패턴을 직접 구현하며 학습.
- **핵심 3대 패턴**
  1. **대기열(Waiting Room)** — 서비스 서버 앞단에서 트래픽 자체를 줄인다.
  2. **캐시/락 기반 선점** — 좌석 같은 동시성 충돌 지점은 RDB 트랜잭션이 아니라
     Redis + Lua script로 원자적 처리한다.
  3. **큐 기반 비동기 확정** — 쓰기 요청을 큐에 쌓고 워커가 순차 처리한다(백프레셔).

## 아키텍처

### 현재 (Phase 1 — MVP)

```
                 reserve(선점)         confirm(결제확정)
client ──────────────┐                      │
                     ▼                      ▼
                ┌──────────┐   xadd    ┌──────────────────────┐
                │ FastAPI  │ ────────► │ Redis                │
                │  (api)   │           │  - seat:{id} 선점 락   │
                └──────────┘           │  - booking_confirm_   │
                                       │    stream (큐)         │
                                       └──────────┬───────────┘
                                          xread   │
                                       ┌──────────▼───────────┐
                                       │ Worker (별도 프로세스)  │
                                       │  큐 소비 → DB 최종 반영  │
                                       └──────────┬───────────┘
                                                  ▼
                                       ┌──────────────────────┐
                                       │ PostgreSQL            │
                                       │  bookings / seat=sold │
                                       └──────────────────────┘
```

- **Redis**: 실시간 좌석 선점 락(TTL, source of truth) + 결제 확정 큐(Stream). (대기열 예정)
- **Worker**: Stream 을 소비해 DB 에 자기 속도로만 쓰기(백프레셔). 멱등 처리로 재처리 안전.
- **PostgreSQL**: 최종 확정된 예매(bookings)와 좌석 상태(sold)를 기록하는 영속 저장소.

예매 흐름: `reserve`(Redis 락으로 선점, DB 안 씀) → `confirm`(Stream 적재 후 즉시 202)
→ `Worker`(비동기로 DB 반영, 좌석 `sold`).

### 다음 Phase 확장 계획

- Phase 2: 대기열 / 예매 / 결제 서비스를 별도 FastAPI 프로세스로 분리(MSA).
- Phase 3: Redis Stream → Kafka(파티셔닝·컨슈머 그룹).
- Phase 4: Docker Compose → Kubernetes(HPA / 스케줄 기반 스케일링) + Nginx Ingress.
- Phase 5: Prometheus + Grafana 관측성.

## 왜 이렇게 설계했는가

- **Redis vs DB 역할 분리**: 좌석 선점은 밀리초 단위 경합이 일어나는 지점이라 RDB
  트랜잭션으로 처리하면 락 경합·커넥션 고갈로 무너진다. 실시간 선점은 Redis가 맡고,
  DB는 "최종 확정 상태"만 자기 속도로 기록한다.
- **Lua script 원자적 락**: `GET` 후 `SET`을 따로 호출하면 두 요청이 동시에 빈 좌석을
  보고 둘 다 선점하는 race condition이 생긴다. → *(Phase 1에서 직접 재현 후 해결 예정)*
- **큐 기반 비동기 확정**: API 서버는 DB에 직접 쓰지 않고 큐에 적재만 한다. 트래픽이 튀어도
  큐에 쌓였다가 워커가 서서히 처리 → DB는 항상 감당 가능한 속도로만 요청을 받는다.

## 실행 방법

```bash
docker-compose up --build
```

- API: http://localhost:8000  (Swagger: http://localhost:8000/docs) / Worker 는 함께 기동됨
- 헬스체크: `GET /health` → DB/Redis 연결 상태 확인
- 환경변수는 `.env.example` 참고 (좌석 TTL, 대기열 입장 인원, JWT 등)

주요 API:

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/performances/{id}/seats` | 좌석 목록/상태 조회 |
| POST | `/seats/{id}/reserve` | 좌석 선점(Redis 원자 락, TTL) — `{"user_id":1}` |
| POST | `/seats/{id}/confirm` | 결제 확정(Stream 적재, 즉시 202) — `{"user_id":1}` |

검증 스크립트:

```bash
python loadtest/reproduce_race.py --seat 10 --concurrency 30   # 동시 선점 → 1건만 성공
python loadtest/e2e_booking.py --seat 5 --user 777             # 선점→확정→워커 반영 e2e
```

## 부하테스트 결과

> Phase 1 진행 중. "대기열 없음 vs 있음", "DB 트랜잭션 vs Redis 락" 비교 수치를
> 여기에 표/그래프로 채워나갈 예정.

**좌석 1석에 동시 예매 요청 30건**을 쏜 결과 (`loadtest/reproduce_race.py`):

| 시나리오 | 200 성공 | 409 거절 | 실제 예매된 건수 | 중복 예매 |
|---|---|---|---|---|
| DB 조회+삽입만 (락 없음) | **28** | 2 | **28** | ❌ 발생 (28중복) |
| Redis Lua script 락 | **1** | 29 | **1** | ✅ 없음 |

> 락이 없으면 30개 요청 중 28개가 동시에 `available` 을 보고 통과해 한 좌석이
> 28번 예매됐다. Redis Lua script(단일 스레드 원자 실행) 락 도입 후에는 정확히
> 1건만 통과하고 나머지 29건은 409 로 깔끔하게 거절된다.

## Phase별 진행 상황

- [ ] **Phase 1: MVP** (진행 중)
  - [x] 프로젝트 초기 세팅 (FastAPI + Redis + Postgres + Docker Compose)
  - [x] 좌석 선점 API (DB 트랜잭션만 → race condition 재현) ✅ 28중복 재현
  - [x] Redis Lua script 락으로 원자성 보장 ✅ 30요청→1성공/29거절
  - [x] 결제 확정 비동기 큐(Redis Stream) + Worker ✅ 선점/확정 분리, 멱등 반영
  - [ ] 대기열(Sorted Set) + TTL JWT
  - [ ] 부하테스트 비교 및 결과 기록
- [ ] Phase 2: MSA 전환
- [ ] Phase 3: Kafka
- [ ] Phase 4: K8s
- [ ] Phase 5: 관측성

## 트러블슈팅 로그

> 겪은 문제와 해결 과정을 시간순으로 기록한다. (race condition, 락 경합 등)

### #1 좌석 선점 race condition (DB 트랜잭션만 사용)

- **증상**: 좌석 1석에 동시 예매 요청 30건을 보냈더니 28건이 "예매 성공"으로 처리되고,
  `bookings` 테이블에 같은 좌석 예매 row 가 28개 쌓임.
- **원인**: 선점 로직이 `SELECT status`(확인) → `INSERT/UPDATE`(반영) 두 단계로 나뉘어
  있고 그 사이가 원자적이지 않다. 여러 요청이 거의 동시에 들어오면 모두 아직
  `available` 인 상태를 읽고 통과해버린다(check-then-act race).
- **재현**: `python loadtest/reproduce_race.py --seat 1 --concurrency 30`
- **해결**: 확인과 선점을 하나의 원자적 연산으로 묶어야 한다. RDB 행 잠금
  (`SELECT ... FOR UPDATE`)으로도 막을 수 있지만, 초당 수만 건이 몰리는 좌석 선점
  경로에서는 DB 커넥션·락 경합이 병목이 된다. 그래서 실시간 선점은 **Redis + Lua
  script(단일 스레드에서 원자 실행)**로 앞단에서 처리하고, DB 는 최종 확정만 맡긴다.
  선점 키에는 TTL 을 걸어 결제 미완료 시 자동 해제되게 했다.
- **결과**: 동일 조건(30 동시 요청)에서 **1건만 성공, 29건 409 거절, 중복 0**.
  DB 에는 defense-in-depth 로 `bookings(seat_id)` unique 인덱스도 복원해 최후 방어선을 뒀다.
