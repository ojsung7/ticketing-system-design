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
        ┌──────────┐      ┌─────────┐
client → │ FastAPI  │ ───→ │  Redis  │  ← 좌석 선점 락(source of truth), 대기열, 큐
        │  (api)   │      └─────────┘
        └────┬─────┘            │
             │            ┌─────┴─────┐
             │            │  Worker   │  ← 큐 소비 → DB 최종 반영 (예정)
             ▼            └─────┬─────┘
        ┌──────────┐           │
        │ Postgres │ ←─────────┘  ← 확정 상태(sold) 기록
        └──────────┘
```

- **Redis**: 실시간 좌석 선점 여부의 source of truth, 대기열(Sorted Set), 결제 확정 큐(Stream).
- **PostgreSQL**: 최종 확정된 예매(bookings)와 좌석 상태를 기록하는 영속 저장소.

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

- API: http://localhost:8000  (Swagger: http://localhost:8000/docs)
- 헬스체크: `GET /health` → DB/Redis 연결 상태 확인
- 환경변수는 `.env.example` 참고 (좌석 TTL, 대기열 입장 인원, JWT 등)

## 부하테스트 결과

> Phase 1 진행 중. "대기열 없음 vs 있음", "DB 트랜잭션 vs Redis 락" 비교 수치를
> 여기에 표/그래프로 채워나갈 예정.

| 시나리오 | TPS | 에러율 | 평균 응답시간 | 중복 예매 발생 |
|---|---|---|---|---|
| DB 트랜잭션만 (락 없음) | - | - | - | - |
| Redis Lua script 락 | - | - | - | - |

## Phase별 진행 상황

- [ ] **Phase 1: MVP** (진행 중)
  - [x] 프로젝트 초기 세팅 (FastAPI + Redis + Postgres + Docker Compose)
  - [ ] 좌석 선점 API (DB 트랜잭션만 → race condition 재현)
  - [ ] Redis Lua script 락으로 원자성 보장
  - [ ] 결제 확정 비동기 큐(Redis Stream) + Worker
  - [ ] 대기열(Sorted Set) + TTL JWT
  - [ ] 부하테스트 비교 및 결과 기록
- [ ] Phase 2: MSA 전환
- [ ] Phase 3: Kafka
- [ ] Phase 4: K8s
- [ ] Phase 5: 관측성

## 트러블슈팅 로그

> 겪은 문제와 해결 과정을 시간순으로 기록한다. (race condition, 락 경합 등)

- _아직 없음 — 좌석 선점 API 구현 후 race condition 재현부터 기록 시작._
