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

### 런타임 아키텍처 (Phase 1~3 구현, compose 로 실동작)

MSA 3개 서비스 + **결제→예매 확정을 Kafka 토픽으로 통신**. 파티션 키를 `performance_id`
로 잡아 공연별로 처리량을 분산하고, **컨슈머 그룹**으로 워커를 수평 확장한다.
Postgres 는 booking 만 소유. (배포는 Phase 4 K8s, 관측성은 Phase 5 참고)

```
                  ┌──────────────────┐
        ┌────────►│ queue-service    │  대기열(Sorted Set) + JWT 발급  (Redis)
        │  enter  │      :8001       │
        │  status └──────────────────┘
 client │         ┌──────────────────┐         ┌──────────────────┐
        ├────────►│ booking-service  │◄── 락 ──►│ Redis            │
        │ reserve │      :8002       │         │ - waiting_queue   │
        │ (JWT)   └──────────────────┘         │ - seat:{id} 락    │
        │         ┌──────────────────┐         └──────────────────┘
        └────────►│ payment-service  │  produce (key=performance_id)
          confirm │      :8003       │────────┐
          (JWT)   └──────────────────┘        ▼
                     ┌───────────────────────────────────┐
                     │ Kafka topic: booking-confirm       │
                     │  part0   part1   part2  (공연별 분산) │
                     └───┬────────┬────────┬──────────────┘
              consumer   │        │        │   group=booking-confirmers
                group  ┌─▼──┐   ┌─▼──┐   ┌─▼──┐
                       │ w1 │   │ w2 │   │ w3 │  booking-worker × N (수평 확장)
                       └─┬──┘   └─┬──┘   └─┬──┘
                         └────────┼────────┘
                                  ▼
                       ┌──────────────────┐
                       │ PostgreSQL       │  bookings / seat=sold (booking 소유)
                       └──────────────────┘
```

- **queue-service** (`:8001`): 대기열 + TTL JWT 발급. Redis 만 사용.
- **booking-service** (`:8002`): 좌석 조회·선점(Redis 원자 락). Postgres 소유. JWT 필수.
- **payment-service** (`:8003`): 결제 확정 → Kafka `booking-confirm` 토픽에 발행(key=performance_id).
- **booking-worker**: Kafka 컨슈머 그룹으로 소비 → DB 반영. `--scale booking-worker=N` 으로 확장.
- **공유 코드**(`common/`): 설정·Redis/DB/Kafka 클라이언트·JWT. 서비스 간 토큰 검증은 동일 `JWT_SECRET`.

예매 흐름: `queue/enter` → `queue/status`(앞쪽 N명이면 JWT 발급) → **booking** `reserve`(락)
→ **payment** `payments/confirm`(Kafka 발행, 202) → **booking-worker**(컨슈머 그룹, DB 반영 `sold`).

### Phase 4 — Kubernetes 배포 설계 (매니페스트)

`docker-compose` 로 실제 동작을 검증한 뒤(Phase 1~3), K8s 배포는 **매니페스트 + 설계
문서**로 남겼다(`k8s/`). 실무도 트래픽이 커지며 단계적으로 K8s 를 도입하므로, 그 구성을
그대로 적용 가능한 형태로 설계했다. 자세한 내용은 **[k8s/README.md](k8s/README.md)**.

- Deployment/Service/**HPA**(queue·booking·payment), StatefulSet(postgres·kafka)
- **Nginx Ingress** 단일 진입점 + IP당 rate limiting
- **스케줄 기반 스케일링** 우선(CronJob): 티켓 오픈은 예측 가능한 스파이크라 오픈 전
  미리 스케일업하고 HPA 는 미세조정 안전망으로. 워커는 KEDA 컨슈머 랙 스케일 권장.

### Phase 5 — 관측성 (Prometheus + Grafana)

세 서비스와 worker 를 Prometheus 로 계측하고 Grafana 대시보드로 시각화한다.
코어 스택은 가볍게 유지하고 모니터링은 오버레이로 필요할 때만 함께 띄운다(`monitoring/`).

```bash
docker-compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d
# Prometheus  http://localhost:9090   /  Grafana http://localhost:3300 (admin/admin)
```

- 각 서비스 `/metrics` 노출(prometheus-fastapi-instrumentator) + 도메인 커스텀 메트릭:
  `ticketing_waiting_queue_size`(대기열 적체), `ticketing_seat_reserve_total{result}`,
  `ticketing_payment_confirm_published_total`, `ticketing_booking_confirmed_total`.
- Grafana 대시보드(자동 프로비저닝): TPS·상태별 응답율·p95 지연·대기열 적체·좌석 선점
  성공/충돌·확정 파이프라인(발행 vs 소비 gap = 적체).

## 왜 이렇게 설계했는가

- **Redis vs DB 역할 분리**: 좌석 선점은 밀리초 단위 경합이 일어나는 지점이라 RDB
  트랜잭션으로 처리하면 락 경합·커넥션 고갈로 무너진다. 실시간 선점은 Redis가 맡고,
  DB는 "최종 확정 상태"만 자기 속도로 기록한다.
- **Lua script 원자적 락**: `GET` 후 `SET`을 따로 호출하면 두 요청이 동시에 빈 좌석을
  보고 둘 다 선점하는 race condition이 생긴다. → *(Phase 1에서 재현→해결, 아래 부하테스트 참고)*
- **큐 기반 비동기 확정**: 결제 서비스는 DB에 직접 쓰지 않고 이벤트만 발행한다. 트래픽이 튀어도
  큐에 쌓였다가 워커가 서서히 처리 → DB는 항상 감당 가능한 속도로만 요청을 받는다.
- **서비스 분리 & 데이터 소유권**: booking 만 Postgres 를 소유하고, payment 는 이벤트로
  확정을 요청한다. 서비스가 DB 를 공유하지 않아 결합도를 낮추고 독립 배포·확장이 가능하다.
- **Kafka 파티셔닝 & 컨슈머 그룹**(Phase 3): 확정 이벤트를 `performance_id` 키로 파티셔닝해
  공연별 처리량을 분산하고 같은 공연 내 순서를 보장한다. 컨슈머 그룹으로 워커를 N개까지
  늘리면 파티션이 워커에 분배돼 수평 확장된다. 오프셋은 DB 반영 후 커밋(at-least-once) +
  `ON CONFLICT DO NOTHING` 멱등 처리로 재처리·리밸런싱에도 중복 예매가 생기지 않는다.
  (왜 Redis Stream 에서 넘어왔나: 파티션 단위 병렬성·컨슈머 그룹 리밸런싱·오프셋 관리 등
   대규모 이벤트 처리에 필요한 기능이 Kafka 에 갖춰져 있기 때문.)

## 실행 방법

```bash
docker-compose up --build

# 컨슈머 그룹 수평 확장(워커 3개 = 토픽 파티션 3개):
docker-compose up -d --scale booking-worker=3
```

- 서비스: queue `:8001`, booking `:8002`, payment `:8003`
  (+ booking-worker, postgres, redis, **kafka**)
- Swagger: 각 서비스 `/docs` (예: http://localhost:8002/docs)
- 헬스체크: `GET :8001/health`, `:8002/health`, `:8003/health`
- 환경변수는 `.env.example` 참고. `JWT_SECRET` 은 세 서비스가 동일해야 함(compose 에서 공유).
- 파티셔닝 시연: `python loadtest/kafka_partition_demo.py` 후
  `docker-compose logs booking-worker` 로 공연별 파티션·워커 분배 확인.

주요 API (서비스별):

| 서비스 | 메서드 | 경로 | 설명 |
|---|---|---|---|
| queue `:8001` | POST | `/queue/enter` | 대기열 진입 — `{"user_id":1}` |
| queue `:8001` | GET | `/queue/status?user_id=1` | 순번 조회. 입장 가능 시 `entry_token` 발급 |
| booking `:8002` | GET | `/performances/{id}/seats` | 좌석 목록/상태 조회 |
| booking `:8002` | POST | `/seats/{id}/reserve` | 좌석 선점(Redis 원자 락) — **토큰 필요** |
| payment `:8003` | POST | `/payments/confirm` | 결제 확정(Kafka 발행, 202) — **토큰 필요**, `{"user_id":1,"seat_id":5,"performance_id":1}` |

> `reserve`/`payments/confirm` 은 `Authorization: Bearer <entry_token>` 헤더가 있어야 진입 가능(대기열 게이트).

검증 스크립트:

```bash
python loadtest/reproduce_race.py --seat 10 --concurrency 30   # 동시 선점 → 1건만 성공
python loadtest/e2e_booking.py --seat 5 --user 777             # 선점→확정→워커 반영 e2e
```

## 부하테스트 결과

### ① 동시성 정확성 — DB 트랜잭션 vs Redis 락

**좌석 1석에 동시 예매 요청 30건**을 쏜 결과 (`loadtest/reproduce_race.py`):

| 시나리오 | 200 성공 | 409 거절 | 실제 예매된 건수 | 중복 예매 |
|---|---|---|---|---|
| DB 조회+삽입만 (락 없음) | **28** | 2 | **28** | ❌ 발생 (28중복) |
| Redis Lua script 락 | **1** | 29 | **1** | ✅ 없음 |

> 락이 없으면 30개 요청 중 28개가 동시에 `available` 을 보고 통과해 한 좌석이
> 28번 예매됐다. Redis Lua script(단일 스레드 원자 실행) 락 도입 후에는 정확히
> 1건만 통과하고 나머지 29건은 409 로 깔끔하게 거절된다.

### ② 대기열 효과 — 없음 vs 있음 (Locust, 500 users / 25s)

같은 코드 경로(`queue/enter → queue/status → reserve`)에서 **입장 인원 제한
(`ALLOWED_ENTRY_COUNT`) 하나만** 바꿔 비교 (`loadtest/locustfile.py`):

| reserve(예매 backend) | 대기열 없음 (allowed=10M) | 대기열 있음 (allowed=50) |
|---|---|---|
| 처리율 | 1088 req/s | **524 req/s** (≈ 절반) |
| 평균 지연 | 125 ms | **74 ms** |
| p95 지연 | 180 ms | **130 ms** |
| p99 지연 | 220 ms | **180 ms** |
| 실패율 | 0% | 0% |
| 흡수된 폴링(queue/status) | 1093 req/s | **2139 req/s** |

> **대기열이 backend 로 가는 실제 예매 요청률을 절반 이하로 낮추고 지연도 줄였다.**
> 몰린 트래픽은 DB 를 건드리지 않는 값싼 폴링(Redis `zrank`)으로 흡수된다 —
> "정직하게 다 받지 말고, 앞단에서 걸러 뒷단이 감당 가능한 속도로 들어오게" 라는
> 설계 철학이 수치로 확인된다.
>
> _측정 환경: 단일 노트북에서 api·worker·postgres·redis·locust 를 함께 구동(CPU
> bound). 두 시나리오 모두 동일 조건이라 상대 비교에는 유효하다._

## Phase별 진행 상황

- [x] **Phase 1: MVP** ✅ 완료
  - [x] 프로젝트 초기 세팅 (FastAPI + Redis + Postgres + Docker Compose)
  - [x] 좌석 선점 API (DB 트랜잭션만 → race condition 재현) ✅ 28중복 재현
  - [x] Redis Lua script 락으로 원자성 보장 ✅ 30요청→1성공/29거절
  - [x] 결제 확정 비동기 큐(Redis Stream) + Worker ✅ 선점/확정 분리, 멱등 반영
  - [x] 대기열(Sorted Set) + TTL JWT ✅ 토큰 없인 reserve/confirm 401 차단
  - [x] 부하테스트 비교 및 결과 기록 ✅ 대기열 backend 부하 절반↓ 확인
- [x] **Phase 2: MSA 전환** ✅ 완료
  - [x] queue / booking / payment 서비스 분리 (독립 FastAPI + Dockerfile)
  - [x] 공유 시크릿 JWT 로 서비스 간 토큰 검증
  - [x] payment → booking 확정을 Redis Stream 이벤트로 통신(데이터 소유권 분리)
- [x] **Phase 3: Kafka** ✅ 완료
  - [x] Redis Stream → Kafka 토픽(`booking-confirm`) 교체 (aiokafka)
  - [x] 파티션 키 = `performance_id` (공연별 처리량 분산·순서 보장)
  - [x] 컨슈머 그룹 + 워커 수평 확장 (`--scale booking-worker=3`, 파티션 분배 확인)
  - [x] DB 반영 후 오프셋 커밋(at-least-once) + 멱등(ON CONFLICT) 재처리 안전
- [x] **Phase 4: K8s** ✅ 매니페스트 + 설계 문서 (`k8s/`)
  - [x] Deployment/Service/HPA (queue·booking·payment), StatefulSet(postgres·kafka)
  - [x] Nginx Ingress 단일 진입점 + rate limiting
  - [x] 스케줄 기반 스케일링(CronJob) 우선 + HPA 미세조정, 워커 KEDA 랙 스케일 설계
- [x] **Phase 5: 관측성** ✅ Prometheus 계측 + Grafana 대시보드
  - [x] 3서비스 + worker `/metrics` (HTTP 메트릭 + 도메인 커스텀 메트릭)
  - [x] Prometheus 스크레이프(4 타깃 up) + Grafana 자동 프로비저닝 대시보드
  - [x] 대기열 적체·선점 성공/충돌·확정 발행/소비 gap 가시화

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

### #2 Kafka 컨슈머 그룹 초기화 경쟁 (`GroupCoordinatorNotAvailableError`)

- **증상**: 워커 기동 직후 로그에 `Group Coordinator Request failed: [Error 15]
  GroupCoordinatorNotAvailableError` 가 잠깐 찍힌 뒤 정상 소비로 넘어감.
- **원인**: 브로커는 떠 있지만 컨슈머 그룹 오프셋을 저장하는 내부 토픽
  (`__consumer_offsets`)이 아직 생성/리더 선출 전이라, 그룹 코디네이터가 잠시 불가.
- **해결**: 워커의 `consumer.start()` 를 재시도 루프로 감싸고, compose 의 kafka
  healthcheck(`start_period`)로 브로커 준비를 기다리게 했다. 일시적 경고이며 재처리는
  멱등 처리로 안전. 운영에서는 오프셋 토픽 복제본/ISR 설정으로 코디네이터 가용성을 높인다.

### #3 파티셔닝 검증 (performance_id 키)

- **관찰**: 공연 1/2/3 확정 이벤트가 각각 파티션 p0/p2/p2 로 갈리고, 컨슈머 그룹의
  워커 3개 중 서로 다른 워커가 파티션을 나눠 처리함(`docker-compose logs booking-worker`).
- **의미**: 같은 공연은 항상 같은 파티션(순서 보장), 공연이 늘면 파티션에 분산되어
  워커 수를 늘리는 만큼 병렬 처리량이 확장됨을 확인.
