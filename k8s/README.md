# Phase 4 — Kubernetes 배포 설계

> **주의(설계 산출물)**: 이 디렉터리는 "확장 가능하도록 이렇게 설계했다"를 보여주는
> **매니페스트 + 설계 문서**다. 로컬에서 실제 클러스터를 굴리지 않아도, 실무 클러스터에
> 그대로 적용 가능한 형태로 작성했다. Phase 1~3 은 `docker-compose` 로 실제 동작·검증했다.

## 구성 개요

```
                       ┌──────────────────────────┐
   external traffic ──►│ Nginx Ingress (단일 진입점) │  rate limiting (IP당 rps/conn)
                       └───────────┬──────────────┘
              /queue* │  /performances*,/seats* │  /payments*
                      ▼            ▼             ▼
                 ┌────────┐   ┌─────────┐   ┌─────────┐
                 │ queue  │   │ booking │   │ payment │   각 Deployment + Service + HPA
                 │ (HPA)  │   │ (HPA)   │   │ (HPA)   │
                 └────────┘   └────┬────┘   └────┬────┘
                                   │ Redis 락     │ Kafka produce
                    ┌──────────────┴──┐      ┌────▼────────────┐
                    │ redis / postgres │      │ kafka (topic)   │
                    └──────────────────┘      └────┬────────────┘
                                                   │ consumer group
                                          ┌────────▼────────┐
                                          │ booking-worker  │  (KEDA lag scaler 권장)
                                          └─────────────────┘
```

## 매니페스트 목록 (적용 순서)

| 파일 | 내용 |
|---|---|
| `00-namespace.yaml` | `ticketing` 네임스페이스 |
| `01-config.yaml` | ConfigMap(비밀 아닌 설정) + Secret(JWT/DB 등) |
| `10-postgres.yaml` | Postgres StatefulSet + headless Service + PVC |
| `11-redis.yaml` | Redis Deployment + Service |
| `12-kafka.yaml` | Kafka(KRaft) StatefulSet + Service (파티션 6) |
| `20-queue.yaml` | queue Deployment + Service + HPA |
| `21-booking.yaml` | booking Deployment + Service + HPA |
| `22-payment.yaml` | payment Deployment + Service + HPA |
| `23-booking-worker.yaml` | worker Deployment (+ KEDA ScaledObject 예시) |
| `30-ingress.yaml` | Nginx Ingress(경로 라우팅 + rate limiting) |
| `40-scheduled-scaling.yaml` | 스케줄 기반 스케일업/다운 CronJob + RBAC |

## 배포 방법 (실제 클러스터 기준)

```bash
# 0) 서비스 이미지 빌드 후 레지스트리에 push (매니페스트의 image 경로에 맞춰)
#    예: docker build -f services/queue/Dockerfile -t ghcr.io/ojsung7/ticketing-queue:latest .
#        docker push ghcr.io/ojsung7/ticketing-queue:latest   (booking/payment 동일)

# 1) Nginx Ingress Controller 설치 (없다면)
#    helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
#    helm install ingress-nginx ingress-nginx/ingress-nginx

# 2) 매니페스트 적용
kubectl apply -f k8s/

# 3) DB 스키마 초기화 (init.sql 을 Job/psql 로 1회 실행 — compose 의 initdb 대체)
```

로컬에서 가볍게 확인만 하려면 `kind` 또는 `minikube` + `kubectl apply -f k8s/` 로
매니페스트 유효성/스케줄 동작을 시험할 수 있다.

## 스케일링 전략 — 왜 스케줄 기반을 우선하는가

- **HPA(반응형)** 는 부하가 **올라간 뒤에** 지표를 보고 늘린다. 티켓 오픈처럼 0초에
  수만 명이 몰리는 상황에선 스케일업이 뒤따라와 초기 몇 초를 놓치기 쉽다.
- **티켓 오픈은 예측 가능한 스파이크**다. 그래서 오픈 시각 **전에** 미리 목표 규모로
  키워두는 **스케줄 기반 스케일링**(`40-scheduled-scaling.yaml` CronJob)을 1차로 쓰고,
  HPA 는 그 위에서 예측이 빗나간 부분을 미세 조정하는 2차 안전망으로 둔다.
- **booking-worker** 는 요청형이 아니라 Kafka 컨슈머다. CPU 가 아니라 **컨슈머 랙**이
  스케일 신호이므로 KEDA 의 Kafka 트리거로 파티션 수까지 늘리는 것이 정석
  (`23-booking-worker.yaml` 주석의 ScaledObject 예시).

## 데이터/상태 계층 주의

- Postgres/Redis/Kafka 는 학습용으로 클러스터 내부에 두었지만, 실무에서는 **관리형
  서비스**(RDS + Read Replica, Elasticache/Redis Cluster, MSK/Strimzi)를 권장한다.
  상태 저장 백엔드는 스케일·백업·장애조치 요구사항이 앱과 달라 분리 운영이 안전하다.
- Secret 은 데모 평문값이다. 실제로는 sealed-secrets / External Secrets / Vault 로 주입한다.
