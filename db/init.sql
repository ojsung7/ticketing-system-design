-- Phase 1 스키마
-- seats.status 는 "최종 확정 상태" 기록용이고,
-- 실시간 선점 여부(source of truth)는 Redis 가 담당한다. (README 참고)

CREATE TABLE IF NOT EXISTS performances (
    id        SERIAL PRIMARY KEY,
    name      VARCHAR(200) NOT NULL,
    show_time TIMESTAMP    NOT NULL
);

CREATE TABLE IF NOT EXISTS seats (
    id             SERIAL PRIMARY KEY,
    performance_id INT REFERENCES performances(id),
    seat_number    VARCHAR(10) NOT NULL,
    status         VARCHAR(20) NOT NULL DEFAULT 'available'  -- available / reserved / sold
);

CREATE TABLE IF NOT EXISTS bookings (
    id        SERIAL PRIMARY KEY,
    seat_id   INT REFERENCES seats(id),
    user_id   INT NOT NULL,
    booked_at TIMESTAMP NOT NULL DEFAULT now()
);

-- 좌석당 예매는 1건만 허용 (defense-in-depth).
-- 실시간 동시성은 앞단의 Redis 원자적 락으로 이미 걸러지지만, 애플리케이션 버그나
-- 락 우회 상황에 대비한 DB 레벨 최후 방어선으로 unique 제약을 둔다.
-- (직전 커밋에서는 app 레벨 race condition 을 눈에 보이게 재현하려고 잠시 제거했었다.)
CREATE UNIQUE INDEX IF NOT EXISTS uq_bookings_seat ON bookings(seat_id);

-- ── 시드 데이터 ────────────────────────────────────────────────
-- 공연 3개: performance_id 를 Kafka 파티션 키로 써서 공연별 처리량 분산을 학습한다.
INSERT INTO performances (name, show_time)
VALUES
    ('데모 콘서트 2026', '2026-12-31 20:00:00'),
    ('데모 뮤지컬 2026', '2026-11-15 19:30:00'),
    ('데모 페스티벌 2026', '2026-10-01 18:00:00')
ON CONFLICT DO NOTHING;

-- 공연 1번: 좌석 1000개 (부하테스트용, 좌석 고갈로 인한 409 노이즈 최소화).
INSERT INTO seats (performance_id, seat_number, status)
SELECT 1, 'A' || g, 'available'
FROM generate_series(1, 1000) AS g
WHERE NOT EXISTS (SELECT 1 FROM seats WHERE performance_id = 1);

-- 공연 2, 3번: 각 200석 (파티셔닝 시연용).
INSERT INTO seats (performance_id, seat_number, status)
SELECT 2, 'B' || g, 'available'
FROM generate_series(1, 200) AS g
WHERE NOT EXISTS (SELECT 1 FROM seats WHERE performance_id = 2);

INSERT INTO seats (performance_id, seat_number, status)
SELECT 3, 'C' || g, 'available'
FROM generate_series(1, 200) AS g
WHERE NOT EXISTS (SELECT 1 FROM seats WHERE performance_id = 3);
