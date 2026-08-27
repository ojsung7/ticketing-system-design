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

-- 한 좌석은 한 번만 예매될 수 있다 (Phase 1 에서 race condition 을 재현할 때
-- DB 레벨 방어선이 어떻게 동작하는지 관찰하기 위한 제약).
CREATE UNIQUE INDEX IF NOT EXISTS uq_bookings_seat ON bookings(seat_id);

-- ── 시드 데이터 ────────────────────────────────────────────────
INSERT INTO performances (name, show_time)
VALUES ('데모 콘서트 2026', '2026-12-31 20:00:00')
ON CONFLICT DO NOTHING;

-- 공연 1번에 좌석 50개 (A1 ~ A50)
INSERT INTO seats (performance_id, seat_number, status)
SELECT 1, 'A' || g, 'available'
FROM generate_series(1, 50) AS g
WHERE NOT EXISTS (SELECT 1 FROM seats WHERE performance_id = 1);
