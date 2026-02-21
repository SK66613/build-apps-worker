-- 0005_wheel_prizes_cost_coins.sql
ALTER TABLE wheel_prizes ADD COLUMN cost_coins INTEGER;

-- (опционально) если у тебя часто грузятся призы по app_public_id
CREATE INDEX IF NOT EXISTS idx_wheel_prizes_app_code
ON wheel_prizes(app_public_id, code);
