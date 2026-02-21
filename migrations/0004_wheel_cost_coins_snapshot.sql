-- 0004_wheel_cost_coins_snapshot.sql

-- 1) wheel_prizes: add cost_coins (sebest in coins)
ALTER TABLE wheel_prizes ADD COLUMN cost_coins INTEGER;

-- 2) wheel_spins: snapshot fields (history)
ALTER TABLE wheel_spins ADD COLUMN coin_value_cents INTEGER;
ALTER TABLE wheel_spins ADD COLUMN prize_cost_coins INTEGER;

-- (optional, if you want) indexes for analytics speed
CREATE INDEX IF NOT EXISTS idx_wheel_spins_app_created ON wheel_spins(app_public_id, ts_created);
CREATE INDEX IF NOT EXISTS idx_wheel_spins_created ON wheel_spins(ts_created);
