-- 0004_wheel_cost_coins_snapshot.sql
-- This migration previously attempted to add wheel_spins.coin_value_cents (already exists in DB).
-- Make it idempotent/no-op to unblock further migrations.

-- keep something useful & safe:
CREATE INDEX IF NOT EXISTS idx_wheel_spins_app_created
ON wheel_spins(app_public_id, ts_created);

CREATE INDEX IF NOT EXISTS idx_wheel_spins_app_prize
ON wheel_spins(app_public_id, prize_code);

-- no-op
SELECT 1;
