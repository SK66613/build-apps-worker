-- 0006_wheel_redeems_cost_coins.sql
-- Add cost_coins to wheel_redeems (cost in coins, not cents/currency)

ALTER TABLE wheel_redeems
  ADD COLUMN cost_coins INTEGER NOT NULL DEFAULT 0;

-- helpful indexes (safe)
CREATE INDEX IF NOT EXISTS idx_wheel_redeems_app_tg_status
  ON wheel_redeems(app_public_id, tg_id, status);

CREATE UNIQUE INDEX IF NOT EXISTS ux_wheel_redeems_redeem_code
  ON wheel_redeems(redeem_code);
