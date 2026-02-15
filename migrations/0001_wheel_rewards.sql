-- 0001_wheel_rewards.sql
-- Columns were applied earlier via manual execute; this migration only ensures indexes exist.

CREATE INDEX IF NOT EXISTS idx_wheel_redeems_user_active
ON wheel_redeems(app_public_id, tg_id, status);

CREATE INDEX IF NOT EXISTS idx_wheel_redeems_status
ON wheel_redeems(status);
