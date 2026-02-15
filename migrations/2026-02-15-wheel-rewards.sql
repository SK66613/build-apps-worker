-- add reward wallet fields to wheel_redeems

ALTER TABLE wheel_redeems ADD COLUMN img TEXT;
ALTER TABLE wheel_redeems ADD COLUMN expires_at TEXT;
ALTER TABLE wheel_redeems ADD COLUMN cost_cent INTEGER DEFAULT 0;
ALTER TABLE wheel_redeems ADD COLUMN cost_currency TEXT;
ALTER TABLE wheel_redeems ADD COLUMN meta TEXT;

CREATE INDEX IF NOT EXISTS idx_wheel_redeems_user_active
ON wheel_redeems(app_public_id, tg_id, status);

CREATE INDEX IF NOT EXISTS idx_wheel_redeems_status
ON wheel_redeems(status);
