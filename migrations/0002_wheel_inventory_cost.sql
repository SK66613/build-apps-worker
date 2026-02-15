-- 0002_wheel_inventory_cost.sql
-- Inventory (qty) + prize economics fields for wheel_prizes.
-- Reserve stock on "issued" (wheel_redeems insert), and prize stops participating when qty_left <= 0.

-- ===== wheel_prizes: add columns =====
ALTER TABLE wheel_prizes ADD COLUMN kind TEXT NOT NULL DEFAULT 'item';
-- kind: 'item' | 'coins' | 'discount' | 'service'

ALTER TABLE wheel_prizes ADD COLUMN img TEXT;

ALTER TABLE wheel_prizes ADD COLUMN cost_cent INTEGER NOT NULL DEFAULT 0;
ALTER TABLE wheel_prizes ADD COLUMN cost_currency TEXT NOT NULL DEFAULT 'RUB';

ALTER TABLE wheel_prizes ADD COLUMN track_qty INTEGER NOT NULL DEFAULT 0;        -- 0/1 учитывать остаток
ALTER TABLE wheel_prizes ADD COLUMN qty_total INTEGER;                           -- всего (опционально)
ALTER TABLE wheel_prizes ADD COLUMN qty_left  INTEGER;                           -- осталось
ALTER TABLE wheel_prizes ADD COLUMN stop_when_zero INTEGER NOT NULL DEFAULT 1;   -- 1: не участвовать при qty_left<=0

-- ===== backfill defaults =====
UPDATE wheel_prizes
SET kind = CASE WHEN COALESCE(coins,0) > 0 THEN 'coins' ELSE 'item' END
WHERE kind IS NULL OR kind = '';

UPDATE wheel_prizes
SET cost_currency = 'RUB'
WHERE cost_currency IS NULL OR cost_currency = '';

-- ===== indexes / constraints =====
-- (у тебя могут быть дубли вручную — если вдруг упадёт unique, сначала почистим)
CREATE UNIQUE INDEX IF NOT EXISTS idx_wheel_prizes_uq_app_code
  ON wheel_prizes(app_public_id, code);

CREATE INDEX IF NOT EXISTS idx_wheel_prizes_app_active
  ON wheel_prizes(app_public_id, active);

-- wheel_redeems: redeem_code uniqueness per app
CREATE UNIQUE INDEX IF NOT EXISTS idx_wheel_redeems_uq_app_redeem_code
  ON wheel_redeems(app_public_id, redeem_code);

CREATE INDEX IF NOT EXISTS idx_wheel_redeems_app_status_issued_at
  ON wheel_redeems(app_public_id, status, issued_at);

CREATE INDEX IF NOT EXISTS idx_wheel_redeems_app_tg_issued_at
  ON wheel_redeems(app_public_id, tg_id, issued_at);

