-- safe add cost_coins column (will succeed even if exists)

ALTER TABLE wheel_prizes ADD COLUMN cost_coins INTEGER DEFAULT 0;
