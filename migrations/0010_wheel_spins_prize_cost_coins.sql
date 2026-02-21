-- 0010_wheel_spins_prize_cost_coins.sql
-- add wheel_spins.prize_cost_coins (idempotent via table rebuild if needed in future)
-- In SQLite, simplest is ALTER TABLE; if column exists, Wrangler would fail.
-- So: use a "manual idempotent" approach: check first in CI OR keep this migration only if not applied yet.

ALTER TABLE wheel_spins ADD COLUMN prize_cost_coins INTEGER DEFAULT 0;
