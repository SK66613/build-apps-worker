-- wheel_spins: snapshot поля для аналитики "только по spins"

ALTER TABLE wheel_spins ADD COLUMN prize_kind TEXT;                 -- 'coins'|'item'
ALTER TABLE wheel_spins ADD COLUMN prize_cost_cent INTEGER;         -- себестоимость item-приза (в центах)
ALTER TABLE wheel_spins ADD COLUMN prize_cost_currency TEXT;        -- 'RUB' и т.п.

ALTER TABLE wheel_spins ADD COLUMN coin_value_cents INTEGER;        -- стоимость 1 монеты (в центах) на момент спина
-- (опционально, если захочешь себестоимость монеты отдельно)
-- ALTER TABLE wheel_spins ADD COLUMN coin_cost_cent INTEGER;

-- дефолты/нормализация для старых строк
UPDATE wheel_spins SET prize_kind = COALESCE(prize_kind, CASE WHEN COALESCE(prize_coins,0) > 0 THEN 'coins' ELSE 'item' END);
UPDATE wheel_spins SET prize_cost_cent = COALESCE(prize_cost_cent, 0);
UPDATE wheel_spins SET prize_cost_currency = COALESCE(prize_cost_currency, 'RUB');
