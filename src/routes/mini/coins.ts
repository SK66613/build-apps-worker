// src/routes/mini/coins.ts

/**
 * Coins / Ledger helpers (D1)
 *
 * Ожидается таблица coins_ledger примерно такая:
 *  - id INTEGER PK AUTOINCREMENT
 *  - app_id TEXT/INT
 *  - app_public_id TEXT
 *  - tg_id TEXT
 *  - delta INTEGER
 *  - balance_before INTEGER
 *  - balance_after INTEGER
 *  - src TEXT
 *  - ref_id TEXT
 *  - note TEXT
 *  - event_id TEXT UNIQUE (или хотя бы индекс)
 *  - ts TEXT DEFAULT datetime('now')
 *
 * И таблица app_users с полем coins.
 */

function toInt(n: any, d = 0) {
  const x = Number(n);
  return Number.isFinite(x) ? Math.trunc(x) : d;
}

async function ledgerEventExists(db: any, appPublicId: string, tgId: any, eventId: string) {
  if (!eventId) return false;
  const row = await db
    .prepare(
      `SELECT id
       FROM coins_ledger
       WHERE app_public_id=? AND tg_id=? AND event_id=?
       LIMIT 1`
    )
    .bind(String(appPublicId), String(tgId), String(eventId))
    .first();
  return !!row;
}

export async function getLastBalance(db: any, appPublicId: string, tgId: any) {
  const row = await db
    .prepare(
      `SELECT balance_after
       FROM coins_ledger
       WHERE app_public_id=? AND tg_id=?
       ORDER BY id DESC
       LIMIT 1`
    )
    .bind(String(appPublicId), String(tgId))
    .first();

  if (row) return toInt((row as any).balance_after, 0);

  // fallback на app_users
  const u = await db
    .prepare(`SELECT coins FROM app_users WHERE app_public_id=? AND tg_user_id=? LIMIT 1`)
    .bind(String(appPublicId), String(tgId))
    .first();

  return u ? toInt((u as any).coins, 0) : 0;
}

/**
 * Начисление/списание монет.
 * delta может быть отрицательной (списание).
 * eventId — для идемпотентности (важно давать уникальный ключ события).
 */
export async function awardCoins(
  db: any,
  appId: any,
  appPublicId: string,
  tgId: any,
  delta: any,
  src: string,
  refId: string,
  note: string,
  eventId: string
) {
  const d = toInt(delta, 0);
  if (!d) {
    const have0 = await getLastBalance(db, appPublicId, tgId);
    return { ok: true, delta: 0, balance_before: have0, balance_after: have0, idempotent: false };
  }

  // если событие уже было — возвращаем текущее состояние (идемпотентно)
  if (eventId) {
    const existed = await ledgerEventExists(db, appPublicId, tgId, eventId);
    if (existed) {
      const have = await getLastBalance(db, appPublicId, tgId);
      return { ok: true, idempotent: true, balance_after: have };
    }
  }

  const before = await getLastBalance(db, appPublicId, tgId);
  const after = before + d;

  // не даём уйти в минус на уровне awardCoins (обычно минус проверяется через spendCoinsIfEnough)
  // но если где-то зовёшь awardCoins с отрицательной дельтой напрямую — защитим:
  if (after < 0) {
    return { ok: false, error: "NEGATIVE_BALANCE", have: before, need: Math.abs(d) };
  }

  // пытаемся вставить в ledger; если на event_id UNIQUE — дубль поймаем и сделаем идемпотентным
  try {
    await db
      .prepare(
        `INSERT INTO coins_ledger
           (app_id, app_public_id, tg_id, delta, balance_before, balance_after, src, ref_id, note, event_id, ts)
         VALUES
           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`
      )
      .bind(
        String(appId ?? ""),
        String(appPublicId),
        String(tgId),
        d,
        before,
        after,
        String(src || ""),
        String(refId || ""),
        String(note || ""),
        String(eventId || "")
      )
      .run();
  } catch (e: any) {
    const msg = String(e?.message || e);
    // если это дубль по UNIQUE(event_id) — считаем идемпотентным успехом
    if (/unique|constraint/i.test(msg)) {
      const have = await getLastBalance(db, appPublicId, tgId);
      return { ok: true, idempotent: true, balance_after: have };
    }
    return { ok: false, error: "LEDGER_INSERT_FAILED", msg };
  }

  // обновим app_users.coins (как кэш)
  try {
    await db
      .prepare(
        `UPDATE app_users
         SET coins = ?
         WHERE app_public_id = ? AND tg_user_id = ?`
      )
      .bind(after, String(appPublicId), String(tgId))
      .run();
  } catch (_) {
    // не критично
  }

  return { ok: true, idempotent: false, delta: d, balance_before: before, balance_after: after };
}

/**
 * Списать cost, если хватает.
 * Делает атомарность на уровне логики (проверка+запись).
 * Для защиты от дублей — eventId обязателен (дай уникальный ключ на операцию).
 */
export async function spendCoinsIfEnough(
  db: any,
  appId: any,
  appPublicId: string,
  tgId: any,
  cost: any,
  src: string,
  refId: string,
  note: string,
  eventId: string
) {
  const c = Math.max(0, toInt(cost, 0));
  if (!c) {
    const have0 = await getLastBalance(db, appPublicId, tgId);
    return { ok: true, spent: 0, have: have0, balance_after: have0 };
  }

  // идемпотентность: если уже было событие списания
  if (eventId) {
    const existed = await ledgerEventExists(db, appPublicId, tgId, eventId);
    if (existed) {
      const have = await getLastBalance(db, appPublicId, tgId);
      return { ok: true, idempotent: true, spent: c, balance_after: have };
    }
  }

  const have = await getLastBalance(db, appPublicId, tgId);
  if (have < c) return { ok: false, error: "NOT_ENOUGH_COINS", have, need: c };

  // списание через awardCoins с отрицательной дельтой (там есть защита)
  const res: any = await awardCoins(db, appId, appPublicId, tgId, -c, src, refId, note, eventId);
  if (!res?.ok) return res;

  return { ok: true, idempotent: !!res.idempotent, spent: c, have, balance_after: res.balance_after };
}
