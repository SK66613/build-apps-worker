// src/handlers/analyticsHandlers.ts
// Cabinet analytics handlers (real implementation; extracted from legacy).
// Keep signatures/back-compat stable for existing cabinet UI.

import type { Env } from "../index";
import { jsonResponse } from "../services/http";
import { getCanonicalPublicIdForApp } from "../services/apps";
import {
  parseRangeOrDefault as _parseRangeOrDefault,
  daysBetweenInclusive as _daysBetweenInclusive,
} from "../services/analyticsRange";

// NOTE: legacy used a helper for YYYY-MM-DD + N days.
// Keep it local to avoid circular deps and to make the handler self-contained.
function addDaysIso(isoDate: string, days: number): string {
  // isoDate: YYYY-MM-DD
  const m = String(isoDate || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return "";
  const y = Number(m[1]);
  const mo = Number(m[2]) - 1;
  const d = Number(m[3]);
  const dt = new Date(Date.UTC(y, mo, d));
  dt.setUTCDate(dt.getUTCDate() + Number(days || 0));
  return dt.toISOString().slice(0, 10);
}

function toInt(v: any, d = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return d;
  return Math.trunc(n);
}

const json = (obj: any, status = 200, request: Request | null = null) =>
  jsonResponse(obj, status, request as any);

// ===== app_settings helper (coin value / currency) =====

async function getAppSettingsForPublicId(env: Env, appPublicId: string) {
  try {
    const row = await env.DB.prepare(
      `SELECT coin_value_cents, currency
       FROM app_settings
       WHERE app_public_id = ?
       LIMIT 1`
    )
      .bind(appPublicId)
      .first();

    const coin_value_cents = Number(row?.coin_value_cents ?? 100);
    const currency = String(row?.currency ?? "RUB");

    return {
      coin_value_cents:
        Number.isFinite(coin_value_cents) && coin_value_cents > 0
          ? Math.floor(coin_value_cents)
          : 100,
      currency: (currency || "RUB").toUpperCase().slice(0, 8),
    };
  } catch (_) {
    return { coin_value_cents: 100, currency: "RUB" };
  }
}

// ===== helper: table columns =====
async function getTableCols(db: any, table: string): Promise<Set<string>> {
  const res: any = await db.prepare(`PRAGMA table_info(${table})`).all();
  const cols = new Set<string>();
  for (const r of res?.results || []) cols.add(String(r.name || ""));
  return cols;
}

// ============================================================================
// CABINET: WHEEL STATS (PRIZE TABLE)
// ============================================================================

export async function handleCabinetWheelStats(appId, request, env, ownerId) {
  const url = new URL(request.url);

  const from = String(url.searchParams.get("from") || "").trim();
  const to = String(url.searchParams.get("to") || "").trim();

  const fromOk = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : null;
  const toOk = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : null;

  const toPlus1 = toOk ? addDaysIso(toOk, 1) : null;

  const fromTs = fromOk ? `${fromOk} 00:00:00` : "1970-01-01 00:00:00";
  const toTs = toPlus1 ? `${toPlus1} 00:00:00` : "2999-12-31 00:00:00";

  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) {
    return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);
  }

  const db = env.DB;
  const s = await getAppSettingsForPublicId(env, appPublicId);
  const coinValueCents = Math.max(1, Number(s.coin_value_cents || 100));
  const currency = String(s.currency || "RUB");

  // ⚠️ Важно: wheel_prizes может быть на разных схемах.
  // Делаем безопасный SELECT: берём только те колонки, которые реально есть.
  const pCols = await getTableCols(db, "wheel_prizes");

  const p = {
    code: "p.code",
    title: pCols.has("title") ? "p.title" : "p.code",
    weight: pCols.has("weight") ? "p.weight" : "0",
    active: pCols.has("active") ? "p.active" : "1",
    kind: pCols.has("kind") ? "p.kind" : "''",
    coins: pCols.has("coins") ? "p.coins" : "0",
    img: pCols.has("img") ? "p.img" : "NULL",
    cost_coins: pCols.has("cost_coins") ? "p.cost_coins" : "0",
    track_qty: pCols.has("track_qty") ? "p.track_qty" : "0",
    qty_left: pCols.has("qty_left") ? "p.qty_left" : "NULL",
    stop_when_zero: pCols.has("stop_when_zero") ? "p.stop_when_zero" : "0",
    app_id: pCols.has("app_id") ? "p.app_id" : "NULL",
    app_public_id: pCols.has("app_public_id") ? "p.app_public_id" : "NULL",
  };

  const rows = await db
    .prepare(
      `
      WITH agg AS (
        SELECT
          prize_code AS code,
          SUM(CASE WHEN ts_issued   IS NOT NULL THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN ts_redeemed IS NOT NULL THEN 1 ELSE 0 END) AS redeemed
        FROM wheel_spins
        WHERE (app_id = ? OR app_public_id = ?)
          AND ts_created >= ?
          AND ts_created < ?
        GROUP BY prize_code
      )
      SELECT
        ${p.code}  AS prize_code,
        ${p.title} AS title,
        COALESCE(a.wins, 0)     AS wins,
        COALESCE(a.redeemed, 0) AS redeemed,

        ${p.weight} AS weight,
        ${p.active} AS active,

        ${p.kind} AS kind,
        ${p.coins} AS coins,
        ${p.img}  AS img,

        -- ✅ source of truth for item-economics
        ${p.cost_coins} AS cost_coins,

        ${p.track_qty} AS track_qty,
        ${p.qty_left} AS qty_left,
        ${p.stop_when_zero} AS stop_when_zero
      FROM wheel_prizes p
      LEFT JOIN agg a ON a.code = p.code
      WHERE (${p.app_id} = ? OR ${p.app_public_id} = ?)
      ORDER BY COALESCE(a.wins,0) DESC, p.code ASC
    `
    )
    .bind(appId, appPublicId, fromTs, toTs, appId, appPublicId)
    .all();

  // back-compat: отдаем cost_cent/cost_currency как “derived” от cost_coins
  const items = (rows?.results || []).map((r) => {
    const cost_coins = Number(r.cost_coins ?? 0);
    const cost_cent = Math.max(0, Math.floor(cost_coins * coinValueCents));

    return {
      prize_code: String(r.prize_code || ""),
      title: String(r.title || ""),
      wins: Number(r.wins || 0),
      redeemed: Number(r.redeemed || 0),

      weight: Number(r.weight ?? 0),
      active: Number(r.active ?? 0) ? 1 : 0,

      kind: String(r.kind || ""),
      coins: Number(r.coins ?? 0),
      img: r.img ?? null,

      // new
      cost_coins,

      // legacy for UI formatting
      cost_cent,
      cost_currency: currency,

      track_qty: Number(r.track_qty ?? 0) ? 1 : 0,
      qty_left: r.qty_left === null || r.qty_left === undefined ? null : Number(r.qty_left),
      stop_when_zero: Number(r.stop_when_zero ?? 0) ? 1 : 0,
    };
  });

  return json({ ok: true, items }, 200, request);
}

// ============================================================================
// CABINET: WHEEL TIMESERIES (history from wheel_spins snapshots)
// - ROI in coins (no currency)
// - plus cents for formatting if you want (derived from coin_value_cents snapshots)
// ============================================================================

export async function handleCabinetWheelTimeseries(appId, request, env, ownerId) {
  const url = new URL(request.url);

  const from = String(url.searchParams.get("from") || "").trim();
  const to = String(url.searchParams.get("to") || "").trim();

  const fromOk = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : null;
  const toOk = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : null;

  const toPlus1 = toOk ? addDaysIso(toOk, 1) : null;

  const fromTs = fromOk ? `${fromOk} 00:00:00` : "1970-01-01 00:00:00";
  const toTs = toPlus1 ? `${toPlus1} 00:00:00` : "2999-12-31 00:00:00";

  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) {
    return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);
  }

  const db = env.DB;
  const wCols = await getTableCols(db, "wheel_spins");

  // Required snapshots for “ideal”
  const hasPrizeCostCoins = wCols.has("prize_cost_coins");
  const hasPrizeKind = wCols.has("prize_kind");
  const hasCoinValueCents = wCols.has("coin_value_cents");
  const hasPrizeCoins = wCols.has("prize_coins");
  const hasSpinCost = wCols.has("spin_cost");

  // Fallback snapshots (older schema)
  const hasPrizeCostCent = wCols.has("prize_cost_cent");

  // If no snapshots at all — return empty (so cabinet doesn't белый экран)
  if (!hasSpinCost) {
    const settings = await getAppSettingsForPublicId(env, appPublicId);
    return json(
      {
        ok: true,
        settings: { coin_value_cents: settings.coin_value_cents, currency: settings.currency },
        days: [],
        note: "wheel_spins.spin_cost column missing (old schema)",
      },
      200,
      request
    );
  }

  // Payout in coins:
  // - for coins-prize: payout_coins = prize_coins (snapshot)
  // - for item-prize: payout_coins = prize_cost_coins (snapshot) (preferred)
  // - fallback: if only prize_cost_cent exists -> convert back to coins via coin_value_cents (best-effort)
  const payoutIssuedCoinsExpr = (() => {
    if (hasPrizeKind && hasPrizeCoins && hasPrizeCostCoins) {
      return `
        COALESCE(SUM(
          CASE
            WHEN ts_issued IS NULL THEN 0
            WHEN prize_kind = 'coins' THEN COALESCE(prize_coins,0)
            ELSE COALESCE(prize_cost_coins,0)
          END
        ), 0)
      `;
    }

    // fallback without prize_kind (assume item): use prize_cost_coins if exists
    if (hasPrizeCostCoins) {
      return `
        COALESCE(SUM(
          CASE
            WHEN ts_issued IS NULL THEN 0
            ELSE COALESCE(prize_cost_coins,0)
          END
        ), 0)
      `;
    }

    // last-resort: prize_cost_cent / coin_value_cents -> coins
    if (hasPrizeCostCent && hasCoinValueCents) {
      return `
        COALESCE(SUM(
          CASE
            WHEN ts_issued IS NULL THEN 0
            ELSE CAST((COALESCE(prize_cost_cent,0) / CASE WHEN COALESCE(coin_value_cents,0) <= 0 THEN 1 ELSE COALESCE(coin_value_cents,0) END) AS INTEGER)
          END
        ), 0)
      `;
    }

    return "0";
  })();

  const payoutRedeemedCoinsExpr = (() => {
    if (hasPrizeKind && hasPrizeCoins && hasPrizeCostCoins) {
      return `
        COALESCE(SUM(
          CASE
            WHEN ts_redeemed IS NULL THEN 0
            WHEN prize_kind = 'coins' THEN COALESCE(prize_coins,0)
            ELSE COALESCE(prize_cost_coins,0)
          END
        ), 0)
      `;
    }

    if (hasPrizeCostCoins) {
      return `
        COALESCE(SUM(
          CASE
            WHEN ts_redeemed IS NULL THEN 0
            ELSE COALESCE(prize_cost_coins,0)
          END
        ), 0)
      `;
    }

    if (hasPrizeCostCent && hasCoinValueCents) {
      return `
        COALESCE(SUM(
          CASE
            WHEN ts_redeemed IS NULL THEN 0
            ELSE CAST((COALESCE(prize_cost_cent,0) / CASE WHEN COALESCE(coin_value_cents,0) <= 0 THEN 1 ELSE COALESCE(coin_value_cents,0) END) AS INTEGER)
          END
        ), 0)
      `;
    }

    return "0";
  })();

  // Optional cents (for pretty UI), derived from snapshots coin_value_cents if it exists.
  const revenueCentsExpr = hasCoinValueCents
    ? `COALESCE(SUM(COALESCE(spin_cost,0) * COALESCE(coin_value_cents,0)), 0)`
    : `0`;

  const payoutIssuedCentsExpr =
    hasCoinValueCents && (hasPrizeCostCoins || (hasPrizeKind && hasPrizeCoins))
      ? `COALESCE(SUM(
          CASE
            WHEN ts_issued IS NULL THEN 0
            WHEN ${hasPrizeKind ? "prize_kind = 'coins'" : "0"} THEN COALESCE(prize_coins,0) * COALESCE(coin_value_cents,0)
            ELSE COALESCE(${hasPrizeCostCoins ? "prize_cost_coins" : "0"},0) * COALESCE(coin_value_cents,0)
          END
        ), 0)`
      : `0`;

  const payoutRedeemedCentsExpr =
    hasCoinValueCents && (hasPrizeCostCoins || (hasPrizeKind && hasPrizeCoins))
      ? `COALESCE(SUM(
          CASE
            WHEN ts_redeemed IS NULL THEN 0
            WHEN ${hasPrizeKind ? "prize_kind = 'coins'" : "0"} THEN COALESCE(prize_coins,0) * COALESCE(coin_value_cents,0)
            ELSE COALESCE(${hasPrizeCostCoins ? "prize_cost_coins" : "0"},0) * COALESCE(coin_value_cents,0)
          END
        ), 0)`
      : `0`;

  const rows = await db
    .prepare(
      `
      SELECT
        substr(ts_created, 1, 10) AS date,

        COUNT(*) AS spins,
        COALESCE(SUM(COALESCE(spin_cost,0)), 0) AS spin_cost_coins,

        SUM(CASE WHEN ts_issued   IS NOT NULL THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN ts_redeemed IS NOT NULL THEN 1 ELSE 0 END) AS redeemed,

        -- ✅ ROI in COINS (no currency)
        ${payoutIssuedCoinsExpr}  AS payout_issued_coins,
        ${payoutRedeemedCoinsExpr} AS payout_redeemed_coins,

        -- optional money (formatting)
        ${revenueCentsExpr} AS revenue_cents,
        ${payoutIssuedCentsExpr} AS payout_issued_cents,
        ${payoutRedeemedCentsExpr} AS payout_redeemed_cents

      FROM wheel_spins
      WHERE (app_id = ? OR app_public_id = ?)
        AND ts_created >= ?
        AND ts_created < ?
      GROUP BY substr(ts_created, 1, 10)
      ORDER BY date ASC
    `
    )
    .bind(appId, appPublicId, fromTs, toTs)
    .all();

  const days = (rows?.results || []).map((r) => {
    const spin_cost_coins = Number(r.spin_cost_coins || 0);
    const payout_issued_coins = Number(r.payout_issued_coins || 0);
    const payout_redeemed_coins = Number(r.payout_redeemed_coins || 0);

    const revenue_cents = Number(r.revenue_cents || 0);
    const payout_issued_cents = Number(r.payout_issued_cents || 0);
    const payout_redeemed_cents = Number(r.payout_redeemed_cents || 0);

    return {
      date: String(r.date || ""),
      spins: Number(r.spins || 0),
      spin_cost_coins,
      wins: Number(r.wins || 0),
      redeemed: Number(r.redeemed || 0),

      // ✅ coins-first (ROI without currency)
      payout_issued_coins,
      payout_redeemed_coins,
      profit_issued_coins: spin_cost_coins - payout_issued_coins,
      profit_redeemed_coins: spin_cost_coins - payout_redeemed_coins,

      // optional money (for charts/tooltips in cabinet)
      revenue_cents,
      payout_issued_cents,
      payout_redeemed_cents,
      profit_issued_cents: revenue_cents - payout_issued_cents,
      profit_redeemed_cents: revenue_cents - payout_redeemed_cents,
    };
  });

  // UI settings (only for formatting in cabinet)
  const settings = await getAppSettingsForPublicId(env, appPublicId);

  // cumulative
  let cumIssuedCoins = 0;
  let cumRedeemedCoins = 0;
  let cumIssuedCents = 0;
  let cumRedeemedCents = 0;

  for (const d of days) {
    cumIssuedCoins += Number((d as any).profit_issued_coins || 0);
    cumRedeemedCoins += Number((d as any).profit_redeemed_coins || 0);
    (d as any).cum_profit_issued_coins = cumIssuedCoins;
    (d as any).cum_profit_redeemed_coins = cumRedeemedCoins;

    cumIssuedCents += Number((d as any).profit_issued_cents || 0);
    cumRedeemedCents += Number((d as any).profit_redeemed_cents || 0);
    (d as any).cum_profit_issued_cents = cumIssuedCents;
    (d as any).cum_profit_redeemed_cents = cumRedeemedCents;
  }

  return json(
    {
      ok: true,
      settings: { coin_value_cents: settings.coin_value_cents, currency: settings.currency },
      days,
    },
    200,
    request
  );
}

// ============================================================================
// The rest of handlers (as you had) — unchanged (best-effort / safe)
// ============================================================================

export async function handleCabinetSummary(appId, request, env, ownerId) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to");

  const db = env.DB;

  // Best-effort KPIs (если таблиц нет — вернём нули)
  let opens = 0,
    dau = 0,
    orders = 0,
    amount_cents = 0;

  try {
    const row = await db
      .prepare(
        `SELECT COUNT(1) AS cnt
         FROM events
         WHERE app_public_id = ?
           AND type='open'
           ${from ? "AND datetime(created_at) >= datetime(?)" : ""}
           ${to ? "AND datetime(created_at) <  datetime(?)" : ""}`
      )
      .bind(publicId, ...(from ? [from] : []), ...(to ? [to] : []))
      .first();
    opens = Number(row?.cnt || 0);
  } catch (_) {}

  try {
    const row = await db
      .prepare(
        `SELECT COUNT(DISTINCT tg_user_id) AS cnt
         FROM app_users
         WHERE app_public_id = ?
           ${from ? "AND datetime(last_seen) >= datetime(?)" : ""}
           ${to ? "AND datetime(last_seen) <  datetime(?)" : ""}`
      )
      .bind(publicId, ...(from ? [from] : []), ...(to ? [to] : []))
      .first();
    dau = Number(row?.cnt || 0);
  } catch (_) {}

  try {
    const row = await db
      .prepare(
        `SELECT COUNT(1) AS orders, COALESCE(SUM(amount_cents),0) AS amount_cents
         FROM sales
         WHERE app_public_id = ?
           ${from ? "AND datetime(created_at) >= datetime(?)" : ""}
           ${to ? "AND datetime(created_at) <  datetime(?)" : ""}`
      )
      .bind(publicId, ...(from ? [from] : []), ...(to ? [to] : []))
      .first();
    orders = Number(row?.orders || 0);
    amount_cents = Number(row?.amount_cents || 0);
  } catch (_) {}

  return json(
    {
      ok: true,
      kpi: {
        opens,
        dau,
        sales_orders: orders,
        sales_amount: amount_cents / 100,
      },
    },
    200,
    request
  );
}

export async function handleCabinetActivity(appId, request, env, ownerId) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const limit = Math.max(1, Math.min(200, Number(url.searchParams.get("limit") || 100)));

  try {
    const rows = await env.DB.prepare(
      `SELECT id, type, created_at, payload
       FROM events
       WHERE app_public_id = ?
       ORDER BY id DESC
       LIMIT ?`
    )
      .bind(publicId, limit)
      .all();
    return json({ ok: true, items: rows.results || [] }, 200, request);
  } catch (e) {
    return json({ ok: true, items: [] }, 200, request);
  }
}

export async function handleCabinetCustomers(appId, request, env, ownerId) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const q = String(url.searchParams.get("query") || "").trim();
  const limit = Math.max(1, Math.min(200, Number(url.searchParams.get("limit") || 50)));

  try {
    const rows = await env.DB.prepare(
      `SELECT tg_user_id, tg_username, coins, first_seen, last_seen, total_opens, total_spins, total_prizes
       FROM app_users
       WHERE app_public_id = ?
         ${q ? "AND (tg_user_id LIKE ? OR tg_username LIKE ?)" : ""}
       ORDER BY datetime(last_seen) DESC
       LIMIT ?`
    )
      .bind(publicId, ...(q ? [`%${q}%`, `%${q}%`] : []), limit)
      .all();
    return json({ ok: true, customers: rows.results || [] }, 200, request);
  } catch (e) {
    return json({ ok: true, customers: [] }, 200, request);
  }
}

export async function handleCabinetSalesStats(appId, request, env, ownerId) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to");

  try {
    const rows = await env.DB.prepare(
      `SELECT substr(created_at,1,10) AS day,
              COUNT(1) AS orders,
              COALESCE(SUM(amount_cents),0) AS amount_cents
       FROM sales
       WHERE app_public_id = ?
         ${from ? "AND date(created_at) >= date(?)" : ""}
         ${to ? "AND date(created_at) <= date(?)" : ""}
       GROUP BY substr(created_at,1,10)
       ORDER BY day ASC`
    )
      .bind(publicId, ...(from ? [from] : []), ...(to ? [to] : []))
      .all();
    return json({ ok: true, series: rows.results || [] }, 200, request);
  } catch (e) {
    return json({ ok: true, series: [] }, 200, request);
  }
}

export async function handleCabinetPassportStats(appId, request, env, ownerId) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to");

  try {
    const rows = await env.DB.prepare(
      `SELECT substr(created_at,1,10) AS day,
              COUNT(1) AS issued,
              SUM(CASE WHEN status='redeemed' THEN 1 ELSE 0 END) AS redeemed
       FROM passport_rewards
       WHERE app_public_id = ?
         ${from ? "AND date(created_at) >= date(?)" : ""}
         ${to ? "AND date(created_at) <= date(?)" : ""}
       GROUP BY substr(created_at,1,10)
       ORDER BY day ASC`
    )
      .bind(publicId, ...(from ? [from] : []), ...(to ? [to] : []))
      .all();
    return json({ ok: true, series: rows.results || [] }, 200, request);
  } catch (e) {
    return json({ ok: true, series: [] }, 200, request);
  }
}

export async function handleCabinetCalendarBookings(appId, request, env, ownerId) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const limit = Math.max(1, Math.min(200, Number(url.searchParams.get("limit") || 50)));

  try {
    const rows = await env.DB.prepare(
      `SELECT id, date, time, name, phone, status, created_at
       FROM cal_bookings
       WHERE app_public_id = ?
       ORDER BY datetime(created_at) DESC
       LIMIT ?`
    )
      .bind(publicId, limit)
      .all();
    return json({ ok: true, bookings: rows.results || [] }, 200, request);
  } catch (e) {
    return json({ ok: true, bookings: [] }, 200, request);
  }
}

export async function handleCabinetProfitReport(appId, request, env, ownerId) {
  // Пока "условный" profit: revenue + reward placeholders
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to");

  let amount_cents = 0;
  try {
    const row = await env.DB.prepare(
      `SELECT COALESCE(SUM(amount_cents),0) AS amount_cents
       FROM sales
       WHERE app_public_id = ?
         ${from ? "AND date(created_at) >= date(?)" : ""}
         ${to ? "AND date(created_at) <= date(?)" : ""}`
    )
      .bind(publicId, ...(from ? [from] : []), ...(to ? [to] : []))
      .first();
    amount_cents = Number(row?.amount_cents || 0);
  } catch (_) {}

  return json(
    {
      ok: true,
      revenue: amount_cents / 100,
      reward_cost: null,
      net: null,
      note: "profit model not configured yet (now we use prize_cost_coins snapshots in wheel timeseries)",
    },
    200,
    request
  );
}

export async function handleCabinetOverview(appId, request, env) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const { from, to } = _parseRangeOrDefault(url);
  const days = _daysBetweenInclusive(from, to);

  // previous window (same length)
  const fromD = new Date(from + "T00:00:00Z");
  const toD = new Date(to + "T00:00:00Z");
  const spanDays = Math.max(1, Math.round(((toD as any) - (fromD as any)) / (24 * 3600 * 1000)) + 1);
  const prevToD = new Date(fromD.getTime() - 24 * 3600 * 1000);
  const prevFromD = new Date(prevToD.getTime() - (spanDays - 1) * 24 * 3600 * 1000);
  const prevFrom = prevFromD.toISOString().slice(0, 10);
  const prevTo = prevToD.toISOString().slice(0, 10);

  const db = env.DB;

  async function safeAll(stmt, binds) {
    try {
      return await db.prepare(stmt).bind(...binds).all();
    } catch (_) {
      return { results: [] };
    }
  }
  async function safeFirst(stmt, binds) {
    try {
      return await db.prepare(stmt).bind(...binds).first();
    } catch (_) {
      return null;
    }
  }

  const salesRows = await safeAll(
    `
    SELECT date(created_at) AS d,
           COUNT(1) AS sales_count,
           COALESCE(SUM(amount_cents),0) AS revenue_cents
    FROM sales
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
    GROUP BY date(created_at)
  `,
    [publicId, from, to]
  );
  const salesMap = new Map((salesRows.results || []).map((r) => [String(r.d), r]));

  const newRows = await safeAll(
    `
    SELECT date(first_seen) AS d, COUNT(1) AS new_customers
    FROM app_users
    WHERE app_public_id = ?
      AND date(first_seen) BETWEEN date(?) AND date(?)
    GROUP BY date(first_seen)
  `,
    [publicId, from, to]
  );
  const newMap = new Map((newRows.results || []).map((r) => [String(r.d), r]));

  const actRows = await safeAll(
    `
    SELECT date(created_at) AS d, COUNT(DISTINCT tg_user_id) AS active_customers
    FROM events
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
    GROUP BY date(created_at)
  `,
    [publicId, from, to]
  );
  const actMap = new Map((actRows.results || []).map((r) => [String(r.d), r]));

  const coinRows = await safeAll(
    `
    SELECT date(ts) AS d,
      COALESCE(SUM(CASE WHEN delta>0 THEN delta ELSE 0 END),0) AS coins_issued,
      COALESCE(SUM(CASE WHEN delta<0 THEN -delta ELSE 0 END),0) AS coins_redeemed
    FROM coins_ledger
    WHERE app_public_id = ?
      AND date(ts) BETWEEN date(?) AND date(?)
    GROUP BY date(ts)
  `,
    [publicId, from, to]
  );
  const coinMap = new Map((coinRows.results || []).map((r) => [String(r.d), r]));

  const series = days.map((d) => {
    const s = salesMap.get(d) || {};
    const revenue = Number(s.revenue_cents || 0) / 100;
    const sales_count = Number(s.sales_count || 0);
    const avg_check = sales_count ? revenue / sales_count : 0;

    const nw = newMap.get(d) || {};
    const ac = actMap.get(d) || {};
    const cc = coinMap.get(d) || {};
    return {
      d,
      revenue,
      sales_count,
      avg_check,
      new_customers: Number(nw.new_customers || 0),
      active_customers: Number(ac.active_customers || 0),
      coins_issued: Number(cc.coins_issued || 0),
      coins_redeemed: Number(cc.coins_redeemed || 0),
      qr_scans: sales_count,
    };
  });

  function sum(key) {
    return series.reduce((a, p) => a + Number((p as any)[key] || 0), 0);
  }

  const prevSales =
    (await safeFirst(
      `
    SELECT COUNT(1) AS sales_count, COALESCE(SUM(amount_cents),0) AS revenue_cents
    FROM sales
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
  `,
      [publicId, prevFrom, prevTo]
    )) || {};
  const prevRevenue = Number(prevSales.revenue_cents || 0) / 100;
  const prevSalesCount = Number(prevSales.sales_count || 0);
  const prevAvg = prevSalesCount ? prevRevenue / prevSalesCount : 0;

  const prevNew =
    (await safeFirst(
      `
    SELECT COUNT(1) AS new_customers
    FROM app_users
    WHERE app_public_id = ?
      AND date(first_seen) BETWEEN date(?) AND date(?)
  `,
      [publicId, prevFrom, prevTo]
    )) || {};
  const prevActive =
    (await safeFirst(
      `
    SELECT COUNT(DISTINCT tg_user_id) AS active_customers
    FROM events
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
  `,
      [publicId, prevFrom, prevTo]
    )) || {};
  const prevCoins =
    (await safeFirst(
      `
    SELECT
      COALESCE(SUM(CASE WHEN delta>0 THEN delta ELSE 0 END),0) AS coins_issued,
      COALESCE(SUM(CASE WHEN delta<0 THEN -delta ELSE 0 END),0) AS coins_redeemed
    FROM coins_ledger
    WHERE app_public_id = ?
      AND date(ts) BETWEEN date(?) AND date(?)
  `,
      [publicId, prevFrom, prevTo]
    )) || {};

  const revenue = sum("revenue");
  const sales_count = sum("sales_count");
  const avg_check = sales_count ? revenue / sales_count : 0;

  const coins_issued = sum("coins_issued");
  const coins_redeemed = sum("coins_redeemed");

  const qr_scans = sum("qr_scans");
  const new_customers = sum("new_customers");
  const active_customers = Math.round(sum("active_customers"));

  const kpi = {
    revenue,
    revenue_prev: prevRevenue,
    sales_count,
    sales_count_prev: prevSalesCount,
    avg_check,
    avg_check_prev: prevAvg,
    coins_issued,
    coins_issued_prev: Number(prevCoins.coins_issued || 0),
    coins_redeemed,
    coins_redeemed_prev: Number(prevCoins.coins_redeemed || 0),
    qr_scans,
    qr_scans_prev: prevSalesCount,
    new_customers,
    new_customers_prev: Number(prevNew.new_customers || 0),
    active_customers,
    active_customers_prev: Number(prevActive.active_customers || 0),
  };

  const topCustRows = await safeAll(
    `
    SELECT CAST(tg_id AS TEXT) AS id,
           COALESCE(MAX(tg_username), '') AS title,
           COALESCE(SUM(amount_cents),0) AS revenue_cents,
           COUNT(1) AS sales_count
    FROM sales
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
    GROUP BY tg_id
    ORDER BY revenue_cents DESC
    LIMIT 7
  `,
    [publicId, from, to]
  );
  const top_customers = (topCustRows.results || []).map((r) => ({
    id: String(r.id || ""),
    title: String(r.title || r.id || "User"),
    value: Math.round(Number(r.revenue_cents || 0) / 100),
    sub: `${Number(r.sales_count || 0)} checks`,
  }));

  const topPrizeRows = await safeAll(
    `
    SELECT
      prize_code AS prize_code,
      COALESCE(MAX(prize_title), prize_code) AS title,
      SUM(CASE WHEN ts_issued IS NOT NULL THEN 1 ELSE 0 END)   AS wins,
      SUM(CASE WHEN ts_redeemed IS NOT NULL THEN 1 ELSE 0 END) AS redeemed
    FROM wheel_spins
    WHERE app_public_id = ?
      AND date(ts_created) BETWEEN date(?) AND date(?)
    GROUP BY prize_code
    ORDER BY wins DESC
    LIMIT 7
  `,
    [publicId, from, to]
  );
  const top_prizes = (topPrizeRows.results || []).map((r) => ({
    prize_code: String(r.prize_code || ""),
    title: String(r.title || r.prize_code || "Prize"),
    wins: Number(r.wins || 0),
    redeemed: Number(r.redeemed || 0),
  }));

  const live = [];
  const alerts = [];
  const top_cashiers = [];

  return json(
    {
      ok: true,
      kpi,
      series,
      live,
      alerts,
      top_customers,
      top_prizes,
      top_cashiers,
    },
    200,
    request
  );
}

export async function handleCabinetProfit(appId, request, env) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const { from, to } = _parseRangeOrDefault(url);
  const days = _daysBetweenInclusive(from, to);

  const db = env.DB;

  async function safeAll(stmt, binds) {
    try {
      return await db.prepare(stmt).bind(...binds).all();
    } catch (_) {
      return { results: [] };
    }
  }
  async function safeFirst(stmt, binds) {
    try {
      return await db.prepare(stmt).bind(...binds).first();
    } catch (_) {
      return null;
    }
  }

  let coin_value = 0;
  const s = await safeFirst(`SELECT coin_value_cents FROM app_settings WHERE app_public_id=? LIMIT 1`, [publicId]);
  if (s && s.coin_value_cents !== undefined && s.coin_value_cents !== null) coin_value = Number(s.coin_value_cents || 0) / 100;

  const salesRows = await safeAll(
    `
    SELECT date(created_at) AS d,
           COUNT(1) AS checks,
           COALESCE(SUM(amount_cents),0) AS revenue_cents
    FROM sales
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
    GROUP BY date(created_at)
  `,
    [publicId, from, to]
  );
  const salesMap = new Map((salesRows.results || []).map((r) => [String(r.d), r]));

  const coinRows = await safeAll(
    `
    SELECT date(ts) AS d,
      COALESCE(SUM(CASE WHEN delta>0 THEN delta ELSE 0 END),0) AS coins_issued,
      COALESCE(SUM(CASE WHEN delta<0 THEN -delta ELSE 0 END),0) AS coins_redeemed
    FROM coins_ledger
    WHERE app_public_id = ?
      AND date(ts) BETWEEN date(?) AND date(?)
    GROUP BY date(ts)
  `,
    [publicId, from, to]
  );
  const coinMap = new Map((coinRows.results || []).map((r) => [String(r.d), r]));

  const outRow = (await safeFirst(`SELECT COALESCE(SUM(coins),0) AS outstanding FROM app_users WHERE app_public_id=?`, [publicId])) || {};
  const outstanding_coins = Number(outRow.outstanding || 0);

  const series = days.map((d) => {
    const s = salesMap.get(d) || {};
    const revenue = Number(s.revenue_cents || 0) / 100;
    const checks = Number(s.checks || 0);
    const c = coinMap.get(d) || {};
    const coins_issued = Number(c.coins_issued || 0);
    const coins_redeemed = Number(c.coins_redeemed || 0);

    const cogs = 0;
    const gross_profit = revenue - cogs;
    const issued_cost = coins_issued * coin_value;
    const redeemed_cost = coins_redeemed * coin_value;

    const liability_value = outstanding_coins * coin_value;
    const net_profit = gross_profit - redeemed_cost;

    return {
      d,
      revenue,
      cogs,
      gross_profit,
      net_profit,
      redeemed_cost,
      issued_cost,
      liability_value,
    };
  });

  function sum(key) {
    return series.reduce((a, p) => a + Number((p as any)[key] || 0), 0);
  }

  const revenue = sum("revenue");
  const checks = (salesRows.results || []).reduce((a, r) => a + Number(r.checks || 0), 0);
  const avg_check = checks ? revenue / checks : 0;

  const coinsIssued = (coinRows.results || []).reduce((a, r) => a + Number(r.coins_issued || 0), 0);
  const coinsRedeemed = (coinRows.results || []).reduce((a, r) => a + Number(r.coins_redeemed || 0), 0);

  const cogs = 0;
  const gross_profit = revenue - cogs;
  const gross_margin_pct = revenue ? (gross_profit / revenue) * 100 : 0;

  const issued_cost = coinsIssued * coin_value;
  const redeemed_cost = coinsRedeemed * coin_value;
  const liability_value = outstanding_coins * coin_value;

  const net_profit = gross_profit - redeemed_cost;
  const reward_rate_pct = revenue ? (redeemed_cost / revenue) * 100 : 0;

  const kpi = {
    revenue,
    cogs,
    gross_profit,
    gross_margin_pct,
    coins_issued: coinsIssued,
    coins_redeemed: coinsRedeemed,
    outstanding_coins,
    coin_value,
    issued_cost,
    redeemed_cost,
    liability_value,
    net_profit,
    reward_rate_pct,
    avg_check,
    checks,
  };

  const live = [];
  const alerts = [];
  const top_drivers = [];

  return json({ ok: true, kpi, series, live, alerts, top_drivers }, 200, request);
}

export async function handleCabinetWheelPrizesGet(appId, request, env, ownerId) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  try {
    const rows = await env.DB.prepare(
      `SELECT id, code, title, weight, coins, active
       FROM wheel_prizes
       WHERE app_public_id = ?
       ORDER BY id ASC`
    )
      .bind(publicId)
      .all();
    return json({ ok: true, items: rows.results || [] }, 200, request);
  } catch (e) {
    return json({ ok: true, items: [] }, 200, request);
  }
}

export async function handleCabinetWheelPrizesUpdate(appId, request, env, ownerId) {
  let body;
  try {
    body = await request.json();
  } catch (_) {
    return json({ ok: false, error: "BAD_JSON" }, 400, request);
  }

  const items = Array.isArray(body?.items) ? body.items : null;
  if (!items || !items.length) {
    return json({ ok: false, error: "NO_ITEMS" }, 400, request);
  }

  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) {
    return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);
  }

  const cols = await getTableCols(env.DB, "wheel_prizes");

  // LIVE fields: меняем из аналитики/тюнинга без publish
  const allowed = new Set([
    "active",
    "track_qty",
    "qty_left",
    "stop_when_zero",

    "weight",

    // ✅ new: cost in coins
    "cost_coins",
  ]);

  let updated = 0;

  for (const it of items) {
    const code = String(it?.prize_code || it?.code || "").trim();
    if (!code) continue;

    const sets: string[] = [];
    const vals: any[] = [];

    function setIf(name: string, v: any) {
      if (!allowed.has(name)) return;
      if (!cols.has(name)) return;
      if (v === undefined) return; // undefined = не трогаем поле
      sets.push(`${name} = ?`);
      vals.push(v);
    }

    if (it.active !== undefined) {
      setIf("active", toInt(it.active, 1) ? 1 : 0);
    }
    if (it.weight !== undefined) {
      setIf("weight", Math.max(0, toInt(it.weight, 0)));
    }

    if (it.cost_coins !== undefined) {
      setIf("cost_coins", Math.max(0, toInt(it.cost_coins, 0)));
    }

    if (it.track_qty !== undefined) {
      setIf("track_qty", toInt(it.track_qty, 0) ? 1 : 0);
    }
    if (it.qty_left !== undefined) {
      setIf("qty_left", Math.max(0, toInt(it.qty_left, 0)));
    }
    if (it.stop_when_zero !== undefined) {
      setIf("stop_when_zero", toInt(it.stop_when_zero, 0) ? 1 : 0);
    }

    if (!sets.length) continue;

    if (cols.has("updated_at")) {
      sets.push(`updated_at = datetime('now')`);
    }

    vals.push(appPublicId, code);

    const res = await env.DB.prepare(
      `
      UPDATE wheel_prizes
      SET ${sets.join(", ")}
      WHERE app_public_id = ?
        AND code = ?
    `
    )
      .bind(...vals)
      .run();

    if (res?.meta?.changes) updated += res.meta.changes;
  }

  return json({ ok: true, updated }, 200, request);
}

