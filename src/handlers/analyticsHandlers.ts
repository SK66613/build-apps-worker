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

// ===============================
// WHEEL: stats
// ===============================
export async function handleCabinetWheelStats(appId: any, request: Request, env: Env, ownerId: any) {
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

  // try v2 schema (with kind/cost/etc.)
  try {
    const rows = await db
      .prepare(
        `
        WITH agg AS (
          SELECT
            prize_code AS code,
            COUNT(*) AS wins,
            SUM(CASE WHEN status='redeemed' THEN 1 ELSE 0 END) AS redeemed
          FROM wheel_redeems
          WHERE (app_id = ? OR app_public_id = ?)
            AND issued_at >= ?
            AND issued_at < ?
          GROUP BY prize_code
        )
        SELECT
          p.code  AS prize_code,
          p.title AS title,
          COALESCE(a.wins, 0)     AS wins,
          COALESCE(a.redeemed, 0) AS redeemed,

          p.weight AS weight,
          p.active AS active,

          p.kind AS kind,
          p.coins AS coins,
          p.img AS img,

          p.cost_cent AS cost_cent,
          p.cost_currency AS cost_currency,

          p.track_qty AS track_qty,
          p.qty_left AS qty_left,
          p.stop_when_zero AS stop_when_zero
        FROM wheel_prizes p
        LEFT JOIN agg a ON a.code = p.code
        WHERE (p.app_id = ? OR p.app_public_id = ?)
        ORDER BY COALESCE(a.wins,0) DESC, p.code ASC
      `
      )
      .bind(appId, appPublicId, fromTs, toTs, appId, appPublicId)
      .all();

    const items = (rows?.results || []).map((r: any) => ({
      prize_code: String(r.prize_code || ""),
      title: String(r.title || ""),
      wins: Number(r.wins || 0),
      redeemed: Number(r.redeemed || 0),

      weight: Number(r.weight ?? 0),
      active: Number(r.active ?? 0) ? 1 : 0,

      kind: String(r.kind || ""),
      coins: Number(r.coins ?? 0),
      img: r.img ?? null,

      cost_cent: Number(r.cost_cent ?? 0),
      cost_currency: String(r.cost_currency || ""),

      track_qty: Number(r.track_qty ?? 0) ? 1 : 0,
      qty_left: r.qty_left === null || r.qty_left === undefined ? null : Number(r.qty_left),
      stop_when_zero: Number(r.stop_when_zero ?? 0) ? 1 : 0,
    }));

    return json({ ok: true, items }, 200, request);
  } catch (e: any) {
    const msg = String(e?.message || e);

    // fallback: old schema WITHOUT kind/cost columns
    // IMPORTANT: this is exactly your case (no such column: kind)
    if (!msg.includes("no such column: kind")) {
      // other db error -> return explicit
      return json({ ok: false, error: "DB_ERROR", message: msg }, 500, request);
    }

    const rows = await db
      .prepare(
        `
        WITH agg AS (
          SELECT
            prize_code AS code,
            COUNT(*) AS wins,
            SUM(CASE WHEN status='redeemed' THEN 1 ELSE 0 END) AS redeemed
          FROM wheel_redeems
          WHERE (app_id = ? OR app_public_id = ?)
            AND issued_at >= ?
            AND issued_at < ?
          GROUP BY prize_code
        )
        SELECT
          p.code  AS prize_code,
          p.title AS title,
          COALESCE(a.wins, 0)     AS wins,
          COALESCE(a.redeemed, 0) AS redeemed,
          p.weight AS weight,
          p.active AS active,
          p.coins AS coins,
          p.img AS img
        FROM wheel_prizes p
        LEFT JOIN agg a ON a.code = p.code
        WHERE (p.app_id = ? OR p.app_public_id = ?)
        ORDER BY COALESCE(a.wins,0) DESC, p.code ASC
      `
      )
      .bind(appId, appPublicId, fromTs, toTs, appId, appPublicId)
      .all();

    const items = (rows?.results || []).map((r: any) => ({
      prize_code: String(r.prize_code || ""),
      title: String(r.title || ""),
      wins: Number(r.wins || 0),
      redeemed: Number(r.redeemed || 0),

      weight: Number(r.weight ?? 0),
      active: Number(r.active ?? 0) ? 1 : 0,

      // старые базы: kind/cost/qty не было
      kind: "",
      coins: Number(r.coins ?? 0),
      img: r.img ?? null,

      cost_cent: 0,
      cost_currency: "",
      track_qty: 0,
      qty_left: null,
      stop_when_zero: 0,
    }));

    return json({ ok: true, items }, 200, request);
  }
}

// ===============================
// WHEEL: timeseries (FACT by dates, NO kind needed)
// ===============================
export async function handleCabinetWheelTimeseries(appId: any, request: Request, env: Env, ownerId: any) {
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

  const settings = await getAppSettingsForPublicId(env, appPublicId);
  const coinValueCents = settings.coin_value_cents;

  const db = env.DB;

  // NOTE: dates via substr('YYYY-MM-DD HH:MM:SS', 1, 10)
  const rows = await db
    .prepare(
      `
      WITH spins AS (
        SELECT
          substr(ts_created, 1, 10) AS d,
          COUNT(*) AS spins,
          COALESCE(SUM(COALESCE(spin_cost, 0)), 0) AS spin_cost_coins
        FROM wheel_spins
        WHERE (app_id = ? OR app_public_id = ?)
          AND ts_created >= ?
          AND ts_created < ?
        GROUP BY substr(ts_created, 1, 10)
      ),
      redeems AS (
        SELECT
          substr(issued_at, 1, 10) AS d,
          COUNT(*) AS wins,
          SUM(CASE WHEN status='redeemed' THEN 1 ELSE 0 END) AS redeemed
        FROM wheel_redeems
        WHERE (app_id = ? OR app_public_id = ?)
          AND issued_at >= ?
          AND issued_at < ?
        GROUP BY substr(issued_at, 1, 10)
      ),
      days AS (
        SELECT d FROM spins
        UNION
        SELECT d FROM redeems
      )
      SELECT
        days.d AS date,
        COALESCE(spins.spins, 0) AS spins,
        COALESCE(spins.spin_cost_coins, 0) AS spin_cost_coins,
        COALESCE(redeems.wins, 0) AS wins,
        COALESCE(redeems.redeemed, 0) AS redeemed
      FROM days
      LEFT JOIN spins   ON spins.d = days.d
      LEFT JOIN redeems ON redeems.d = days.d
      ORDER BY days.d ASC
    `
    )
    .bind(appId, appPublicId, fromTs, toTs, appId, appPublicId, fromTs, toTs)
    .all();

  const days = (rows?.results || []).map((r: any) => {
    const spins = Number(r.spins || 0);
    const spin_cost_coins = Number(r.spin_cost_coins || 0);

    // факт-выручка по фактической стоимости спинов из wheel_spins
    const revenue_cent = Math.round(spin_cost_coins * coinValueCents);

    return {
      date: String(r.date || ""),
      spins,
      wins: Number(r.wins || 0),
      redeemed: Number(r.redeemed || 0),
      spin_cost_coins,
      revenue_cent,
    };
  });

  return json({ ok: true, coin_value_cents: coinValueCents, days }, 200, request);
}

// ===============================
// Other handlers (оставил как у тебя, без изменений логики)
// ===============================

export async function handleCabinetSummary(appId: any, request: Request, env: Env, ownerId: any) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to");

  const db = env.DB;

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

export async function handleCabinetActivity(appId: any, request: Request, env: Env, ownerId: any) {
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

export async function handleCabinetCustomers(appId: any, request: Request, env: Env, ownerId: any) {
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

export async function handleCabinetSalesStats(appId: any, request: Request, env: Env, ownerId: any) {
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

export async function handleCabinetPassportStats(appId: any, request: Request, env: Env, ownerId: any) {
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

export async function handleCabinetCalendarBookings(appId: any, request: Request, env: Env, ownerId: any) {
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

export async function handleCabinetProfitReport(appId: any, request: Request, env: Env, ownerId: any) {
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
      note: "profit model not configured yet (need coin_value_cents + prize cost_cents)",
    },
    200,
    request
  );
}

// overview/profit + wheel prizes update/get — оставляю как у тебя (ниже)
export async function handleCabinetOverview(appId: any, request: Request, env: Env) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const { from, to } = _parseRangeOrDefault(url);
  const days = _daysBetweenInclusive(from, to);

  const fromD = new Date(from + "T00:00:00Z");
  const toD = new Date(to + "T00:00:00Z");
  const spanDays = Math.max(1, Math.round((toD.getTime() - fromD.getTime()) / (24 * 3600 * 1000)) + 1);
  const prevToD = new Date(fromD.getTime() - 24 * 3600 * 1000);
  const prevFromD = new Date(prevToD.getTime() - (spanDays - 1) * 24 * 3600 * 1000);
  const prevFrom = prevFromD.toISOString().slice(0, 10);
  const prevTo = prevToD.toISOString().slice(0, 10);

  const db = env.DB;

  async function safeAll(stmt: string, binds: any[]) {
    try {
      return await db.prepare(stmt).bind(...binds).all();
    } catch (_) {
      return { results: [] as any[] };
    }
  }
  async function safeFirst(stmt: string, binds: any[]) {
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
  const salesMap = new Map((salesRows.results || []).map((r: any) => [String(r.d), r]));

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
  const newMap = new Map((newRows.results || []).map((r: any) => [String(r.d), r]));

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
  const actMap = new Map((actRows.results || []).map((r: any) => [String(r.d), r]));

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
  const coinMap = new Map((coinRows.results || []).map((r: any) => [String(r.d), r]));

  const series = days.map((d: string) => {
    const s: any = salesMap.get(d) || {};
    const revenue = Number(s.revenue_cents || 0) / 100;
    const sales_count = Number(s.sales_count || 0);
    const avg_check = sales_count ? revenue / sales_count : 0;

    const nw: any = newMap.get(d) || {};
    const ac: any = actMap.get(d) || {};
    const cc: any = coinMap.get(d) || {};
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

  function sum(key: string) {
    return series.reduce((a: number, p: any) => a + Number(p[key] || 0), 0);
  }

  const prevSales: any =
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

  const prevNew: any =
    (await safeFirst(
      `
    SELECT COUNT(1) AS new_customers
    FROM app_users
    WHERE app_public_id = ?
      AND date(first_seen) BETWEEN date(?) AND date(?)
  `,
      [publicId, prevFrom, prevTo]
    )) || {};
  const prevActive: any =
    (await safeFirst(
      `
    SELECT COUNT(DISTINCT tg_user_id) AS active_customers
    FROM events
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
  `,
      [publicId, prevFrom, prevTo]
    )) || {};
  const prevCoins: any =
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

  return json(
    {
      ok: true,
      kpi,
      series,
      live: [],
      alerts: [],
      top_customers: [],
      top_prizes: [],
      top_cashiers: [],
    },
    200,
    request
  );
}

export async function handleCabinetProfit(appId: any, request: Request, env: Env) {
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const url = new URL(request.url);
  const { from, to } = _parseRangeOrDefault(url);
  const days = _daysBetweenInclusive(from, to);

  const db = env.DB;

  async function safeAll(stmt: string, binds: any[]) {
    try {
      return await db.prepare(stmt).bind(...binds).all();
    } catch (_) {
      return { results: [] as any[] };
    }
  }
  async function safeFirst(stmt: string, binds: any[]) {
    try {
      return await db.prepare(stmt).bind(...binds).first();
    } catch (_) {
      return null;
    }
  }

  let coin_value = 0;
  const s: any = await safeFirst(`SELECT coin_value_cents FROM app_settings WHERE app_public_id=? LIMIT 1`, [publicId]);
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
  const salesMap = new Map((salesRows.results || []).map((r: any) => [String(r.d), r]));

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
  const coinMap = new Map((coinRows.results || []).map((r: any) => [String(r.d), r]));

  const outRow: any = (await safeFirst(`SELECT COALESCE(SUM(coins),0) AS outstanding FROM app_users WHERE app_public_id=?`, [publicId])) || {};
  const outstanding_coins = Number(outRow.outstanding || 0);

  const series = days.map((d: string) => {
    const s: any = salesMap.get(d) || {};
    const revenue = Number(s.revenue_cents || 0) / 100;
    const c: any = coinMap.get(d) || {};
    const coins_issued = Number(c.coins_issued || 0);
    const coins_redeemed = Number(c.coins_redeemed || 0);

    const cogs = 0;
    const gross_profit = revenue - cogs;
    const issued_cost = coins_issued * coin_value;
    const redeemed_cost = coins_redeemed * coin_value;

    const liability_value = outstanding_coins * coin_value;
    const net_profit = gross_profit - redeemed_cost;

    return { d, revenue, cogs, gross_profit, net_profit, redeemed_cost, issued_cost, liability_value };
  });

  function sum(key: string) {
    return series.reduce((a: number, p: any) => a + Number(p[key] || 0), 0);
  }

  const revenue = sum("revenue");
  const coinsIssued = (coinRows.results || []).reduce((a: number, r: any) => a + Number(r.coins_issued || 0), 0);
  const coinsRedeemed = (coinRows.results || []).reduce((a: number, r: any) => a + Number(r.coins_redeemed || 0), 0);

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
    avg_check: 0,
    checks: 0,
  };

  return json({ ok: true, kpi, series, live: [], alerts: [], top_drivers: [] }, 200, request);
}

export async function handleCabinetWheelPrizesGet(appId: any, request: Request, env: Env, ownerId: any) {
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

export async function handleCabinetWheelPrizesUpdate(appId: any, request: Request, env: Env, ownerId: any) {
  let body: any;
  try {
    body = await request.json();
  } catch (_) {
    return json({ ok: false, error: "BAD_JSON" }, 400, request);
  }

  const items = Array.isArray(body?.items) ? body.items : null;
  if (!items || !items.length) {
    return json({ ok: false, error: "NO_ITEMS" }, 400, request);
  }

  const norm: Array<{ code: string; weight: number; active: 0 | 1 }> = [];
  for (const it of items) {
    const code = String(it?.prize_code || it?.code || "").trim();
    if (!code) continue;

    const weight = Math.max(0, toInt(it.weight, 0));
    const active = toInt(it.active, 1) ? 1 : 0;

    norm.push({ code, weight, active });
  }

  if (!norm.length) {
    return json({ ok: false, error: "NO_VALID_ITEMS" }, 400, request);
  }

  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) {
    return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);
  }

  let updated = 0;

  for (const it of norm) {
    const res = await env.DB.prepare(
      `
      UPDATE wheel_prizes
      SET weight = ?, active = ?
      WHERE app_public_id = ?
        AND code = ?
    `
    )
      .bind(it.weight, it.active, appPublicId, it.code)
      .run();

    if ((res as any)?.meta?.changes) {
      updated += (res as any).meta.changes;
    }
  }

  return json({ ok: true, updated }, 200, request);
}
