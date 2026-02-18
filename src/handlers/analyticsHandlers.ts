// src/handlers/analyticsHandlers.ts
// Cabinet analytics handlers (real implementation; extracted from legacy).
// Keep signatures/back-compat stable for existing cabinet UI.

import type { Env } from "../index";
import { jsonResponse } from "../services/http";
import { getCanonicalPublicIdForApp } from "../services/apps";
import { parseRangeOrDefault as _parseRangeOrDefault, daysBetweenInclusive as _daysBetweenInclusive } from "../services/analyticsRange";

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

const json = (obj: any, status = 200, request: Request | null = null) => jsonResponse(obj, status, request as any);


export async function handleCabinetWheelStats(appId, request, env, ownerId){
  const url = new URL(request.url);

  // from/to как в React: YYYY-MM-DD
  const from = String(url.searchParams.get('from') || '').trim();
  const to   = String(url.searchParams.get('to') || '').trim();

  // Фоллбек если не передали
  const fromOk = /^\d{4}-\d{2}-\d{2}$/.test(from) ? from : null;
  const toOk   = /^\d{4}-\d{2}-\d{2}$/.test(to) ? to : null;

  // toExclusive = to + 1 day
  const toPlus1 = toOk ? addDaysIso(toOk, 1) : null;

  // диапазон в формате datetime (как у тебя хранится issued_at: 'YYYY-MM-DD HH:MM:SS')
  const fromTs = fromOk ? `${fromOk} 00:00:00` : '1970-01-01 00:00:00';
  const toTs   = (toPlus1 ? `${toPlus1} 00:00:00` : '2999-12-31 00:00:00');

  // канонический public_id нужен для wheel_prizes
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) {
    return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 404, request);
  }

  const db = env.DB;

  // wins считаем по wheel_redeems (факт выигрыша / выдачи redeem_code)
  // redeemed — по status='redeemed' (подтверждено кассиром)
  const rows = await db.prepare(`
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

      -- ✅ твои реальные колонки (экономика/тип)
      p.kind          AS kind,
      p.coins         AS coins,
      p.img           AS img,
      p.cost_cent     AS cost_cent,
      p.cost_currency AS cost_currency,
      p.spin_cost     AS spin_cost,

      -- ✅ остатки / авто-выкл
      p.track_qty      AS track_qty,
      p.qty_total      AS qty_total,
      p.qty_left       AS qty_left,
      p.stop_when_zero AS stop_when_zero

    FROM wheel_prizes p
    LEFT JOIN agg a ON a.code = p.code
    WHERE (p.app_id = ? OR p.app_public_id = ?)
    ORDER BY COALESCE(a.wins,0) DESC, p.code ASC
  `).bind(
    appId, appPublicId, fromTs, toTs,
    appId, appPublicId
  ).all();

  const results = (rows as any)?.results || [];

  const items = results.map((r: any) => ({
    prize_code: String(r.prize_code || ''),
    title: String(r.title || ''),
    wins: Number(r.wins || 0),
    redeemed: Number(r.redeemed || 0),

    weight: Number(r.weight ?? 0),
    active: Number(r.active ?? 0) ? 1 : 0,

    // экономика/тип (как в твоей таблице)
    kind: r.kind ? String(r.kind) : undefined,
    coins: (r.coins === null || r.coins === undefined) ? 0 : Number(r.coins),
    img: r.img ? String(r.img) : null,
    cost_cent: (r.cost_cent === null || r.cost_cent === undefined) ? 0 : Number(r.cost_cent),
    cost_currency: r.cost_currency ? String(r.cost_currency) : null,
    spin_cost: (r.spin_cost === null || r.spin_cost === undefined) ? 0 : Number(r.spin_cost),

    // остатки
    track_qty: Number(r.track_qty ?? 0) ? 1 : 0,
    qty_total: (r.qty_total === null || r.qty_total === undefined) ? null : Number(r.qty_total),
    qty_left:  (r.qty_left  === null || r.qty_left  === undefined) ? null : Number(r.qty_left),
    stop_when_zero: Number(r.stop_when_zero ?? 0) ? 1 : 0,
  }));

  return json({ ok:true, items }, 200, request);
}


export async function handleCabinetSummary(appId, request, env, ownerId){
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);

  const url = new URL(request.url);
  const from = url.searchParams.get('from');
  const to   = url.searchParams.get('to');

  const db = env.DB;

  // Best-effort KPIs (если таблиц нет — вернём нули)
  let opens = 0, dau = 0, orders = 0, amount_cents = 0;

  try{
    const row = await db.prepare(
      `SELECT COUNT(1) AS cnt
       FROM events
       WHERE app_public_id = ?
         AND type='open'
         ${from ? "AND datetime(created_at) >= datetime(?)" : ""}
         ${to   ? "AND datetime(created_at) <  datetime(?)" : ""}`
    ).bind(publicId, ...(from?[from]:[]), ...(to?[to]:[])).first();
    opens = Number(row?.cnt||0);
  }catch(_){}

  try{
    const row = await db.prepare(
      `SELECT COUNT(DISTINCT tg_user_id) AS cnt
       FROM app_users
       WHERE app_public_id = ?
         ${from ? "AND datetime(last_seen) >= datetime(?)" : ""}
         ${to   ? "AND datetime(last_seen) <  datetime(?)" : ""}`
    ).bind(publicId, ...(from?[from]:[]), ...(to?[to]:[])).first();
    dau = Number(row?.cnt||0);
  }catch(_){}

  try{
    const row = await db.prepare(
      `SELECT COUNT(1) AS orders, COALESCE(SUM(amount_cents),0) AS amount_cents
       FROM sales
       WHERE app_public_id = ?
         ${from ? "AND datetime(created_at) >= datetime(?)" : ""}
         ${to   ? "AND datetime(created_at) <  datetime(?)" : ""}`
    ).bind(publicId, ...(from?[from]:[]), ...(to?[to]:[])).first();
    orders = Number(row?.orders||0);
    amount_cents = Number(row?.amount_cents||0);
  }catch(_){}

  return json({
    ok:true,
    kpi:{
      opens,
      dau,
      sales_orders: orders,
      sales_amount: amount_cents/100
    }
  }, 200, request);
}

export async function handleCabinetActivity(appId, request, env, ownerId){
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);

  const url = new URL(request.url);
  const limit = Math.max(1, Math.min(200, Number(url.searchParams.get('limit')||100)));

  try{
    const rows = await env.DB.prepare(
      `SELECT id, type, created_at, payload
       FROM events
       WHERE app_public_id = ?
       ORDER BY id DESC
       LIMIT ?`
    ).bind(publicId, limit).all();
    return json({ ok:true, items: rows.results || [] }, 200, request);
  }catch(e){
    return json({ ok:true, items: [] }, 200, request);
  }
}

export async function handleCabinetCustomers(appId, request, env, ownerId){
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);

  const url = new URL(request.url);
  const q = String(url.searchParams.get('query')||'').trim();
  const limit = Math.max(1, Math.min(200, Number(url.searchParams.get('limit')||50)));

  try{
    const rows = await env.DB.prepare(
      `SELECT tg_user_id, tg_username, coins, first_seen, last_seen, total_opens, total_spins, total_prizes
       FROM app_users
       WHERE app_public_id = ?
         ${q ? "AND (tg_user_id LIKE ? OR tg_username LIKE ?)" : ""}
       ORDER BY datetime(last_seen) DESC
       LIMIT ?`
    ).bind(publicId, ...(q?[`%${q}%`,`%${q}%`]:[]), limit).all();
    return json({ ok:true, customers: rows.results || [] }, 200, request);
  }catch(e){
    return json({ ok:true, customers: [] }, 200, request);
  }
}

export async function handleCabinetSalesStats(appId, request, env, ownerId){
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);

  const url = new URL(request.url);
  const from = url.searchParams.get('from');
  const to   = url.searchParams.get('to');

  try{
    const rows = await env.DB.prepare(
      `SELECT substr(created_at,1,10) AS day,
              COUNT(1) AS orders,
              COALESCE(SUM(amount_cents),0) AS amount_cents
       FROM sales
       WHERE app_public_id = ?
         ${from ? "AND date(created_at) >= date(?)" : ""}
         ${to   ? "AND date(created_at) <= date(?)" : ""}
       GROUP BY substr(created_at,1,10)
       ORDER BY day ASC`
    ).bind(publicId, ...(from?[from]:[]), ...(to?[to]:[])).all();
    return json({ ok:true, series: rows.results || [] }, 200, request);
  }catch(e){
    return json({ ok:true, series: [] }, 200, request);
  }
}

export async function handleCabinetPassportStats(appId, request, env, ownerId){
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);

  const url = new URL(request.url);
  const from = url.searchParams.get('from');
  const to   = url.searchParams.get('to');

  try{
    const rows = await env.DB.prepare(
      `SELECT substr(created_at,1,10) AS day,
              COUNT(1) AS issued,
              SUM(CASE WHEN status='redeemed' THEN 1 ELSE 0 END) AS redeemed
       FROM passport_rewards
       WHERE app_public_id = ?
         ${from ? "AND date(created_at) >= date(?)" : ""}
         ${to   ? "AND date(created_at) <= date(?)" : ""}
       GROUP BY substr(created_at,1,10)
       ORDER BY day ASC`
    ).bind(publicId, ...(from?[from]:[]), ...(to?[to]:[])).all();
    return json({ ok:true, series: rows.results || [] }, 200, request);
  }catch(e){
    return json({ ok:true, series: [] }, 200, request);
  }
}

export async function handleCabinetCalendarBookings(appId, request, env, ownerId){
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);

  const url = new URL(request.url);
  const limit = Math.max(1, Math.min(200, Number(url.searchParams.get('limit')||50)));

  try{
    const rows = await env.DB.prepare(
      `SELECT id, date, time, name, phone, status, created_at
       FROM cal_bookings
       WHERE app_public_id = ?
       ORDER BY datetime(created_at) DESC
       LIMIT ?`
    ).bind(publicId, limit).all();
    return json({ ok:true, bookings: rows.results || [] }, 200, request);
  }catch(e){
    return json({ ok:true, bookings: [] }, 200, request);
  }
}

export async function handleCabinetProfitReport(appId, request, env, ownerId){
  // Пока "условный" profit: revenue + reward placeholders
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);

  const url = new URL(request.url);
  const from = url.searchParams.get('from');
  const to   = url.searchParams.get('to');

  let amount_cents = 0;
  try{
    const row = await env.DB.prepare(
      `SELECT COALESCE(SUM(amount_cents),0) AS amount_cents
       FROM sales
       WHERE app_public_id = ?
         ${from ? "AND date(created_at) >= date(?)" : ""}
         ${to   ? "AND date(created_at) <= date(?)" : ""}`
    ).bind(publicId, ...(from?[from]:[]), ...(to?[to]:[])).first();
    amount_cents = Number(row?.amount_cents||0);
  }catch(_){}

  return json({
    ok:true,
    revenue: amount_cents/100,
    reward_cost: null,
    net: null,
    note: 'profit model not configured yet (need coin_value_cents + prize cost_cents)'
  }, 200, request);
}

export async function handleCabinetOverview(appId, request, env){
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);

  const url = new URL(request.url);
  const { from, to } = _parseRangeOrDefault(url);
  const days = _daysBetweenInclusive(from, to);

  // previous window (same length)
  const fromD = new Date(from+'T00:00:00Z');
  const toD   = new Date(to+'T00:00:00Z');
  const spanDays = Math.max(1, Math.round((toD - fromD)/(24*3600*1000))+1);
  const prevToD = new Date(fromD.getTime() - 24*3600*1000);
  const prevFromD = new Date(prevToD.getTime() - (spanDays-1)*24*3600*1000);
  const prevFrom = prevFromD.toISOString().slice(0,10);
  const prevTo   = prevToD.toISOString().slice(0,10);

  const db = env.DB;

  // helper: safe query
  async function safeAll(stmt, binds){
    try{ return await db.prepare(stmt).bind(...binds).all(); }
    catch(_){ return { results: [] }; }
  }
  async function safeFirst(stmt, binds){
    try{ return await db.prepare(stmt).bind(...binds).first(); }
    catch(_){ return null; }
  }

  // sales per day
  const salesRows = await safeAll(`
    SELECT date(created_at) AS d,
           COUNT(1) AS sales_count,
           COALESCE(SUM(amount_cents),0) AS revenue_cents
    FROM sales
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
    GROUP BY date(created_at)
  `, [publicId, from, to]);

  const salesMap = new Map((salesRows.results||[]).map(r => [String(r.d), r]));

  // new customers per day
  const newRows = await safeAll(`
    SELECT date(first_seen) AS d, COUNT(1) AS new_customers
    FROM app_users
    WHERE app_public_id = ?
      AND date(first_seen) BETWEEN date(?) AND date(?)
    GROUP BY date(first_seen)
  `, [publicId, from, to]);
  const newMap = new Map((newRows.results||[]).map(r => [String(r.d), r]));

  // active customers per day (from events)
  const actRows = await safeAll(`
    SELECT date(created_at) AS d, COUNT(DISTINCT tg_user_id) AS active_customers
    FROM events
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
    GROUP BY date(created_at)
  `, [publicId, from, to]);
  const actMap = new Map((actRows.results||[]).map(r => [String(r.d), r]));

  // coins issued/redeemed per day (ledger)
  const coinRows = await safeAll(`
    SELECT date(ts) AS d,
      COALESCE(SUM(CASE WHEN delta>0 THEN delta ELSE 0 END),0) AS coins_issued,
      COALESCE(SUM(CASE WHEN delta<0 THEN -delta ELSE 0 END),0) AS coins_redeemed
    FROM coins_ledger
    WHERE app_public_id = ?
      AND date(ts) BETWEEN date(?) AND date(?)
    GROUP BY date(ts)
  `, [publicId, from, to]);
  const coinMap = new Map((coinRows.results||[]).map(r => [String(r.d), r]));

  // qr_scans best-effort: count sales (if you have dedicated events later — replace)
  const series = days.map(d => {
    const s = salesMap.get(d) || {};
    const revenue = Number(s.revenue_cents||0)/100;
    const sales_count = Number(s.sales_count||0);
    const avg_check = sales_count ? revenue / sales_count : 0;

    const nw = newMap.get(d) || {};
    const ac = actMap.get(d) || {};
    const cc = coinMap.get(d) || {};
    return {
      d,
      revenue,
      sales_count,
      avg_check,
      new_customers: Number(nw.new_customers||0),
      active_customers: Number(ac.active_customers||0),
      coins_issued: Number(cc.coins_issued||0),
      coins_redeemed: Number(cc.coins_redeemed||0),
      qr_scans: sales_count,
    };
  });

  function sum(key){
    return series.reduce((a,p)=>a+Number(p[key]||0),0);
  }

  // previous KPI (totals) best-effort
  const prevSales = await safeFirst(`
    SELECT COUNT(1) AS sales_count, COALESCE(SUM(amount_cents),0) AS revenue_cents
    FROM sales
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
  `, [publicId, prevFrom, prevTo]) || {};
  const prevRevenue = Number(prevSales.revenue_cents||0)/100;
  const prevSalesCount = Number(prevSales.sales_count||0);
  const prevAvg = prevSalesCount ? prevRevenue/prevSalesCount : 0;

  const prevNew = await safeFirst(`
    SELECT COUNT(1) AS new_customers
    FROM app_users
    WHERE app_public_id = ?
      AND date(first_seen) BETWEEN date(?) AND date(?)
  `, [publicId, prevFrom, prevTo]) || {};
  const prevActive = await safeFirst(`
    SELECT COUNT(DISTINCT tg_user_id) AS active_customers
    FROM events
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
  `, [publicId, prevFrom, prevTo]) || {};
  const prevCoins = await safeFirst(`
    SELECT
      COALESCE(SUM(CASE WHEN delta>0 THEN delta ELSE 0 END),0) AS coins_issued,
      COALESCE(SUM(CASE WHEN delta<0 THEN -delta ELSE 0 END),0) AS coins_redeemed
    FROM coins_ledger
    WHERE app_public_id = ?
      AND date(ts) BETWEEN date(?) AND date(?)
  `, [publicId, prevFrom, prevTo]) || {};

  const revenue = sum('revenue');
  const sales_count = sum('sales_count');
  const avg_check = sales_count ? revenue/sales_count : 0;

  const coins_issued = sum('coins_issued');
  const coins_redeemed = sum('coins_redeemed');

  const qr_scans = sum('qr_scans');
  const new_customers = sum('new_customers');
  const active_customers = Math.round(sum('active_customers')); // not perfect but ok

  const kpi = {
    revenue,
    revenue_prev: prevRevenue,
    sales_count,
    sales_count_prev: prevSalesCount,
    avg_check,
    avg_check_prev: prevAvg,
    coins_issued,
    coins_issued_prev: Number(prevCoins.coins_issued||0),
    coins_redeemed,
    coins_redeemed_prev: Number(prevCoins.coins_redeemed||0),
    qr_scans,
    qr_scans_prev: prevSalesCount,
    new_customers,
    new_customers_prev: Number(prevNew.new_customers||0),
    active_customers,
    active_customers_prev: Number(prevActive.active_customers||0),
  };

  // top customers: by revenue
  const topCustRows = await safeAll(`
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
  `, [publicId, from, to]);
  const top_customers = (topCustRows.results||[]).map(r => ({
    id: String(r.id||''),
    title: String(r.title||r.id||'User'),
    value: Math.round(Number(r.revenue_cents||0)/100),
    sub: `${Number(r.sales_count||0)} checks`,
  }));

  // top prizes: by wins/redeemed (best-effort from wheel_redeems)
  const topPrizeRows = await safeAll(`
    SELECT prize_code AS prize_code,
           COALESCE(MAX(prize_title), prize_code) AS title,
           COUNT(1) AS wins,
           SUM(CASE WHEN status='redeemed' THEN 1 ELSE 0 END) AS redeemed
    FROM wheel_redeems
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
    GROUP BY prize_code
    ORDER BY wins DESC
    LIMIT 7
  `, [publicId, from, to]);
  const top_prizes = (topPrizeRows.results||[]).map(r => ({
    prize_code: String(r.prize_code||''),
    title: String(r.title||r.prize_code||'Prize'),
    wins: Number(r.wins||0),
    redeemed: Number(r.redeemed||0),
  }));

  // live + alerts placeholders (front expects arrays)
  const live = [];
  const alerts = [];
  const top_cashiers = [];

  return json({
    ok:true,
    kpi,
    series,
    live,
    alerts,
    top_customers,
    top_prizes,
    top_cashiers
  }, 200, request);
}

export async function handleCabinetProfit(appId, request, env){
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);

  const url = new URL(request.url);
  const { from, to } = _parseRangeOrDefault(url);
  const days = _daysBetweenInclusive(from, to);

  const db = env.DB;

  async function safeAll(stmt, binds){
    try{ return await db.prepare(stmt).bind(...binds).all(); }
    catch(_){ return { results: [] }; }
  }
  async function safeFirst(stmt, binds){
    try{ return await db.prepare(stmt).bind(...binds).first(); }
    catch(_){ return null; }
  }

  // coin_value (money per coin) from app_settings if exists
  let coin_value = 0;
  const s = await safeFirst(`SELECT coin_value_cents FROM app_settings WHERE app_public_id=? LIMIT 1`, [publicId]);
  if (s && s.coin_value_cents !== undefined && s.coin_value_cents !== null) coin_value = Number(s.coin_value_cents||0)/100;

  // sales per day
  const salesRows = await safeAll(`
    SELECT date(created_at) AS d,
           COUNT(1) AS checks,
           COALESCE(SUM(amount_cents),0) AS revenue_cents
    FROM sales
    WHERE app_public_id = ?
      AND date(created_at) BETWEEN date(?) AND date(?)
    GROUP BY date(created_at)
  `, [publicId, from, to]);
  const salesMap = new Map((salesRows.results||[]).map(r => [String(r.d), r]));

  const coinRows = await safeAll(`
    SELECT date(ts) AS d,
      COALESCE(SUM(CASE WHEN delta>0 THEN delta ELSE 0 END),0) AS coins_issued,
      COALESCE(SUM(CASE WHEN delta<0 THEN -delta ELSE 0 END),0) AS coins_redeemed
    FROM coins_ledger
    WHERE app_public_id = ?
      AND date(ts) BETWEEN date(?) AND date(?)
    GROUP BY date(ts)
  `, [publicId, from, to]);
  const coinMap = new Map((coinRows.results||[]).map(r => [String(r.d), r]));

  // outstanding coins: sum coins from app_users (best-effort)
  const outRow = await safeFirst(`SELECT COALESCE(SUM(coins),0) AS outstanding FROM app_users WHERE app_public_id=?`, [publicId]) || {};
  const outstanding_coins = Number(outRow.outstanding||0);

  const series = days.map(d => {
    const s = salesMap.get(d) || {};
    const revenue = Number(s.revenue_cents||0)/100;
    const checks = Number(s.checks||0);
    const c = coinMap.get(d) || {};
    const coins_issued = Number(c.coins_issued||0);
    const coins_redeemed = Number(c.coins_redeemed||0);

    const cogs = 0;
    const gross_profit = revenue - cogs;
    const issued_cost = coins_issued * coin_value;
    const redeemed_cost = coins_redeemed * coin_value;

    // liability is "total outstanding" (constant) spread or shown as last known; we show daily same
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

  function sum(key){ return series.reduce((a,p)=>a+Number(p[key]||0),0); }

  const revenue = sum('revenue');
  const checks = Math.round(sum('revenue') ? (salesRows.results||[]).reduce((a,r)=>a+Number(r.checks||0),0) : (salesRows.results||[]).reduce((a,r)=>a+Number(r.checks||0),0));
  const avg_check = checks ? revenue/checks : 0;

  const coinsIssued = (coinRows.results||[]).reduce((a,r)=>a+Number(r.coins_issued||0),0);
  const coinsRedeemed = (coinRows.results||[]).reduce((a,r)=>a+Number(r.coins_redeemed||0),0);

  const cogs = 0;
  const gross_profit = revenue - cogs;
  const gross_margin_pct = revenue ? (gross_profit/revenue)*100 : 0;

  const issued_cost = coinsIssued * coin_value;
  const redeemed_cost = coinsRedeemed * coin_value;
  const liability_value = outstanding_coins * coin_value;

  const net_profit = gross_profit - redeemed_cost;
  const reward_rate_pct = revenue ? (redeemed_cost/revenue)*100 : 0;

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
    checks
  };

  const live = [];
  const alerts = [];
  const top_drivers = []; // future: what eats profit

  return json({ ok:true, kpi, series, live, alerts, top_drivers }, 200, request);
}

export async function handleCabinetWheelPrizesGet(appId, request, env, ownerId){
  const publicId = await getCanonicalPublicIdForApp(appId, env);
  if (!publicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);

  try{
    const rows = await env.DB.prepare(
      `SELECT id, code, title, weight, coins, active
       FROM wheel_prizes
       WHERE app_public_id = ?
       ORDER BY id ASC`
    ).bind(publicId).all();
    return json({ ok:true, items: rows.results || [] }, 200, request);
  }catch(e){
    return json({ ok:true, items: [] }, 200, request);
  }
}

export async function handleCabinetWheelPrizesUpdate(appId, request, env, ownerId){
  let body;
  try{
    body = await request.json();
  }catch(_){
    return json({ ok:false, error:'BAD_JSON' }, 400, request);
  }

  const items = Array.isArray(body?.items) ? body.items : null;
  if (!items || !items.length){
    return json({ ok:false, error:'NO_ITEMS' }, 400, request);
  }

  const norm = [];
  for (const it of items){
    const code = String(it?.prize_code || it?.code || '').trim();
    if (!code) continue;

    const weight = Math.max(0, toInt(it.weight, 0));
    const active = toInt(it.active, 1) ? 1 : 0;

    norm.push({ code, weight, active });
  }

  if (!norm.length){
    return json({ ok:false, error:'NO_VALID_ITEMS' }, 400, request);
  }

  const appPublicId = await resolveWheelAppPublicId(appId, env);
  if (!appPublicId){
    return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 404, request);
  }

  let updated = 0;

  for (const it of norm){
    const res = await env.DB.prepare(`
      UPDATE wheel_prizes
      SET weight = ?, active = ?
      WHERE app_public_id = ?
        AND code = ?
    `).bind(it.weight, it.active, appPublicId, it.code).run();

    if (res?.meta?.changes){
      updated += res.meta.changes;
    }
  }

  return json({ ok:true, updated }, 200, request);
}
