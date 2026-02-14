// src/routes/mini/state.ts
import { passportGetIssued } from "./passport";

/**
 * Вынесено из src/routes/mini.ts без изменения логики.
 * Экспортируем также утилиты, потому что mini.ts использует nowISO/getLastBalance и т.д.
 */

export function nowISO() {
  return new Date().toISOString();
}

export async function getLastBalance(db: any, appPublicId: string, tgId: any) {
  const row = await db
    .prepare(
      `SELECT balance_after
       FROM coins_ledger
       WHERE app_public_id = ? AND tg_id = ?
       ORDER BY id DESC LIMIT 1`
    )
    .bind(String(appPublicId), String(tgId))
    .first();

  return row ? Number((row as any).balance_after || 0) : 0;
}

export async function styleTitle(db: any, appPublicId: string, styleId: string) {
  const row = await db
    .prepare(`SELECT title FROM styles_dict WHERE app_public_id = ? AND style_id = ? LIMIT 1`)
    .bind(String(appPublicId), String(styleId))
    .first();

  return row && (row as any).title ? String((row as any).title) : String(styleId || "");
}

export async function stylesTotalCount(db: any, appPublicId: string) {
  const row = await db
    .prepare(`SELECT COUNT(DISTINCT style_id) as cnt FROM styles_dict WHERE app_public_id = ?`)
    .bind(String(appPublicId))
    .first();
  return row ? Number((row as any).cnt || 0) : 0;
}

export async function buildLeaderboard(db: any, appPublicId: string, dateStr: string, mode: string, topN: number) {
  const rows = await db
    .prepare(
      `SELECT tg_id, best_score
       FROM games_results_daily
       WHERE app_public_id = ? AND date = ? AND mode = ?
       ORDER BY best_score DESC
       LIMIT ?`
    )
    .bind(String(appPublicId), String(dateStr), String(mode), Number(topN || 10))
    .all();

  return (rows.results || []).map((r: any) => ({
    tg_id: String(r.tg_id || ""),
    best_score: Number(r.best_score || 0),
  }));
}

export async function buildLeaderboardAllTime(db: any, appPublicId: string, mode: string, topN: number) {
  const rows = await db
    .prepare(
      `SELECT tg_id, best_score
       FROM games_results_alltime
       WHERE app_public_id = ? AND mode = ?
       ORDER BY best_score DESC
       LIMIT ?`
    )
    .bind(String(appPublicId), String(mode), Number(topN || 10))
    .all();

  return (rows.results || []).map((r: any) => ({
    tg_id: String(r.tg_id || ""),
    best_score: Number(r.best_score || 0),
  }));
}

export async function refsTotal(db: any, appPublicId: string, referrerTgId: any) {
  const row = await db
    .prepare(
      `SELECT COUNT(1) as cnt
       FROM referrals
       WHERE app_public_id = ? AND referrer_tg_id = ?`
    )
    .bind(String(appPublicId), String(referrerTgId))
    .first();

  return row ? Number((row as any).cnt || 0) : 0;
}

export async function buildState(db: any, appId: any, appPublicId: string, tgId: any, cfg: any = {}) {
  const out: any = {
    bot_username: "",
    coins: 0,
    last_prizes: [],
    styles: [],
    styles_user: [],
    styles_count: 0,
    styles_total: 0,
    last_stamp_id: "",
    last_stamp_name: "",
    game_today_best: 0,
    game_plays_today: 0,
    leaderboard_today: [],
    leaderboard_alltime: [],
    config: {},
    wheel: { claim_cooldown_left_ms: 0, has_unclaimed: false, last_prize_code: "", last_prize_title: "" },
    ref_total: 0,
    passport_reward: null,
  };

  // bot username for referral links (active bot)
  try {
    const pid = String(appPublicId || "").trim();
    const b = await db
      .prepare(
        `SELECT username
         FROM bots
         WHERE app_public_id = ? AND status = 'active'
         ORDER BY id DESC
         LIMIT 1`
      )
      .bind(pid)
      .first();
    out.bot_username = b && (b as any).username ? String((b as any).username).replace(/^@/, "").trim() : "";
  } catch (e) {
    console.log("[ref] bot username lookup error", e);
    out.bot_username = "";
  }

  // coins
  out.coins = await getLastBalance(db, appPublicId, tgId);
  if (!out.coins) {
    const u = await db
      .prepare(`SELECT coins FROM app_users WHERE app_public_id = ? AND tg_user_id = ?`)
      .bind(appPublicId, String(tgId))
      .first();
    out.coins = u ? Number((u as any).coins || 0) : 0;
  }

  // last prizes (10) из bonus_claims
  const lp = await db
    .prepare(
      `SELECT prize_id, prize_name, prize_value, ts
       FROM bonus_claims
       WHERE app_public_id = ? AND tg_id = ?
       AND (claim_status IS NULL OR claim_status = 'ok')
       ORDER BY id DESC LIMIT 10`
    )
    .bind(appPublicId, String(tgId))
    .all();

  out.last_prizes = (lp.results || []).map((r: any) => ({
    prize_id: r.prize_id || "",
    prize_name: r.prize_name || "",
    prize_value: Number(r.prize_value || 0),
    ts: r.ts || nowISO(),
  }));

  // styles_user
  const su = await db
    .prepare(
      `SELECT style_id, status, ts
       FROM styles_user
       WHERE app_public_id = ? AND tg_id = ? AND status = 'collected'
       ORDER BY ts DESC`
    )
    .bind(appPublicId, String(tgId))
    .all();

  out.styles_user = (su.results || []).map((r: any) => ({
    style_id: String(r.style_id || ""),
    status: String(r.status || "collected"),
    ts: r.ts || "",
  }));

  let lastTs = 0,
    lastSid = "";
  const seen = new Set<string>();

  for (const r of su.results || []) {
    const sid = String((r as any).style_id || "");
    if (sid) seen.add(sid);

    const tms = (r as any).ts ? Date.parse((r as any).ts) || 0 : 0;
    if (sid && tms > lastTs) {
      lastTs = tms;
      lastSid = sid;
    }
  }

  out.styles = Array.from(seen);
  out.last_stamp_id = lastSid;
  out.last_stamp_name = lastSid ? await styleTitle(db, appPublicId, lastSid) : "";
  out.styles_count = out.styles.length;
  out.styles_total = await stylesTotalCount(db, appPublicId);

  // passport reward snapshot (if issued)
  try {
    const rw = await passportGetIssued(db, appPublicId, tgId, "default");
    out.passport_reward = rw
      ? {
          prize_code: String((rw as any).prize_code || ""),
          prize_title: String((rw as any).prize_title || ""),
          coins: Number((rw as any).coins || 0),
          redeem_code: String((rw as any).redeem_code || ""),
          status: String((rw as any).status || "issued"),
          issued_at: (rw as any).issued_at || "",
        }
      : null;
  } catch (_) {
    out.passport_reward = null;
  }

  // game snapshot today
  const today = new Date().toISOString().slice(0, 10);
  const mode = "daily";
  const g = await db
    .prepare(
      `SELECT best_score, plays FROM games_results_daily
       WHERE app_public_id = ? AND date = ? AND mode = ? AND tg_id = ?
       ORDER BY id DESC LIMIT 1`
    )
    .bind(appPublicId, today, mode, String(tgId))
    .first();

  if (g) {
    out.game_today_best = Number((g as any).best_score || 0);
    out.game_plays_today = Number((g as any).plays || 0);
  }

  const topN = Number(cfg?.LEADERBOARD_TOP_N || 10) || 10;
  out.leaderboard_today = await buildLeaderboard(db, appPublicId, today, mode, topN);
  out.leaderboard_alltime = await buildLeaderboardAllTime(db, appPublicId, mode, topN);

  // config snapshot
  const cdH = Number(cfg?.WHEEL_CLAIM_COOLDOWN_H || 24);
  out.config = {
    SPIN_COST: Number(cfg?.SPIN_COST || 0),
    SPIN_COOLDOWN_SEC: Number(cfg?.SPIN_COOLDOWN_SEC || 0),
    SPIN_DAILY_LIMIT: Number(cfg?.SPIN_DAILY_LIMIT || 0),
    QUIZ_COINS_PER_CORRECT: Number(cfg?.QUIZ_COINS_PER_CORRECT || 0),
    QUIZ_COINS_MAX_PER_SUBMIT: Number(cfg?.QUIZ_COINS_MAX_PER_SUBMIT || 0),
    STYLE_COLLECT_COINS: Number(cfg?.STYLE_COLLECT_COINS || 0),
    LEADERBOARD_TOP_N: topN,
    WHEEL_SPIN_COST: Number(cfg?.WHEEL_SPIN_COST || 0),
    WHEEL_CLAIM_COOLDOWN_H: cdH,
  };

  // wheel: last won?
  out.wheel.claim_cooldown_left_ms = 0;

  const lastWon = await db
    .prepare(
      `SELECT id, prize_code, prize_title
       FROM wheel_spins
       WHERE app_public_id = ? AND tg_id = ? AND status = 'won'
       ORDER BY id DESC LIMIT 1`
    )
    .bind(appPublicId, String(tgId))
    .first();

  if (lastWon) {
    out.wheel.has_unclaimed = true;
    out.wheel.last_prize_code = (lastWon as any).prize_code || "";
    out.wheel.last_prize_title = (lastWon as any).prize_title || "";

    const rr = await db
      .prepare(
        `SELECT redeem_code, status, issued_at, redeemed_at
         FROM wheel_redeems
         WHERE app_public_id = ? AND spin_id = ?
         LIMIT 1`
      )
      .bind(appPublicId, Number((lastWon as any).id))
      .first();

    if (rr) {
      out.wheel.redeem_code = (rr as any).redeem_code || "";
      out.wheel.redeem_status = (rr as any).status || "issued";
      out.wheel.redeem_issued_at = (rr as any).issued_at || "";
      out.wheel.redeem_redeemed_at = (rr as any).redeemed_at || "";
    } else {
      out.wheel.redeem_code = "";
      out.wheel.redeem_status = "";
    }
  } else {
    out.wheel.has_unclaimed = false;
    out.wheel.last_prize_code = "";
    out.wheel.last_prize_title = "";
    out.wheel.redeem_code = "";
    out.wheel.redeem_status = "";
  }

  out.ref_total = await refsTotal(db, appPublicId, tgId);

  return out;
}
