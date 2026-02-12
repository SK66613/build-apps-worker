// src/routes/mini.ts
import type { Env } from "../index";
import { json } from "../utils/http";
import { tgSendMessage } from "../services/telegramSend";
import { decryptToken } from "../services/crypto";
import { getCanonicalPublicIdForApp } from "../services/apps";

/**
 * ВАЖНОЕ исправление №1:
 *  - правильный парсинг /api/mini/<publicId>/<type>
 *  - больше НЕ используем .pop() для publicId (иначе publicId становился "state"/"spin" и т.п.)
 *
 * ВАЖНОЕ исправление №2:
 *  - type тоже берём из 4-го сегмента (или из body/query) — без .pop() “как попало”
 */

function safeJson(obj: any, maxLen = 8000) {
  try {
    const s = JSON.stringify(obj);
    return s.length > maxLen ? s.slice(0, maxLen) : s;
  } catch (_) {
    return null;
  }
}

function parseInitDataUser(initData: string) {
  try {
    const p = new URLSearchParams(initData || "");
    const userRaw = p.get("user");
    if (!userRaw) return null;
    const u = JSON.parse(userRaw);
    if (!u || !u.id) return null;
    return u;
  } catch (_) {
    return null;
  }
}

async function verifyInitDataSignature(initData: string, botToken: string) {
  if (!initData || !botToken) return false;

  const params = new URLSearchParams(initData);
  const hash = params.get("hash");
  if (!hash) return false;
  params.delete("hash");

  const arr: string[] = [];
  for (const [k, v] of params.entries()) arr.push(`${k}=${v}`);
  arr.sort();
  const dataCheckString = arr.join("\n");

  const enc = new TextEncoder();

  // secret_key = HMAC_SHA256(key="WebAppData", data=bot_token)
  const webAppKey = await crypto.subtle.importKey(
    "raw",
    enc.encode("WebAppData"),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const secretKeyBuf = await crypto.subtle.sign("HMAC", webAppKey, enc.encode(botToken));

  // calc_hash = HMAC_SHA256(key=secret_key, data=data_check_string)
  const secretKey = await crypto.subtle.importKey(
    "raw",
    secretKeyBuf,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const sig = await crypto.subtle.sign("HMAC", secretKey, enc.encode(dataCheckString));

  const calcHash = Array.from(new Uint8Array(sig))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  return calcHash === hash.toLowerCase();
}

async function resolveAppContextByPublicId(publicId: string, env: Env) {
  const map = await env.APPS.get("app:by_public:" + publicId, "json");
  if (!map || !(map as any).appId) return { ok: false, status: 404, error: "UNKNOWN_PUBLIC_ID" as const };
  const appId = (map as any).appId;
  const canonicalPublicId = (await getCanonicalPublicIdForApp(appId, env)) || publicId;
  return { ok: true, appId, publicId: canonicalPublicId };
}

async function getBotTokenForApp(publicId: string, env: Env, appIdFallback: any = null) {
  if (!env.BOT_SECRETS || !env.BOT_TOKEN_KEY) return null;

  const tryGet = async (key: string) => {
    const raw = await env.BOT_SECRETS.get(key);
    if (!raw) return null;

    let cipher = raw;
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object" && parsed.cipher) cipher = parsed.cipher;
    } catch (_) {}

    try {
      return await decryptToken(cipher, env.BOT_TOKEN_KEY);
    } catch (e) {
      console.error("[botToken] decrypt error for key", key, e);
      return null;
    }
  };

  const tok1 = await tryGet("bot_token:public:" + publicId);
  if (tok1) return tok1;

  if (appIdFallback) {
    const tok2 = await tryGet("bot_token:app:" + appIdFallback);
    if (tok2) return tok2;
  }

  return null;
}

async function requireTgAndVerify(publicId: string, initDataRaw: string | null, env: Env) {
  const ctx = await resolveAppContextByPublicId(publicId, env);
  if (!(ctx as any).ok) return ctx as any;

  const botToken = await getBotTokenForApp((ctx as any).publicId, env, (ctx as any).appId);
  // Если токен есть — включаем строгую проверку initData
  if (botToken) {
    if (!initDataRaw) return { ok: false, status: 403, error: "NO_INIT_DATA" as const };
    const ok = await verifyInitDataSignature(initDataRaw, botToken);
    if (!ok) return { ok: false, status: 403, error: "BAD_SIGNATURE" as const };
  }

  return { ok: true, ...(ctx as any) };
}

async function upsertAppUser(db: any, appId: any, appPublicId: string, tg: any) {
  const tgId = String(tg.id);
  const row = await db
    .prepare(`SELECT id, coins FROM app_users WHERE app_public_id = ? AND tg_user_id = ? LIMIT 1`)
    .bind(appPublicId, tgId)
    .first();

  if (!row) {
    const ins = await db
      .prepare(
        `INSERT INTO app_users (app_id, app_public_id, tg_user_id, tg_username, first_seen, last_seen, coins)
         VALUES (?, ?, ?, ?, datetime('now'), datetime('now'), 0)`
      )
      .bind(appId, appPublicId, tgId, tg.username || null)
      .run();
    return { id: Number(ins.lastInsertRowid), coins: 0 };
  } else {
    await db
      .prepare(`UPDATE app_users SET tg_username = ?, last_seen = datetime('now') WHERE id = ?`)
      .bind(tg.username || null, (row as any).id)
      .run();
    return { id: (row as any).id, coins: Number((row as any).coins || 0) };
  }
}

async function setUserCoins(db: any, appPublicId: string, tgId: any, coins: number) {
  await db
    .prepare(`UPDATE app_users SET coins = ? WHERE app_public_id = ? AND tg_user_id = ?`)
    .bind(Number(coins || 0), String(appPublicId), String(tgId))
    .run();
}

// ================== STATE HELPERS ==================
function nowISO() {
  return new Date().toISOString();
}

async function getLastBalance(db: any, appPublicId: string, tgId: any) {
  const row = await db
    .prepare(
      `SELECT balance_after FROM coins_ledger
       WHERE app_public_id = ? AND tg_id = ?
       ORDER BY id DESC LIMIT 1`
    )
    .bind(appPublicId, String(tgId))
    .first();
  return row ? Number((row as any).balance_after || 0) : 0;
}

// ================== STYLES HELPERS (needed by buildState) ==================
async function styleTitle(db: any, appPublicId: string, styleId: string) {
  const row = await db
    .prepare(
      `SELECT title
       FROM styles_dict
       WHERE app_public_id = ? AND style_id = ?
       LIMIT 1`
    )
    .bind(String(appPublicId), String(styleId || ""))
    .first();
  return row ? String((row as any).title || "") : "";
}

async function stylesTotalCount(db: any, appPublicId: string) {
  const row = await db
    .prepare(`SELECT COUNT(DISTINCT style_id) as cnt FROM styles_dict WHERE app_public_id = ?`)
    .bind(appPublicId)
    .first();
  return row ? Number((row as any).cnt || 0) : 0;
}

async function passportCollectedCount(db: any, appPublicId: string, tgId: any) {
  const row = await db
    .prepare(
      `SELECT COUNT(DISTINCT style_id) as cnt
       FROM styles_user
       WHERE app_public_id = ? AND tg_id = ? AND status='collected'`
    )
    .bind(String(appPublicId), String(tgId))
    .first();
  return row ? Number((row as any).cnt || 0) : 0;
}

// ================== COINS ==================
async function awardCoins(
  db: any,
  appId: any,
  appPublicId: string,
  tgId: any,
  delta: any,
  src: any,
  ref_id: any,
  note: any,
  event_id: any
) {
  if (event_id) {
    const ex = await db.prepare(`SELECT balance_after FROM coins_ledger WHERE event_id = ? LIMIT 1`).bind(event_id).first();
    if (ex) return { ok: true, reused: true, balance: Number((ex as any).balance_after || 0) };
  }

  const last = await getLastBalance(db, appPublicId, tgId);
  const bal = Math.max(0, last + Number(delta || 0));

  await db
    .prepare(
      `INSERT INTO coins_ledger (app_id, app_public_id, tg_id, event_id, src, ref_id, delta, balance_after, note)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      appId,
      appPublicId,
      String(tgId),
      event_id || null,
      String(src || ""),
      String(ref_id || ""),
      Number(delta || 0),
      bal,
      String(note || "")
    )
    .run();

  await setUserCoins(db, appPublicId, tgId, bal);
  return { ok: true, balance: bal };
}

async function spendCoinsIfEnough(
  db: any,
  appId: any,
  appPublicId: string,
  tgId: any,
  cost: any,
  src: any,
  ref_id: any,
  note: any,
  event_id: any
) {
  cost = Math.max(0, Math.floor(Number(cost || 0)));
  if (cost <= 0) return { ok: true, spent: 0, balance: await getLastBalance(db, appPublicId, tgId) };

  const last = await getLastBalance(db, appPublicId, tgId);
  if (last < cost) return { ok: false, error: "NOT_ENOUGH_COINS", have: last, need: cost };

  const res = await awardCoins(db, appId, appPublicId, tgId, -cost, src, ref_id, note, event_id);
  return { ok: true, spent: cost, balance: (res as any).balance };
}

// ================== REFERRALS ==================
async function bindReferralOnce(db: any, appPublicId: string, inviteeTgId: any, referrerTgId: any) {
  const a = String(appPublicId || "");
  const invitee = String(inviteeTgId || "");
  const ref = String(referrerTgId || "").trim();

  if (!a || !invitee || !ref) return { ok: false, skipped: true, reason: "empty" };
  if (ref === invitee) return { ok: false, skipped: true, reason: "self" };

  const ex = await db.prepare(`SELECT id FROM referrals WHERE app_public_id=? AND invitee_tg_id=? LIMIT 1`).bind(a, invitee).first();
  if (ex) return { ok: true, skipped: true, reason: "already_bound" };

  await db
    .prepare(
      `INSERT INTO referrals (app_public_id, referrer_tg_id, invitee_tg_id, confirmed, created_at)
       VALUES (?, ?, ?, 1, datetime('now'))`
    )
    .bind(a, ref, invitee)
    .run();

  return { ok: true, bound: true };
}

// ================== PINS ==================
async function useOneTimePin(db: any, appPublicId: string, tgId: any, pin: any, styleId: any) {
  const row = await db
    .prepare(
      `SELECT id, used_at, target_tg_id, style_id
       FROM pins_pool
       WHERE app_public_id = ? AND pin = ?
       LIMIT 1`
    )
    .bind(String(appPublicId), String(pin || ""))
    .first();

  if (!row) return { ok: false, error: "pin_invalid" };
  if ((row as any).used_at) return { ok: false, error: "pin_used" };

  if (String((row as any).target_tg_id || "") !== String(tgId)) return { ok: false, error: "pin_invalid" };
  if (styleId && String((row as any).style_id || "") !== String(styleId)) return { ok: false, error: "pin_invalid" };

  await db
    .prepare(
      `UPDATE pins_pool
       SET used_at = datetime('now')
       WHERE id = ? AND used_at IS NULL`
    )
    .bind(Number((row as any).id))
    .run();

  return { ok: true };
}

// ================== PASSPORT REWARD ==================
async function passportGetIssued(db: any, appPublicId: string, tgId: any, passportKey: string) {
  return await db
    .prepare(
      `SELECT id, prize_code, prize_title, coins, redeem_code, status, issued_at
       FROM passport_rewards
       WHERE app_public_id = ? AND tg_id = ? AND passport_key = ?
       ORDER BY id DESC
       LIMIT 1`
    )
    .bind(appPublicId, String(tgId), String(passportKey || "default"))
    .first();
}

function randomRedeemCode(len = 10) {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  let s = "";
  for (let i = 0; i < len; i++) s += alphabet[bytes[i] % alphabet.length];
  return "SG-" + s.slice(0, 4) + "-" + s.slice(4, 8) + (len > 8 ? "-" + s.slice(8) : "");
}

async function passportIssueRewardIfCompleted(db: any, env: Env, ctx: any, tgId: any, cfg: any) {
  const passportKey = String(cfg?.passport?.passport_key || "default");
  const prizeCode = String(cfg?.passport?.reward_prize_code || "").trim();
  if (!prizeCode) return { ok: true, skipped: true, reason: "NO_REWARD_PRIZE_CODE" };

  const total = await stylesTotalCount(db, ctx.publicId);
  if (!total) return { ok: true, skipped: true, reason: "NO_STYLES_TOTAL" };

  const got = await passportCollectedCount(db, ctx.publicId, tgId);
  if (got < total) return { ok: true, skipped: true, reason: "NOT_COMPLETED", got, total };

  const ex = await db
    .prepare(
      `SELECT id, prize_code, prize_title, coins, redeem_code, status
       FROM passport_rewards
       WHERE app_public_id=? AND tg_id=? AND passport_key=?
       ORDER BY id DESC
       LIMIT 1`
    )
    .bind(ctx.publicId, String(tgId), passportKey)
    .first();

  if (ex && String((ex as any).status) === "issued") {
    return { ok: true, issued: true, reused: true, reward: ex, got, total };
  }

  const pr = await db
    .prepare(`SELECT code, title, coins FROM wheel_prizes WHERE app_public_id = ? AND code = ? LIMIT 1`)
    .bind(ctx.publicId, prizeCode)
    .first();

  if (!pr) return { ok: false, error: "REWARD_PRIZE_NOT_FOUND", prize_code: prizeCode };

  const prizeTitle = String((pr as any).title || prizeCode);
  const prizeCoins = Math.max(0, Math.floor(Number((pr as any).coins || 0)));

  const botToken = await getBotTokenForApp(ctx.publicId, env, ctx.appId).catch(() => null);

  let redeemCode = "";
  for (let i = 0; i < 8; i++) {
    redeemCode = randomRedeemCode(10);
    try {
      if (ex && (ex as any).id) {
        await db
          .prepare(
            `UPDATE passport_rewards
             SET status='issued',
                 issued_at=datetime('now'),
                 redeemed_at=NULL,
                 redeemed_by_tg=NULL,
                 prize_code=?,
                 prize_title=?,
                 coins=?,
                 redeem_code=?
             WHERE id=?`
          )
          .bind(prizeCode, prizeTitle, prizeCoins, redeemCode, Number((ex as any).id))
          .run();
      } else {
        await db
          .prepare(
            `INSERT INTO passport_rewards
             (app_id, app_public_id, tg_id, passport_key, prize_code, prize_title, coins, redeem_code, status, issued_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'issued', datetime('now'))`
          )
          .bind(ctx.appId, ctx.publicId, String(tgId), passportKey, prizeCode, prizeTitle, prizeCoins, redeemCode)
          .run();
      }
      break;
    } catch (e: any) {
      const msg = String(e?.message || e);
      if (/unique|constraint/i.test(msg)) continue;
      throw e;
    }
  }
  if (!redeemCode) return { ok: false, error: "PASSPORT_REDEEM_CREATE_FAILED" };

  try {
    await db
      .prepare(
        `INSERT INTO bonus_claims (app_id, app_public_id, tg_id, prize_id, prize_name, prize_value, claim_status)
         VALUES (?, ?, ?, ?, ?, ?, 'pending')`
      )
      .bind(ctx.appId, ctx.publicId, String(tgId), prizeCode, prizeTitle, prizeCoins)
      .run();
  } catch (_) {}

  let botUsername = "";
  try {
    const b = await db
      .prepare(
        `SELECT username FROM bots
         WHERE app_public_id = ? AND status='active'
         ORDER BY id DESC LIMIT 1`
      )
      .bind(ctx.publicId)
      .first();
    botUsername = b && (b as any).username ? String((b as any).username).replace(/^@/, "").trim() : "";
  } catch (_) {
    botUsername = "";
  }

  const deepLink = botUsername ? `https://t.me/${botUsername}?start=redeem_${encodeURIComponent(redeemCode)}` : "";

  try {
    if (botToken) {
      const lines = [
        `🏁 Паспорт заполнен!`,
        `🎁 Ваш приз: <b>${prizeTitle}</b>`,
        prizeCoins > 0 ? `🪙 Монеты: <b>${prizeCoins}</b> (после подтверждения кассиром)` : "",
        ``,
        `✅ Код выдачи: <code>${redeemCode}</code>`,
        deepLink ? `Откройте ссылку:\n${deepLink}` : `Покажите код кассиру.`,
      ].filter(Boolean);

      await tgSendMessage(env, botToken, String(tgId), lines.join("\n"), {}, { appPublicId: ctx.publicId, tgUserId: String(tgId) });
    }
  } catch (e) {
    console.error("[passport.reward] tgSendMessage redeem failed", e);
  }

  return {
    ok: true,
    issued: true,
    reward: { prize_code: prizeCode, prize_title: prizeTitle, coins: prizeCoins, redeem_code: redeemCode },
    got,
    total,
  };
}

// ================== WHEEL ==================
async function pickWheelPrize(db: any, appPublicId: string) {
  const rows = await db
    .prepare(`SELECT code, title, weight, coins, active, img FROM wheel_prizes WHERE app_public_id = ?`)
    .bind(appPublicId)
    .all();

  const list = (rows.results || [])
    .filter((r: any) => Number(r.active || 0) && Number(r.weight || 0) > 0)
    .map((r: any) => ({
      code: String(r.code),
      title: String(r.title || r.code),
      weight: Number(r.weight),
      coins: Number(r.coins || 0),
      img: String((r as any).img || ""),
    }));

  if (!list.length) return null;

  const sum = list.reduce((a: number, b: any) => a + b.weight, 0);
  let rnd = Math.random() * sum;
  let acc = 0;
  for (const it of list) {
    acc += it.weight;
    if (rnd <= acc) return it;
  }
  return list[list.length - 1];
}

async function ensureWheelRedeem(
  db: any,
  ctx: any,
  tgId: any,
  spinId: number,
  prize: { code: string; title: string; coins: number }
) {
  // если уже есть redeem — вернём его
  const ex = await db
    .prepare(
      `SELECT id, redeem_code, status, issued_at, redeemed_at
       FROM wheel_redeems
       WHERE app_public_id=? AND spin_id=?
       LIMIT 1`
    )
    .bind(ctx.publicId, spinId)
    .first();

  if (ex) {
    return {
      redeem_code: String((ex as any).redeem_code || ""),
      status: String((ex as any).status || "issued"),
      issued_at: (ex as any).issued_at || "",
      redeemed_at: (ex as any).redeemed_at || "",
      reused: true,
    };
  }

  // создаём новый redeem
  let redeemCode = "";
  for (let i = 0; i < 8; i++) {
    redeemCode = randomRedeemCode(10);
    try {
      await db
        .prepare(
          `INSERT INTO wheel_redeems
           (app_id, app_public_id, tg_id, spin_id, prize_code, prize_title, coins, redeem_code, status, issued_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'issued', datetime('now'))`
        )
        .bind(ctx.appId, ctx.publicId, String(tgId), spinId, prize.code, prize.title, Number(prize.coins || 0), redeemCode)
        .run();
      break;
    } catch (e: any) {
      const msg = String(e?.message || e);
      if (/unique|constraint/i.test(msg)) continue;
      throw e;
    }
  }

  if (!redeemCode) throw new Error("WHEEL_REDEEM_CREATE_FAILED");
  return { redeem_code: redeemCode, status: "issued", issued_at: nowISO(), redeemed_at: "", reused: false };
}

// ================== QUIZ ==================
async function quizFinish(db: any, appId: any, appPublicId: string, tgId: any, data: any) {
  const quizId = String(data.quiz_id || "beer_profile_v1");
  const score = Number(data.score || 0);

  const profile = data.profile || {};
  const answersJson = data.answers_json || JSON.stringify(profile || {});
  const now = nowISO();

  await db
    .prepare(
      `INSERT INTO profile_quiz
       (app_id, app_public_id, tg_id, quiz_id, status, score,
        bday_day,bday_month, scene, evening_scene, beer_character, experiments, focus,
        anti_flavors, snacks, budget, time_of_day, comms, birthday_optin,
        answers_json, created_at, updated_at)
       VALUES (?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(app_public_id, tg_id, quiz_id) DO UPDATE SET
         status='completed', score=excluded.score,
         bday_day=excluded.bday_day, bday_month=excluded.bday_month,
         scene=excluded.scene, evening_scene=excluded.evening_scene,
         beer_character=excluded.beer_character, experiments=excluded.experiments, focus=excluded.focus,
         anti_flavors=excluded.anti_flavors, snacks=excluded.snacks, budget=excluded.budget,
         time_of_day=excluded.time_of_day, comms=excluded.comms, birthday_optin=excluded.birthday_optin,
         answers_json=excluded.answers_json, updated_at=excluded.updated_at`
    )
    .bind(
      appId,
      appPublicId,
      String(tgId),
      quizId,
      score,
      Number(data.bday_day || 0),
      Number(data.bday_month || 0),
      String(profile.scene || ""),
      String(profile.evening_scene || ""),
      String(profile.beer_character || ""),
      String(profile.experiments || ""),
      String(profile.focus || ""),
      String(profile.anti_flavors || ""),
      String(profile.snacks || ""),
      String(profile.budget || ""),
      String(profile.time_of_day || ""),
      String(profile.comms || ""),
      String(profile.birthday_optin || ""),
      String(answersJson || ""),
      now,
      now
    )
    .run();

  if (score > 0) {
    await awardCoins(db, appId, appPublicId, tgId, score, "profile_quiz", quizId, "profile quiz reward", null);
  }

  const fresh = await buildState(db, appId, appPublicId, tgId, {});
  return { ok: true, status: "completed", score, fresh_state: fresh };
}

// ================== LEADERBOARD / REFS ==================
async function buildLeaderboard(db: any, appPublicId: string, dateStr: string, mode: string, topN: number) {
  const rows = await db
    .prepare(
      `SELECT gr.tg_id, gr.best_score, au.tg_username
       FROM games_results_daily gr
       LEFT JOIN app_users au ON au.app_public_id = gr.app_public_id AND au.tg_user_id = gr.tg_id
       WHERE gr.app_public_id = ? AND gr.date = ? AND gr.mode = ?
       GROUP BY gr.tg_id
       ORDER BY gr.best_score DESC
       LIMIT ?`
    )
    .bind(appPublicId, dateStr, mode, topN)
    .all();

  return (rows.results || []).map((r: any) => ({
    tg_id: String(r.tg_id),
    username: r.tg_username || "",
    first_name: "",
    last_name: "",
    score: Number(r.best_score || 0),
  }));
}

async function buildLeaderboardAllTime(db: any, appPublicId: string, mode: string, topN: number) {
  const rows = await db
    .prepare(
      `SELECT gr.tg_id, MAX(gr.best_score) as best_score, au.tg_username
       FROM games_results_daily gr
       LEFT JOIN app_users au ON au.app_public_id = gr.app_public_id AND au.tg_user_id = gr.tg_id
       WHERE gr.app_public_id = ? AND gr.mode = ?
       GROUP BY gr.tg_id
       ORDER BY best_score DESC
       LIMIT ?`
    )
    .bind(appPublicId, mode, topN)
    .all();

  return (rows.results || []).map((r: any) => ({
    tg_id: String(r.tg_id),
    username: r.tg_username || "",
    first_name: "",
    last_name: "",
    score: Number(r.best_score || 0),
  }));
}

async function refsTotal(db: any, appPublicId: string, referrerTgId: any) {
  const r = await db
    .prepare(`SELECT COUNT(1) AS c FROM referrals WHERE app_public_id=? AND referrer_tg_id=?`)
    .bind(String(appPublicId), String(referrerTgId))
    .first();
  return Number((r as any)?.c || 0);
}

// ================== BUILD STATE ==================
async function buildState(db: any, appId: any, appPublicId: string, tgId: any, cfg: any = {}) {
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

  // bot username (active bot)
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
    const u = await db.prepare(`SELECT coins FROM app_users WHERE app_public_id = ? AND tg_user_id = ?`).bind(appPublicId, String(tgId)).first();
    out.coins = u ? Number((u as any).coins || 0) : 0;
  }

  // last prizes (10) из bonus_claims
  const lp = await db
    .prepare(
      `SELECT prize_id, prize_name, prize_value, ts
       FROM bonus_claims
       WHERE app_public_id = ? AND tg_id = ?
       AND (claim_status IS NULL OR claim_status = 'ok' OR claim_status = 'pending')
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

  let lastTs = 0;
  let lastSid = "";
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
    WHEEL_SPIN_COST: Number(cfg?.WHEEL_SPIN_COST ?? cfg?.SPIN_COST ?? 0),
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

// ================== MINI API ==================
function parseMiniPath(url: URL) {
  const parts = (url.pathname || "").split("/").filter(Boolean);
  // ожидаем: /api/mini/<publicId>/<type>
  const isMini = parts[0] === "api" && parts[1] === "mini";
  const pathPublicId = isMini && parts[2] ? String(parts[2]) : "";
  const pathType = isMini && parts[3] ? String(parts[3]) : "";
  return { pathPublicId, pathType };
}

async function handleMiniApi(request: Request, env: Env, url: URL) {
  const db: any = env.DB;

  // читаем JSON
  let body: any = {};
  try {
    body = await request.json();
  } catch (_) {}

  const { pathPublicId, pathType } = parseMiniPath(url);

  // ✅ FIX: publicId НЕ через .pop()
  const publicId =
    String(url.searchParams.get("public_id") || "") ||
    String(body.public_id || body.publicId || "") ||
    pathPublicId ||
    "";

  // ✅ FIX: type НЕ через .pop()
  let type =
    String(body.type || "") ||
    String(url.searchParams.get("type") || "") ||
    pathType ||
    "";

  // совместимость алиасов
  if (type === "claim") type = "claim_prize";
  if (type === "quiz") type = "quiz_state";

  const initDataRaw = body.init_data || body.initData || null;
  const tg = body.tg_user || {};

  if (!publicId) return json({ ok: false, error: "NO_PUBLIC_ID" }, 400, request);
  if (!tg || !tg.id) return json({ ok: false, error: "NO_TG_USER_ID" }, 400, request);

  const ctx = await requireTgAndVerify(publicId, initDataRaw, env);
  if (!(ctx as any).ok) return json({ ok: false, error: (ctx as any).error || "AUTH_FAILED" }, (ctx as any).status || 403, request);

  await upsertAppUser(db, (ctx as any).appId, (ctx as any).publicId, tg);

  const payload = body.payload || {};

  // ====== state
  if (type === "state") {
    const appObj = await env.APPS.get("app:" + (ctx as any).appId, "json").catch(() => null);
    const cfg =
      (appObj as any) &&
      ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        ? ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        : {};

    // referral start_param
    let startParam = "";
    try {
      const p = new URLSearchParams(String(initDataRaw || ""));
      startParam = String(p.get("start_param") || "");
    } catch (_) {}

    if (startParam.startsWith("ref_")) {
      const refTgId = startParam.slice(4).trim();
      await bindReferralOnce(db, (ctx as any).publicId, String(tg.id), refTgId);
    }

    const state = await buildState(db, (ctx as any).appId, (ctx as any).publicId, tg.id, cfg);
    return json({ ok: true, state }, 200, request);
  }

  // ====== quiz_finish
  if (type === "quiz_finish") {
    const res = await quizFinish(db, (ctx as any).appId, (ctx as any).publicId, tg.id, payload || {});
    return json(res, 200, request);
  }

  // ====== wheel.spin
  if (type === "wheel.spin" || type === "wheel_spin" || type === "spin") {
    const appObj = await env.APPS.get("app:" + (ctx as any).appId, "json").catch(() => null);
    const cfg =
      (appObj as any) &&
      ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        ? ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        : {};

    const cost = Number(cfg?.WHEEL_SPIN_COST ?? cfg?.SPIN_COST ?? 0);
    const spend = await spendCoinsIfEnough(
      db,
      (ctx as any).appId,
      (ctx as any).publicId,
      tg.id,
      cost,
      "wheel_spin",
      "",
      "wheel spin cost",
      `wheel:spin:${(ctx as any).publicId}:${tg.id}:${Date.now()}:${cost}`
    );

    if (!(spend as any).ok) return json(spend, 400, request);

    const prize = await pickWheelPrize(db, (ctx as any).publicId);
    if (!prize) return json({ ok: false, error: "NO_WHEEL_PRIZES" }, 400, request);

    const ins = await db
      .prepare(
        `INSERT INTO wheel_spins (app_id, app_public_id, tg_id, status, prize_code, prize_title, coins, ts_spin)
         VALUES (?, ?, ?, 'won', ?, ?, ?, datetime('now'))`
      )
      .bind((ctx as any).appId, (ctx as any).publicId, String(tg.id), prize.code, prize.title, Number(prize.coins || 0))
      .run();

    const spinId = Number((ins as any).lastInsertRowid || 0);

    // бонус-лог (pending, если монеты/приз подтверждаются позже)
    try {
      await db
        .prepare(
          `INSERT INTO bonus_claims (app_id, app_public_id, tg_id, prize_id, prize_name, prize_value, claim_status)
           VALUES (?, ?, ?, ?, ?, ?, 'pending')`
        )
        .bind((ctx as any).appId, (ctx as any).publicId, String(tg.id), prize.code, prize.title, Number(prize.coins || 0))
        .run();
    } catch (_) {}

    const fresh = await buildState(db, (ctx as any).appId, (ctx as any).publicId, tg.id, cfg);
    return json({ ok: true, spin_id: spinId, prize, balance: (spend as any).balance, fresh_state: fresh }, 200, request);
  }

  // ====== wheel.claim_prize (создать redeem-код/ссылку для кассира)
  if (type === "claim_prize" || type === "wheel.claim" || type === "wheel_claim") {
    const appObj = await env.APPS.get("app:" + (ctx as any).appId, "json").catch(() => null);
    const cfg =
      (appObj as any) &&
      ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        ? ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        : {};

    const lastWon = await db
      .prepare(
        `SELECT id, prize_code, prize_title, coins
         FROM wheel_spins
         WHERE app_public_id = ? AND tg_id = ? AND status='won'
         ORDER BY id DESC LIMIT 1`
      )
      .bind((ctx as any).publicId, String(tg.id))
      .first();

    if (!lastWon) return json({ ok: false, error: "NO_UNCLAIMED_PRIZE" }, 400, request);

    const spinId = Number((lastWon as any).id);
    const prize = {
      code: String((lastWon as any).prize_code || ""),
      title: String((lastWon as any).prize_title || ""),
      coins: Math.max(0, Math.floor(Number((lastWon as any).coins || 0))),
    };

    const rr = await ensureWheelRedeem(db, ctx, tg.id, spinId, prize);

    // (опционально) отправим сообщение в бота с кодом/ссылкой
    try {
      const botToken = await getBotTokenForApp((ctx as any).publicId, env, (ctx as any).appId).catch(() => null);
      if (botToken) {
        let botUsername = "";
        try {
          const b = await db
            .prepare(
              `SELECT username FROM bots
               WHERE app_public_id = ? AND status='active'
               ORDER BY id DESC LIMIT 1`
            )
            .bind((ctx as any).publicId)
            .first();
          botUsername = b && (b as any).username ? String((b as any).username).replace(/^@/, "").trim() : "";
        } catch (_) {}

        const deepLink = botUsername ? `https://t.me/${botUsername}?start=redeem_${encodeURIComponent(rr.redeem_code)}` : "";

        const lines = [
          `🎁 Приз: <b>${prize.title}</b>`,
          prize.coins > 0 ? `🪙 Монеты: <b>${prize.coins}</b> (после подтверждения кассиром)` : "",
          ``,
          `✅ Код выдачи: <code>${rr.redeem_code}</code>`,
          deepLink ? `Откройте ссылку:\n${deepLink}` : `Покажите код кассиру.`,
        ].filter(Boolean);

        await tgSendMessage(env, botToken, String(tg.id), lines.join("\n"), {}, { appPublicId: (ctx as any).publicId, tgUserId: String(tg.id) });
      }
    } catch (e) {
      console.error("[wheel.claim] tgSendMessage failed", e);
    }

    const fresh = await buildState(db, (ctx as any).appId, (ctx as any).publicId, tg.id, cfg);
    return json(
      {
        ok: true,
        spin_id: spinId,
        prize,
        redeem: { redeem_code: rr.redeem_code, status: rr.status, issued_at: rr.issued_at },
        fresh_state: fresh,
      },
      200,
      request
    );
  }

  // ====== style collect
  if (type === "style.collect" || type === "style_collect") {
    const styleId = String((payload && (payload.style_id || payload.styleId || payload.code)) || "").trim();
    const pin = String((payload && payload.pin) || "").trim();
    if (!styleId) return json({ ok: false, error: "NO_STYLE_ID" }, 400, request);

    const appObj = await env.APPS.get("app:" + (ctx as any).appId, "json").catch(() => null);
    const cfg =
      (appObj as any) &&
      ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        ? ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        : {};

    const requirePin = !!(cfg && cfg.passport && cfg.passport.require_pin);

    if (requirePin) {
      const pres = await useOneTimePin(db, (ctx as any).publicId, tg.id, pin, styleId);
      if (!pres || !(pres as any).ok) return json(pres || { ok: false, error: "pin_invalid" }, 400, request);
    }

    const up = await db
      .prepare(
        `UPDATE styles_user
         SET status='collected', ts=datetime('now')
         WHERE app_public_id=? AND tg_id=? AND style_id=?`
      )
      .bind((ctx as any).publicId, String(tg.id), styleId)
      .run();

    if (!up || !up.meta || !up.meta.changes) {
      await db
        .prepare(
          `INSERT INTO styles_user (app_id, app_public_id, tg_id, style_id, status, ts)
           VALUES (?, ?, ?, ?, 'collected', datetime('now'))`
        )
        .bind((ctx as any).appId, (ctx as any).publicId, String(tg.id), styleId)
        .run();
    }

    try {
      await passportIssueRewardIfCompleted(db, env, ctx, tg.id, cfg);
    } catch (e) {
      console.error("[passport.reward] failed", e);
    }

    const fresh = await buildState(db, (ctx as any).appId, (ctx as any).publicId, tg.id, cfg);
    return json({ ok: true, style_id: styleId, fresh_state: fresh }, 200, request);
  }

  // ====== pin_use
  if (type === "pin_use") {
    const { pin, style_id } = payload || {};
    const res = await useOneTimePin(db, (ctx as any).publicId, tg.id, pin, style_id);
    return json(res, (res as any).ok ? 200 : 400, request);
  }

  // ====== calendar.free_slots (оставил как у тебя, логика без изменений)
  if (type === "calendar.free_slots" || type === "calendar_free_slots") {
    const p = (body && body.payload) || {};
    const date = p.date && /^\d{4}-\d{2}-\d{2}$/.test(p.date) ? p.date : new Date().toISOString().slice(0, 10);
    const reqDur = Number(p.duration_min || 60);

    const toMin = (hhmm: string) => {
      const [h, m] = String(hhmm).split(":").map((n) => +n);
      return h * 60 + m;
    };
    const fmt = (m: number) => String(Math.floor(m / 60)).padStart(2, "0") + ":" + String(m % 60).padStart(2, "0");

    const w = new Date(date + "T00:00:00").getDay();

    const cfgRow = await db
      .prepare(
        `SELECT work_start_min AS ws, work_end_min AS we, slot_step_min AS step, capacity_per_slot AS cap
         FROM calendar_cfg
         WHERE app_public_id = ? AND (weekday = ? OR weekday IS NULL)
         ORDER BY (weekday IS NULL) ASC LIMIT 1`
      )
      .bind((ctx as any).publicId, w)
      .first();

    if (!cfgRow) return json({ ok: true, date, slots: [] }, 200, request);

    const ws = Number((cfgRow as any).ws || 600);
    const we = Number((cfgRow as any).we || 1080);
    const step = Number((cfgRow as any).step || 30);
    const cap = Number((cfgRow as any).cap || 1);

    const booked = await db
      .prepare(
        `SELECT time, duration_min FROM cal_bookings
         WHERE app_public_id = ? AND date = ? AND status = 'new'`
      )
      .bind((ctx as any).publicId, date)
      .all();

    const holds = await db
      .prepare(
        `SELECT time, duration_min FROM cal_holds
         WHERE app_public_id = ? AND date = ? AND expires_at > datetime('now') AND tg_id <> ?`
      )
      .bind((ctx as any).publicId, date, String(tg.id))
      .all();

    const busy = new Map<number, number>();
    function addBusy(startMin: number, durMin: number) {
      for (let t = startMin; t < startMin + durMin; t += step) busy.set(t, (busy.get(t) || 0) + 1);
    }
    for (const r of booked.results || []) addBusy(toMin((r as any).time), Number((r as any).duration_min || step));
    for (const r of holds.results || []) addBusy(toMin((r as any).time), Number((r as any).duration_min || step));

    const slots: string[] = [];
    const maxStart = we - reqDur;
    for (let start = ws; start <= maxStart; start += step) {
      let ok = true;
      for (let t = start; t < start + reqDur; t += step) {
        if ((busy.get(t) || 0) >= cap) {
          ok = false;
          break;
        }
      }
      if (ok) slots.push(fmt(start));
    }

    return json({ ok: true, date, slots }, 200, request);
  }

  return json({ ok: false, error: "UNKNOWN_TYPE", type }, 400, request);
}

export async function routeMiniApi(request: Request, env: Env, url: URL): Promise<Response> {
  return await handleMiniApi(request, env, url);
}
