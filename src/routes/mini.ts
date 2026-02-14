// src/routes/mini.ts
import type { Env } from "../index";
import { json } from "../utils/http";
import { tgSendMessage } from "../services/telegramSend";
import { decryptToken } from "../services/crypto";
import { getCanonicalPublicIdForApp } from "../services/apps";

// modules
import { handlePassportMiniApi } from "./mini/passport";
import { handleWheelMiniApi } from "./mini/wheel";
import { buildState } from "./mini/state";
import { awardCoins, spendCoinsIfEnough } from "./mini/coins";

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
    return { id: Number((ins as any).lastInsertRowid || (ins as any).meta?.last_row_id || 0), coins: 0 };
  } else {
    await db
      .prepare(`UPDATE app_users SET tg_username = ?, last_seen = datetime('now') WHERE id = ?`)
      .bind(tg.username || null, (row as any).id)
      .run();
    return { id: (row as any).id, coins: Number((row as any).coins || 0) };
  }
}

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

// ================== QUIZ ==================
async function quizFinish(db: any, appId: any, appPublicId: string, tgId: any, data: any) {
  const quizId = String(data.quiz_id || "beer_profile_v1");
  const score = Number(data.score || 0);

  const profile = data.profile || {};
  const answersJson = data.answers_json || JSON.stringify(profile || {});
  const now = new Date().toISOString();

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
    await awardCoins(
      db,
      appId,
      appPublicId,
      tgId,
      score,
      "profile_quiz",
      quizId,
      "profile quiz reward",
      `quiz:reward:${appPublicId}:${tgId}:${quizId}:${score}`
    );
  }

  const fresh = await buildState(db, appId, appPublicId, tgId, {});
  return { ok: true, status: "completed", score, fresh_state: fresh };
}

// ================== MINI API ==================
async function handleMiniApi(request: Request, env: Env, url: URL) {
  const db: any = env.DB;
  const publicId = url.searchParams.get("public_id") || (url.pathname || "").split("/").pop() || "";

  // читаем JSON
  let body: any = {};
  try {
    body = await request.json();
  } catch (_) {}

  const initDataRaw = body.init_data || body.initData || null;
  const tg = body.tg_user || {};
  if (!tg || !tg.id) return json({ ok: false, error: "NO_TG_USER_ID" }, 400, request);

  const ctx = await requireTgAndVerify(publicId, initDataRaw, env);
  if (!(ctx as any).ok) return json({ ok: false, error: (ctx as any).error || "AUTH_FAILED" }, (ctx as any).status || 403, request);

  await upsertAppUser(db, (ctx as any).appId, (ctx as any).publicId, tg);

  // type resolution
  let type = body.type || url.searchParams.get("type") || "";
  if (!type) {
    const seg = (url.pathname || "").split("/").filter(Boolean).pop();
    type = seg || "";
  }
  if (type === "claim") type = "claim_prize";
  if (type === "quiz") type = "quiz_state";

  const payload = body.payload || {};

  // ====== state
  if (type === "state") {
    const appObj = await env.APPS.get("app:" + (ctx as any).appId, "json").catch(() => null);
    const cfg =
      (appObj as any) && ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
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

  // ====== wheel module
  {
    const resp = await handleWheelMiniApi({
      request,
      env,
      db,
      type,
      payload,
      tg,
      ctx,
      buildState,
      spendCoinsIfEnough,
      awardCoins,
    });
    if (resp) return resp;
  }

  // ====== passport module
  {
    const resp = await handlePassportMiniApi({
      request,
      env,
      db,
      type,
      payload,
      tg,
      ctx,
      buildState,
    });
    if (resp) return resp;
  }

  // ====== quiz_finish
  if (type === "quiz_finish") {
    const res = await quizFinish(db, (ctx as any).appId, (ctx as any).publicId, tg.id, payload || {});
    return json(res, 200, request);
  }

  // ====== calendar.free_slots (оставил как у тебя)
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
