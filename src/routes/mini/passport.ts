// src/routes/mini/passport.ts
import type { Env } from "../../index";
import { json } from "../../utils/http";
import { tgSendMessage } from "../../services/telegramSend";
import { decryptToken } from "../../services/crypto";

/**
 * ВАЖНО:
 * - Файл автономный, не импортирует mini.ts (чтобы не было циклических импортов).
 * - Поэтому тут есть локальные helpers для redeem-кода, токена бота и подсчётов.
 */

function randomRedeemCodeLocal(len = 10) {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  let s = "";
  for (let i = 0; i < len; i++) s += alphabet[bytes[i] % alphabet.length];
  return "SG-" + s.slice(0, 4) + "-" + s.slice(4, 8) + (len > 8 ? "-" + s.slice(8) : "");
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

// ====== styles totals / collected
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

// ================== PINS ==================
export async function useOneTimePin(db: any, appPublicId: string, tgId: any, pin: any, styleId: any) {
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
export async function passportGetIssued(db: any, appPublicId: string, tgId: any, passportKey: string) {
  return await db
    .prepare(
      `SELECT id, prize_code, prize_title, coins, redeem_code, status, issued_at
       FROM passport_rewards
       WHERE app_public_id=? AND tg_id=? AND passport_key=? AND status='issued'
       ORDER BY id DESC
       LIMIT 1`
    )
    .bind(appPublicId, String(tgId), String(passportKey || "default"))
    .first();
}

async function passportIssueRewardIfCompleted(db: any, env: Env, ctx: any, tgId: any, cfg: any) {
  const passportKey = String(cfg?.passport?.passport_key || "default");
  const prizeCode = String(cfg?.passport?.reward_prize_code || "").trim();
  if (!prizeCode) return { ok: true, skipped: true, reason: "NO_REWARD_PRIZE_CODE" };

  const total = await stylesTotalCount(db, ctx.publicId);
  if (!total) return { ok: true, skipped: true, reason: "NO_STYLES_TOTAL" };

  const got = await passportCollectedCount(db, ctx.publicId, tgId);
  if (got < total) return { ok: true, skipped: true, reason: "NOT_COMPLETED", got, total };

  // ✅ если уже есть issued — не плодим новые строки
  const existingIssued: any = await db
    .prepare(
      `SELECT id, prize_code, prize_title, coins, redeem_code, status, issued_at
       FROM passport_rewards
       WHERE app_public_id=? AND tg_id=? AND passport_key=? AND status='issued'
       ORDER BY id DESC
       LIMIT 1`
    )
    .bind(ctx.publicId, String(tgId), passportKey)
    .first();

  if (existingIssued) {
    return { ok: true, issued: true, reused: true, reward: existingIssued, got, total };
  }

  // ✅ берём приз из wheel_prizes
  const pr: any = await db
    .prepare(`SELECT code, title, coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`)
    .bind(ctx.publicId, prizeCode)
    .first();

  if (!pr) return { ok: false, error: "REWARD_PRIZE_NOT_FOUND", prize_code: prizeCode };

  const prizeTitle = String(pr.title || prizeCode);
  const prizeCoins = Math.max(0, Math.floor(Number(pr.coins || 0)));

  // ✅ создаём строку в passport_rewards (история)
  let redeemCode = "";
  for (let i = 0; i < 8; i++) {
    redeemCode = randomRedeemCodeLocal(10);
    try {
      await db
        .prepare(
          `INSERT INTO passport_rewards
           (app_id, app_public_id, tg_id, passport_key, prize_code, prize_title, coins, redeem_code, status, issued_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'issued', datetime('now'))`
        )
        .bind(ctx.appId, ctx.publicId, String(tgId), passportKey, prizeCode, prizeTitle, prizeCoins, redeemCode)
        .run();
      break;
    } catch (e: any) {
      const msg = String(e?.message || e);
      if (/unique|constraint/i.test(msg)) continue;
      throw e;
    }
  }
  if (!redeemCode) return { ok: false, error: "PASSPORT_REDEEM_CREATE_FAILED" };

  // Сообщение пользователю
  const botToken = await getBotTokenForApp(ctx.publicId, env, ctx.appId).catch(() => null);

  let botUsername = "";
  try {
    const b = await db
      .prepare(`SELECT username FROM bots WHERE app_public_id=? AND status='active' ORDER BY id DESC LIMIT 1`)
      .bind(ctx.publicId)
      .first();
    botUsername = b && (b as any).username ? String((b as any).username).replace(/^@/, "").trim() : "";
  } catch (_) {}

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

type PassportArgs = {
  request: Request;
  env: Env;
  db: any;
  type: string;
  payload: any;
  tg: any;
  ctx: any;
  buildState: (db: any, appId: any, appPublicId: string, tgId: any, cfg: any) => Promise<any>;
};

export async function handlePassportMiniApi(args: PassportArgs): Promise<Response | null> {
  const { request, env, db, type, payload, tg, ctx, buildState } = args;

  // ====== style collect (как у тебя)
  if (type === "style.collect" || type === "style_collect") {
    const styleId = String((payload && (payload.style_id || payload.styleId || payload.code)) || "").trim();
    const pin = String((payload && payload.pin) || "").trim();
    if (!styleId) return json({ ok: false, error: "NO_STYLE_ID" }, 400, request);

    const appObj = await env.APPS.get("app:" + (ctx as any).appId, "json").catch(() => null);
    const cfg =
      (appObj as any) && ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
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

  return null;
}
