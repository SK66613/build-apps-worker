// src/handlers/cabinetApiHandlers.ts
// Cabinet (cookie session) CRUD + bots + broadcasts + dialogs.
// Реализация вынесена из монолита _legacyImpl.

import type { Env } from "../index";
import { requireSession as requireSessionSvc } from "../services/session";
import { getSeedConfig } from "../services/templates";
import { getCanonicalPublicIdForApp } from "../services/apps";
import { encryptToken } from "../services/crypto";
import {
  ensureBotWebhookSecretForPublicId,
  getBotWebhookSecretForPublicId,
  getBotTokenForApp,
} from "../services/bots";
import { tgSendMessage } from "../services/telegramSend";
import { json } from "../utils/http";
import { extractSalesSettingsFromBlueprint, upsertSalesSettings } from "../services/salesSettings";
import { extractRuntimeConfigFromBlueprint } from "../services/runtimeConfig";
import { syncRuntimeTablesFromConfig } from "../services/runtimeSync";

// -------------------- session --------------------

export async function requireSession(request: Request, env: Env) {
  return await requireSessionSvc(request as any, env as any);
}

// -------------------- apps --------------------

function slugify(input: any, maxLen = 64) {
  const s = String(input || "").trim().toLowerCase();
  return s
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, maxLen);
}

export async function ensureAppOwner(appId: any, ownerId: any, env: Env) {
  const row: any = await env.DB.prepare(`SELECT id, owner_id FROM apps WHERE id = ?`).bind(appId).first();
  if (!row) return { ok: false, status: 404 };
  if (Number(row.owner_id) !== Number(ownerId)) return { ok: false, status: 403 };
  return { ok: true };
}

export async function listMyApps(env: Env, ownerId: any, request: Request) {
  const res = await env.DB.prepare(
    `SELECT id, owner_id, title, public_id, status, created_at, updated_at, last_published_at
     FROM apps
     WHERE owner_id = ?
     ORDER BY created_at DESC`
  )
    .bind(ownerId)
    .all();

  return json({ ok: true, apps: res.results || [] }, 200, request);
}

export async function createApp(request: Request, env: Env, _url: URL, ownerId: any) {
  const body: any = await request.json().catch(() => ({}));
  const title = String(body.title || "New mini-app").trim() || "New mini-app";

  const rawSlug = (body.slug ?? body.id ?? body.appId ?? "").toString().trim();
  const explicit = !!rawSlug && rawSlug.toLowerCase() !== "auto";

  const base = slugify(explicit ? rawSlug : title, 42);
  if (!base) return json({ ok: false, error: "INVALID_SLUG" }, 400, request);

  let appId = base;

  const tryInsert = async () => {
    const publicId = "app-" + appId + "-" + Math.random().toString(36).slice(2, 6);
    const templateId = body.template_id || body.templateId || null;
    const config = body.config || getSeedConfig(templateId);

    await env.DB.prepare(
      `INSERT INTO apps (id, owner_id, title, public_id, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, 'draft', datetime('now'), datetime('now'))`
    )
      .bind(appId, ownerId, title, publicId)
      .run();

    const now = new Date().toISOString();
    const appObj = {
      id: appId,
      publicId,
      title,
      config,
      createdAt: now,
      updatedAt: now,
      lastPublishedAt: null,
    };

    await env.APPS.put("app:" + appId, JSON.stringify(appObj));
    await env.APPS.put("app:by_public:" + publicId, JSON.stringify({ appId }));

    const publicUrl = "https://mini.salesgenius.ru/m/" + publicId;
    return json({ ok: true, id: appId, publicId, title, publicUrl }, 200, request);
  };

  if (explicit) {
    try {
      return await tryInsert();
    } catch (_) {
      return json({ ok: false, error: "APP_ALREADY_EXISTS" }, 409, request);
    }
  }

  for (let i = 0; i < 8; i++) {
    if (i > 0) appId = `${base}_${Math.random().toString(36).slice(2, 6)}`;
    try {
      return await tryInsert();
    } catch (e: any) {
      const msg = (e && (e.message || String(e))) || "";
      if (!/constraint|unique/i.test(msg)) {
        return json({ ok: false, error: "DB_ERROR", message: msg }, 500, request);
      }
    }
  }

  return json({ ok: false, error: "APP_ALREADY_EXISTS" }, 409, request);
}

export async function getApp(appId: any, env: Env, request: Request) {
  const raw = await env.APPS.get("app:" + appId);
  if (!raw) return json({ ok: false, error: "NOT_FOUND" }, 404, request);

  const appObj = JSON.parse(raw);
  const payload = {
    id: appObj.id,
    title: appObj.title,
    config: appObj.config,
    publicId: appObj.publicId,
    createdAt: appObj.createdAt,
    updatedAt: appObj.updatedAt,
    lastPublishedAt: appObj.lastPublishedAt,
  };
  return json({ ok: true, ...payload, app: payload }, 200, request);
}

export async function saveApp(appId: any, request: Request, env: Env) {
  const body: any = await request.json().catch(() => ({}));
  const newConfig = body.config;
  const newTitle = body.title;

  const raw = await env.APPS.get("app:" + appId);
  if (!raw) return json({ ok: false, error: "NOT_FOUND" }, 404, request);

  const appObj: any = JSON.parse(raw);
  if (newConfig) appObj.config = newConfig;
  if (typeof newTitle === "string" && newTitle.trim()) appObj.title = newTitle.trim();
  appObj.updatedAt = new Date().toISOString();

  await env.APPS.put("app:" + appId, JSON.stringify(appObj));

  // update D1 title/updated_at
  if (typeof newTitle === "string" && newTitle.trim()) {
    await env.DB.prepare(`UPDATE apps SET title = ?, updated_at = datetime('now') WHERE id = ?`)
      .bind(newTitle.trim(), appId)
      .run();
  } else {
    await env.DB.prepare(`UPDATE apps SET updated_at = datetime('now') WHERE id = ?`).bind(appId).run();
  }

  // sales settings sync
  try {
    const salesCfg = extractSalesSettingsFromBlueprint(appObj.config || null);
    await upsertSalesSettings(appId, String(appObj.publicId || ""), salesCfg, env);
  } catch (e) {
    console.error("[sales_settings] sync on save failed", e);
  }

  return json({ ok: true }, 200, request);
}

export async function deleteApp(appId: any, env: Env, request: Request) {
  let publicId: string | null = null;
  const raw = await env.APPS.get("app:" + appId);
  if (raw) {
    try {
      publicId = JSON.parse(raw)?.publicId || null;
    } catch (_) {
      publicId = null;
    }
  }

  await env.APPS.delete("app:" + appId);
  if (publicId) await env.APPS.delete("app:by_public:" + publicId);

  await env.DB.prepare("DELETE FROM apps WHERE id = ?").bind(appId).run();
  return json({ ok: true }, 200, request);
}

export async function publishApp(appId: any, env: Env, _url: URL, request: Request) {
  const raw = await env.APPS.get("app:" + appId);
  if (!raw) return json({ ok: false, error: "CONFIG_NOT_FOUND" }, 404, request);

  let appObj: any;
  try {
    appObj = JSON.parse(raw);
  } catch (_e) {
    return json({ ok: false, error: "CONFIG_PARSE_ERROR" }, 500, request);
  }
  

  if (!appObj.publicId) {
    const suffix = Math.random().toString(36).slice(2, 6);
    appObj.publicId = `app-${appId}-${suffix}`;
  }

  const publicId = String(appObj.publicId);
const draftKey = "app:draft:" + appId;
const liveKey  = "app:live:" + publicId;

// 1) берём blueprint для публикации: draft → иначе fallback на старое appObj.config
let bpRaw = await env.APPS.get(draftKey);
if (!bpRaw) {
  bpRaw = JSON.stringify(appObj.config || null);
}
let bp: any = null;
try { bp = JSON.parse(bpRaw); } catch (_) { bp = null; }

// 2) записываем LIVE конфиг (то, что увидят пользователи)
await env.APPS.put(liveKey, JSON.stringify(bp));


  appObj.public_id = appObj.publicId;
  appObj.lastPublishedAt = new Date().toISOString();
  appObj.updatedAt = new Date().toISOString();

  await env.APPS.put("app:" + appId, JSON.stringify(appObj));
  await env.APPS.put("app:by_public:" + appObj.publicId, JSON.stringify({ appId, publicId: appObj.publicId }));

  // update D1
  try {
    await env.DB.prepare(
      `UPDATE apps SET public_id = ?, updated_at = datetime('now'), published_at = datetime('now') WHERE id = ?`
    )
      .bind(appObj.publicId, appId)
      .run();
  } catch (_) {
    try {
      await env.DB.prepare(`UPDATE apps SET updated_at = datetime('now') WHERE id = ?`).bind(appId).run();
    } catch (_) {}
  }

  // sales settings sync on publish
  try {
    const salesCfg = extractSalesSettingsFromBlueprint(bp || null);

    await upsertSalesSettings(appId, String(appObj.publicId || ""), salesCfg, env);
  } catch (e) {
    console.error("[sales_settings] sync on publish failed", e);
  }

// runtime config: ALWAYS derive from blueprint on publish (so edits reflect in D1)
let runtimeCfg = extractRuntimeConfigFromBlueprint(bp || null);

// persist runtimeCfg into app object (so mini/state uses freshest config)
appObj.app_config = runtimeCfg;
try {
  await env.APPS.put("app:" + appId, JSON.stringify(appObj));
} catch (_) {}

// (optional) light debug in response
// runtimeCfg.__debug = {
//   wheel_prizes: Array.isArray(runtimeCfg?.wheel?.prizes) ? runtimeCfg.wheel.prizes.length : 0,
//   passport_styles: Array.isArray(runtimeCfg?.passport?.styles) ? runtimeCfg.passport.styles.length : 0,
// };


  const syncStats = await syncRuntimeTablesFromConfig(appId, appObj.publicId, runtimeCfg, env);
  const publicUrl = "https://mini.salesgenius.ru/m/" + encodeURIComponent(appObj.publicId);
  return json({ ok: true, appId, publicId: appObj.publicId, publicUrl, sync: syncStats }, 200, request);
}

// -------------------- bot integration --------------------

function extractTelegramBotId(token: any) {
  if (!token) return null;
  const m = String(token).match(/^(\d+):/);
  return m ? m[1] : null;
}

export async function saveBotIntegration(appId: any, env: Env, body: any, ownerId: any, request: Request) {
  const usernameRaw = (body.username || body.botUsername || "").trim();
  const tokenRaw = (body.token || body.botToken || "").trim();
  if (!tokenRaw) return json({ ok: false, error: "NO_TOKEN" }, 400, request);

  if (!env.BOT_SECRETS) return json({ ok: false, error: "NO_BOT_SECRETS" }, 500, request);
  if (!env.BOT_TOKEN_KEY || env.BOT_TOKEN_KEY.length < 16) return json({ ok: false, error: "BAD_MASTER_KEY" }, 500, request);

  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 500, request);

  const username = usernameRaw || null;
  const tgBotId = extractTelegramBotId(tokenRaw);

  try {
    const cipher = await encryptToken(tokenRaw, env.BOT_TOKEN_KEY);
    const kvKey = "bot_token:public:" + appPublicId;
    await env.BOT_SECRETS.put(kvKey, JSON.stringify({ cipher }));

    const wh = await ensureBotWebhookSecretForPublicId(appPublicId, env);

    const existing: any = await env.DB.prepare(
      "SELECT id FROM bots WHERE owner_id = ? AND app_public_id = ? LIMIT 1"
    )
      .bind(ownerId, appPublicId)
      .first();

    let botRowId: any;
    if (!existing) {
      const ins = await env.DB.prepare(
        `INSERT INTO bots
           (owner_id, title, username, tg_bot_id, status, created_at, updated_at, app_id, app_public_id)
         VALUES
           (?, ?, ?, ?, 'active', datetime('now'), datetime('now'), ?, ?)`
      )
        .bind(ownerId, "Bot for app " + appId, username, tgBotId, appId, appPublicId)
        .run();
      botRowId = Number((ins as any).lastInsertRowid);
    } else {
      botRowId = existing.id;
      await env.DB.prepare(
        `UPDATE bots
         SET username      = ?,
             tg_bot_id     = ?,
             status        = 'active',
             app_id        = ?,
             app_public_id = ?,
             updated_at    = datetime('now')
         WHERE id = ? AND owner_id = ?`
      )
        .bind(username, tgBotId, appId, appPublicId, botRowId, ownerId)
        .run();
    }

        // ====== IMPORTANT: реально ставим webhook в Telegram, иначе апдейтов не будет ======
    try {
      const webhookUrl =
        "https://app.salesgenius.ru/api/tg/webhook/" +
        encodeURIComponent(appPublicId) +
        "?s=" +
        encodeURIComponent(wh.secret);

      const r = await fetch(`https://api.telegram.org/bot${tokenRaw}/setWebhook`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          url: webhookUrl,
          allowed_updates: ["message", "callback_query", "pre_checkout_query"],
          drop_pending_updates: true,
        }),
      });

      const j: any = await r.json().catch(() => null);
      if (!r.ok || !j || !j.ok) {
        console.error("[bot] setWebhook failed", r.status, j);
      }
    } catch (e) {
      console.error("[bot] setWebhook exception", e);
    }


    return json(
      {
        ok: true,
        bot_id: botRowId,
        app_public_id: appPublicId,
        kv_key: kvKey,
        webhook: {
          secret: wh.secret,
          created: wh.created,
          kv_key: wh.kv_key || null,
          url:
            "https://app.salesgenius.ru/api/tg/webhook/" +
            encodeURIComponent(appPublicId) +
            "?s=" +
            encodeURIComponent(wh.secret),
        },
      },
      200,
      request
    );
  } catch (e) {
    console.error("[bot] saveBotIntegration failed", e);
    return json({ ok: false, error: "INTERNAL_ERROR" }, 500, request);
  }
}

export async function getBotIntegration(appId: any, env: Env, ownerId: any, request: Request) {
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);

  const kvKey = "bot_token:public:" + appPublicId;
  const raw = await env.BOT_SECRETS?.get(kvKey);
  const hasToken = !!raw;

  let webhookUrl: string | null = null;
  try {
    const sec = await getBotWebhookSecretForPublicId(appPublicId, env);
    if (sec) {
      webhookUrl =
        "https://app.salesgenius.ru/api/tg/webhook/" +
        encodeURIComponent(appPublicId) +
        "?s=" +
        encodeURIComponent(sec);
    }
  } catch (e) {
    console.warn("[bot] getBotIntegration: webhook secret read failed", e);
  }

  const bot: any = await env.DB.prepare(
    "SELECT id, username, tg_bot_id, status, updated_at FROM bots WHERE owner_id = ? AND app_public_id = ? LIMIT 1"
  )
    .bind(ownerId, appPublicId)
    .first();

  return json(
    {
      ok: true,
      linked: !!(bot && hasToken),
      app_id: appId,
      app_public_id: appPublicId,
      username: bot ? (bot.username || null) : null,
      tg_bot_id: bot ? (bot.tg_bot_id || null) : null,
      status: bot ? (bot.status || null) : null,
      updated_at: bot ? (bot.updated_at || null) : null,
      kv_key: kvKey,
      webhook: { url: webhookUrl },
    },
    200,
    request
  );
}

export async function deleteBotIntegration(appId: any, env: Env, ownerId: any, request: Request) {
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);

  try {
    if (env.BOT_SECRETS) {
      await env.BOT_SECRETS.delete("bot_token:public:" + appPublicId);
      await env.BOT_SECRETS.delete("bot_whsec:public:" + appPublicId);
    }
  } catch (e) {
    console.warn("[bot] deleteBotIntegration: KV delete failed", e);
  }

  try {
    await env.DB.prepare("DELETE FROM bots WHERE owner_id = ? AND app_public_id = ?").bind(ownerId, appPublicId).run();
  } catch (e) {
    console.warn("[bot] deleteBotIntegration: D1 delete failed", e);
  }

  return json({ ok: true, app_id: appId, app_public_id: appPublicId }, 200, request);
}

// -------------------- broadcasts --------------------

function normalizeSegment(seg: any) {
  seg = String(seg || "").trim();
  return seg || "bot_active";
}

function segmentWhere(segment: string) {
  switch (segment) {
    case "all":
      return { where: "app_public_id = ?", binds: [] as any[] };
    case "mini_active_7d":
      return { where: "app_public_id = ? AND last_seen >= datetime('now','-7 day')", binds: [] as any[] };
    case "bot_active":
    default:
      return {
        where: "app_public_id = ? AND bot_started_at IS NOT NULL AND COALESCE(bot_status,'') != 'blocked'",
        binds: [] as any[],
      };
  }
}

export async function listBroadcasts(appId: any, env: Env, ownerId: any, request: Request) {
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);

  const rows = await env.DB.prepare(
    `SELECT id, title, segment, status, total, sent, failed, blocked, created_at, updated_at
     FROM broadcasts
     WHERE app_public_id = ? AND (owner_id = ? OR owner_id IS NULL)
     ORDER BY id DESC
     LIMIT 50`
  )
    .bind(appPublicId, ownerId)
    .all();

  return json({ ok: true, app_public_id: appPublicId, items: rows.results || [] }, 200, request);
}

export async function createAndSendBroadcast(appId: any, env: Env, ownerId: any, request: Request) {
  const body: any = await request.json().catch(() => null);
  if (!body) return json({ ok: false, error: "BAD_JSON" }, 400, request);

  const text = String(body.text || "").trim();
  if (!text) return json({ ok: false, error: "TEXT_REQUIRED" }, 400, request);

  const title = body.title ? String(body.title).trim() : null;
  const segment = normalizeSegment(body.segment || "bot_active");
  const btnText = body.btn_text ? String(body.btn_text).trim() : null;
  const btnUrl = body.btn_url ? String(body.btn_url).trim() : null;

  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);

  const ins = await env.DB.prepare(
    `INSERT INTO broadcasts (app_public_id, owner_id, title, text, segment, btn_text, btn_url, status)
     VALUES (?, ?, ?, ?, ?, ?, ?, 'sending')`
  )
    .bind(appPublicId, ownerId, title, text, segment, btnText, btnUrl)
    .run();

  const broadcastId = (ins as any)?.meta?.last_row_id ? Number((ins as any).meta.last_row_id) : null;
  if (!broadcastId) return json({ ok: false, error: "BROADCAST_CREATE_FAILED" }, 500, request);

  const seg = segmentWhere(segment);
  const audience = await env.DB.prepare(
    `SELECT tg_user_id
     FROM app_users
     WHERE ${seg.where}
     ORDER BY COALESCE(bot_last_seen, last_seen) DESC
     LIMIT 5000`
  )
    .bind(appPublicId, ...seg.binds)
    .all();

  const users: any[] = audience?.results || [];
  const total = users.length;

  await env.DB.prepare(`UPDATE broadcasts SET total=?, updated_at=datetime('now') WHERE id=?`)
    .bind(total, broadcastId)
    .run();

  const botToken = await getBotTokenForApp(appPublicId, env, appId);
  if (!botToken) {
    await env.DB.prepare(`UPDATE broadcasts SET status='failed', updated_at=datetime('now') WHERE id=?`)
      .bind(broadcastId)
      .run();
    return json({ ok: false, error: "BOT_TOKEN_NOT_FOUND", broadcast_id: broadcastId }, 400, request);
  }

  let extra: any = {};
  if (btnText && btnUrl) {
    extra.reply_markup = { inline_keyboard: [[{ text: btnText, url: btnUrl }]] };
  }

  let sent = 0,
    failed = 0,
    blocked = 0;

  for (const u of users) {
    const tgUserId = String(u.tg_user_id);
    await env.DB.prepare(
      `INSERT OR IGNORE INTO broadcast_jobs (broadcast_id, app_public_id, tg_user_id, status)
       VALUES (?, ?, ?, 'queued')`
    )
      .bind(broadcastId, appPublicId, tgUserId)
      .run();

    try {
      const resp = await tgSendMessage(env, botToken, tgUserId, text, extra, { appPublicId, tgUserId });
      if (resp.ok) {
        sent++;
        await env.DB.prepare(
          `UPDATE broadcast_jobs SET status='sent', updated_at=datetime('now')
           WHERE broadcast_id=? AND tg_user_id=?`
        )
          .bind(broadcastId, tgUserId)
          .run();
      } else {
        const errText = await resp.text().catch(() => "");
        const isBlocked = resp.status === 403 && /blocked|bot was blocked/i.test(errText || "");
        if (isBlocked) {
          blocked++;
          await env.DB.prepare(
            `UPDATE broadcast_jobs SET status='blocked', error=?, updated_at=datetime('now')
             WHERE broadcast_id=? AND tg_user_id=?`
          )
            .bind(errText.slice(0, 400), broadcastId, tgUserId)
            .run();
        } else {
          failed++;
          await env.DB.prepare(
            `UPDATE broadcast_jobs SET status='failed', error=?, updated_at=datetime('now')
             WHERE broadcast_id=? AND tg_user_id=?`
          )
            .bind(errText.slice(0, 400), broadcastId, tgUserId)
            .run();
        }
      }
    } catch (e: any) {
      failed++;
      await env.DB.prepare(
        `UPDATE broadcast_jobs SET status='failed', error=?, updated_at=datetime('now')
         WHERE broadcast_id=? AND tg_user_id=?`
      )
        .bind(String(e?.message || e).slice(0, 400), broadcastId, tgUserId)
        .run();
    }
  }

  await env.DB.prepare(
    `UPDATE broadcasts
     SET status='done', sent=?, failed=?, blocked=?, updated_at=datetime('now')
     WHERE id=?`
  )
    .bind(sent, failed, blocked, broadcastId)
    .run();

  return json({ ok: true, broadcast_id: broadcastId, app_public_id: appPublicId, total, sent, failed, blocked }, 200, request);
}

// -------------------- dialogs --------------------

export async function listDialogs(appId: any, env: Env, _ownerId: any, request: Request) {
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);

  const url = new URL(request.url);
  const range = String(url.searchParams.get("range") || "all").trim();

  let rangeWhere = "";
  if (range === "today") rangeWhere = " AND COALESCE(u.bot_last_seen, u.bot_started_at) >= datetime('now','start of day')";
  if (range === "7d") rangeWhere = " AND COALESCE(u.bot_last_seen, u.bot_started_at) >= datetime('now','-7 day')";
  if (range === "30d") rangeWhere = " AND COALESCE(u.bot_last_seen, u.bot_started_at) >= datetime('now','-30 day')";

  const rows = await env.DB.prepare(
    `SELECT
       u.tg_user_id,
       u.tg_username,
       u.bot_last_seen,
       u.bot_started_at,
       COALESCE(u.bot_total_msgs_in,0)  AS in_count,
       COALESCE(u.bot_total_msgs_out,0) AS out_count,
       (
         SELECT m.text
         FROM bot_messages m
         WHERE m.app_public_id = u.app_public_id AND m.tg_user_id = u.tg_user_id
         ORDER BY m.id DESC
         LIMIT 1
       ) AS last_text,
       (
         SELECT m.direction
         FROM bot_messages m
         WHERE m.app_public_id = u.app_public_id AND m.tg_user_id = u.tg_user_id
         ORDER BY m.id DESC
         LIMIT 1
       ) AS last_dir
     FROM app_users u
     WHERE u.app_public_id = ?
       AND u.bot_started_at IS NOT NULL
       ${rangeWhere}
     ORDER BY COALESCE(u.bot_last_seen, u.bot_started_at) DESC
     LIMIT 500`
  )
    .bind(appPublicId)
    .all();

  return json({ ok: true, app_public_id: appPublicId, items: rows.results || [] }, 200, request);
}

export async function getDialogMessages(appId: any, tgUserId: any, env: Env, _ownerId: any, request: Request) {
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);

  const url = new URL(request.url);
  const limit = Math.max(10, Math.min(200, Number(url.searchParams.get("limit") || 80)));
  const beforeId = url.searchParams.get("before_id");

  let sql = `
    SELECT id, direction, msg_type, text, created_at
    FROM bot_messages
    WHERE app_public_id = ? AND tg_user_id = ?
  `;
  const binds: any[] = [appPublicId, String(tgUserId)];
  if (beforeId) {
    sql += ` AND id < ? `;
    binds.push(Number(beforeId));
  }
  sql += ` ORDER BY id DESC LIMIT ? `;
  binds.push(limit);

  const rows = await env.DB.prepare(sql).bind(...binds).all();
  const items = (rows.results || []).slice().reverse();
  return json({ ok: true, app_public_id: appPublicId, tg_user_id: String(tgUserId), items }, 200, request);
}

export async function sendDialogMessage(appId: any, tgUserId: any, env: Env, _ownerId: any, request: Request) {
  const body: any = await request.json().catch(() => null);
  if (!body) return json({ ok: false, error: "BAD_JSON" }, 400, request);

  const text = String(body.text || "").trim();
  if (!text) return json({ ok: false, error: "TEXT_REQUIRED" }, 400, request);

  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok: false, error: "APP_PUBLIC_ID_NOT_FOUND" }, 404, request);

  const botToken = await getBotTokenForApp(appPublicId, env, appId);
  if (!botToken) return json({ ok: false, error: "BOT_TOKEN_NOT_FOUND" }, 400, request);

  const resp = await tgSendMessage(env, botToken, String(tgUserId), text, {}, { appPublicId, tgUserId: String(tgUserId) });
  if (!resp.ok) {
    const errText = await resp.text().catch(() => "");
    return json({ ok: false, error: "TG_SEND_FAILED", status: resp.status, details: errText.slice(0, 500) }, 502, request);
  }

  return json({ ok: true }, 200, request);
}
