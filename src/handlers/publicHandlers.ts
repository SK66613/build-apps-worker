// src/handlers/publicHandlers.ts
// Public (no cookie session) handlers used by /api/public/*.

import type { Env } from '../index';
import { jsonResponse } from '../services/http';
import { getBotTokenForApp } from '../services/botToken';
import { getCanonicalPublicIdForApp, resolveAppContextByPublicId } from '../services/apps';
import { verifyInitDataSignature, parseInitDataUser } from '../services/telegramInitData';
import { tgCreateInvoiceLinkStars } from '../services/telegramApi';
import { awardCoins } from '../services/coinsLedger';

// ================== Telegram Stars ==================

export async function handleStarsCreate(publicId: string, request: Request, env: Env): Promise<Response> {
  let body: any = {};
  try { body = await request.json(); } catch (_e) {}

  const tg = body.tg_user || body.tg || {};
  if (!tg || !tg.id) return jsonResponse({ ok: false, error: 'NO_TG_USER_ID' }, 400, request);

  const items = Array.isArray(body.items) ? body.items : [];
  if (!items.length) return jsonResponse({ ok: false, error: 'NO_ITEMS' }, 400, request);

  const botToken = await getBotTokenForApp(publicId, env, null);
  if (!botToken) return jsonResponse({ ok: false, error: 'NO_BOT_TOKEN_FOR_APP' }, 400, request);

  let totalStars = 0;
  const normItems = items.map((it: any) => {
    const qty = Math.max(1, Math.floor(Number(it.qty || 1)));
    const stars = Math.max(1, Math.floor(Number(it.stars || 0)));
    const amount = qty * stars;
    totalStars += amount;
    return {
      product_id: String(it.product_id || it.id || ''),
      title: String(it.title || ''),
      qty,
      stars,
      amount,
      meta_json: it.meta ? JSON.stringify(it.meta) : null,
    };
  });

  if (totalStars <= 0) return jsonResponse({ ok: false, error: 'BAD_TOTAL' }, 400, request);

  const orderId = crypto.randomUUID();
  const title = String(body.title || 'Покупка');
  const description = String(body.description || 'Оплата в Telegram Stars');
  const photo_url = body.photo_url ? String(body.photo_url) : '';

  const invoicePayload = `order:${orderId}`;

  await env.DB.prepare(`
    INSERT INTO stars_orders
      (id, app_public_id, tg_id, title, description, photo_url, total_stars, status, invoice_payload, created_at)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, 'created', ?, datetime('now'))
  `).bind(orderId, publicId, String(tg.id), title, description, photo_url, totalStars, invoicePayload).run();

  for (const it of normItems) {
    if (!it.product_id) continue;
    await env.DB.prepare(`
      INSERT INTO stars_order_items
        (order_id, app_public_id, product_id, title, qty, stars, amount, meta_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(orderId, publicId, it.product_id, it.title, it.qty, it.stars, it.amount, it.meta_json).run();
  }

  const invoice_link = await tgCreateInvoiceLinkStars(botToken, {
    title,
    description,
    payload: invoicePayload,
    stars: totalStars,
    photo_url,
  });

  await env.DB.prepare(`
    UPDATE stars_orders SET invoice_link = ? WHERE id = ? AND app_public_id = ?
  `).bind(invoice_link, orderId, publicId).run();

  return jsonResponse({ ok: true, order_id: orderId, invoice_link, total_stars: totalStars }, 200, request);
}

export async function handleStarsOrderGet(publicId: string, orderId: string | number, request: Request, env: Env): Promise<Response> {
  const row: any = await env.DB.prepare(`
    SELECT id, app_public_id, tg_id, total_stars, status, created_at, paid_at
    FROM stars_orders
    WHERE id = ? AND app_public_id = ?
    LIMIT 1
  `).bind(String(orderId), String(publicId)).first();

  if (!row) return jsonResponse({ ok: false, error: 'NOT_FOUND' }, 404, request);
  return jsonResponse({ ok: true, order: row }, 200, request);
}

// ================== Public config ==================

export async function getPublicConfig(publicId: string, env: Env, request: Request): Promise<Response> {
  // 0) сперва пробуем LIVE (то, что должно отдавать боевое мини)
  try{
    const liveRaw = await env.APPS.get('app:live:' + publicId);
    if (liveRaw) {
      let liveCfg: any = null;
      try { liveCfg = JSON.parse(liveRaw); } catch(_e) { liveCfg = null; }

      // source поможет быстро понимать что отдаём
      const payload = {
        publicId,
        title: (liveCfg && liveCfg?.app?.title) ? String(liveCfg.app.title) : undefined,
        config: liveCfg,
        source: 'live',
      };

      return jsonResponse({ ok: true, ...payload, app: payload }, 200, request);
    }
  }catch(e){
    console.warn('[public/config] live read failed', e);
  }

  // 1) fallback: legacy (старое поведение)
  const map: any = await env.APPS.get('app:by_public:' + publicId, 'json');
  if (!map || !map.appId) {
    return jsonResponse({ ok: false, error: 'NOT_FOUND' }, 404, request);
  }
  const appId = map.appId;

  const raw = await env.APPS.get('app:' + appId);
  if (!raw) return jsonResponse({ ok: false, error: 'NOT_FOUND' }, 404, request);

  const appObj: any = JSON.parse(raw);

  const payload = {
    publicId,
    title: appObj.title,
    config: appObj.config,
    source: 'legacy',
  };

  return jsonResponse({ ok: true, ...payload, app: payload }, 200, request);
}


// ================== Public events (mini-app -> D1) ==================

export async function handlePublicEvent(publicId: string, request: Request, env: Env): Promise<Response> {
  let body: any;
  try {
    body = await request.json();
  } catch (_e) {
    return jsonResponse({ ok: false, error: 'BAD_JSON' }, 400, request);
  }

  const tg = body.tg_user || {};
  const type = body.type;
  const payload = body.payload || {};
  const initDataRaw = body.init_data || body.initData || null;

  if (!type) {
    return jsonResponse({ ok: false, error: 'NO_TYPE' }, 400, request);
  }

  // publicId -> appId
  const map: any = await env.APPS.get('app:by_public:' + publicId, 'json');
  if (!map || !map.appId) {
    return jsonResponse({ ok: false, error: 'UNKNOWN_PUBLIC_ID' }, 404, request);
  }
  const appId = map.appId;

  // canonical public id
  const canonicalPublicId = (await getCanonicalPublicIdForApp(appId, env)) || publicId;

  // verify signature if possible
  try {
    const botToken = await getBotTokenForApp(canonicalPublicId, env, appId);
    if (botToken) {
      if (!initDataRaw) {
        return jsonResponse({ ok: false, error: 'NO_INIT_DATA' }, 403, request);
      }
      const ok = await verifyInitDataSignature(initDataRaw, botToken);
      if (!ok) {
        return jsonResponse({ ok: false, error: 'BAD_SIGNATURE' }, 403, request);
      }
    }
  } catch (e) {
    console.error('[event] verifyInitData failed', e);
  }

  const tgId = tg.id ? String(tg.id) : null;
  if (!tgId) {
    return jsonResponse({ ok: false, error: 'NO_TG_USER_ID' }, 400, request);
  }

  const db = env.DB;

  let userRow: any = await db
    .prepare('SELECT id, total_opens, total_spins, total_prizes FROM app_users WHERE app_public_id = ? AND tg_user_id = ?')
    .bind(canonicalPublicId, tgId)
    .first();

  let appUserId: any;
  if (!userRow) {
    const ins = await db
      .prepare('INSERT INTO app_users (app_id, app_public_id, tg_user_id, tg_username, first_seen, last_seen) VALUES (?, ?, ?, ?, datetime("now"), datetime("now"))')
      .bind(appId, canonicalPublicId, tgId, tg.username || null)
      .run();
    appUserId = Number((ins as any).lastInsertRowid);
  } else {
    appUserId = userRow.id;
    await db
      .prepare('UPDATE app_users SET tg_username = ?, last_seen = datetime("now") WHERE id = ?')
      .bind(tg.username || null, appUserId)
      .run();
  }

  await db
    .prepare('INSERT INTO events (app_id, app_public_id, app_user_id, type, payload) VALUES (?, ?, ?, ?, ?)')
    .bind(appId, canonicalPublicId, appUserId, type, JSON.stringify(payload))
    .run();

  if (type === 'open') {
    await db.prepare('UPDATE app_users SET total_opens = total_opens + 1 WHERE id = ?').bind(appUserId).run();

    // referral bind from start_param
    try {
      let startParam = '';
      if (initDataRaw) {
        const p = new URLSearchParams(String(initDataRaw));
        startParam = String(p.get('start_param') || '');
      }

      if (startParam.startsWith('ref_')) {
        const refTgId = startParam.slice(4).trim();
        if (refTgId && refTgId !== String(tgId)) {
          const ex: any = await db.prepare(
            `SELECT id FROM referrals
             WHERE app_public_id = ? AND invitee_tg_id = ?
             LIMIT 1`
          ).bind(canonicalPublicId, String(tgId)).first();

          if (!ex) {
            await db.prepare(
              `INSERT INTO referrals (app_public_id, referrer_tg_id, invitee_tg_id, confirmed, created_at)
               VALUES (?, ?, ?, 1, datetime('now'))`
            ).bind(canonicalPublicId, String(refTgId), String(tgId)).run();
          }
        }
      }
    } catch (e) {
      console.warn('[ref] bind failed', e);
    }

  } else if (type === 'spin') {
    await db.prepare('UPDATE app_users SET total_spins = total_spins + 1 WHERE id = ?').bind(appUserId).run();

  } else if (type === 'prize') {
    await db.prepare('UPDATE app_users SET total_prizes = total_prizes + 1 WHERE id = ?').bind(appUserId).run();

    const code = payload.code || null;
    const title = payload.title || null;

    await db.prepare('INSERT INTO prizes (app_id, app_public_id, app_user_id, prize_code, prize_title) VALUES (?, ?, ?, ?, ?)')
      .bind(appId, canonicalPublicId, appUserId, code, title)
      .run();

  } else if (type === 'game_submit' || type === 'game.submit') {
    const gameId = String((payload && (payload.game_id || payload.game)) || 'flappy');
    const mode = String((payload && payload.mode) || 'daily');
    const score = Number((payload && payload.score) || 0);
    const dur = Number((payload && (payload.duration_ms || payload.durationMs)) || 0);

    const dateStr = new Date().toISOString().slice(0, 10);

    // upsert best score
    // game_scores schema: app_public_id, tg_id, game_id, mode, date, score, duration_ms, updated_at
    // IMPORTANT: if the table is missing in some tenants, do not crash the whole public event endpoint.
    try {
      const existing: any = await db.prepare(
        `SELECT id, score FROM game_scores
         WHERE app_public_id = ? AND tg_id = ? AND game_id = ? AND mode = ? AND date = ?
         LIMIT 1`
      ).bind(canonicalPublicId, String(tgId), gameId, mode, dateStr).first();

      let bestScore = score;
      if (existing) {
        bestScore = Math.max(Number(existing.score || 0), score);
        await db.prepare(
          `UPDATE game_scores
           SET score = ?, duration_ms = ?, updated_at = datetime('now')
           WHERE id = ?`
        ).bind(bestScore, dur, existing.id).run();
      } else {
        await db.prepare(
          `INSERT INTO game_scores (app_id, app_public_id, tg_id, game_id, mode, date, score, duration_ms, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`
        ).bind(appId, canonicalPublicId, String(tgId), gameId, mode, dateStr, bestScore, dur).run();
      }
    } catch (e) {
      console.warn('[game_scores] upsert skipped (table missing?)', e);
    }

    // reward coins based on score (simple: 1 coin per 10 score)
    const coins = Math.max(0, Math.floor(score / 10));
    if (coins > 0) {
      await awardCoins(
        db,
        appId,
        canonicalPublicId,
        String(tgId),
        coins,
        'game_score',
        String(gameId),
        `game:${gameId}:${mode}:${dateStr}`,
        `game:submit:${canonicalPublicId}:${tgId}:${gameId}:${mode}:${dateStr}:${score}:${coins}`
      );
    }
  }

  return jsonResponse({ ok: true }, 200, request);
}

// ================== Sales token (QR) ==================

function randomSaleToken(lenBytes = 16) {
  const b = crypto.getRandomValues(new Uint8Array(lenBytes));
  return btoa(String.fromCharCode(...b))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/,'');
}

function saleTokKey(token: string) { return `sale_tok:${token}`; }

async function getSalesSettingsFromD1(publicId: string, env: Env): Promise<{ cashback_percent: number; ttl_sec: number; cashier_ids: string[] } | null> {
  try {
    const row: any = await env.DB.prepare(`
      SELECT cashback_percent, ttl_sec,
             cashier1_tg_id, cashier2_tg_id, cashier3_tg_id, cashier4_tg_id, cashier5_tg_id
      FROM sales_settings
      WHERE app_public_id = ?
      LIMIT 1
    `).bind(publicId).first();

    if (!row) return null;

    const ids = [row.cashier1_tg_id, row.cashier2_tg_id, row.cashier3_tg_id, row.cashier4_tg_id, row.cashier5_tg_id]
      .map((x: any) => (x ? String(x) : ''))
      .filter(Boolean);

    return {
      cashback_percent: Number(row.cashback_percent ?? 10),
      ttl_sec: Number(row.ttl_sec ?? 300),
      cashier_ids: ids,
    };
  } catch (e) {
    console.error('[sales_settings] read failed', e);
    return null;
  }
}

export async function handleSalesToken(publicId: string, request: Request, env: Env): Promise<Response> {
  let body: any = {};
  try { body = await request.json(); } catch (_e) {}

  const initData = body.init_data || body.initData || '';

  const ss = await getSalesSettingsFromD1(publicId, env);
  const ttl = Math.max(60, Math.min(600, Number(ss?.ttl_sec || 300)));

  const ctx = await resolveAppContextByPublicId(publicId, env);
  if (!ctx || !ctx.ok) {
    return jsonResponse({ ok: false, error: 'APP_NOT_FOUND' }, 404, request);
  }

  // bot token (compat: try by publicId + fallback)
  let botToken: string | null = null;
  botToken = await getBotTokenForApp((ctx.publicId as string) || publicId, env, (ctx.appId as string) || null);
  if (!botToken) {
    return jsonResponse({ ok: false, error: 'BOT_TOKEN_MISSING' }, 400, request);
  }

  const okSig = await verifyInitDataSignature(String(initData || ''), botToken);
  if (!okSig) {
    return jsonResponse({ ok: false, error: 'BAD_INITDATA' }, 403, request);
  }

  const u: any = parseInitDataUser(String(initData || ''));
  if (!u) {
    return jsonResponse({ ok: false, error: 'NO_USER' }, 400, request);
  }

  if (!env.BOT_SECRETS) {
    return jsonResponse({ ok: false, error: 'KV_MISSING(BOT_SECRETS)' }, 500, request);
  }

  const token = randomSaleToken(16);

  await env.BOT_SECRETS.put(
    saleTokKey(token),
    JSON.stringify({
      appId: ctx.appId,
      appPublicId: ctx.publicId || publicId,
      customerTgId: String(u.id),
      cashierIds: ss?.cashier_ids || [],
      cashbackPercent: Number(ss?.cashback_percent || 10),
      createdAt: Date.now(),
    }),
    { expirationTtl: ttl }
  );

  // bot username -> deep link
  let botUsername = '';
  try {
    const pid = String(publicId || (ctx && (ctx.publicId as string)) || '').trim();
    const b: any = await env.DB.prepare(`
      SELECT username FROM bots
      WHERE app_public_id = ? AND status = 'active'
      ORDER BY id DESC LIMIT 1
    `).bind(pid).first();

    botUsername = (b && b.username) ? String(b.username).replace(/^@/, '').trim() : '';
  } catch (e) {
    console.log('[sales/token] bot lookup error', e);
    botUsername = '';
  }

  const deep_link = botUsername ? `https://t.me/${botUsername}?start=sale_${token}` : '';

  return jsonResponse({ ok: true, token, ttl_sec: ttl, bot_username: botUsername, deep_link }, 200, request);
}
