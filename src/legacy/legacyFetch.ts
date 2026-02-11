import type { Env } from "../index";


// ================== ОСНОВНОЙ WORKER ==================
export async function legacyFetch(request: Request, env: any, ctx: ExecutionContext) {

    const url = new URL(request.url);
    if (url.pathname.startsWith('/blocks/')) {
      if (request.method === 'OPTIONS') {
        return new Response(null, { status: 204, headers: corsHeaders(request) });
      }
      if (request.method !== 'GET') {
        return new Response('Method Not Allowed', { status: 405, headers: corsHeaders(request) });
      }
      return handleBlocksProxy(request, env); // ✅ env, не url
    }
    const pathname = url.pathname;

    try {
      if (request.method === 'OPTIONS') {
        return new Response(null, { status: 204, headers: corsHeaders(request) });
      }

      // AUTH
      if (pathname === '/api/auth/register' && request.method === 'POST') {
        return handleRegister(request, env, url);
      }
      if (pathname === '/api/auth/login' && request.method === 'POST') {
        return handleLogin(request, env);
      }
      if (pathname === '/api/auth/logout' && request.method === 'POST') {
        return handleLogout(request);
      }
      if (pathname === '/api/auth/me' && request.method === 'GET') {
        return handleMe(request, env);
      }
      if (pathname === '/api/auth/confirm' && request.method === 'GET') {
        return handleConfirmEmail(url, env, request);
      }

      // Sales QR token (one-time)
const mSaleTok = pathname.match(/^\/api\/public\/app\/([^/]+)\/sales\/token$/);
if (mSaleTok && request.method === 'POST') {
  const publicId = decodeURIComponent(mSaleTok[1]);
  return handleSalesToken(publicId, request, env);
}

//pay
// Stars: create invoice link
const mStarsCreate = pathname.match(/^\/api\/public\/app\/([^/]+)\/stars\/create$/);
if (mStarsCreate && request.method === 'POST') {
  const publicId = decodeURIComponent(mStarsCreate[1]);
  return handleStarsCreate(publicId, request, env);
}

// Stars: get order status
const mStarsGet = pathname.match(/^\/api\/public\/app\/([^/]+)\/stars\/order\/([^/]+)$/);
if (mStarsGet && request.method === 'GET') {
  const publicId = decodeURIComponent(mStarsGet[1]);
  const orderId = decodeURIComponent(mStarsGet[2]);
  return handleStarsOrderGet(publicId, orderId, request, env);
}




      // Public events from miniapp
      const mEvent = pathname.match(/^\/api\/public\/app\/([^/]+)\/event$/);
      if (mEvent && request.method === 'POST') {
        const publicId = decodeURIComponent(mEvent[1]);
        return handlePublicEvent(publicId, request, env);
      }

      // Telegram webhook (Variant A): POST /api/tg/webhook/:publicId?s=...
const mTg = pathname.match(/^\/api\/tg\/webhook\/([^/]+)$/);
if (mTg && request.method === 'POST') {
  const publicId = decodeURIComponent(mTg[1]);
  return handleTelegramWebhook(publicId, request, env);
}


      /* ⬇️ ДОБАВИТЬ ВОТ ЭТО — мини-API для опубликованных мини-аппов */
if (pathname.startsWith('/api/mini/')) {
  return handleMiniApi(request, env, url);
}

// /app/:publicId (и /m/:publicId) — редиректим на публичный runtime на mini.salesgenius.ru
if (pathname.startsWith('/app/') || pathname.startsWith('/m/')) {
  const parts = pathname.split('/').filter(Boolean); // ['app', '<id>'] или ['m','<id>']
  const publicId = parts[1] || '';
  const target = 'https://mini.salesgenius.ru/m/' + encodeURIComponent(publicId);
  return Response.redirect(target, 302);
}


      // Root
      if (pathname === '/' && request.method === 'GET') {
        return new Response('build-apps worker is alive', { status: 200 });
      }

      // ===== APPS AUTH API =====
      // GET /api/templates — каталог шаблонов для создания проекта
      if (pathname === '/api/templates' && request.method === 'GET') {
        const s = await requireSession(request, env);
        if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
        return json({ ok:true, items: TEMPLATE_CATALOG }, 200, request);
      }

      // GET /api/my/apps
      if (pathname === '/api/my/apps' && request.method === 'GET') {
        const s = await requireSession(request, env);
        if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
        return listMyApps(env, s.uid, request);
      }

      // GET /api/apps  (alias for new sg-cabinet-react)
if (pathname === '/api/apps' && request.method === 'GET') {
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  return listMyApps(env, s.uid, request);
}


      // POST /api/app
      if (request.method === 'POST' && pathname === '/api/app') {
        const s = await requireSession(request, env);
        if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
        return createApp(request, env, url, s.uid);
      }

      // /api/app/:id GET/PUT
      const appMatch = pathname.match(/^\/api\/app\/([^/]+)$/);
      if (appMatch) {
        const appId = decodeURIComponent(appMatch[1]);
        const s = await requireSession(request, env);
        if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);

        const ownerCheck = await ensureAppOwner(appId, s.uid, env);
        if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

        if (request.method === 'GET')  return getApp(appId, env, request);
        if (request.method === 'PUT')  return saveApp(appId, request, env);
        if (request.method === 'DELETE') return deleteApp(appId, env, request);
      }

      // POST /api/app/:id/publish
      const pubMatch = pathname.match(/^\/api\/app\/([^/]+)\/publish$/);
      if (pubMatch && request.method === 'POST') {
        const appId = decodeURIComponent(pubMatch[1]);
        const s = await requireSession(request, env);
        if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);

        const ownerCheck = await ensureAppOwner(appId, s.uid, env);
        if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

        return publishApp(appId, env, url, request);
      }
      // /api/app/:id/bot GET/PUT/DELETE
      const botMatch = pathname.match(/^\/api\/app\/([^/]+)\/bot$/);
      if (botMatch) {
        const appId = decodeURIComponent(botMatch[1]);
        const s = await requireSession(request, env);
        if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);

        const ownerCheck = await ensureAppOwner(appId, s.uid, env);
        if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

        if (request.method === 'DELETE') {
          const body = await request.json().catch(() => ({}));
          if (body.action === 'unlink') {
            return deleteBotIntegration(appId, env, s.uid, request);
          }
          return json({ ok:false, error:'METHOD_NOT_ALLOWED' }, 405, request);
        }

        if (request.method === 'GET') {
          return getBotIntegration(appId, env, s.uid, request);
        }
        

        if (request.method === 'PUT') {
          const body = await request.json().catch(() => ({}));
          return saveBotIntegration(appId, env, body, s.uid, request);
        }

        return json({ ok:false, error:'METHOD_NOT_ALLOWED' }, 405, request);
      }

      // /api/app/:id/broadcasts GET  (список кампаний)
const bcListMatch = pathname.match(/^\/api\/app\/([^/]+)\/broadcasts$/);
if (bcListMatch && request.method === 'GET') {
  const appId = decodeURIComponent(bcListMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);

  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  return listBroadcasts(appId, env, s.uid, request);
}

// /api/app/:id/broadcast POST (создать + отправить)
const bcSendMatch = pathname.match(/^\/api\/app\/([^/]+)\/broadcast$/);
if (bcSendMatch && request.method === 'POST') {
  const appId = decodeURIComponent(bcSendMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);

  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  return createAndSendBroadcast(appId, env, s.uid, request);
}

// /api/app/:id/dialogs GET  (список диалогов)
const dlgListMatch = pathname.match(/^\/api\/app\/([^/]+)\/dialogs$/);
if (dlgListMatch && request.method === 'GET') {
  const appId = decodeURIComponent(dlgListMatch[1]);

  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);

  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  return listDialogs(appId, env, s.uid, request);
}

// /api/app/:id/dialog/:tgUserId  GET (сообщения) / POST (отправка)
const dlgMatch = pathname.match(/^\/api\/app\/([^/]+)\/dialog\/([^/]+)$/);
if (dlgMatch) {
  const appId = decodeURIComponent(dlgMatch[1]);
  const tgUserId = decodeURIComponent(dlgMatch[2]);


  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);

  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  if (request.method === 'GET')  return getDialogMessages(appId, tgUserId, env, s.uid, request);
  if (request.method === 'POST') return sendDialogMessage(appId, tgUserId, env, s.uid, request);

  return json({ ok:false, error:'METHOD_NOT_ALLOWED' }, 405, request);
}


  


//  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ СЕКРЕТНЫХ ТОКЕНОВ БОТОВ
// ======================================================

async function encryptToken(plain, masterKey) {
  const enc = new TextEncoder();
  const masterBytes = enc.encode(masterKey);
  const keyBytes = await crypto.subtle.digest('SHA-256', masterBytes);

  const key = await crypto.subtle.importKey(
    'raw',
    keyBytes,
    { name: 'AES-GCM' },
    false,
    ['encrypt']
  );

  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv },
    key,
    enc.encode(plain)
  );

  const buf = new Uint8Array(iv.byteLength + ciphertext.byteLength);
  buf.set(iv, 0);
  buf.set(new Uint8Array(ciphertext), iv.byteLength);

  return btoa(String.fromCharCode(...buf));
}

async function decryptToken(cipherText, masterKey) {
  const raw = Uint8Array.from(atob(cipherText), c => c.charCodeAt(0));
  const iv = raw.slice(0, 12);
  const data = raw.slice(12);

  const enc = new TextEncoder();
  const masterBytes = enc.encode(masterKey);
  const keyBytes = await crypto.subtle.digest('SHA-256', masterBytes);

  const key = await crypto.subtle.importKey(
    'raw',
    keyBytes,
    { name: 'AES-GCM' },
    false,
    ['decrypt']
  );

  const plain = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv },
    key,
    data
  );

  return new TextDecoder().decode(plain);
}

async function verifyInitDataSignature(initData, botToken) {
  if (!initData || !botToken) return false;

  const params = new URLSearchParams(initData);
  const hash = params.get('hash');
  if (!hash) return false;
  params.delete('hash');

  // data_check_string: key=value lines sorted by key
  const arr = [];
  for (const [k, v] of params.entries()) arr.push(`${k}=${v}`);
  arr.sort();
  const dataCheckString = arr.join('\n');

  const enc = new TextEncoder();

  // secret_key = HMAC_SHA256(key="WebAppData", data=bot_token)
  const webAppKey = await crypto.subtle.importKey(
    'raw',
    enc.encode('WebAppData'),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );

  const secretKeyBuf = await crypto.subtle.sign(
    'HMAC',
    webAppKey,
    enc.encode(botToken)
  );

  // calc_hash = HMAC_SHA256(key=secret_key, data=data_check_string)
  const secretKey = await crypto.subtle.importKey(
    'raw',
    secretKeyBuf,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );

  const sig = await crypto.subtle.sign(
    'HMAC',
    secretKey,
    enc.encode(dataCheckString)
  );

  const calcHash = Array.from(new Uint8Array(sig))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');

  return calcHash === hash.toLowerCase();
}


async function getBotTokenForApp(publicId, env, appIdFallback = null) {
  // Primary key: bot_token:public:<publicId>
  // Legacy fallback: bot_token:app:<appId> (if you still have old secrets)
  if (!env.BOT_SECRETS || !env.BOT_TOKEN_KEY) return null;

  const tryGet = async (key) => {
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



  // new canonical storage
  const tok1 = await tryGet("bot_token:public:" + publicId);
  if (tok1) return tok1;

  // legacy fallback (optional)
  if (appIdFallback) {
    const tok2 = await tryGet("bot_token:app:" + appIdFallback);
    if (tok2) return tok2;
  }

  return null;
}

// ================== TG WEBHOOK SECRET (Variant A) ==================

function timingSafeEqual(a, b) {
  a = String(a || '');
  b = String(b || '');
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i++) out |= (a.charCodeAt(i) ^ b.charCodeAt(i));
  return out === 0;
}

function randomSecret(len = 24) {
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

async function getBotWebhookSecretForPublicId(publicId, env) {
  if (!env.BOT_SECRETS || !env.BOT_TOKEN_KEY) return null;

  const raw = await env.BOT_SECRETS.get('bot_whsec:public:' + publicId);
  if (!raw) return null;

  let cipher = raw;
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object' && parsed.cipher) cipher = parsed.cipher;
  } catch (_) {}

  try {
    return await decryptToken(cipher, env.BOT_TOKEN_KEY);
  } catch (e) {
    console.error('[botWhSec] decrypt error for publicId', publicId, e);
    return null;
  }
}

async function ensureBotWebhookSecretForPublicId(publicId, env) {
  const existing = await getBotWebhookSecretForPublicId(publicId, env);
  if (existing) {
    return { secret: existing, created: false, kv_key: 'bot_whsec:public:' + publicId };
  }

  const secretPlain = randomSecret(24);
  const cipher = await encryptToken(secretPlain, env.BOT_TOKEN_KEY);

  const kvKey = 'bot_whsec:public:' + publicId;
  await env.BOT_SECRETS.put(kvKey, JSON.stringify({ cipher }));

  return { secret: secretPlain, created: true, kv_key: kvKey };
}


async function getCanonicalPublicIdForApp(appId, env){
  // 1) KV app:<appId> (самое быстрое)
  try {
    const raw = await env.APPS.get('app:' + appId);
    if (raw) {
      const obj = JSON.parse(raw);
      if (obj && obj.publicId) return String(obj.publicId);
    }
  } catch (_) {}

  // 2) fallback: D1 apps.public_id
  try {
    const row = await env.DB
      .prepare('SELECT public_id FROM apps WHERE id = ? LIMIT 1')
      .bind(appId)
      .first();
    if (row && row.public_id) return String(row.public_id);
  } catch (e) {
    console.error('[publicId] getCanonicalPublicIdForApp failed', e);
  }

  return null;
}

// =============== pay ===============
async function handleStarsCreate(publicId, request, env){
  // ожидаем JSON:
  // { tg_user:{id,username}, title, description, photo_url, items:[{product_id,title,stars,qty,meta?}] }
  let body = {};
  try { body = await request.json(); } catch(_){}

  const tg = body.tg_user || body.tg || {};
  if (!tg || !tg.id) return json({ ok:false, error:'NO_TG_USER_ID' }, 400, request);

  const items = Array.isArray(body.items) ? body.items : [];
  if (!items.length) return json({ ok:false, error:'NO_ITEMS' }, 400, request);

  // IMPORTANT: invoice will be issued by THIS app's owner bot (multi-tenant)
  const botToken = await getBotTokenForApp(publicId, env, null);
  if (!botToken) return json({ ok:false, error:'NO_BOT_TOKEN_FOR_APP' }, 400, request);

  // total stars
  let totalStars = 0;
  const normItems = items.map(it=>{
    const qty = Math.max(1, Math.floor(Number(it.qty || 1)));
    const stars = Math.max(1, Math.floor(Number(it.stars || 0)));
    const amount = qty * stars;
    totalStars += amount;
    return {
      product_id: String(it.product_id || it.id || ''),
      title: String(it.title || ''),
      qty, stars, amount,
      meta_json: it.meta ? JSON.stringify(it.meta) : null
    };
  });

  if (totalStars <= 0) return json({ ok:false, error:'BAD_TOTAL' }, 400, request);

  // create order
  const orderId = crypto.randomUUID();
  const title = String(body.title || 'Покупка');
  const description = String(body.description || 'Оплата в Telegram Stars');
  const photo_url = body.photo_url ? String(body.photo_url) : '';

  const invoicePayload = `order:${orderId}`; // must be stable

  await env.DB.prepare(`
    INSERT INTO stars_orders
      (id, app_public_id, tg_id, title, description, photo_url, total_stars, status, invoice_payload, created_at)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, 'created', ?, datetime('now'))
  `).bind(orderId, publicId, String(tg.id), title, description, photo_url, totalStars, invoicePayload).run();

  // items
  for (const it of normItems){
    if (!it.product_id) continue;
    await env.DB.prepare(`
      INSERT INTO stars_order_items
        (order_id, app_public_id, product_id, title, qty, stars, amount, meta_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(orderId, publicId, it.product_id, it.title, it.qty, it.stars, it.amount, it.meta_json).run();
  }

  // create invoice link
  const invoice_link = await tgCreateInvoiceLinkStars(botToken, {
    title, description, payload: invoicePayload, stars: totalStars, photo_url
  });

  await env.DB.prepare(`
    UPDATE stars_orders SET invoice_link = ? WHERE id = ? AND app_public_id = ?
  `).bind(invoice_link, orderId, publicId).run();

  return json({ ok:true, order_id: orderId, invoice_link, total_stars: totalStars }, 200, request);
}

async function handleStarsOrderGet(publicId, orderId, request, env){
  const row = await env.DB.prepare(`
    SELECT id, app_public_id, tg_id, total_stars, status, created_at, paid_at
    FROM stars_orders
    WHERE id = ? AND app_public_id = ?
    LIMIT 1
  `).bind(String(orderId), String(publicId)).first();

  if (!row) return json({ ok:false, error:'NOT_FOUND' }, 404, request);
  return json({ ok:true, order: row }, 200, request);
}



// =============== PUBLIC EVENTS (из мини-аппа в D1) ===============
async function handlePublicEvent(publicId, request, env) {
  let body;
  try {
    body = await request.json();
  } catch (e) {
    return json({ ok: false, error: 'BAD_JSON' }, 400, request);
  }

  const tg = body.tg_user || {};
  const type = body.type;
  const payload = body.payload || {};
  const initDataRaw = body.init_data || body.initData || null;

  if (!type) {
    return json({ ok: false, error: 'NO_TYPE' }, 400, request);
  }

  // publicId -> appId (KV)
  const map = await env.APPS.get('app:by_public:' + publicId, 'json');
  if (!map || !map.appId) {
    return json({ ok: false, error: 'UNKNOWN_PUBLIC_ID' }, 404, request);
  }
  const appId = map.appId;

  // Канонический public_id (единая истина)
  const canonicalPublicId = (await getCanonicalPublicIdForApp(appId, env)) || publicId;

  // Проверка подписи initData, если есть токен
  try {
    const botToken = await getBotTokenForApp(canonicalPublicId, env, appId);
    if (botToken) {
      if (!initDataRaw) {
        return json({ ok: false, error: 'NO_INIT_DATA' }, 403, request);
      }
      const ok = await verifyInitDataSignature(initDataRaw, botToken);
      if (!ok) {
        return json({ ok: false, error: 'BAD_SIGNATURE' }, 403, request);
      }
    }
  } catch (e) {
    console.error('[event] verifyInitData failed', e);
  }

  const tgId = tg.id ? String(tg.id) : null;
  if (!tgId) {
    return json({ ok: false, error: 'NO_TG_USER_ID' }, 400, request);
  }

  const db = env.DB;

  // app_users: ищем и пишем по app_public_id (НО app_id тоже обязателен из-за NOT NULL)
  let userRow = await db
    .prepare(
      'SELECT id, total_opens, total_spins, total_prizes FROM app_users WHERE app_public_id = ? AND tg_user_id = ?',
    )
    .bind(canonicalPublicId, tgId)
    .first();

  let appUserId;
  if (!userRow) {
    const ins = await db
      .prepare(
        'INSERT INTO app_users (app_id, app_public_id, tg_user_id, tg_username, first_seen, last_seen) VALUES (?, ?, ?, ?, datetime("now"), datetime("now"))',
      )
      .bind(appId, canonicalPublicId, tgId, tg.username || null)
      .run();
    appUserId = Number(ins.lastInsertRowid);
  } else {
    appUserId = userRow.id;
    await db
      .prepare(
        'UPDATE app_users SET tg_username = ?, last_seen = datetime("now") WHERE id = ?',
      )
      .bind(tg.username || null, appUserId)
      .run();
  }
  

  // events: пишем app_id (для джойнов) + app_public_id (канон)
  await db
    .prepare(
      'INSERT INTO events (app_id, app_public_id, app_user_id, type, payload) VALUES (?, ?, ?, ?, ?)',
    )
    .bind(appId, canonicalPublicId, appUserId, type, JSON.stringify(payload))
    .run();

  if (type === 'open') {
    await db
      .prepare('UPDATE app_users SET total_opens = total_opens + 1 WHERE id = ?')
      .bind(appUserId)
      .run();


    // ===== REFERRAL bind from start_param (t.me/... ?startapp=ref_123) =====
    try {
          let startParam = '';
          if (initDataRaw) {
            const p = new URLSearchParams(String(initDataRaw));
            startParam = String(p.get('start_param') || '');
          }
      
          // ref_5624722739
          if (startParam.startsWith('ref_')) {
            const refTgId = startParam.slice(4).trim();
      
            // не даём саморефералку
            if (refTgId && refTgId !== String(tgId)) {
              // 1) уже есть реферер для invitee? тогда не трогаем
              const ex = await db.prepare(
                `SELECT id FROM referrals
                 WHERE app_public_id = ? AND invitee_tg_id = ?
                 LIMIT 1`
              ).bind(canonicalPublicId, String(tgId)).first();
      
              if (!ex) {
                // 2) создаём запись
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
    await db
      .prepare('UPDATE app_users SET total_spins = total_spins + 1 WHERE id = ?')
      .bind(appUserId)
      .run();
  } else if (type === 'prize') {
    await db
      .prepare('UPDATE app_users SET total_prizes = total_prizes + 1 WHERE id = ?')
      .bind(appUserId)
      .run();

    const code = payload.code || null;
    const title = payload.title || null;

    // prizes: app_id + app_public_id
    await db
      .prepare(
        'INSERT INTO prizes (app_id, app_public_id, app_user_id, prize_code, prize_title) VALUES (?, ?, ?, ?, ?)',
      )
      .bind(appId, canonicalPublicId, appUserId, code, title)
      .run();
  }

  else if (type === 'game_submit' || type === 'game.submit') {
    const gameId = String((payload && (payload.game_id || payload.game)) || 'flappy');
    const mode   = String((payload && payload.mode) || 'daily');
    const score  = Number((payload && payload.score) || 0);
    const dur    = Number((payload && (payload.duration_ms || payload.durationMs)) || 0);

    const dateStr = new Date().toISOString().slice(0,10);

    const ex = await db.prepare(
      `SELECT id, best_score, plays, duration_ms_total
       FROM games_results_daily
       WHERE app_public_id = ? AND tg_id = ? AND date = ? AND mode = ?`
    ).bind(canonicalPublicId, tgId, dateStr, mode).first();

    if (!ex) {
      await db.prepare(
        `INSERT INTO games_results_daily
           (app_id, app_public_id, date, mode, tg_id, best_score, plays, duration_ms_total, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, 1, ?, datetime('now'))`
      ).bind(appId, canonicalPublicId, dateStr, mode, tgId, score, dur).run();
    } else {
      const best = Math.max(Number(ex.best_score || 0), score);

      await db.prepare(
        `UPDATE games_results_daily
         SET best_score = ?,
             plays = plays + 1,
             duration_ms_total = duration_ms_total + ?,
             updated_at = datetime('now')
         WHERE id = ?`
      ).bind(best, dur, ex.id).run();
    }

    // ===== начислить монеты за игру =====
    let coinsEarned = Number((payload && payload.coins) || 0);
    if (!Number.isFinite(coinsEarned)) coinsEarned = 0;
    coinsEarned = Math.max(0, Math.floor(coinsEarned));

    const MAX_COINS_PER_PLAY = 200;
    if (coinsEarned > MAX_COINS_PER_PLAY) coinsEarned = MAX_COINS_PER_PLAY;

    if (coinsEarned > 0) {
      try {
        // ✅ идемпотентность: один и тот же submit не начислится дважды
        const ledgerEventId =
          `game:${canonicalPublicId}:${tgId}:${dateStr}:${mode}:${gameId}:${score}:${dur}:${coinsEarned}`;

        await awardCoins(
          db,
          appId,
          canonicalPublicId,
          tgId,
          coinsEarned,
          'game',
          gameId,
          'Flappy +' + String(coinsEarned),
          ledgerEventId
        );
      } catch (e) {
        console.error('[game_submit] awardCoins failed', e);
      }
    } else {
      console.log('[game_submit] coinsEarned=0 payload.coins=', (payload && payload.coins));
    }
  }


  

  return json({ ok: true }, 200, request);
}


// ================================
// Templates (каталог 3–5 шаблонов)
// ================================
// Шаблоны живут в воркере для старта (дальше можно вынести в KV/D1).
const TEMPLATE_CATALOG = [
  { id: 'blank',        title: 'Пустой',        desc: 'Чистый проект с одной страницей' },
  { id: 'beer_club',     title: 'Craft Beer',   desc: 'Главная + Игры + Бонусы + Профиль' },
  { id: 'coffee_loyalty',title: 'Coffee',       desc: 'Лояльность + меню + карта' },
  { id: 'quiz_lead',     title: 'Квиз',         desc: 'Квиз-воронка для лидов' },
];

function getSeedConfig(templateId) {
  const id = (templateId || 'blank').toString();

  // helper: deep clone defaults + overrides
  const clone = (x)=> JSON.parse(JSON.stringify(x||{}));
  const rnd = ()=> Math.random().toString(36).slice(2,9);
  const mkId = ()=> 'b_' + rnd();

  // ВАЖНО: ключи блоков должны существовать в templates.js (BlockRegistry)
  // (promo, infoCardPlain, gamesList, infoCardChevron, spacer, beerHero, beerIntroSlider, beerStartList)
  const mkBlock = (key, defaults, overrides={})=>{
    const bid = mkId();
    return {
      ref: { id: bid, key, type: key },     // то что лежит в route.blocks[]
      props: { [bid]: { ...clone(defaults), ...clone(overrides) } } // то что лежит в BP.blocks[id]
    };
  };

  // базовая структура BP
  const base = {
    theme: {},
    nav: { routes: [{ path: '/', title: 'Главная', id: 'home', icon: 'home' }] },
    routes: [{ path: '/', id: 'home', title: 'Главная', blocks: [] }],
    blocks: {} // <--- обязательно
  };

  // helper: применить набор блоков на страницу
  const setRouteBlocks = (routePath, blocksArr)=>{
    const route = base.routes.find(r=>r.path===routePath);
    if(!route) return;
    route.blocks = blocksArr.map(b=>b.ref);
    blocksArr.forEach(b=> Object.assign(base.blocks, b.props));
  };

  // ===== templates nav/routes =====
  if (id === 'beer_club') {
    base.nav.routes = [
      { path: '/',          title: 'Главная',  id: 'home',      icon: 'home' },
      { path: '/play',      title: 'Играть',   id: 'play',      icon: 'game' },
      { path: '/bonuses',   title: 'Бонусы',   id: 'bonuses',   icon: 'gift' },
      { path: '/profile',   title: 'Профиль',  id: 'profile',   icon: 'user' },
    ];
    base.routes = base.nav.routes.map(r => ({ path: r.path, id: r.id, title: r.title, blocks: [] }));
  }

  if (id === 'coffee_loyalty') {
    base.nav.routes = [
      { path: '/',          title: 'Главная',     id: 'home',    icon: 'home' },
      { path: '/menu',      title: 'Меню',        id: 'menu',    icon: 'menu' },
      { path: '/loyalty',   title: 'Лояльность',  id: 'loyalty', icon: 'star' },
      { path: '/profile',   title: 'Профиль',     id: 'profile', icon: 'user' },
    ];
    base.routes = base.nav.routes.map(r => ({ path: r.path, id: r.id, title: r.title, blocks: [] }));
  }

  if (id === 'quiz_lead') {
    base.nav.routes = [
      { path: '/',        title: 'Квиз',      id: 'quiz',     icon: 'quiz' },
      { path: '/result',  title: 'Результат', id: 'result',   icon: 'check' },
    ];
    base.routes = base.nav.routes.map(r => ({ path: r.path, id: r.id, title: r.title, blocks: [] }));
  }

  // ===== seed blocks per template (рандомно, но различимо) =====
  // Дефолты мы НЕ тянем с фронта — просто задаём минимум props, остальное заполнится в редакторе
  if (id === 'blank') {
    const b1 = mkBlock('infoCardPlain',
      { icon:'', title:'Пустой проект', sub:'Добавь блоки из библиотеки', imgSide:'left', action:'none', link:'', sheet_id:'', sheet_path:'' }
    );
    const sp = mkBlock('spacer', { size: 12 });
    setRouteBlocks('/', [b1, sp]);
  }

  if (id === 'coffee_loyalty') {
    const promo = mkBlock('promo', { interval: 3200, slides: [
      { img:'', action:'link', link:'#menu', sheet_id:'', sheet_path:'' },
      { img:'', action:'link', link:'#loyalty', sheet_id:'', sheet_path:'' },
      { img:'', action:'link', link:'#profile', sheet_id:'', sheet_path:'' }
    ]});
    const card = mkBlock('infoCardPlain',
      { icon:'beer/img/beer_hero.jpg', title:'Coffee', sub:'Шаблон Coffee', imgSide:'left', action:'none', link:'', sheet_id:'', sheet_path:'' }
    );
    setRouteBlocks('/', [promo, card]);

    const menu = mkBlock('gamesList', { title:'Меню (демо)', cards: [
      { icon:'beer/img/game1.png', title:'Эспрессо', sub:'120₽', btn:'Добавить', action:'none', link:'', sheet_id:'', sheet_path:'' },
      { icon:'beer/img/game2.png', title:'Капучино', sub:'180₽', btn:'Добавить', action:'none', link:'', sheet_id:'', sheet_path:'' }
    ]});
    setRouteBlocks('/menu', [menu]);

    const loyalty = mkBlock('infoCardChevron',
      { icon:'beer/img/beer_hero.jpg', title:'Лояльность', sub:'Штампы / баллы', action:'none', link:'#', sheet_id:'', sheet_path:'' }
    );
    setRouteBlocks('/loyalty', [loyalty]);
  }

  if (id === 'beer_club') {
    const hero = mkBlock('beerHero', { title:'Craft Beer Club', text:'Шаблон Beer', img:'beer/img/beer_hero.jpg' });
    const intro = mkBlock('beerIntroSlider', { slides: [
      { title:'Как работает', text:'Играй и копи монеты', primary:'Продолжить', ghost:'' },
      { title:'Погнали', text:'Первый спин — подарок', primary:'Играть', ghost:'' }
    ]});
    const start = mkBlock('beerStartList', { title:'С чего начать' });
    setRouteBlocks('/', [hero, intro, start]);

    const play = mkBlock('gamesList', { title:'Игры', cards: [
      { icon:'beer/img/game1.png', title:'Bumblebee', sub:'Долети — получи приз', btn:'Играть', action:'link', link:'#play_bumble', sheet_id:'', sheet_path:'' }
    ]});
    setRouteBlocks('/play', [play]);

    const wheel = mkBlock('bonus_wheel_one', { title:'Колесо бонусов', spin_cost: 10, prizes: [] });
    setRouteBlocks('/bonuses', [wheel]);
  }

  if (id === 'quiz_lead') {
    const h = mkBlock('infoCardChevron', { icon:'', title:'Квиз', sub:'Шаблон Quiz', action:'none', link:'#', sheet_id:'', sheet_path:'' });
    const sp = mkBlock('spacer', { size: 10 });
    const c = mkBlock('infoCardPlain', { icon:'', title:'Вопрос 1', sub:'Пока демо', imgSide:'right', action:'none', link:'', sheet_id:'', sheet_path:'' });
    setRouteBlocks('/', [h, sp, c]);

    const res = mkBlock('infoCardPlain', { icon:'', title:'Результат', sub:'Спасибо! Мы свяжемся.', imgSide:'left', action:'none', link:'', sheet_id:'', sheet_path:'' });
    setRouteBlocks('/result', [res]);
  }

  return base;
}


// ================== HELPERS: CORS / JSON ==================
// ВАЖНО: corsHeaders должен существовать в той же области видимости, где вызывается json().
// Поэтому держим всё рядом и без "const corsHeaders = ..." (чтобы не поймать TDZ/ReferenceError при бандлинге).

const CORS_ALLOW = new Set([
  "https://app.salesgenius.ru",
  "https://mini.salesgenius.ru",
  "https://blocks.salesgenius.ru",   // CDN с блоками (Pages/GH Pages)
  "https://ru.salesgenius.ru",
  'https://apps.salesgenius.ru',       // GH Pages (RU зеркало)
  // Telegram WebApp (desktop / mobile)
  "https://web.telegram.org",
  "https://web.telegram.org/k",
  "https://web.telegram.org/z",
  
]);


function corsHeaders(request) {
  const origin = request?.headers?.get("Origin") || "";
  if (!origin) return { "Vary": "Origin" }; // нет Origin — не CORS

  if (!CORS_ALLOW.has(origin)) return { "Vary": "Origin" };

  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": request.headers.get("Access-Control-Request-Headers") || "Content-Type, Authorization",
    "Vary": "Origin",
  };
}


function json(obj, status = 200, request = null) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "Content-Type": "application/json; charset=UTF-8",
      ...corsHeaders(request),
    },
  });
}






// ================== BLOCKS PROXY (/blocks/*) ==================

const BLOCKS_UPSTREAMS = [
  // ЕДИНСТВЕННЫЙ корректный апстрим для блоков/манифестов
  'https://blocks.salesgenius.ru/sg-blocks/'
];

// ВРЕМЕННО: выключаем кэш для /blocks/*
const BLOCKS_CACHE_DISABLED = true;

// небольшая подмога: пробуем несколько апстримов по очереди
// (сейчас фактически 1, но оставляем цикл на будущее)
async function fetchFromUpstreams(pathRel, request) {
  let lastErr = null;

  // нормализуем относительный путь
  const rel = String(pathRel || '').replace(/^\//, '');

  // манифесты не кэшируем надолго — иначе “прыгают версии / не обновляется”
  const isManifest =
    rel.endsWith('index.json') ||
    rel.includes('/index.json') ||
    rel.endsWith('manifest.json') ||
    rel.includes('/manifest.json');

  // TTL: манифест короткий, ассеты — длинный
  const EDGE_TTL_OK = isManifest ? 60 : 86400;      // edge cache
  const BROWSER_TTL_OK = isManifest ? 60 : 86400;   // Cache-Control для клиента

  for (const base of BLOCKS_UPSTREAMS) {
    try {
      const u = new URL(rel, base);

      // Edge cache key (только GET)
      const cacheKey = new Request(u.toString(), { method: 'GET' });
      const cache = caches.default;

      // 1) try cache (только если кэш НЕ отключён)
      if (!BLOCKS_CACHE_DISABLED) {
        const cached = await cache.match(cacheKey);
        if (cached) {
          // добавим диагностику, чтобы ты в DevTools видел что пришло из кэша
          const resp = new Response(cached.body, cached);
          resp.headers.set('X-SG-Upstream', base);
          resp.headers.set('X-SG-Cache', 'HIT');
          return resp;
        }
      }

      // 2) fetch (с хинтами CF кэша только если кэш НЕ отключён)
      const r = await fetch(u.toString(), {
        method: 'GET',
        // полезно передать исходные заголовки accept/if-none-match (не обязательно)
        headers: {
          'Accept': request.headers.get('Accept') || '*/*',
          ...(BLOCKS_CACHE_DISABLED ? { 'Cache-Control': 'no-cache' } : {})
        },
        cf: BLOCKS_CACHE_DISABLED
          ? {
              cacheEverything: false,
              cacheTtl: 0,
              cacheTtlByStatus: {
                "200-299": 0,
                "404": 0,
                "500-599": 0
              }
            }
          : {
              cacheEverything: true,
              cacheTtl: EDGE_TTL_OK,
              cacheTtlByStatus: {
                "200-299": EDGE_TTL_OK,
                "404": 60,
                "500-599": 0
              }
            }
      });

      // 404: смысла фолбэкать обычно нет — сразу отдаём
      if (r.status === 404) {
        const resp404 = new Response(r.body, r);
        resp404.headers.set('X-SG-Upstream', base);
        resp404.headers.set('X-SG-Cache', BLOCKS_CACHE_DISABLED ? 'BYPASS' : 'MISS');
        if (BLOCKS_CACHE_DISABLED) {
          resp404.headers.set('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
          resp404.headers.set('Pragma', 'no-cache');
          resp404.headers.set('Expires', '0');
        }
        return resp404;
      }

      // кэшируем только удачные ответы
      if (r.ok) {
        // защита от “поймали HTML вместо JSON” (бывает при неправильном апстриме/редиректе)
        // для манифестов проверяем Content-Type
        if (isManifest) {
          const ct = (r.headers.get('content-type') || '').toLowerCase();
          if (ct && !ct.includes('application/json') && !ct.includes('text/json')) {
            // не кэшируем и пробуем другой апстрим (если когда-то появится)
            lastErr = new Error(`Manifest looks non-JSON: ${ct} from ${base}`);
            continue;
          }
        }

        const resp = new Response(r.body, r);

        if (BLOCKS_CACHE_DISABLED) {
          // клиентский кеш-контроль: запрещаем кэш
          resp.headers.set('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
          resp.headers.set('Pragma', 'no-cache');
          resp.headers.set('Expires', '0');
          resp.headers.set('X-SG-Cache', 'BYPASS');
        } else {
          // клиентский кеш-контроль
          resp.headers.set('Cache-Control', `public, max-age=${BROWSER_TTL_OK}`);
          resp.headers.set('X-SG-Cache', 'MISS');

          // кладём в edge cache
          await cache.put(cacheKey, resp.clone());
        }

        // диагностика
        resp.headers.set('X-SG-Upstream', base);

        return resp;
      }

      // если апстрим ответил ошибкой — пробуем следующий (если будет)
      lastErr = new Error(`Upstream ${base} responded ${r.status}`);
    } catch (e) {
      lastErr = e;
    }
  }

  throw lastErr || new Error('No upstreams');
}

async function handleBlocksProxy(request, env) {
  const url = new URL(request.url);

  // /blocks/<rel>
  let rel = url.pathname.replace(/^\/blocks\/+/, '');
  rel = rel.replace(/^\/+/, '');
  // если фронт случайно прислал /blocks/blocks/blocks/...
  if (rel.startsWith('blocks/blocks/')) {
    rel = rel.replace(/^blocks\//, ''); // станет blocks/...
  }

  // пробуем несколько апстримов по очереди
  const upstreamResp = await fetchFromUpstreams(rel, request);

  // копируем заголовки и добавляем CORS
  const headers = new Headers(upstreamResp.headers);
  const ch = corsHeaders(request);
  for (const [k, v] of Object.entries(ch)) headers.set(k, v);

  // ВАЖНО: не перебиваем заголовки, если мы отключили кэш
  if (BLOCKS_CACHE_DISABLED) {
    headers.set('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
    headers.set('Pragma', 'no-cache');
    headers.set('Expires', '0');
  } else {
    // (опционально) более агрессивный кэш для статики блоков
    if (!headers.has('Cache-Control')) headers.set('Cache-Control', 'public, max-age=300');
  }

  return new Response(upstreamResp.body, {
    status: upstreamResp.status,
    headers
  });
}




// ================== AUTH: email + password ==================

// простой base64url для JWT-подобных токенов
function base64UrlEncode(bytes) {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  let base64 = btoa(binary);
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlDecode(str) {
  let base64 = str.replace(/-/g, "+").replace(/_/g, "/");
  const pad = base64.length % 4;
  if (pad) {
    base64 += "=".repeat(4 - pad);
  }
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function randomToken(len = 32) {
  const buf = new Uint8Array(len);
  crypto.getRandomValues(buf);
  return base64UrlEncode(buf);
}

async function hashPassword(password, salt) {
  const enc = new TextEncoder();
  const data = enc.encode(salt + ":" + password);
  const hash = await crypto.subtle.digest("SHA-256", data);
  const bytes = new Uint8Array(hash);
  let hex = "";
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i].toString(16).padStart(2, "0");
  }
  return hex;
}

function generateSalt(len = 16) {
  const buf = new Uint8Array(len);
  crypto.getRandomValues(buf);
  let hex = "";
  for (let i = 0; i < buf.length; i++) {
    hex += buf[i].toString(16).padStart(2, "0");
  }
  return hex;
}

function randomHex(len = 32) {
  const bytesCount = Math.ceil(len / 2);
  const arr = new Uint8Array(bytesCount);
  crypto.getRandomValues(arr);
  return Array.from(arr, b => b.toString(16).padStart(2, '0')).join('').slice(0, len);
}

async function signToken(payload, secret) {
  const enc = new TextEncoder();
  const header = { alg: "HS256", typ: "JWT" };
  const headerBytes = enc.encode(JSON.stringify(header));
  const payloadBytes = enc.encode(JSON.stringify(payload));

  const headerB64 = base64UrlEncode(headerBytes);
  const payloadB64 = base64UrlEncode(payloadBytes);

  const data = headerB64 + "." + payloadB64;

  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(data));
  const sigB64 = base64UrlEncode(new Uint8Array(sig));

  return data + "." + sigB64;
}

async function verifyToken(token, secret) {
  if (!token) return null;
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  const [headerB64, payloadB64, sigB64] = parts;
  const enc = new TextEncoder();
  const data = headerB64 + "." + payloadB64;

  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const expectedSig = await crypto.subtle.sign("HMAC", key, enc.encode(data));
  const expectedSigB64 = base64UrlEncode(new Uint8Array(expectedSig));

  if (expectedSigB64 !== sigB64) return null;

  try {
    const bytes = base64UrlDecode(payloadB64);
    const jsonStr = new TextDecoder().decode(bytes);
    const payload = JSON.parse(jsonStr);
    if (payload.exp && Math.floor(Date.now() / 1000) > payload.exp) {
      return null;
    }
    return payload;
  } catch (e) {
    console.error("[auth] verifyToken JSON error", e);
    return null;
  }
}

function getSessionTokenFromRequest(request) {
  const cookie = request.headers.get("Cookie") || "";
  const m = cookie.match(/(?:^|;\s*)sg_session=([^;]+)/);
  return m ? m[1] : null;
}

async function getSession(request, env) {
  const secret = env.SESSION_SECRET;
  if (!secret || secret.length < 16) {
    return null;
  }
  const token = getSessionTokenFromRequest(request);
  if (!token) return null;
  try {
    return await verifyToken(token, secret);
  } catch (e) {
    console.error("[auth] getSession verify error", e);
    return null;
  }
}

async function requireSession(request, env){
  const s = await getSession(request, env);
  if (!s || !s.uid) return null;
  return s;
}

async function createSessionCookie(user, env) {
  const secret = env.SESSION_SECRET;
  if (!secret || secret.length < 16) {
    console.error("[auth] SESSION_SECRET missing or too short");
    return null;
  }
  const now = Math.floor(Date.now() / 1000);
  const exp = now + 60 * 60 * 24 * 30;
  const payload = {
    uid: user.id,
    email: user.email,
    exp,
  };
  const token = await signToken(payload, secret);
  const maxAge = 60 * 60 * 24 * 30;
  return "sg_session=" + token
  + "; Path=/; Domain=.salesgenius.ru; HttpOnly; Secure; SameSite=None; Max-Age=" + maxAge;

}

async function sendVerificationEmail(email, confirmUrl, env) {
  console.log("[auth] sendVerificationEmail to", email, "url:", confirmUrl);
}

// POST /api/auth/register
async function handleRegister(request, env, url) {
  const db = env.DB;

  let body;
  try {
    body = await request.json();
  } catch (e) {
    return json({ ok: false, error: 'BAD_JSON' }, 400, request);
  }

  const email = String(body.email || '').trim().toLowerCase();
  const password = String(body.password || '').trim();

  if (!email || !email.includes('@') || !password || password.length < 6) {
    return json({ ok: false, error: 'BAD_INPUT' }, 400, request);
  }

  let existing = await db
    .prepare('SELECT id, is_verified FROM users WHERE email = ?')
    .bind(email)
    .first();

  let userId;

  if (!existing) {
    const salt = generateSalt();
    const hash = await hashPassword(password, salt);

    await db
      .prepare(
        `INSERT INTO users (email, password_hash, salt, is_verified, created_at, updated_at)
         VALUES (?, ?, ?, 0, datetime('now'), datetime('now'))`
      )
      .bind(email, hash, salt)
      .run();

    existing = await db
      .prepare('SELECT id, is_verified FROM users WHERE email = ?')
      .bind(email)
      .first();

    if (!existing || !existing.id) {
      console.error('[auth] cannot reselect user after insert for', email, existing);
      return json({ ok: false, error: 'NO_USER_ID' }, 500, request);
    }

    userId = existing.id;
  } else {
    userId = existing.id;
    if (existing.is_verified) {
      return json({ ok: false, error: 'ALREADY_VERIFIED' }, 400, request);
    }
  }

  const token = randomHex(32);

  await db
    .prepare(
      `INSERT INTO email_verifications (user_id, token, created_at, expires_at)
       VALUES (?, ?, datetime('now'), datetime('now', '+1 day'))`
    )
    .bind(userId, token)
    .run();

  const base = url.origin;
  const confirmUrl = `${base}/api/auth/confirm?token=${encodeURIComponent(token)}`;

  await sendVerificationEmail(email, confirmUrl, env);

  return json({ ok: true, needVerify: true }, 200, request);
}

// GET /api/auth/confirm?token=...
async function handleConfirmEmail(url, env, request) {
  const db = env.DB;
  if (!db) {
    return json({ ok: false, error: "NO_DB" }, 500, request);
  }

  const token = (url.searchParams.get("token") || "").trim();
  if (!token) {
    return json({ ok: false, error: "NO_TOKEN" }, 400, request);
  }

  const row = await db
    .prepare(
      "SELECT ev.id, ev.user_id " +
      "FROM email_verifications ev " +
      "JOIN users u ON u.id = ev.user_id " +
      "WHERE ev.token = ? " +
      "  AND ev.used_at IS NULL " +
      "  AND ev.expires_at > datetime('now') " +
      "ORDER BY ev.created_at DESC " +
      "LIMIT 1"
    )
    .bind(token)
    .first();

  if (!row) {
    return json({ ok: false, error: "TOKEN_INVALID_OR_EXPIRED" }, 400, request);
  }

  await db
    .prepare(
      "UPDATE users SET is_verified = 1, updated_at = datetime('now') WHERE id = ?"
    )
    .bind(row.user_id)
    .run();

  await db
    .prepare(
      "UPDATE email_verifications SET used_at = datetime('now') WHERE id = ?"
    )
    .bind(row.id)
    .run();

  return json({ ok: true }, 200, request);
}

// POST /api/auth/login
async function handleLogin(request, env) {
  const db = env.DB;
  if (!db) {
    return json({ ok: false, error: "NO_DB" }, 500, request);
  }

  let body;
  try {
    body = await request.json();
  } catch (e) {
    return json({ ok: false, error: "BAD_JSON" }, 400, request);
  }

  let email = (body.email || "").trim().toLowerCase();
  const password = String(body.password || "");

  if (!email || !password) {
    return json({ ok: false, error: "EMAIL_OR_PASSWORD_MISSING" }, 400, request);
  }

  const user = await db
    .prepare(
      "SELECT id, email, password_hash, salt, is_verified " +
      "FROM users WHERE email = ?"
    )
    .bind(email)
    .first();

  if (!user || !user.password_hash || !user.salt) {
    return json({ ok: false, error: "BAD_CREDENTIALS" }, 400, request);
  }

  const hash = await hashPassword(password, user.salt);
  if (hash !== user.password_hash) {
    return json({ ok: false, error: "BAD_CREDENTIALS" }, 400, request);
  }

  // if (!user.is_verified) {
  //   return json({ ok: false, error: "EMAIL_NOT_VERIFIED" }, 400, request);
  // }

  const cookie = await createSessionCookie(
    { id: user.id, email: user.email },
    env
  );
  if (!cookie) {
    return json({ ok: false, error: "NO_SESSION_SECRET" }, 500, request);
  }

  const resp = json({ ok: true, userId: user.id, email: user.email }, 200, request);
  resp.headers.append("Set-Cookie", cookie);
  return resp;
}

// POST /api/auth/logout
async function handleLogout(request) {
  const resp = json({ ok: true }, 200, request);
  resp.headers.append(
    "Set-Cookie",
    "sg_session=; Path=/; Domain=.salesgenius.ru; HttpOnly; Secure; SameSite=None; Max-Age=0"
  );
  
  return resp;
}

// GET /api/auth/me
async function handleMe(request, env) {
  const db = env.DB;
  if (!db) {
    return json({ ok: false, error: "NO_DB" }, 500, request);
  }
  const session = await getSession(request, env);
  if (!session) {
    return json({ ok: true, authenticated: false }, 200, request);
  }

  const user = await db
    .prepare("SELECT id, email, is_verified, created_at FROM users WHERE id = ?")
    .bind(session.uid)
    .first();

  if (!user) {
    return json({ ok: true, authenticated: false }, 200, request);
  }

  return json({
    ok: true,
    authenticated: true,
    user: {
      id: user.id,
      email: user.email,
      is_verified: !!user.is_verified,
      created_at: user.created_at,
    },
  }, 200, request);
}

// ================== APPS: D1 + KV ==================

function slugify(input, maxLen = 64){
  const s = String(input || '').trim().toLowerCase();
  const out = s
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, maxLen);
  return out;
}

async function ensureAppOwner(appId, ownerId, env){
  const row = await env.DB.prepare(
    `SELECT id, owner_id FROM apps WHERE id = ?`
  ).bind(appId).first();
  if (!row) return { ok:false, status:404 };
  if (Number(row.owner_id) !== Number(ownerId)) return { ok:false, status:403 };
  return { ok:true };
}

// список мини-аппов для кабинета
async function listMyApps(env, ownerId, request) {
  const res = await env.DB
    .prepare(
      `SELECT id, owner_id, title, public_id, status, created_at, updated_at, last_published_at
       FROM apps
       WHERE owner_id = ?
       ORDER BY created_at DESC`,
    )
    .bind(ownerId)
    .all();

  const rows = res.results || [];
  return json({ ok: true, apps: rows }, 200, request);
}

// POST /api/app — создать новый мини-апп (D1 row + KV config)
async function createApp(request, env, url, ownerId) {
  const body = await request.json().catch(() => ({}));
  const title = String(body.title || 'New mini-app').trim() || 'New mini-app';

  // slug/id rules:
  // - if body.slug absent or equals 'auto' -> generate unique id from title + short suffix
  // - if slug provided explicitly -> use it as-is (after slugify) and return 409 if taken
  const rawSlug = (body.slug ?? body.id ?? body.appId ?? '').toString().trim();
  const explicit = !!rawSlug && rawSlug.toLowerCase() !== 'auto';

  const base = slugify(explicit ? rawSlug : title, 42);
  if (!base) return json({ ok:false, error:'INVALID_SLUG' }, 400, request);

  let appId = base;
  const tryInsert = async () => {
    // public_id should also be unique-ish; include random
    const publicId = 'app-' + appId + '-' + Math.random().toString(36).slice(2, 6);

    const templateId = body.template_id || body.templateId || null;
    const config = body.config || getSeedConfig(templateId);

    // 1) D1 INSERT
    await env.DB.prepare(
      `INSERT INTO apps (id, owner_id, title, public_id, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, 'draft', datetime('now'), datetime('now'))`
    ).bind(appId, ownerId, title, publicId).run();

    // 2) KV
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

    await env.APPS.put('app:' + appId, JSON.stringify(appObj));
    await env.APPS.put('app:by_public:' + publicId, JSON.stringify({ appId }));

    const publicUrl = 'https://mini.salesgenius.ru/m/' + publicId;
    return json({ ok:true, id: appId, publicId, title, publicUrl }, 200, request);
    
  };

  if (explicit) {
    try {
      return await tryInsert();
    } catch (e) {
      // id уже занят (или другая constraint ошибка)
      return json({ ok:false, error:'APP_ALREADY_EXISTS' }, 409, request);
    }
  }

  // auto: retry with suffix if conflict
  for (let i = 0; i < 8; i++) {
    if (i > 0) appId = `${base}_${Math.random().toString(36).slice(2, 6)}`;
    try {
      return await tryInsert();
    } catch (e) {
      // only retry on "UNIQUE constraint failed" / "constraint"
      const msg = (e && (e.message || String(e))) || '';
      if (!/constraint|unique/i.test(msg)) {
        return json({ ok:false, error:'DB_ERROR', message: msg }, 500, request);
      }
    }
  }
  return json({ ok:false, error:'APP_ALREADY_EXISTS' }, 409, request);
}

async function getApp(appId, env, request) {
  const raw = await env.APPS.get('app:' + appId);
  if (!raw) return json({ ok:false, error:'NOT_FOUND' }, 404, request);

  const appObj = JSON.parse(raw);

  // Backward/forward compatible payload: some фронты ждут data.app.*, другие — плоские поля.
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
async function saveApp(appId, request, env) {
  const body = await request.json().catch(() => ({}));
  const newConfig = body.config;
  const newTitle = body.title;

  let appObj;
  const raw = await env.APPS.get('app:' + appId);

  if (!raw) {
    return json({ ok:false, error:'NOT_FOUND' }, 404, request);
  } else {
    appObj = JSON.parse(raw);
    if (newConfig) appObj.config = newConfig;
    if (typeof newTitle === 'string' && newTitle.trim()) appObj.title = newTitle.trim();
    appObj.updatedAt = new Date().toISOString();
  }

  await env.APPS.put('app:' + appId, JSON.stringify(appObj));

  // обновим title в D1 (чтобы кабинет/селектор видел)
  if (typeof newTitle === 'string' && newTitle.trim()) {
    await env.DB.prepare(
      `UPDATE apps SET title = ?, updated_at = datetime('now') WHERE id = ?`
    ).bind(newTitle.trim(), appId).run();
  } else {
    await env.DB.prepare(
      `UPDATE apps SET updated_at = datetime('now') WHERE id = ?`
    ).bind(appId).run();
  }

    // === SALES SETTINGS SYNC (как у колеса, но на SAVE) ===
    try{
      const salesCfg = extractSalesSettingsFromBlueprint(appObj.config || null);
      await upsertSalesSettings(appId, appObj.publicId, salesCfg, env);
    }catch(e){
      console.error('[sales_settings] sync on save failed', e);
    }
  

  return json({ ok: true }, 200, request);
}

// DELETE /api/app/:id — удалить проект полностью (D1 + KV)
async function deleteApp(appId, env, request) {
  // 1) достаём publicId из KV (если есть)
  let publicId = null;
  const raw = await env.APPS.get('app:' + appId);
  if (raw) {
    try { publicId = JSON.parse(raw)?.publicId || null; } catch(_){ publicId = null; }
  }

  // 2) KV remove
  await env.APPS.delete('app:' + appId);
  if (publicId) {
    await env.APPS.delete('app:by_public:' + publicId);
  }

  // 3) D1 remove (таблица apps)
  await env.DB.prepare('DELETE FROM apps WHERE id = ?').bind(appId).run();

  // (опционально) каскадные данные. Пока оставим — таблицы app_users/events/prizes
  // можно чистить отдельно или по расписанию.

  return json({ ok: true }, 200, request);
}

// Публикация: KV + D1 + publicUrl на preview

function extractRuntimeConfigFromBlueprint(BP){
  // Extract wheel/passport/profile runtime config from constructor blueprint.
  // IMPORTANT: In Studio, block instances in routes are lightweight ({id,key,type}),
  // and props are stored in BP.blocks[blockId]. Some older blueprints may also keep inst.props.
  const cfg = {
    wheel: { spin_cost:0, claim_cooldown_h:24, daily_limit:0, prizes:[] },
    passport: {
      require_pin:false,
      collect_coins:0,
      grid_cols:3,
      styles:[],
      reward_prize_code:'',   // ✅ NEW
      passport_key:'default'  // (опционально)
    },
    profile_quiz: { coins_per_correct:0, max_per_submit:0 },
    leaderboard: { top_n:10 }
  };

  const routes = Array.isArray(BP && BP.routes) ? BP.routes : [];
  const blocksDict = (BP && BP.blocks && typeof BP.blocks === 'object') ? BP.blocks : {};

  const insts = [];
  try{
    for (const rt of routes){
      const blocks = (rt && Array.isArray(rt.blocks)) ? rt.blocks : [];
      for (const b of blocks) insts.push(b);
    }
  }catch(_){}

  const getProps = (inst)=>{
    if (!inst) return {};
    if (inst.props && typeof inst.props === 'object') return inst.props;
    const id = inst.id != null ? String(inst.id) : '';
    const p = id && blocksDict[id];
    return (p && typeof p === 'object') ? p : {};
  };

  // Wheel
  const wheel = insts.find(b => b && (b.key==='bonus_wheel_one' || b.type==='bonus_wheel_one'));
  if (wheel){
    const p = getProps(wheel);
    cfg.wheel.spin_cost        = Number(p.spin_cost || 0);
    cfg.wheel.claim_cooldown_h = Number(p.claim_cooldown_h || 24);
    cfg.wheel.daily_limit      = Number(p.daily_limit || 0);

// allow both shapes: prizes[] or sectors[]
const arr = Array.isArray(p.prizes) ? p.prizes : (Array.isArray(p.sectors) ? p.sectors : []);

cfg.wheel.prizes = arr.map(pr => {
  const wRaw = Number(pr && pr.weight);
  const cRaw = Number(pr && pr.coins);

  return ({
    code:   String((pr && pr.code) || '').trim(),
    // берём name из Studio, но поддерживаем title для совместимости
    title:  String((pr && (pr.title || pr.name || pr.code)) || '').trim(),

    // ВАЖНО: 0 — валидно (никогда), но NaN/undefined -> 1
    weight: Number.isFinite(wRaw) ? Math.max(0, Math.round(wRaw)) : 1,

    // coins: 0 валидно, NaN -> 0
    coins:  Number.isFinite(cRaw) ? Math.max(0, Math.round(cRaw)) : 0,

    active: (pr && pr.active === false) ? false : true
  });
}).filter(x => x.code);

  }

  // Styles passport
  const passp = insts.find(b => b && (b.key==='styles_passport_one' || b.type==='styles_passport_one'));
  if (passp){
    const p = getProps(passp);
    cfg.passport.require_pin   = !!p.require_pin;
    cfg.passport.collect_coins = Number(p.collect_coins || 0);
    if (isFinite(p.grid_cols)) cfg.passport.grid_cols = Number(p.grid_cols);

        // ✅ NEW: привязка приза паспорта к призам колеса (wheel_prizes.code)
        cfg.passport.reward_prize_code = String(p.reward_prize_code || '').trim();

        // (опционально) ключ паспорта, если потом захочешь несколько паспортов
        if (p.passport_key !== undefined) cfg.passport.passport_key = String(p.passport_key || 'default').trim();
    

    const arr = Array.isArray(p.styles) ? p.styles : [];
    cfg.passport.styles = arr.map(s => ({
      code:   String((s && s.code) || '').trim(),
      name:   String((s && (s.name || s.code)) || '').trim(),
      active: (s && s.active === false) ? false : true
    })).filter(x=>x.code);
  }

  // Profile quiz
  const prof = insts.find(b => b && (b.key==='profile' || b.type==='profile'));
  if (prof){
    const p = getProps(prof);
    if (isFinite(p.coins_per_correct)) cfg.profile_quiz.coins_per_correct = Number(p.coins_per_correct);
    if (isFinite(p.max_per_submit))    cfg.profile_quiz.max_per_submit    = Number(p.max_per_submit);
  }

  return cfg;
}

// helper-парсер кассиров и извлечение sales settings из blueprint
function extractSalesSettingsFromBlueprint(BP){
  const out = {
    ttl_sec: 300,
    cashback_percent: 10,
    cashier1_tg_id: '',
    cashier2_tg_id: '',
    cashier3_tg_id: '',
    cashier4_tg_id: '',
    cashier5_tg_id: ''
  };

  const routes = Array.isArray(BP && BP.routes) ? BP.routes : [];
  const blocksDict = (BP && BP.blocks && typeof BP.blocks === 'object') ? BP.blocks : {};

  const insts = [];
  try{
    for (const rt of routes){
      const blocks = (rt && Array.isArray(rt.blocks)) ? rt.blocks : [];
      for (const b of blocks) insts.push(b);
    }
  }catch(_){}

  const getProps = (inst)=>{
    if (!inst) return {};
    if (inst.props && typeof inst.props === 'object') return inst.props;
    const id = inst.id != null ? String(inst.id) : '';
    const p = id && blocksDict[id];
    return (p && typeof p === 'object') ? p : {};
  };

  // ищем первый sales_qr
  const sq = insts.find(b => b && (b.key === 'sales_qr_one' || b.type === 'sales_qr_one'));
  if (!sq) return out;

  const p = getProps(sq);
  const pick = (v)=>String(v ?? '').trim();

  const ttl = Number(p.ttl_sec ?? 300);
  out.ttl_sec = Math.max(60, Math.min(600, isFinite(ttl) ? ttl : 300));

  const cb = Number(p.cashback_percent ?? 10);
  out.cashback_percent = Math.max(0, Math.min(100, isFinite(cb) ? cb : 10));

  // 5 кассиров (новый формат)
  out.cashier1_tg_id = pick(p.cashier1_tg_id);
  out.cashier2_tg_id = pick(p.cashier2_tg_id);
  out.cashier3_tg_id = pick(p.cashier3_tg_id);
  out.cashier4_tg_id = pick(p.cashier4_tg_id);
  out.cashier5_tg_id = pick(p.cashier5_tg_id);

  return out;
}


// upsert в D1: sales_settings
async function upsertSalesSettings(appId, publicId, salesCfg, env){
  const db = env.DB;
  if (!db) return { ok:false, error:'DB_BINDING_MISSING' };

  const ttl = Math.max(60, Math.min(600, Number(salesCfg?.ttl_sec ?? 300)));
  const cb  = Math.max(0,  Math.min(100, Number(salesCfg?.cashback_percent ?? 10)));

  const pick = (v)=>{
    const s = String(v ?? '').trim();
    return s ? s : null;
  };

  const c1 = pick(salesCfg?.cashier1_tg_id);
  const c2 = pick(salesCfg?.cashier2_tg_id);
  const c3 = pick(salesCfg?.cashier3_tg_id);
  const c4 = pick(salesCfg?.cashier4_tg_id);
  const c5 = pick(salesCfg?.cashier5_tg_id);

  await db.prepare(`
    INSERT INTO sales_settings
      (app_public_id, cashier1_tg_id, cashier2_tg_id, cashier3_tg_id, cashier4_tg_id, cashier5_tg_id,
       cashback_percent, ttl_sec, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(app_public_id) DO UPDATE SET
      cashier1_tg_id=excluded.cashier1_tg_id,
      cashier2_tg_id=excluded.cashier2_tg_id,
      cashier3_tg_id=excluded.cashier3_tg_id,
      cashier4_tg_id=excluded.cashier4_tg_id,
      cashier5_tg_id=excluded.cashier5_tg_id,
      cashback_percent=excluded.cashback_percent,
      ttl_sec=excluded.ttl_sec,
      updated_at=datetime('now')
  `).bind(
    String(publicId), c1, c2, c3, c4, c5, cb, ttl
  ).run();

  return { ok:true };
}




async function syncRuntimeTablesFromConfig(appId, publicId, cfg, env) {
  // Sync wheel_prizes + styles_dict from constructor config so runtime APIs work.
  const out = { wheelInserted: 0, stylesInserted: 0 };
  try {
    const db = env.DB;
    if (!db) {
      out.error = 'DB_BINDING_MISSING: env.DB is not bound to a D1 database';
      return out;
    }

// ---- wheel_prizes ----
await db.prepare(`DELETE FROM wheel_prizes WHERE app_public_id = ?`).bind(publicId).run();
const prizes = (cfg && cfg.wheel && Array.isArray(cfg.wheel.prizes)) ? cfg.wheel.prizes : [];

for (const p of prizes) {
  if (!p) continue;

  const code = String(p.code || '').trim();

  // title: поддерживаем и старые/новые формы
  const title = String((p.title || p.name || p.code) || '').trim();

  // weight: 0 — валидно (никогда), NaN/undefined -> 1
  const wRaw = Number(p.weight);
  const weight = Number.isFinite(wRaw) ? Math.max(0, Math.round(wRaw)) : 1;

  // coins: 0 валидно, NaN -> 0
  const cRaw = Number(p.coins);
  const coins = Number.isFinite(cRaw) ? Math.max(0, Math.round(cRaw)) : 0;

  const active = (p.active === false) ? 0 : 1;

  await db.prepare(`
    INSERT INTO wheel_prizes (app_id, app_public_id, code, title, weight, coins, active)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).bind(
    appId,
    publicId,
    code,
    title,
    weight,
    coins,
    active
  ).run();

  out.wheelInserted++;
}


    // ---- styles_dict ----
    await db.prepare(`DELETE FROM styles_dict WHERE app_public_id = ?`).bind(publicId).run();
    const styles = (cfg && cfg.passport && Array.isArray(cfg.passport.styles)) ? cfg.passport.styles : [];
    for (const s of styles) {
      if (!s) continue;
      if (s.active === false) continue;
      await db.prepare(`
        INSERT INTO styles_dict (app_id, app_public_id, style_id, title)
        VALUES (?, ?, ?, ?)
      `).bind(
        appId,
        publicId,
        String(s.code || ''),
        String(s.name || s.code || '')
      ).run();
      out.stylesInserted++;
    }
  } catch (e) {
    console.error('[syncRuntimeTablesFromConfig] failed', e);
    out.error = String(e && e.message ? e.message : e);
  }
  return out;
}


async function publishApp(appId, env, url, request) {
  const raw = await env.APPS.get('app:' + appId);
  if (!raw) return json({ ok:false, error:'CONFIG_NOT_FOUND' }, 404, request);

  let appObj;
  try { appObj = JSON.parse(raw); }
  catch (e) { return json({ ok:false, error:'CONFIG_PARSE_ERROR' }, 500, request); }

  // Ensure publicId exists and is persisted
  if (!appObj.publicId) {
    const suffix = Math.random().toString(36).slice(2, 6);
    appObj.publicId = `app-${appId}-${suffix}`;
  }

  // keep legacy aliases (some фронты/старые записи могли использовать public_id/publicId)
  appObj.public_id = appObj.publicId;

  appObj.lastPublishedAt = new Date().toISOString();
  appObj.updatedAt = new Date().toISOString();

  // Persist KV records
  await env.APPS.put('app:' + appId, JSON.stringify(appObj));
  await env.APPS.put('app:by_public:' + appObj.publicId, JSON.stringify({ appId, publicId: appObj.publicId }));

  // Persist D1 for cabinet listings (if column exists)
  try {
    await env.DB.prepare(
      `UPDATE apps SET public_id = ?, updated_at = datetime('now'), published_at = datetime('now') WHERE id = ?`
    ).bind(appObj.publicId, appId).run();
  } catch (_) {
    // ignore if schema doesn't have these columns
    try {
      await env.DB.prepare(`UPDATE apps SET updated_at = datetime('now') WHERE id = ?`).bind(appId).run();
    } catch (_) {}
  }

    // === SALES SETTINGS SYNC on publish ===
    try{
      const salesCfg = extractSalesSettingsFromBlueprint(appObj.config || null);
      await upsertSalesSettings(appId, appObj.publicId, salesCfg, env);
    }catch(e){
      console.error('[sales_settings] sync on publish failed', e);
    }
  

  // sync runtime lookup tables (wheel_prizes, styles_dict) from saved app_config
  let runtimeCfg = (appObj.app_config ?? appObj.runtime_config ?? null);

// If Studio didn't send /config correctly (common when props live in BP.blocks),
// or config is missing/empty — derive it from the saved blueprint.
const looksEmpty =
  !runtimeCfg ||
  typeof runtimeCfg !== 'object' ||
  (Object.keys(runtimeCfg).length === 0) ||
  ((runtimeCfg.wheel && Array.isArray(runtimeCfg.wheel.prizes) && runtimeCfg.wheel.prizes.length === 0) &&
   (runtimeCfg.passport && Array.isArray(runtimeCfg.passport.styles) && runtimeCfg.passport.styles.length === 0));

if (looksEmpty) {
  runtimeCfg = extractRuntimeConfigFromBlueprint(appObj.config || null);
  appObj.app_config = runtimeCfg;
  // persist back so next publish is fast and deterministic
  try { await env.APPS.put('app:' + appId, JSON.stringify(appObj)); } catch(_) {}
}



  const syncStats = await syncRuntimeTablesFromConfig(appId, appObj.publicId, runtimeCfg, env);

  const publicUrl = 'https://mini.salesgenius.ru/m/' + encodeURIComponent(appObj.publicId);

  return json({ ok:true, appId, publicId: appObj.publicId, publicUrl, sync: syncStats }, 200, request);
}

async function getPublicConfig(publicId, env, request) {
  const map = await env.APPS.get('app:by_public:' + publicId, 'json');
  if (!map || !map.appId) {
    return json({ ok:false, error:'NOT_FOUND' }, 404, request);
  }
  const appId = map.appId;

  const raw = await env.APPS.get('app:' + appId);
  if (!raw) return json({ ok:false, error:'NOT_FOUND' }, 404, request);

  const appObj = JSON.parse(raw);

  const payload = {
    publicId,
    title: appObj.title,
    config: appObj.config,
  };

  // совместимость: на всякий кладём и в app.*
  return json({ ok: true, ...payload, app: payload }, 200, request);
}
// ================== BOT INTEGRATION: 1 bot per app ==================

function extractTelegramBotId(token) {
  if (!token) return null;
  const m = String(token).match(/^(\d+):/);
  return m ? m[1] : null;
}

async function saveBotIntegration(appId, env, body, ownerId, request) {
  const usernameRaw = (body.username || body.botUsername || '').trim();
  const tokenRaw    = (body.token || body.botToken || '').trim();

  if (!tokenRaw) {
    console.warn('[bot] saveBotIntegration NO_TOKEN for app', appId);
    return json({ ok: false, error: 'NO_TOKEN' }, 400, request);
  }

  if (!env.BOT_SECRETS) {
    console.error('[bot] BOT_SECRETS binding missing');
    return json({ ok: false, error: 'NO_BOT_SECRETS' }, 500, request);
  }
  if (!env.BOT_TOKEN_KEY || env.BOT_TOKEN_KEY.length < 16) {
    console.error('[bot] BOT_TOKEN_KEY missing or too short');
    return json({ ok: false, error: 'BAD_MASTER_KEY' }, 500, request);
  }

  // 1) Канонический public_id приложения (единая истина)
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) {
    console.error('[bot] APP_PUBLIC_ID_NOT_FOUND for app', appId);
    return json({ ok: false, error: 'APP_PUBLIC_ID_NOT_FOUND' }, 500, request);
  }

  const username = usernameRaw || null;
  
  const tgBotId  = extractTelegramBotId(tokenRaw);

  try {
    const db = env.DB;

    // 2) Шифруем токен мастер-ключом
    const cipher = await encryptToken(tokenRaw, env.BOT_TOKEN_KEY);

    // 3) Пишем секрет строго по public_id (старых ключей больше нет)
    const kvKey = 'bot_token:public:' + appPublicId;
    await env.BOT_SECRETS.put(kvKey, JSON.stringify({ cipher }));

    // 3.1) Создаём webhook-secret (Variant A) один раз на public_id
const wh = await ensureBotWebhookSecretForPublicId(appPublicId, env);





    // 4) Upsert в D1 по (owner_id, app_public_id)
    //    (один бот на одно приложение)
    const existing = await db
      .prepare('SELECT id FROM bots WHERE owner_id = ? AND app_public_id = ? LIMIT 1')
      .bind(ownerId, appPublicId)
      .first();

    let botRowId;

    if (!existing) {
      const ins = await db
        .prepare(
          `INSERT INTO bots
             (owner_id, title, username, tg_bot_id, status, created_at, updated_at, app_id, app_public_id)
           VALUES
             (?, ?, ?, ?, 'active', datetime('now'), datetime('now'), ?, ?)`,
        )
        .bind(
          ownerId,
          'Bot for app ' + appId,
          username,
          tgBotId,
          appId,
          appPublicId,
        )
        .run();

      botRowId = Number(ins.lastInsertRowid);
    } else {
      botRowId = existing.id;

      await db
        .prepare(
          `UPDATE bots
           SET username      = ?,
               tg_bot_id     = ?,
               status        = 'active',
               app_id        = ?,
               app_public_id = ?,
               updated_at    = datetime('now')
           WHERE id = ? AND owner_id = ?`,
        )
        .bind(username, tgBotId, appId, appPublicId, botRowId, ownerId)
        .run();
    }

    return json(
      {
        ok: true,
        bot_id: botRowId,
        app_public_id: appPublicId,
        kv_key: kvKey,
    
        webhook: {
          // secret можно показывать только при created=true, но для первого запуска пусть отдаёт всегда
          secret: wh.secret,
          created: wh.created,
          kv_key: wh.kv_key || null,
          url:
            'https://app.salesgenius.ru/api/tg/webhook/' +
            encodeURIComponent(appPublicId) +
            '?s=' +
            encodeURIComponent(wh.secret),
        },
      },
      200,
      request,
    );
    
  } catch (e) {
    console.error('[bot] saveBotIntegration failed', e);
    return json({ ok: false, error: 'INTERNAL_ERROR' }, 500, request);
  }
}

// ================== TELEGRAM WEBHOOK (Variant A) ==================

async function tgSendMessage(env, botToken, chatId, text, extra = {}, meta = {}) {
  // meta: { appPublicId, tgUserId }
  const payload = {
    chat_id: chatId,
    text: String(text || ''),
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    ...extra,
  };

  const resp = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });

  let respText = '';
  if (!resp.ok) {
    respText = await resp.text().catch(() => '');
    console.error('[tgSendMessage] bad', resp.status, respText.slice(0, 500));
  }

  // log outgoing
  try {
    if (meta && meta.appPublicId && meta.tgUserId) {
      await logBotMessage(env.DB, {
        appPublicId: meta.appPublicId,
        tgUserId: meta.tgUserId,
        direction: 'out',
        msgType: 'text',
        text: String(text || ''),
        chatId: chatId,
        tgMessageId: null,
        payload: { request: payload, ok: resp.ok, status: resp.status, error: resp.ok ? null : respText.slice(0, 500) }
      });

      // update counters + blocked status
      let status = null;
      if (!resp.ok && (resp.status === 403) && /blocked|bot was blocked/i.test(respText || '')) {
        status = 'blocked';
      }
      await bumpBotOutCounters(env.DB, {
        appPublicId: meta.appPublicId,
        tgUserId: meta.tgUserId,
        status
      });
    }
  } catch (e) {
    console.error('[bot] log outgoing failed', e);
  }

  return resp;
}


async function getSalesSettings(db, appPublicId){
  const row = await db.prepare(
    `SELECT cashier1_tg_id, cashier2_tg_id, cashier3_tg_id, cashier4_tg_id, cashier5_tg_id,
            cashback_percent, ttl_sec
     FROM sales_settings
     WHERE app_public_id = ? LIMIT 1`
  ).bind(String(appPublicId)).first();

  const cashiers = [row?.cashier1_tg_id, row?.cashier2_tg_id, row?.cashier3_tg_id, row?.cashier4_tg_id, row?.cashier5_tg_id]
    .map(x => (x ? String(x).trim() : ''))
    .filter(Boolean);

  return {
    cashiers,
    cashback_percent: row ? Number(row.cashback_percent || 10) : 10,
    ttl_sec: row ? Number(row.ttl_sec || 300) : 300
  };
}

function parseAmountToCents(s){
  // принимает: "123", "123.45", "123,45"
  const raw = String(s||'').trim().replace(',', '.');
  if (!raw) return null;
  if (!/^\d+(\.\d{1,2})?$/.test(raw)) return null;
  const parts = raw.split('.');
  const rub = Number(parts[0] || '0');
  const kop = Number((parts[1] || '').padEnd(2,'0'));
  if (!Number.isFinite(rub) || !Number.isFinite(kop)) return null;
  return rub * 100 + kop;
}



async function handleTelegramWebhook(publicId, request, env) {
  // 1) check secret from query (?s=...)
  const url = new URL(request.url);
  const s = url.searchParams.get('s') || '';
  const expected = await getBotWebhookSecretForPublicId(publicId, env);

  if (!expected || !timingSafeEqual(s, expected)) {
    return new Response('FORBIDDEN', { status: 403, headers: corsHeaders(request) });
  }

  // 2) parse update (always return 200 to Telegram)
  let upd;
  try {
    upd = await request.json();
  } catch (e) {
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // 3) dedupe update_id (KV TTL)
  const updateId = upd && upd.update_id != null ? String(upd.update_id) : '';
  if (env.BOT_SECRETS && updateId) {
    const k = `tg_upd:public:${publicId}:${updateId}`;
    const seen = await env.BOT_SECRETS.get(k);
    if (seen) return new Response('OK', { status: 200, headers: corsHeaders(request) });
    await env.BOT_SECRETS.put(k, '1', { expirationTtl: 3600 }); // 1 hour
  }

    // ===== STARS: pre_checkout_query + successful_payment =====
  // Важно: отвечаем быстро, до любой другой логики
  try {
    const botTokenEarly = await getBotTokenForApp(publicId, env, null);
    if (botTokenEarly) {
      // A) pre_checkout_query
      if (upd && upd.pre_checkout_query) {
        const pcq = upd.pre_checkout_query;
        const invPayload = String(pcq.invoice_payload || '');
        const orderId = invPayload.startsWith('order:') ? invPayload.slice(6) : '';

        let ok = true;
        let err = '';

        if (!orderId) { ok = false; err = 'Bad payload'; }
        else {
          const row = await env.DB.prepare(`
            SELECT id, status, total_stars
            FROM stars_orders
            WHERE id = ? AND app_public_id = ?
            LIMIT 1
          `).bind(orderId, publicId).first();

          if (!row) { ok = false; err = 'Order not found'; }
          else if (String(row.status) !== 'created') { ok = false; err = 'Order already processed'; }
        }

        await tgAnswerPreCheckoutQuery(botTokenEarly, pcq.id, ok, err);
        return new Response('OK', { status: 200, headers: corsHeaders(request) });
      }

      // B) successful_payment
      const sp = upd?.message?.successful_payment;
      if (sp) {
        const invPayload = String(sp.invoice_payload || '');
        const orderId = invPayload.startsWith('order:') ? invPayload.slice(6) : '';

        if (orderId) {
          await env.DB.prepare(`
            UPDATE stars_orders
            SET status = 'paid',
                paid_at = datetime('now'),
                telegram_payment_charge_id = ?,
                provider_payment_charge_id = ?,
                paid_total_amount = ?
            WHERE id = ? AND app_public_id = ?
          `).bind(
            String(sp.telegram_payment_charge_id || ''),
            String(sp.provider_payment_charge_id || ''),
            Number(sp.total_amount || 0),
            orderId,
            publicId
          ).run();
        }

        return new Response('OK', { status: 200, headers: corsHeaders(request) });
      }
    }
  } catch (e) {
    console.error('[stars] webhook handler failed', e);
  }





  // 4) extract message/from first (IMPORTANT!)
  const msg =
    upd.message ||
    upd.edited_message ||
    (upd.callback_query ? upd.callback_query.message : null);

  const from =
    (upd.message && upd.message.from) ||
    (upd.edited_message && upd.edited_message.from) ||
    (upd.callback_query && upd.callback_query.from) ||
    null;

  const chatId = msg && msg.chat ? msg.chat.id : (from ? from.id : null);
  const text =
    (upd.message && upd.message.text) ||
    (upd.edited_message && upd.edited_message.text) ||
    (upd.callback_query && upd.callback_query.data) ||
    '';

  if (!chatId || !from) {
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // 5) get bot token from KV
  const botToken = await getBotTokenForApp(publicId, env, null);



  




  if (!botToken) {
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // 6) resolve canonical ctx (for appId + canonical publicId)
  const ctx = await resolveAppContextByPublicId(publicId, env);
  if (!ctx || !ctx.ok) {
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }
  const appPublicId = ctx.publicId || publicId;
  const appId = ctx.appId;




// === CALLBACK QUERIES (inline buttons) ===
if (upd && upd.callback_query && upd.callback_query.data){
  const cq = upd.callback_query;
  const data = String(cq.data || '');
  const cqId = String(cq.id || '');
  const cashierTgId = String(from.id);

  // helper: load sale action context
  async function loadSaleAction(saleId){
    const k = `sale_action:${appPublicId}:${String(saleId||'')}:${cashierTgId}`;
    const raw = env.BOT_SECRETS ? await env.BOT_SECRETS.get(k) : null;
    if (!raw) return null;
    try{ return JSON.parse(raw); }catch(_){ return null; }
  }

  // 1) CANCEL CASHBACK
  if (data.startsWith('sale_cancel:')){
    const saleId = data.slice('sale_cancel:'.length).trim();
    const act = await loadSaleAction(saleId);

    if (!act || !act.customerTgId){
      await tgAnswerCallbackQuery(botToken, cqId, 'Контекст продажи не найден (истёк).', true);
      return new Response('OK', { status: 200, headers: corsHeaders(request) });
    }

    // rollback coins (идемпотентно)
    if (Number(act.cashbackCoins) > 0){
      await awardCoins(
        env.DB,
        appId,
        appPublicId,
        String(act.customerTgId),
        -Math.abs(Number(act.cashbackCoins)),
        'sale_cancel',
        String(act.saleId || saleId),
        'cancel cashback',
        `sale_cancel:${appPublicId}:${String(act.saleId || saleId)}`
      );
    }

    // notify cashier + customer
    await tgSendMessage(env, botToken, String(chatId),
      `↩️ Кэшбэк отменён. Sale #${String(act.saleId||saleId)}.`,
      {}, { appPublicId, tgUserId: cashierTgId }
    );

    try{
      await tgSendMessage(env, botToken, String(act.customerTgId),
        `↩️ Кэшбэк по покупке отменён кассиром.`,
        {}, { appPublicId, tgUserId: String(act.customerTgId) }
      );
    }catch(_){}

    await tgAnswerCallbackQuery(botToken, cqId, 'Готово ✅', false);
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // 2) PIN MENU (choose stamp/day)
  if (data.startsWith('pin_menu:')){
    const saleId = data.slice('pin_menu:'.length).trim();
    const act = await loadSaleAction(saleId);

    if (!act || !act.customerTgId){
      await tgAnswerCallbackQuery(botToken, cqId, 'Контекст продажи не найден (истёк).', true);
      return new Response('OK', { status: 200, headers: corsHeaders(request) });
    }

    // load styles list from styles_dict
    const rows = await env.DB.prepare(
      `SELECT style_id, title
       FROM styles_dict
       WHERE app_public_id = ?
       ORDER BY id ASC`
    ).bind(appPublicId).all();

    const items = (rows && rows.results) ? rows.results : [];
    if (!items.length){
      await tgSendMessage(env, botToken, String(chatId),
        `Нет карточек в styles_dict — нечего выдавать.`,
        {}, { appPublicId, tgUserId: cashierTgId }
      );
      await tgAnswerCallbackQuery(botToken, cqId, 'Нет стилей', true);
      return new Response('OK', { status: 200, headers: corsHeaders(request) });
    }

    // build keyboard 2 columns
    const kb = [];
    for (let i=0;i<items.length;i+=2){
      const a = items[i];
      const b = items[i+1];
      const row = [];
      row.push({ text: String(a.title || a.style_id), callback_data: `pin_make:${saleId}:${String(a.style_id)}` });
      if (b) row.push({ text: String(b.title || b.style_id), callback_data: `pin_make:${saleId}:${String(b.style_id)}` });
      kb.push(row);
    }

    await tgSendMessage(env, botToken, String(chatId),
      `Выбери штамп/день — PIN уйдёт клиенту (клиент: ${String(act.customerTgId)})`,
      { reply_markup: { inline_keyboard: kb } },
      { appPublicId, tgUserId: cashierTgId }
    );

    await tgAnswerCallbackQuery(botToken, cqId, 'Выбери стиль', false);
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // 3) PIN MAKE (generate + send to customer)
  if (data.startsWith('pin_make:')){
    const rest = data.slice('pin_make:'.length);
    const [saleIdRaw, styleIdRaw] = rest.split(':');
    const saleId = String(saleIdRaw||'').trim();
    const styleId = String(styleIdRaw||'').trim();

    const act = await loadSaleAction(saleId);
    if (!act || !act.customerTgId){
      await tgAnswerCallbackQuery(botToken, cqId, 'Контекст продажи не найден (истёк).', true);
      return new Response('OK', { status: 200, headers: corsHeaders(request) });
    }
    if (!styleId){
      await tgAnswerCallbackQuery(botToken, cqId, 'Нет style_id', true);
      return new Response('OK', { status: 200, headers: corsHeaders(request) });
    }

    // title
    let stTitle = '';
    try{
      const r = await env.DB.prepare(
        `SELECT title FROM styles_dict WHERE app_public_id=? AND style_id=? LIMIT 1`
      ).bind(appPublicId, styleId).first();
      stTitle = r ? String(r.title||'') : '';
    }catch(_){}

    const pinRes = await issuePinToCustomer(env.DB, appPublicId, cashierTgId, String(act.customerTgId), styleId);
    if (!pinRes || !pinRes.ok){
      await tgAnswerCallbackQuery(botToken, cqId, 'Не удалось создать PIN', true);
      return new Response('OK', { status: 200, headers: corsHeaders(request) });
    }

    // send PIN to customer (NOT cashier)
    try{
      await tgSendMessage(
        env, botToken, String(act.customerTgId),
        `🔑 Ваш PIN для отметки штампа${stTitle ? ` “${stTitle}”` : ''}:\n<code>${String(pinRes.pin)}</code>\n\n(одноразовый)`,
        {}, { appPublicId, tgUserId: String(act.customerTgId) }
      );
    }catch(e){
      console.error('[pin] send to customer failed', e);
    }

    // notify cashier
    await tgSendMessage(
      env, botToken, String(chatId),
      `✅ PIN отправлен клиенту ${String(act.customerTgId)} для ${stTitle ? `“${stTitle}”` : styleId}.`,
      {}, { appPublicId, tgUserId: cashierTgId }
    );

    await tgAnswerCallbackQuery(botToken, cqId, 'PIN отправлен ✅', false);
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // unknown callback
  await tgAnswerCallbackQuery(botToken, cqId, 'Неизвестное действие', false);
  return new Response('OK', { status: 200, headers: corsHeaders(request) });
}






  // 7) sync user in app_users (bot activity)
  try {
    await upsertAppUserFromBot(env.DB, {
      appId,
      appPublicId,
      tgUserId: from.id,
      tgUsername: from.username || null
    });
  } catch (e) {
    console.error('[bot] upsertAppUserFromBot failed', e);
  }

  // 8) log incoming
  try {
    await logBotMessage(env.DB, {
      appPublicId,
      tgUserId: from.id,
      direction: 'in',
      msgType: pickMsgType(upd),
      text: text || null,
      chatId: chatId,
      tgMessageId: (msg && msg.message_id) ? msg.message_id : null,
      payload: { update: upd }
    });
  } catch (e) {
    console.error('[bot] log incoming failed', e);
  }

  const t = String(text || '').trim();





  // 9) commands / state
  if (t === '/start' || t.startsWith('/start ')) {
    const payload = t.startsWith('/start ') ? t.slice(7).trim() : '';





// === REDEEM FLOW: /start redeem_<code>
if (payload.startsWith('redeem_')) {
  const redeemCode = payload.slice(7).trim();

  // 1) кассир?
  const ss = await getSalesSettings(env.DB, appPublicId);
  const isCashier = ss.cashiers.includes(String(from.id));
  if (!isCashier){
    await tgSendMessage(env, botToken, chatId,
      '⛔️ Вы не зарегистрированы как кассир для этого проекта.',
      {}, { appPublicId, tgUserId: from.id }
    );
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // 2) найти redeem
  const r = await env.DB.prepare(
    `SELECT id, tg_id, prize_title, status
     FROM wheel_redeems
     WHERE app_public_id = ? AND redeem_code = ?
     LIMIT 1`
  ).bind(appPublicId, redeemCode).first();




// === 2b) если в wheel_redeems нет — пробуем passport_rewards (паспортные призы)
if (!r){
  const pr = await env.DB.prepare(
    `SELECT id, tg_id, prize_code, prize_title, coins, passport_key, status
     FROM passport_rewards
     WHERE app_public_id = ? AND redeem_code = ?
     ORDER BY id DESC
     LIMIT 1`
  ).bind(appPublicId, redeemCode).first();

  if (!pr){
    await tgSendMessage(env, botToken, chatId,
      '⛔️ Код недействителен или приз не найден.',
      {}, { appPublicId, tgUserId: from.id }
    );
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  if (String(pr.status) === 'redeemed'){
    await tgSendMessage(env, botToken, chatId,
      'ℹ️ Этот приз уже отмечен как полученный.',
      {}, { appPublicId, tgUserId: from.id }
    );
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // 3b) пометить паспортный приз redeemed (важно: only issued -> redeemed)
  const upd = await env.DB.prepare(
    `UPDATE passport_rewards
     SET status='redeemed',
         redeemed_at=datetime('now'),
         redeemed_by_tg=?
     WHERE id=? AND status='issued'`
  ).bind(String(from.id), Number(pr.id)).run();

  // если не изменилось — значит кто-то уже успел подтвердить (или статус не issued)
  if (!upd || !upd.meta || !upd.meta.changes){
    await tgSendMessage(env, botToken, chatId,
      'ℹ️ Этот приз уже отмечен как полученный.',
      {}, { appPublicId, tgUserId: from.id }
    );
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  const coins = Math.max(0, Math.floor(Number(pr.coins || 0)));

  // 4b) если монетный приз — начисляем монеты ТОЛЬКО после подтверждения кассиром
  if (coins > 0){
    try{
      // ctx для awardCoins (нужен appId)
      const ctx2 = await resolveAppContextByPublicId(appPublicId, env);
      const appId2 = ctx2?.appId || null;

      await awardCoins(
        env.DB,
        appId2,
        appPublicId,
        String(pr.tg_id),
        coins,
        'passport_complete_redeemed',
        String(pr.prize_code || ''),
        String(pr.prize_title || 'Паспорт: приз'),
        `passport:redeem:${appPublicId}:${pr.tg_id}:${pr.id}:${coins}` // event_id (идемпотентность)
      );
    }catch(e){
      console.error('[passport.redeem] awardCoins failed', e);
      // не прерываем выдачу — кассиру уже подтвердили, но залогируем проблему
    }
  }

  // 5b) сбросить штампы (повторяемость паспорта) — после успешного redeem
  try{
    await env.DB.prepare(
      `DELETE FROM styles_user
       WHERE app_public_id=? AND tg_id=?`
    ).bind(appPublicId, String(pr.tg_id)).run();
  }catch(e){
    console.error('[passport.redeem] reset styles_user failed', e);
  }

  // 6b) уведомления кассиру
  await tgSendMessage(env, botToken, chatId,
    `✅ Приз по паспорту выдан.\nКод: <code>${redeemCode}</code>\nПриз: <b>${String(pr.prize_title||'')}</b>` +
    (coins > 0 ? `\n🪙 Монеты: <b>${coins}</b> (начислены)` : ''),
    {}, { appPublicId, tgUserId: from.id }
  );

  // 7b) клиенту
  try{
    await tgSendMessage(env, botToken, String(pr.tg_id),
      `🎉 Ваш приз по паспорту получен!\n<b>${String(pr.prize_title||'')}</b>\n` +
      (coins > 0 ? `🪙 Начислено <b>${coins}</b> монет.\n` : '') +
      `Кассир подтвердил выдачу ✅`,
      {}, { appPublicId, tgUserId: String(pr.tg_id) }
    );
  }catch(_){}

  return new Response('OK', { status: 200, headers: corsHeaders(request) });
}





  if (!r){
    await tgSendMessage(env, botToken, chatId,
      '⛔️ Код недействителен или приз не найден.',
      {}, { appPublicId, tgUserId: from.id }
    );
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  if (String(r.status) === 'redeemed'){
    await tgSendMessage(env, botToken, chatId,
      'ℹ️ Этот приз уже отмечен как полученный.',
      {}, { appPublicId, tgUserId: from.id }
    );
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // 3) пометить redeemed
  await env.DB.prepare(
    `UPDATE wheel_redeems
     SET status='redeemed', redeemed_at=datetime('now'), redeemed_by_tg=?
     WHERE id=?`
  ).bind(String(from.id), Number(r.id)).run();

  // 4) (опционально) обновим wheel_spins для аналитики
  try{
    await env.DB.prepare(
      `UPDATE wheel_spins
       SET status='redeemed', ts_redeemed=datetime('now'), redeemed_by_tg=?
       WHERE app_public_id=? AND redeem_id=?`
    ).bind(String(from.id), appPublicId, Number(r.id)).run();
  }catch(_){}

  // 5) уведомления
  await tgSendMessage(env, botToken, chatId,
    `✅ Приз выдан.\nКод: <code>${redeemCode}</code>\nПриз: <b>${String(r.prize_title||'')}</b>`,
    {}, { appPublicId, tgUserId: from.id }
  );

  // клиенту
  try{
    await tgSendMessage(env, botToken, String(r.tg_id),
      `🎉 Ваш приз получен!\n<b>${String(r.prize_title||'')}</b>\nКассир подтвердил выдачу ✅`,
      {}, { appPublicId, tgUserId: String(r.tg_id) }
    );
  }catch(_){}

  return new Response('OK', { status: 200, headers: corsHeaders(request) });
}




    // === SALE FLOW: /start sale_<token>
    if (payload.startsWith('sale_')) {
      const token = payload.slice(5).trim();

      // 1) token -> KV
      const rawTok = env.BOT_SECRETS ? await env.BOT_SECRETS.get(saleTokKey(token)) : null;

      if (!rawTok){
        await tgSendMessage(env, botToken, chatId, '⛔️ Этот QR устарел. Попросите клиента обновить QR.', {}, { appPublicId, tgUserId: from.id });
        return new Response('OK', { status: 200, headers: corsHeaders(request) });
      }

      let tokObj = null;
      try{ tokObj = JSON.parse(rawTok); }catch(_){}
      const customerTgId = tokObj && tokObj.customerTgId ? String(tokObj.customerTgId) : '';
      const tokenAppPublicId = tokObj && tokObj.appPublicId ? String(tokObj.appPublicId) : appPublicId;

      // 2) кассир в списке?
      const ss = await getSalesSettings(env.DB, tokenAppPublicId);
      const isCashier = ss.cashiers.includes(String(from.id));

      if (!isCashier){
        await tgSendMessage(env, botToken, chatId, '⛔️ Вы не зарегистрированы как кассир для этого проекта.', {}, { appPublicId, tgUserId: from.id });
        return new Response('OK', { status: 200, headers: corsHeaders(request) });
      }

      // 3) сохранить pending sale
      const pendKey = `sale_pending:${tokenAppPublicId}:${from.id}`;
      const pend = {
        appPublicId: tokenAppPublicId,
        customerTgId,
        token,
        cashback_percent: ss.cashback_percent
      };
      if (env.BOT_SECRETS){
        await env.BOT_SECRETS.put(pendKey, JSON.stringify(pend), { expirationTtl: 600 }); // 10 мин
        try { await env.BOT_SECRETS.delete(saleTokKey(token)); } catch(_) {}

      }

      await tgSendMessage(
        env,
        botToken,
        chatId,
        `✅ Клиент: ${customerTgId}\nВведите сумму покупки (например 350 или 350.50):`,
        {},
        { appPublicId: tokenAppPublicId, tgUserId: from.id }
      );

      return new Response('OK', { status: 200, headers: corsHeaders(request) });
    }

    // обычный старт
    await tgSendMessage(env, botToken, chatId, 'Привет! Я бот этого мини-аппа ✅\nКоманда: /profile', {}, { appPublicId, tgUserId: from.id });
    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // === AMOUNT STEP: если кассир ввёл число после sale_pending ===
  try{
    const pendKey = `sale_pending:${appPublicId}:${from.id}`;
    const pendRaw = env.BOT_SECRETS ? await env.BOT_SECRETS.get(pendKey) : null;

    if (pendRaw){
      let pend = null;
      try{ pend = JSON.parse(pendRaw); }catch(_){ pend = null; }

      const cents = parseAmountToCents(t);
      if (cents == null){
        await tgSendMessage(env, botToken, chatId, 'Введите сумму числом (например 350 или 350.50)', {}, { appPublicId, tgUserId: from.id });
        return new Response('OK', { status: 200, headers: corsHeaders(request) });
      }

      const cbp = Math.max(0, Math.min(100, Number(pend?.cashback_percent ?? 10)));
      const cashbackCoins = Math.max(0, Math.floor((cents / 100) * (cbp / 100))); // 10% от суммы в монетах (1 монета = 1 валюта)

      // INSERT sale
      const ins = await env.DB.prepare(
        `INSERT INTO sales (app_id, app_public_id, customer_tg_id, cashier_tg_id, amount_cents, cashback_coins, token, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))`
      ).bind(
        String(appId || ''),
        String(pend.appPublicId || appPublicId),
        String(pend.customerTgId || ''),
        String(from.id),
        Number(cents),
        Number(cashbackCoins),
        String(pend.token || ''),
      ).run();

      const saleId = ins?.meta?.last_row_id ? Number(ins.meta.last_row_id) : null;

      // award coins to customer (идемпотентно по event_id)
      if (pend.customerTgId && cashbackCoins > 0){
        await awardCoins(
          env.DB,
          appId,
          pend.appPublicId || appPublicId,
          String(pend.customerTgId),
          cashbackCoins,
          'sale_cashback',
          String(saleId || ''),
          `Кэшбэк ${cbp}% за покупку`,
          `sale:${pend.appPublicId || appPublicId}:${pend.token || ''}`
        );

        // notify customer
        await tgSendMessage(
          env,
          botToken,
          String(pend.customerTgId),
          `🎉 Начислено ${cashbackCoins} монет за покупку!\nСпасибо ❤️`,
          {},
          { appPublicId: pend.appPublicId || appPublicId, tgUserId: String(pend.customerTgId) }
        );
      }

      // notify cashier
      await tgSendMessage(
        env,
        botToken,
        chatId,
        `✅ Продажа записана.\nСумма: ${(cents/100).toFixed(2)}\nКэшбэк: ${cashbackCoins} монет`,
        {},
        { appPublicId: pend.appPublicId || appPublicId, tgUserId: from.id }
      );

      // === post-sale actions (buttons): cancel cashback / issue PIN ===
try{
  const actionKey = `sale_action:${pend.appPublicId || appPublicId}:${String(saleId||'')}:${String(from.id)}`;
  const actionPayload = {
    appPublicId: String(pend.appPublicId || appPublicId),
    saleId: String(saleId || ''),
    customerTgId: String(pend.customerTgId || ''),
    cashbackCoins: Number(cashbackCoins || 0)
  };

  if (env.BOT_SECRETS && saleId && pend.customerTgId){
    await env.BOT_SECRETS.put(actionKey, JSON.stringify(actionPayload), { expirationTtl: 3600 }); // 1 час
  }

  await tgSendMessage(
    env,
    botToken,
    chatId,
    `Что сделать дальше?`,
    {
      reply_markup: {
        inline_keyboard: [
          [
            { text: '↩️ Отменить кэшбэк', callback_data: `sale_cancel:${String(saleId||'')}` },
            { text: '🔑 Выдать PIN',       callback_data: `pin_menu:${String(saleId||'')}` }
          ]
        ]
      }
    },
    { appPublicId: pend.appPublicId || appPublicId, tgUserId: from.id }
  );
}catch(e){
  console.error('[sale] post actions buttons failed', e);
}


      // clear pending
      if (env.BOT_SECRETS) await env.BOT_SECRETS.delete(pendKey);

      return new Response('OK', { status: 200, headers: corsHeaders(request) });
    }
  }catch(e){
    console.error('[sale_flow] amount step error', e);
  }


  if (t === '/profile') {
    try {
      // cfg (если есть) из KV app:<id>
      const appObj = await env.APPS.get('app:' + ctx.appId, 'json').catch(() => null);
      const cfg = (appObj && (appObj.app_config ?? appObj.runtime_config ?? {})) || {};

      const state = await buildState(env.DB, ctx.appId, appPublicId, String(from.id), cfg || {});
      const lines = [
        `👤 ${from.username ? '@' + from.username : (from.first_name || 'Пользователь')}`,
        `🪙 Монеты: ${Number(state.coins || 0)}`,
        `🎨 Стили: ${Number(state.styles_count || 0)}/${Number(state.styles_total || 0)}`,
        `🎮 Лучший сегодня: ${Number(state.game_today_best || 0)}`,
        `🎟 Рефералы: ${Number(state.ref_total || 0)}`,
      ];

      await tgSendMessage(env, botToken, chatId, lines.join('\n'), {}, { appPublicId, tgUserId: from.id });
    } catch (e) {
      console.error('[tgWebhook] /profile error', e);
      await tgSendMessage(env, botToken, chatId, 'Ошибка при получении профиля 😕', {}, { appPublicId, tgUserId: from.id });
    }

    return new Response('OK', { status: 200, headers: corsHeaders(request) });
  }

  // default
  await tgSendMessage(env, botToken, chatId, 'Принял ✅\nКоманда: /profile', {}, { appPublicId, tgUserId: from.id });
  return new Response('OK', { status: 200, headers: corsHeaders(request) });
}



// ================== BOT LOGGING + SYNC (D1) ==================

function safeJson(obj, maxLen = 8000) {
  try {
    const s = JSON.stringify(obj);
    return s.length > maxLen ? s.slice(0, maxLen) : s;
  } catch (_) {
    return null;
  }
}

function pickMsgType(upd) {
  if (upd && upd.callback_query) return 'callback';
  const txt =
    (upd.message && upd.message.text) ||
    (upd.edited_message && upd.edited_message.text) ||
    '';
  if (txt && String(txt).trim().startsWith('/')) return 'command';
  return 'text';
}

async function logBotMessage(db, {
  appPublicId,
  tgUserId,
  direction,     // 'in'|'out'
  msgType,       // 'text'|'command'|'callback'|'system'
  text = null,
  chatId = null,
  tgMessageId = null,
  payload = null
}) {
  await db.prepare(
    `INSERT INTO bot_messages
      (app_public_id, tg_user_id, direction, msg_type, text, tg_message_id, chat_id, payload_json)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(
    appPublicId,
    String(tgUserId),
    String(direction),
    String(msgType),
    text != null ? String(text) : null,
    tgMessageId != null ? Number(tgMessageId) : null,
    chatId != null ? String(chatId) : null,
    payload ? safeJson(payload) : null
  ).run();
}

async function upsertAppUserFromBot(db, {
  appId,
  appPublicId,
  tgUserId,
  tgUsername = null
}) {
  // Требует UNIQUE(app_public_id, tg_user_id) или idx_app_users_public_tg
  await db.prepare(
    `INSERT INTO app_users (
        app_id, app_public_id, tg_user_id, tg_username,
        bot_started_at, bot_last_seen, bot_status,
        bot_total_msgs_in, bot_total_msgs_out
     ) VALUES (?, ?, ?, ?, datetime('now'), datetime('now'), 'active', 1, 0)
     ON CONFLICT(app_public_id, tg_user_id) DO UPDATE SET
        app_id = excluded.app_id,
        tg_username = COALESCE(excluded.tg_username, app_users.tg_username),
        bot_last_seen = datetime('now'),
        bot_status = COALESCE(app_users.bot_status, 'active'),
        bot_total_msgs_in = COALESCE(app_users.bot_total_msgs_in, 0) + 1`
  ).bind(
    String(appId || ''),
    String(appPublicId),
    String(tgUserId),
    tgUsername ? String(tgUsername) : null
  ).run();
}

async function bumpBotOutCounters(db, {
  appPublicId,
  tgUserId,
  status = null // e.g. 'blocked'
}) {
  await db.prepare(
    `UPDATE app_users
     SET bot_total_msgs_out = COALESCE(bot_total_msgs_out, 0) + 1,
         bot_last_seen = datetime('now'),
         bot_status = COALESCE(?, bot_status)
     WHERE app_public_id = ? AND tg_user_id = ?`
  ).bind(
    status,
    String(appPublicId),
    String(tgUserId)
  ).run();
}

// ================== BROADCASTS (D1) ==================

function normalizeSegment(seg){
  seg = String(seg || '').trim();
  if (!seg) return 'bot_active';
  return seg;
}

function segmentWhere(segment){
  // app_public_id = ? всегда первый bind
  switch (segment) {
    case 'all':
      return { where: "app_public_id = ?", binds: [] };

    case 'bot_active':
      return { where: "app_public_id = ? AND bot_started_at IS NOT NULL AND COALESCE(bot_status,'') != 'blocked'", binds: [] };

    case 'mini_active_7d':
      return { where: "app_public_id = ? AND last_seen >= datetime('now','-7 day')", binds: [] };

    case 'inactive_7d':
      return { where: "app_public_id = ? AND COALESCE(last_seen,'1970-01-01') < datetime('now','-7 day') AND COALESCE(bot_last_seen,'1970-01-01') < datetime('now','-7 day')", binds: [] };

    default:
      return { where: "app_public_id = ? AND bot_started_at IS NOT NULL AND COALESCE(bot_status,'') != 'blocked'", binds: [] };
  }
}

async function listBroadcasts(appId, env, ownerId, request){
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 404, request);

  const rows = await env.DB.prepare(
    `SELECT id, title, segment, status, total, sent, failed, blocked, created_at, updated_at
     FROM broadcasts
     WHERE app_public_id = ? AND (owner_id = ? OR owner_id IS NULL)
     ORDER BY id DESC
     LIMIT 50`
  ).bind(appPublicId, ownerId).all();

  return json({ ok:true, app_public_id: appPublicId, items: rows.results || [] }, 200, request);
}

async function createAndSendBroadcast(appId, env, ownerId, request){
  const body = await request.json().catch(()=>null);
  if (!body) return json({ ok:false, error:'BAD_JSON' }, 400, request);

  const text = String(body.text || '').trim();
  if (!text) return json({ ok:false, error:'TEXT_REQUIRED' }, 400, request);

  const title = body.title ? String(body.title).trim() : null;
  const segment = normalizeSegment(body.segment || 'bot_active');
  const btnText = body.btn_text ? String(body.btn_text).trim() : null;
  const btnUrl  = body.btn_url ? String(body.btn_url).trim() : null;

  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 404, request);

  // create broadcast row
  const ins = await env.DB.prepare(
    `INSERT INTO broadcasts (app_public_id, owner_id, title, text, segment, btn_text, btn_url, status)
     VALUES (?, ?, ?, ?, ?, ?, ?, 'sending')`
  ).bind(appPublicId, ownerId, title, text, segment, btnText, btnUrl).run();

  const broadcastId = ins?.meta?.last_row_id ? Number(ins.meta.last_row_id) : null;
  if (!broadcastId) return json({ ok:false, error:'BROADCAST_CREATE_FAILED' }, 500, request);

  // audience
  const seg = segmentWhere(segment);
  const audience = await env.DB.prepare(
    `SELECT tg_user_id
     FROM app_users
     WHERE ${seg.where}
     ORDER BY COALESCE(bot_last_seen, last_seen) DESC
     LIMIT 5000`
  ).bind(appPublicId, ...seg.binds).all();

  const users = audience?.results || [];
  const total = users.length;

  await env.DB.prepare(
    `UPDATE broadcasts SET total=?, updated_at=datetime('now') WHERE id=?`
  ).bind(total, broadcastId).run();

  // bot token
  const botToken = await getBotTokenForApp(appPublicId, env, appId);
  if (!botToken){
    await env.DB.prepare(`UPDATE broadcasts SET status='failed', updated_at=datetime('now') WHERE id=?`)
      .bind(broadcastId).run();
    return json({ ok:false, error:'BOT_TOKEN_NOT_FOUND', broadcast_id: broadcastId }, 400, request);
  }

  // optional button
  let extra = {};
  if (btnText && btnUrl){
    extra.reply_markup = { inline_keyboard: [[{ text: btnText, url: btnUrl }]] };
  }

  let sent = 0, failed = 0, blocked = 0;

  for (const u of users){
    const tgUserId = String(u.tg_user_id);

    await env.DB.prepare(
      `INSERT OR IGNORE INTO broadcast_jobs (broadcast_id, app_public_id, tg_user_id, status)
       VALUES (?, ?, ?, 'queued')`
    ).bind(broadcastId, appPublicId, tgUserId).run();

    try{
      const resp = await tgSendMessage(env, botToken, tgUserId, text, extra, { appPublicId, tgUserId });

      if (resp.ok){
        sent++;
        await env.DB.prepare(
          `UPDATE broadcast_jobs SET status='sent', updated_at=datetime('now')
           WHERE broadcast_id=? AND tg_user_id=?`
        ).bind(broadcastId, tgUserId).run();
      } else {
        const errText = await resp.text().catch(()=> '');
        const isBlocked = (resp.status === 403) && /blocked|bot was blocked/i.test(errText || '');
        if (isBlocked){
          blocked++;
          await env.DB.prepare(
            `UPDATE broadcast_jobs SET status='blocked', error=?, updated_at=datetime('now')
             WHERE broadcast_id=? AND tg_user_id=?`
          ).bind(errText.slice(0, 400), broadcastId, tgUserId).run();
        } else {
          failed++;
          await env.DB.prepare(
            `UPDATE broadcast_jobs SET status='failed', error=?, updated_at=datetime('now')
             WHERE broadcast_id=? AND tg_user_id=?`
          ).bind(errText.slice(0, 400), broadcastId, tgUserId).run();
        }
      }
    } catch(e){
      failed++;
      await env.DB.prepare(
        `UPDATE broadcast_jobs SET status='failed', error=?, updated_at=datetime('now')
         WHERE broadcast_id=? AND tg_user_id=?`
      ).bind(String(e?.message || e).slice(0, 400), broadcastId, tgUserId).run();
    }
  }

  await env.DB.prepare(
    `UPDATE broadcasts
     SET status='done', sent=?, failed=?, blocked=?, updated_at=datetime('now')
     WHERE id=?`
  ).bind(sent, failed, blocked, broadcastId).run();

  return json({ ok:true, broadcast_id: broadcastId, app_public_id: appPublicId, total, sent, failed, blocked }, 200, request);
}



// ================== DIALOGS (D1) ==================

async function listDialogs(appId, env, ownerId, request){
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 404, request);

  // range filter: today | 7d | 30d | all
  const url = new URL(request.url);
  const range = String(url.searchParams.get('range') || 'all').trim();

  let rangeWhere = '';
  if (range === 'today') rangeWhere = " AND COALESCE(u.bot_last_seen, u.bot_started_at) >= datetime('now','start of day')";
  if (range === '7d')    rangeWhere = " AND COALESCE(u.bot_last_seen, u.bot_started_at) >= datetime('now','-7 day')";
  if (range === '30d')   rangeWhere = " AND COALESCE(u.bot_last_seen, u.bot_started_at) >= datetime('now','-30 day')";
  // all -> no extra filter

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
  ).bind(appPublicId).all();

  return json({ ok:true, app_public_id: appPublicId, items: rows.results || [] }, 200, request);
}


async function getDialogMessages(appId, tgUserId, env, ownerId, request){
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 404, request);

  const url = new URL(request.url);
  const limit = Math.max(10, Math.min(200, Number(url.searchParams.get('limit') || 80)));
  const beforeId = url.searchParams.get('before_id');

  let sql = `
    SELECT id, direction, msg_type, text, created_at
    FROM bot_messages
    WHERE app_public_id = ? AND tg_user_id = ?
  `;
  const binds = [appPublicId, String(tgUserId)];

  if (beforeId){
    sql += ` AND id < ? `;
    binds.push(Number(beforeId));
  }

  sql += ` ORDER BY id DESC LIMIT ? `;
  binds.push(limit);

  const rows = await env.DB.prepare(sql).bind(...binds).all();
  const items = (rows.results || []).slice().reverse(); // чтобы в UI было снизу вверх

  return json({ ok:true, app_public_id: appPublicId, tg_user_id: String(tgUserId), items }, 200, request);
}

async function sendDialogMessage(appId, tgUserId, env, ownerId, request){
  const body = await request.json().catch(()=>null);
  if (!body) return json({ ok:false, error:'BAD_JSON' }, 400, request);

  const text = String(body.text || '').trim();
  if (!text) return json({ ok:false, error:'TEXT_REQUIRED' }, 400, request);

  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 404, request);

  const botToken = await getBotTokenForApp(appPublicId, env, appId);
  if (!botToken) return json({ ok:false, error:'BOT_TOKEN_NOT_FOUND' }, 400, request);




  // ===== PASSPORT PIN: if waiting for PIN, try consume it =====
  try{
    const t = String(text || '').trim();
    if (t && /^\d{3,10}$/.test(t) && env.BOT_SECRETS) {
      const pendKey = `pin_pending:${appPublicId}:${String(from.id)}`;
      const pendRaw = await env.BOT_SECRETS.get(pendKey);

      if (pendRaw) {
        let pend = null;
        try{ pend = JSON.parse(pendRaw); }catch(_){ pend = null; }

        const styleId = String(pend?.style_id || '').trim();
        if (styleId) {
          // проверим pin и проставим стиль тем же кодом, что и в mini-api
          const pres = await useOneTimePin(env.DB, appPublicId, String(from.id), t, styleId);
          if (!pres || !pres.ok){
            await tgSendMessage(env, botToken, chatId,
              `⛔️ PIN не принят: <b>${String(pres?.error || 'pin_invalid')}</b>\nПопробуйте ещё раз.`,
              {}, { appPublicId, tgUserId: from.id }
            );
            return new Response('OK', { status: 200, headers: corsHeaders(request) });
          }

          // upsert styles_user
          const up = await env.DB.prepare(
            `UPDATE styles_user
             SET status='collected', ts=datetime('now')
             WHERE app_public_id=? AND tg_id=? AND style_id=?`
          ).bind(appPublicId, String(from.id), styleId).run();

          if (!up?.meta?.changes) {
            await env.DB.prepare(
              `INSERT INTO styles_user (app_id, app_public_id, tg_id, style_id, status, ts)
               VALUES (?, ?, ?, ?, 'collected', datetime('now'))`
            ).bind(ctx.appId, appPublicId, String(from.id), styleId).run();
          }

          await env.BOT_SECRETS.delete(pendKey);

          await tgSendMessage(env, botToken, chatId,
            `✅ Штамп получен: <b>${styleId}</b>\nОткройте мини-апп — карточка подсветится.`,
            {}, { appPublicId, tgUserId: from.id }
          );

          return new Response('OK', { status: 200, headers: corsHeaders(request) });
        }
      }
    }
  }catch(e){
    console.error('[passport-pin] failed', e);
  }








  // отправляем через твой tgSendMessage — он уже логирует "out" в bot_messages
  const resp = await tgSendMessage(env, botToken, String(tgUserId), text, {}, { appPublicId, tgUserId: String(tgUserId) });

  if (!resp.ok){
    const errText = await resp.text().catch(()=> '');
    return json({ ok:false, error:'TG_SEND_FAILED', status: resp.status, details: errText.slice(0, 500) }, 502, request);
  }

  return json({ ok:true }, 200, request);
}




async function getBotIntegration(appId, env, ownerId, request) {
  // 1) Канонический public_id (единая истина)
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) {
    return json({ ok: false, error: 'APP_PUBLIC_ID_NOT_FOUND' }, 404, request);
  }

  // 2) KV токен — только новый ключ
  const kvKey = 'bot_token:public:' + appPublicId;
  const raw = await env.BOT_SECRETS.get(kvKey);
  const hasToken = !!raw;

  // 2.1) Webhook URL (Variant A): секрет лежит в KV bot_whsec:public:<publicId>
  let webhookUrl = null;
  try {
    const sec = await getBotWebhookSecretForPublicId(appPublicId, env);
    if (sec) {
      webhookUrl =
        'https://app.salesgenius.ru/api/tg/webhook/' +
        encodeURIComponent(appPublicId) +
        '?s=' +
        encodeURIComponent(sec);
    }
  } catch (e) {
    console.warn('[bot] getBotIntegration: webhook secret read failed', e);
  }

  // 3) bots — ищем по (owner_id, app_public_id)
  const bot = await env.DB
    .prepare('SELECT id, username, tg_bot_id, status, updated_at FROM bots WHERE owner_id = ? AND app_public_id = ? LIMIT 1')
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

      webhook: {
        url: webhookUrl,
      },
    },
    200,
    request
  );
}

// ========================== pay ==========================
async function tgApi(botToken, method, payload){
  const r = await fetch(`https://api.telegram.org/bot${botToken}/${method}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const j = await r.json().catch(()=>null);
  if (!r.ok || !j || !j.ok){
    throw new Error(`[tgApi] ${method} failed: HTTP ${r.status} ` + (j ? JSON.stringify(j) : 'nojson'));
  }
  return j.result;
}

// Create invoice link for Telegram Stars (XTR)
async function tgCreateInvoiceLinkStars(botToken, { title, description, payload, stars, photo_url }){
  const data = {
    title: String(title || 'Покупка'),
    description: String(description || ''),
    payload: String(payload || ''),
    currency: 'XTR',
    prices: [{ label: 'Итого', amount: Math.max(1, Math.floor(Number(stars||0))) }],
  };
  if (photo_url) data.photo_url = String(photo_url);

  // provider_token for Stars is not used (omit)
  return await tgApi(botToken, 'createInvoiceLink', data);
}

// Must answer within 10 seconds
async function tgAnswerPreCheckoutQuery(botToken, preCheckoutQueryId, ok, errorMessage=''){
  const data = { pre_checkout_query_id: String(preCheckoutQueryId), ok: !!ok };
  if (!ok && errorMessage) data.error_message = String(errorMessage).slice(0, 200);
  return await tgApi(botToken, 'answerPreCheckoutQuery', data);
}


async function tgAnswerCallbackQuery(botToken, callbackQueryId, text='', showAlert=false){
  try{
    await tgApi(botToken, 'answerCallbackQuery', {
      callback_query_id: callbackQueryId,
      text: text || undefined,
      show_alert: !!showAlert
    });
  }catch(_){}
}




// ========================== MINI-APP API (GAS -> Worker) ==========================

// helper
function nowISO(){ return new Date().toISOString(); }

// resolve app & bot token + required tg user
async function resolveAppContextByPublicId(publicId, env) {
  const map = await env.APPS.get('app:by_public:' + publicId, 'json');
  if (!map || !map.appId) return { ok:false, status:404, error:'UNKNOWN_PUBLIC_ID' };
  const appId = map.appId;
  const canonicalPublicId = (await getCanonicalPublicIdForApp(appId, env)) || publicId;
  return { ok:true, appId, publicId: canonicalPublicId };
}

// verify Telegram initData (если есть токен)
async function requireTgAndVerify(publicId, initDataRaw, env){
  const ctx = await resolveAppContextByPublicId(publicId, env);
  if (!ctx.ok) return ctx;

  const botToken = await getBotTokenForApp(ctx.publicId, env, ctx.appId);
  if (botToken) {
    if (!initDataRaw) return { ok:false, status:403, error:'NO_INIT_DATA' };
    const ok = await verifyInitDataSignature(initDataRaw, botToken);
    if (!ok) return { ok:false, status:403, error:'BAD_SIGNATURE' };
  }
  return { ok:true, ...ctx };
}

// --- USERS UPSERT (аналог _usersUpsert из GAS) ---
async function upsertAppUser(db, appId, appPublicId, tg) {
  const tgId = String(tg.id);
  const row = await db.prepare(
    `SELECT id, coins FROM app_users WHERE app_public_id = ? AND tg_user_id = ? LIMIT 1`
  ).bind(appPublicId, tgId).first();

  if (!row) {
    const ins = await db.prepare(
      `INSERT INTO app_users (app_id, app_public_id, tg_user_id, tg_username, first_seen, last_seen, coins)
       VALUES (?, ?, ?, ?, datetime('now'), datetime('now'), 0)`
    ).bind(appId, appPublicId, tgId, tg.username || null).run();
    return { id: Number(ins.lastInsertRowid), coins: 0 };
  } else {
    await db.prepare(
      `UPDATE app_users SET tg_username = ?, last_seen = datetime('now') WHERE id = ?`
    ).bind(tg.username || null, row.id).run();
    return { id: row.id, coins: Number(row.coins || 0) };
  }
}

// --- COINS LEDGER (аналог _awardCoins / _getLastBalance) ---
async function getLastBalance(db, appPublicId, tgId){
  const row = await db.prepare(
    `SELECT balance_after FROM coins_ledger
     WHERE app_public_id = ? AND tg_id = ?
     ORDER BY id DESC LIMIT 1`
  ).bind(appPublicId, String(tgId)).first();
  return row ? Number(row.balance_after||0) : 0;
}

async function setUserCoins(db, appPublicId, tgId, balance){
  await db.prepare(
    `UPDATE app_users SET coins = ? WHERE app_public_id = ? AND tg_user_id = ?`
  ).bind(balance, appPublicId, String(tgId)).run();
}

async function awardCoins(db, appId, appPublicId, tgId, delta, src, ref_id, note, event_id){
  // идемпотентность по event_id
  if (event_id) {
    const ex = await db.prepare(
      `SELECT balance_after FROM coins_ledger WHERE event_id = ? LIMIT 1`
    ).bind(event_id).first();
    if (ex) return { ok:true, reused:true, balance: Number(ex.balance_after||0) };
  }
  const last = await getLastBalance(db, appPublicId, tgId);
  const bal = Math.max(0, last + Number(delta||0));
  await db.prepare(
    `INSERT INTO coins_ledger (app_id, app_public_id, tg_id, event_id, src, ref_id, delta, balance_after, note)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(appId, appPublicId, String(tgId), event_id||null, String(src||''), String(ref_id||''), Number(delta||0), bal, String(note||'')).run();
  await setUserCoins(db, appPublicId, tgId, bal);
  return { ok:true, balance: bal };
}

async function spendCoinsIfEnough(db, appId, appPublicId, tgId, cost, src, ref_id, note, event_id){
  cost = Math.max(0, Math.floor(Number(cost || 0)));
  if (cost <= 0) return { ok:true, spent:0, balance: await getLastBalance(db, appPublicId, tgId) };

  const last = await getLastBalance(db, appPublicId, tgId);
  if (last < cost){
    return { ok:false, error:'NOT_ENOUGH_COINS', have:last, need:cost };
  }
  // списываем отрицательным delta, идемпотентно по event_id
  const res = await awardCoins(db, appId, appPublicId, tgId, -cost, src, ref_id, note, event_id);
  return { ok:true, spent:cost, balance: res.balance };
}


// --- styles helpers (аналог _styleTitle/_stylesTotalCount) ---
async function styleTitle(db, appPublicId, styleId){
  const row = await db.prepare(
    `SELECT title FROM styles_dict WHERE app_public_id = ? AND style_id = ? LIMIT 1`
  ).bind(appPublicId, String(styleId||'')).first();
  return row ? String(row.title||'') : '';
}

async function stylesTotalCount(db, appPublicId){
  const rows = await db.prepare(
    `SELECT COUNT(DISTINCT style_id) as cnt FROM styles_dict WHERE app_public_id = ?`
  ).bind(appPublicId).first();
  return rows ? Number(rows.cnt||0) : 0;
}



async function passportCollectedCount(db, appPublicId, tgId){
  const row = await db.prepare(
    `SELECT COUNT(DISTINCT style_id) AS cnt
     FROM styles_user
     WHERE app_public_id = ? AND tg_id = ? AND status = 'collected'`
  ).bind(appPublicId, String(tgId)).first();
  return row ? Number(row.cnt || 0) : 0;
}

async function passportGetIssued(db, appPublicId, tgId, passportKey){
  return await db.prepare(
    `SELECT id, prize_code, prize_title, coins, redeem_code, status, issued_at
     FROM passport_rewards
     WHERE app_public_id = ? AND tg_id = ? AND passport_key = ?
     LIMIT 1`
  ).bind(appPublicId, String(tgId), String(passportKey||'default')).first();
}

async function passportIssueRewardIfCompleted(db, env, ctx, tgId, cfg){
  const passportKey = String((cfg?.passport?.passport_key) || 'default');

  // prize_code берём из cfg.passport.reward_prize_code
  const prizeCode = String((cfg?.passport?.reward_prize_code) || '').trim();
  if (!prizeCode) return { ok:true, skipped:true, reason:'NO_REWARD_PRIZE_CODE' };

  const total = await stylesTotalCount(db, ctx.publicId);
  if (!total) return { ok:true, skipped:true, reason:'NO_STYLES_TOTAL' };

  const got = await passportCollectedCount(db, ctx.publicId, tgId);
  if (got < total) return { ok:true, skipped:true, reason:'NOT_COMPLETED', got, total };

  // ===== last reward row (issued/redeemed)
  const ex = await db.prepare(
    `SELECT id, prize_code, prize_title, coins, redeem_code, status
     FROM passport_rewards
     WHERE app_public_id=? AND tg_id=? AND passport_key=?
     ORDER BY id DESC
     LIMIT 1`
  ).bind(ctx.publicId, String(tgId), passportKey).first();

  // уже есть активный (issued) — повторно не выдаём
  if (ex && String(ex.status) === 'issued'){
    return { ok:true, issued:true, reused:true, reward: ex, got, total };
  }

  // ===== берём приз из wheel_prizes
  const pr = await db.prepare(
    `SELECT code, title, coins
     FROM wheel_prizes
     WHERE app_public_id = ? AND code = ?
     LIMIT 1`
  ).bind(ctx.publicId, prizeCode).first();

  if (!pr) return { ok:false, error:'REWARD_PRIZE_NOT_FOUND', prize_code: prizeCode };

  const prizeTitle = String(pr.title || prizeCode);
  const prizeCoins = Math.max(0, Math.floor(Number(pr.coins || 0)));

  // bot token (для уведомления)
  const botToken = await getBotTokenForApp(ctx.publicId, env, ctx.appId).catch(()=>null);

  // ===== ВАЖНО: и для coins, и для physical мы теперь создаём redeem_code
  // чтобы кассир подтверждал выдачу (и монеты начислялись только после подтверждения)
  let redeemCode = '';
  for (let i=0;i<8;i++){
    redeemCode = randomRedeemCode(10);
    try{
      if (ex && ex.id){
        // reuse last row (was redeemed) -> issue again
        await db.prepare(
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
        ).bind(
          prizeCode,
          prizeTitle,
          prizeCoins,
          redeemCode,
          Number(ex.id)
        ).run();
      }else{
        await db.prepare(
          `INSERT INTO passport_rewards
           (app_id, app_public_id, tg_id, passport_key, prize_code, prize_title, coins, redeem_code, status, issued_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'issued', datetime('now'))`
        ).bind(
          ctx.appId, ctx.publicId, String(tgId), passportKey,
          prizeCode, prizeTitle, prizeCoins, redeemCode
        ).run();
      }
      break;
    }catch(e){
      const msg = String(e?.message || e);
      // если redeem_code UNIQUE и коллизия — пробуем снова
      if (/unique|constraint/i.test(msg)) continue;
      throw e;
    }
  }
  if (!redeemCode) return { ok:false, error:'PASSPORT_REDEEM_CREATE_FAILED' };

  // лог бонусов (как “ожидает выдачи”)
  // можно писать claim_status='pending' (если колонка позволяет), иначе не пиши вовсе
  try{
    await db.prepare(
      `INSERT INTO bonus_claims (app_id, app_public_id, tg_id, prize_id, prize_name, prize_value, claim_status)
       VALUES (?, ?, ?, ?, ?, ?, 'pending')`
    ).bind(ctx.appId, ctx.publicId, String(tgId), prizeCode, prizeTitle, prizeCoins).run();
  }catch(_){}

  // deep link
  let botUsername = '';
  try{
    const b = await db.prepare(
      `SELECT username FROM bots
       WHERE app_public_id = ? AND status='active'
       ORDER BY id DESC LIMIT 1`
    ).bind(ctx.publicId).first();
    botUsername = (b && b.username) ? String(b.username).replace(/^@/,'').trim() : '';
  }catch(_){ botUsername = ''; }

  const deepLink = botUsername ? `https://t.me/${botUsername}?start=redeem_${encodeURIComponent(redeemCode)}` : '';

  // уведомление пользователю: теперь ВСЕ призы подтверждаются кассиром
  try{
    if (botToken){
      const lines = [
        `🏁 Паспорт заполнен!`,
        `🎁 Ваш приз: <b>${prizeTitle}</b>`,
        prizeCoins > 0 ? `🪙 Монеты: <b>${prizeCoins}</b> (после подтверждения кассиром)` : '',
        ``,
        `✅ Код выдачи: <code>${redeemCode}</code>`,
        deepLink ? `Откройте ссылку:\n${deepLink}` : `Покажите код кассиру.`
      ].filter(Boolean);

      await tgSendMessage(env, botToken, String(tgId), lines.join('\n'), {}, {
        appPublicId: ctx.publicId,
        tgUserId: String(tgId)
      });
    }
  }catch(e){
    console.error('[passport.reward] tgSendMessage redeem failed', e);
  }

  // ❗ НЕ начисляем монеты здесь
  // ❗ НЕ сбрасываем styles_user здесь
  // Всё это будет в redeem-flow у кассира.

  return {
    ok:true,
    issued:true,
    reward:{ prize_code: prizeCode, prize_title: prizeTitle, coins: prizeCoins, redeem_code: redeemCode },
    got, total
  };
}








// --- leaderboard helpers (аналог _buildLeaderboard/_buildLeaderboardAlltime) ---
async function buildLeaderboard(db, appPublicId, dateStr, mode, topN){
  const rows = await db.prepare(
    `SELECT gr.tg_id, gr.best_score, au.tg_username
     FROM games_results_daily gr
     LEFT JOIN app_users au ON au.app_public_id = gr.app_public_id AND au.tg_user_id = gr.tg_id
     WHERE gr.app_public_id = ? AND gr.date = ? AND gr.mode = ?
     GROUP BY gr.tg_id
     ORDER BY gr.best_score DESC
     LIMIT ?`
  ).bind(appPublicId, dateStr, mode, topN).all();
  return (rows.results||[]).map(r => ({
    tg_id: String(r.tg_id),
    username: r.tg_username || '',
    first_name: '',
    last_name: '',
    score: Number(r.best_score||0)
  }));
}

async function buildLeaderboardAllTime(db, appPublicId, mode, topN){
  const rows = await db.prepare(
    `SELECT gr.tg_id, MAX(gr.best_score) as best_score, au.tg_username
     FROM games_results_daily gr
     LEFT JOIN app_users au ON au.app_public_id = gr.app_public_id AND au.tg_user_id = gr.tg_id
     WHERE gr.app_public_id = ? AND gr.mode = ?
     GROUP BY gr.tg_id
     ORDER BY best_score DESC
     LIMIT ?`
  ).bind(appPublicId, mode, topN).all();
  return (rows.results||[]).map(r => ({
    tg_id: String(r.tg_id),
    username: r.tg_username || '',
    first_name: '',
    last_name: '',
    score: Number(r.best_score||0)
  }));
}



// --- buildState (аналог _buildState из GAS) ---
async function buildState(db, appId, appPublicId, tgId, cfg = {}){
  const out = {
    bot_username: '',
    coins: 0,
    last_prizes: [],
    styles: [],
    styles_user: [],
    styles_count: 0,
    styles_total: 0,
    last_stamp_id: '',
    last_stamp_name: '',
    game_today_best: 0,
    game_plays_today: 0,
    leaderboard_today: [],
    leaderboard_alltime: [],
    config: {},
    wheel: { claim_cooldown_left_ms: 0, has_unclaimed: false, last_prize_code: '', last_prize_title: '' },
    ref_total: 0
  };

// bot username for referral links (active bot)
try{
  const pid = String(appPublicId || '').trim();

  const b = await db.prepare(`
    SELECT username
    FROM bots
    WHERE app_public_id = ? AND status = 'active'
    ORDER BY id DESC
    LIMIT 1
  `).bind(pid).first();

  out.bot_username = b?.username ? String(b.username).replace(/^@/,'').trim() : '';
}catch(e){
  console.log('[ref] bot username lookup error', e);
  out.bot_username = '';
}




  // coins
  out.coins = await getLastBalance(db, appPublicId, tgId);
  if (!out.coins){
    const u = await db.prepare(
      `SELECT coins FROM app_users WHERE app_public_id = ? AND tg_user_id = ?`
    ).bind(appPublicId, String(tgId)).first();
    out.coins = u ? Number(u.coins||0) : 0;
  }

  // last prizes (10) из bonus_claims
  const lp = await db.prepare(
    `SELECT prize_id, prize_name, prize_value, ts
     FROM bonus_claims
     WHERE app_public_id = ? AND tg_id = ?
     AND (claim_status IS NULL OR claim_status = 'ok')
     ORDER BY id DESC LIMIT 10`
  ).bind(appPublicId, String(tgId)).all();
  out.last_prizes = (lp.results||[]).map(r => ({
    prize_id: r.prize_id || '',
    prize_name: r.prize_name || '',
    prize_value: Number(r.prize_value||0),
    ts: r.ts || nowISO()
  }));

  // styles + last stamp
  const su = await db.prepare(
    `SELECT style_id, status, ts
     FROM styles_user
     WHERE app_public_id = ? AND tg_id = ? AND status = 'collected'
     ORDER BY ts DESC`
  ).bind(appPublicId, String(tgId)).all();
  out.styles_user = (su.results || []).map(r => ({
    style_id: String(r.style_id || ''),
    status: String(r.status || 'collected'),
    ts: r.ts || ''
  }));
    
  let lastTs = 0, lastSid = '';
  const seen = new Set();
  
  for (const r of (su.results || [])) {
    const sid = String(r.style_id || '');
    if (sid) seen.add(sid);
  
    const tms = r.ts ? (Date.parse(r.ts) || 0) : 0;
    if (sid && tms > lastTs) { lastTs = tms; lastSid = sid; }
  }
  out.styles = Array.from(seen);
  out.last_stamp_id = lastSid;
  out.last_stamp_name = await styleTitle(db, appPublicId, lastSid);
  out.styles_count = out.styles.length;
  out.styles_total = await stylesTotalCount(db, appPublicId);

    // passport reward snapshot (if issued)
    try{
      const rw = await passportGetIssued(db, appPublicId, tgId, 'default');
      out.passport_reward = rw ? {
        prize_code: String(rw.prize_code || ''),
        prize_title: String(rw.prize_title || ''),
        coins: Number(rw.coins || 0),
        redeem_code: String(rw.redeem_code || ''),
        status: String(rw.status || 'issued'),
        issued_at: rw.issued_at || ''
      } : null;
    }catch(_){
      out.passport_reward = null;
    }
  

  // game snapshot today
  const today = new Date().toISOString().slice(0,10);
  const mode = 'daily';
  const g = await db.prepare(
    `SELECT best_score, plays FROM games_results_daily
     WHERE app_public_id = ? AND date = ? AND mode = ? AND tg_id = ?
     ORDER BY id DESC LIMIT 1`
  ).bind(appPublicId, today, mode, String(tgId)).first();
  if (g){
    out.game_today_best = Number(g.best_score||0);
    out.game_plays_today= Number(g.plays||0);
  }
  const topN = Number(cfg.LEADERBOARD_TOP_N || 10) || 10;
  out.leaderboard_today   = await buildLeaderboard(db, appPublicId, today, mode, topN);
  out.leaderboard_alltime = await buildLeaderboardAllTime(db, appPublicId, mode, topN);

  // config snapshot (минимум — можешь подтянуть из KV/D1 config)
  const cdH = Number(cfg.WHEEL_CLAIM_COOLDOWN_H || 24);
  out.config = {
    SPIN_COST:                 Number(cfg.SPIN_COST || 0),
    SPIN_COOLDOWN_SEC:         Number(cfg.SPIN_COOLDOWN_SEC || 0),
    SPIN_DAILY_LIMIT:          Number(cfg.SPIN_DAILY_LIMIT || 0),
    QUIZ_COINS_PER_CORRECT:    Number(cfg.QUIZ_COINS_PER_CORRECT || 0),
    QUIZ_COINS_MAX_PER_SUBMIT: Number(cfg.QUIZ_COINS_MAX_PER_SUBMIT || 0),
    STYLE_COLLECT_COINS:       Number(cfg.STYLE_COLLECT_COINS || 0),
    LEADERBOARD_TOP_N:         topN,
    WHEEL_SPIN_COST:           Number(cfg.WHEEL_SPIN_COST || 0),
    WHEEL_CLAIM_COOLDOWN_H:    cdH
  };


// wheel: есть ли незабранный "won" и есть ли активный redeem
out.wheel.claim_cooldown_left_ms = 0;

const lastWon = await db.prepare(
  `SELECT id, prize_code, prize_title
   FROM wheel_spins
   WHERE app_public_id = ? AND tg_id = ? AND status = 'won'
   ORDER BY id DESC LIMIT 1`
).bind(appPublicId, String(tgId)).first();

if (lastWon){
  out.wheel.has_unclaimed = true;
  out.wheel.last_prize_code  = lastWon.prize_code || '';
  out.wheel.last_prize_title = lastWon.prize_title || '';

  // если уже выпущен redeem по этому выигрышу — вернём статус (можно показывать "код выдан/получен")
  const rr = await db.prepare(
    `SELECT redeem_code, status, issued_at, redeemed_at
     FROM wheel_redeems
     WHERE app_public_id = ? AND spin_id = ?
     LIMIT 1`
  ).bind(appPublicId, Number(lastWon.id)).first();

  if (rr){
    out.wheel.redeem_code = rr.redeem_code || '';
    out.wheel.redeem_status = rr.status || 'issued';
    out.wheel.redeem_issued_at = rr.issued_at || '';
    out.wheel.redeem_redeemed_at = rr.redeemed_at || '';
  } else {
    out.wheel.redeem_code = '';
    out.wheel.redeem_status = '';
  }
} else {
  out.wheel.has_unclaimed = false;
  out.wheel.last_prize_code = '';
  out.wheel.last_prize_title = '';
  out.wheel.redeem_code = '';
  out.wheel.redeem_status = '';
}


  // ref_total — опционально, если введёшь таблицу referrals
  out.ref_total = await refsTotal(db, appPublicId, tgId);


  return out;
}

// --- pickWheelPrize (аналог _pickWheelPrize_) ---
async function pickWheelPrize(db, appPublicId){
  const rows = await db.prepare(
    `SELECT code, title, weight, coins, active
     FROM wheel_prizes WHERE app_public_id = ?`
  ).bind(appPublicId).all();
  const list = (rows.results||[])
    .filter(r => Number(r.active||0) && Number(r.weight||0)>0)
    .map(r => ({ code:String(r.code), title:String(r.title||r.code), weight:Number(r.weight), coins:Number(r.coins||0) }));
  if (!list.length) return null;
  const sum = list.reduce((a,b)=>a+b.weight,0);
  let rnd = Math.random() * sum, acc = 0;
  for (const it of list){ acc += it.weight; if (rnd <= acc) return it; }
  return list[list.length-1];
}

// --- Квиз профиль: state/finish (аналог _handleProfileQuizState_/Finish_) ---
async function quizState(db, appPublicId, tgId, quizId='beer_profile_v1'){
  const r = await db.prepare(
    `SELECT * FROM profile_quiz WHERE app_public_id = ? AND tg_id = ? AND quiz_id = ? LIMIT 1`
  ).bind(appPublicId, String(tgId), quizId).first();
  if (!r) return { ok:true, status:'not_started' };
  return {
    ok:true,
    status: r.status||'completed',
    score: Number(r.score||0),
    bday_day: Number(r.bday_day||0),
    bday_month: Number(r.bday_month||0),
    profile: {
      scene: r.scene||'', evening_scene: r.evening_scene||'', beer_character: r.beer_character||'',
      experiments: r.experiments||'', focus: r.focus||'', anti_flavors: r.anti_flavors||'',
      snacks: r.snacks||'', budget: r.budget||'', time_of_day: r.time_of_day||'',
      comms: r.comms||'', birthday_optin: r.birthday_optin||''
    },
    answers_json: r.answers_json||''
  };
}

async function quizFinish(db, appId, appPublicId, tgId, data){
  const quizId = String(data.quiz_id || 'beer_profile_v1');
  const score  = Number(data.score||0);

  const profile = data.profile || {};
  const answersJson = data.answers_json || JSON.stringify(profile||{});

  const now = nowISO();

  await db.prepare(
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
  ).bind(
    appId, appPublicId, String(tgId), quizId, score,
    Number(data.bday_day||0), Number(data.bday_month||0),
    String(profile.scene||''), String(profile.evening_scene||''), String(profile.beer_character||''),
    String(profile.experiments||''), String(profile.focus||''),
    String(profile.anti_flavors||''), String(profile.snacks||''), String(profile.budget||''),
    String(profile.time_of_day||''), String(profile.comms||''), String(profile.birthday_optin||''),
    String(answersJson||''), now, now
  ).run();

  if (score>0){
    await awardCoins(db, appId, appPublicId, tgId, score, 'profile_quiz', quizId, 'profile quiz reward', null);
  }

  const fresh = await buildState(db, appId, appPublicId, tgId, {});
  return { ok:true, status:'completed', score, fresh_state: fresh };
}



// --- PINs
function randomPin4(){
  // 1000..9999 (не 0000)
  return String(Math.floor(1000 + Math.random()*9000));
}

async function issuePinToCustomer(db, appPublicId, cashierTgId, customerTgId, styleId){
  let pin = '';
  for (let i=0;i<12;i++){
    pin = randomPin4();
    try{
      await db.prepare(
        `INSERT INTO pins_pool (app_public_id, pin, target_tg_id, style_id, issued_by_tg, issued_at)
         VALUES (?, ?, ?, ?, ?, datetime('now'))`
      ).bind(
        String(appPublicId),
        String(pin),
        String(customerTgId),
        String(styleId),
        String(cashierTgId)
      ).run();
      return { ok:true, pin };
    }catch(e){
      const msg = String(e?.message || e);
      // коллизия UNIQUE(app_public_id,pin) — пробуем ещё
      if (/unique|constraint/i.test(msg)) continue;
      throw e;
    }
  }
  return { ok:false, error:'PIN_CREATE_FAILED' };
}


// --- PINs (одноразовый PIN, привязан к конкретному tg + style) ---
async function useOneTimePin(db, appPublicId, tgId, pin, styleId){
  const row = await db.prepare(
    `SELECT id, used_at, target_tg_id, style_id
     FROM pins_pool
     WHERE app_public_id = ? AND pin = ?
     LIMIT 1`
  ).bind(String(appPublicId), String(pin||'')).first();

  if (!row) return { ok:false, error:'pin_invalid' };
  if (row.used_at) return { ok:false, error:'pin_used' };

  // PIN должен принадлежать этому пользователю
  if (String(row.target_tg_id||'') !== String(tgId)) return { ok:false, error:'pin_invalid' };

  // И под этот style_id (если передали)
  if (styleId && String(row.style_id||'') !== String(styleId)) return { ok:false, error:'pin_invalid' };

  await db.prepare(
    `UPDATE pins_pool
     SET used_at = datetime('now')
     WHERE id = ? AND used_at IS NULL`
  ).bind(Number(row.id)).run();
  

  return { ok:true };
}


// --- REFERALS (_Рефералы_) ---
async function bindReferralOnce(db, appPublicId, inviteeTgId, referrerTgId){
  const a = String(appPublicId||'');
  const invitee = String(inviteeTgId||'');
  const ref = String(referrerTgId||'').trim();

  if (!a || !invitee || !ref) return { ok:false, skipped:true, reason:'empty' };
  if (ref === invitee) return { ok:false, skipped:true, reason:'self' };

  // уже привязан кто-то? (unique по invitee)
  const ex = await db.prepare(
    `SELECT id FROM referrals WHERE app_public_id=? AND invitee_tg_id=? LIMIT 1`
  ).bind(a, invitee).first();

  if (ex) return { ok:true, skipped:true, reason:'already_bound' };

  // вставляем
  await db.prepare(
    `INSERT INTO referrals (app_public_id, referrer_tg_id, invitee_tg_id, confirmed, created_at)
     VALUES (?, ?, ?, 1, datetime('now'))`
  ).bind(a, ref, invitee).run();

  return { ok:true, bound:true };
}

async function refsTotal(db, appPublicId, referrerTgId){
  const r = await db.prepare(
    `SELECT COUNT(1) AS c
     FROM referrals
     WHERE app_public_id=? AND referrer_tg_id=?`
  ).bind(String(appPublicId), String(referrerTgId)).first();
  return Number(r?.c || 0);
}


// ===================== HTTP endpoints =====================
async function handleMiniApi(request, env, url){
  const db = env.DB;
  const publicId = url.searchParams.get('public_id') || url.pathname.split('/').pop();

  if (request.method === 'OPTIONS') {
    return new Response('', { status:204, headers: corsHeaders(request) });
  }

  // читаем JSON
  let body = {};
  try { body = await request.json(); } catch(_){}

  // tg + подпись
  const initDataRaw = body.init_data || body.initData || null;
  const tg = body.tg_user || {};
  if (!tg || !tg.id) return json({ ok:false, error:'NO_TG_USER_ID' }, 400, request);

  // проверка приложения + подписи
  const ctx = await requireTgAndVerify(publicId, initDataRaw, env);
  if (!ctx.ok) return json({ ok:false, error: ctx.error || 'AUTH_FAILED' }, ctx.status||403, request);

  // апсерт пользователя
  const user = await upsertAppUser(db, ctx.appId, ctx.publicId, tg);

// роуты по типу (берём из body / query / последнего сегмента пути)
let type = body.type || url.searchParams.get('type') || '';
if (!type) {
  const seg = (url.pathname || '').split('/').filter(Boolean).pop(); // e.g. 'spin', 'state', 'claim_prize'
  type = seg || '';
}
// алиасы на всякий
if (type === 'claim') type = 'claim_prize';
if (type === 'quiz')  type = 'quiz_state';

const payload = body.payload || {};


if (type === 'state') {
  const appObj = await env.APPS.get('app:' + ctx.appId, 'json').catch(()=>null);
  const cfg = (appObj && (appObj.app_config ?? appObj.runtime_config ?? appObj.config)) || {};   // wheel/passport settings

// referral from start_param (как в GAS)
let startParam = '';
try {
  const p = new URLSearchParams(String(initDataRaw||''));
  startParam = String(p.get('start_param') || '');
} catch(_) {}

if (startParam.startsWith('ref_')) {
  const refTgId = startParam.slice(4).trim();
  await bindReferralOnce(db, ctx.publicId, String(tg.id), refTgId);
}


  const state = await buildState(db, ctx.appId, ctx.publicId, tg.id, cfg);
  return json({ ok:true, state }, 200, request);
}


if (type === 'spin') {
  // cfg (нужен для buildState)
  const appObj = await env.APPS.get('app:' + ctx.appId, 'json').catch(()=>null);
  const cfg = (appObj && (appObj.app_config ?? appObj.runtime_config ?? appObj.config)) || {};
  const spinCost = Math.max(0, Math.floor(Number(cfg?.wheel?.spin_cost ?? cfg?.WHEEL_SPIN_COST ?? 0)));

  // 0) если есть незабранный win — НЕ кидаем 409, а возвращаем ok:true + fresh_state
  const unclaimed = await db.prepare(
    `SELECT id, prize_code, prize_title
     FROM wheel_spins
     WHERE app_public_id = ? AND tg_id = ? AND status = 'won'
     ORDER BY id DESC LIMIT 1`
  ).bind(ctx.publicId, String(tg.id)).first();

  if (unclaimed) {
    // coins берём из wheel_prizes (истина из фронта)
    const pr = await db.prepare(
      `SELECT coins
       FROM wheel_prizes
       WHERE app_public_id=? AND code=? LIMIT 1`
    ).bind(ctx.publicId, String(unclaimed.prize_code||'')).first();

    const prizeCoins = Math.max(0, Math.floor(Number(pr?.coins || 0)));

    // строим свежий state (чтобы фронт сразу включил кнопку)
    let fresh = null;
    try { fresh = await buildState(db, ctx.appId, ctx.publicId, tg.id, cfg); } catch(_){ fresh = {}; }
    fresh = (fresh && typeof fresh === 'object') ? fresh : {};
    fresh.wheel = (fresh.wheel && typeof fresh.wheel === 'object') ? fresh.wheel : {};

    fresh.wheel.has_unclaimed = true;
    fresh.wheel.claim_cooldown_left_ms = 0;
    fresh.wheel.last_prize_code = String(unclaimed.prize_code || '');
    fresh.wheel.last_prize_title = String(unclaimed.prize_title || '');
    fresh.wheel.spin_id = Number(unclaimed.id);

    return json({
      ok: true,
      already_won: true,
      spin_id: Number(unclaimed.id),
      spin_cost: spinCost,
      prize: { code: unclaimed.prize_code || '', title: unclaimed.prize_title || '', coins: prizeCoins },
      fresh_state: fresh
    }, 200, request);
  }

  // 1) создаём "черновик" спина
  const ins = await db.prepare(
    `INSERT INTO wheel_spins (app_id, app_public_id, tg_id, status, prize_code, prize_title, spin_cost)
     VALUES (?, ?, ?, 'new', '', '', ?)`
  ).bind(ctx.appId, ctx.publicId, String(tg.id), spinCost).run();

  const spinId = Number(ins?.meta?.last_row_id || ins?.lastInsertRowid || 0);
  if (!spinId){
    return json({ ok:false, error:'SPIN_CREATE_FAILED' }, 500, request);
  }

  // 2) списать стоимость (если есть)
  if (spinCost > 0){
    const spend = await spendCoinsIfEnough(
      db, ctx.appId, ctx.publicId, tg.id,
      spinCost,
      'wheel_spin_cost',
      String(spinId),
      'Spin cost',
      `wheel:cost:${ctx.publicId}:${tg.id}:${spinId}`
    );
    if (!spend.ok){
      try { await db.prepare(`DELETE FROM wheel_spins WHERE id=?`).bind(spinId).run(); } catch(_){}
      // оставляем как есть: фронт уже умеет ловить 409 NOT_ENOUGH_COINS
      return json({ ok:false, error: spend.error, have: spend.have, need: spend.need }, 409, request);
    }
  }

  // 3) выбрать приз
  const prize = await pickWheelPrize(db, ctx.publicId);
  if (!prize){
    if (spinCost > 0){
      await awardCoins(
        db, ctx.appId, ctx.publicId, tg.id,
        spinCost, 'wheel_refund', String(spinId), 'Refund: no prizes',
        `wheel:refund:${ctx.publicId}:${tg.id}:${spinId}`
      );
    }
    try { await db.prepare(`DELETE FROM wheel_spins WHERE id=?`).bind(spinId).run(); } catch(_){}
    return json({ ok:false, error:'NO_PRIZES' }, 400, request);
  }

  // 4) фиксируем won
  await db.prepare(
    `UPDATE wheel_spins
     SET status='won', prize_code=?, prize_title=?
     WHERE id=?`
  ).bind(String(prize.code||''), String(prize.title||''), spinId).run();

  // 5) coins берём из wheel_prizes (НЕ из code coins_5)
  const pr = await db.prepare(
    `SELECT coins
     FROM wheel_prizes
     WHERE app_public_id=? AND code=? LIMIT 1`
  ).bind(ctx.publicId, String(prize.code||'')).first();

  const prizeCoins = Math.max(0, Math.floor(Number(pr?.coins || 0)));

  // 6) fresh_state: чтобы кнопка "Забрать" появилась сразу
  let fresh = null;
  try { fresh = await buildState(db, ctx.appId, ctx.publicId, tg.id, cfg); } catch(_){ fresh = {}; }
  fresh = (fresh && typeof fresh === 'object') ? fresh : {};
  fresh.wheel = (fresh.wheel && typeof fresh.wheel === 'object') ? fresh.wheel : {};

  fresh.wheel.has_unclaimed = true;
  fresh.wheel.claim_cooldown_left_ms = 0;
  fresh.wheel.last_prize_code = String(prize.code || '');
  fresh.wheel.last_prize_title = String(prize.title || '');
  fresh.wheel.spin_id = Number(spinId);

  return json({
    ok:true,
    prize: { code: prize.code || '', title: prize.title || '', coins: prizeCoins, img: prize.img || '' },
    spin_cost: spinCost,
    spin_id: spinId,
    fresh_state: fresh
  }, 200, request);
}


if (type === 'claim_prize') {
  // cfg для buildState
  const appObj = await env.APPS.get('app:' + ctx.appId, 'json').catch(()=>null);
  const cfg = (appObj && (appObj.app_config ?? appObj.runtime_config ?? appObj.config)) || {};

  // 0) последний won
  const lastWon = await db.prepare(
    `SELECT id, prize_code, prize_title
     FROM wheel_spins
     WHERE app_public_id = ? AND tg_id = ? AND status = 'won'
     ORDER BY id DESC LIMIT 1`
  ).bind(ctx.publicId, String(tg.id)).first();

  if (!lastWon) return json({ ok:false, error:'NOTHING_TO_CLAIM' }, 400, request);

  const spinId = Number(lastWon.id);

  // 1) coins берём из wheel_prizes
  const pr = await db.prepare(
    `SELECT coins
     FROM wheel_prizes
     WHERE app_public_id=? AND code=? LIMIT 1`
  ).bind(ctx.publicId, String(lastWon.prize_code||'')).first();

  const prizeCoins = Math.max(0, Math.floor(Number(pr?.coins || 0)));

   // === A) монетный приз
   if (prizeCoins > 0) {
    await awardCoins(
      db,
      ctx.appId,
      ctx.publicId,
      tg.id,
      prizeCoins,
      'wheel_prize_claim',
      String(lastWon.prize_code||''),
      String(lastWon.prize_title||''),
      `wheel:claim:${ctx.publicId}:${tg.id}:${spinId}:${lastWon.prize_code||''}:${prizeCoins}`
    );

    // закрываем
    await db.prepare(
      `UPDATE wheel_spins
       SET status='claimed', ts_claim=datetime('now')
       WHERE id=? AND status='won'`
    ).bind(spinId).run();

    // (по желанию) бонус-лог
    try{
      await db.prepare(
        `INSERT INTO bonus_claims (app_id, app_public_id, tg_id, prize_id, prize_name, prize_value, claim_status)
         VALUES (?, ?, ?, ?, ?, ?, 'ok')`
      ).bind(ctx.appId, ctx.publicId, String(tg.id), lastWon.prize_code, lastWon.prize_title, prizeCoins).run();
    }catch(_){}

    // ===== NEW: отправка сообщения в бот о начислении монет
    try{
      const botToken = await getBotTokenForApp(ctx.publicId, env, ctx.appId).catch(()=>null);
      if (botToken){
        const msg =
          `✅ Начислено <b>${prizeCoins} 🪙</b>\n` +
          `🎁 Приз: <b>${String(lastWon.prize_title||'Бонус')}</b>`;
        await tgSendMessage(
          env,
          botToken,
          String(tg.id),
          msg,
          {},
          { appPublicId: ctx.publicId, tgUserId: String(tg.id) }
        );
      }
    }catch(e){
      console.error('[wheel.claim] tgSendMessage coins failed', e);
    }
    // ===== /NEW

    // fresh_state: выключаем кнопку
    let fresh = null;
    try { fresh = await buildState(db, ctx.appId, ctx.publicId, tg.id, cfg); } catch(_){ fresh = {}; }
    fresh = (fresh && typeof fresh === 'object') ? fresh : {};
    fresh.wheel = (fresh.wheel && typeof fresh.wheel === 'object') ? fresh.wheel : {};

    fresh.wheel.has_unclaimed = false;
    fresh.wheel.claim_cooldown_left_ms = 0;
    fresh.wheel.last_prize_code = String(lastWon.prize_code || '');
    fresh.wheel.last_prize_title = String(lastWon.prize_title || '');
    fresh.wheel.spin_id = 0;

    return json({
      ok:true,
      claimed:true,
      prize: { code: lastWon.prize_code || '', title: lastWon.prize_title || '', coins: prizeCoins },
      spin_id: spinId,
      fresh_state: fresh
    }, 200, request);
  }


  // === B) физ приз (coins=0) -> redeem
  let redeem = await db.prepare(
    `SELECT id, redeem_code, status
     FROM wheel_redeems
     WHERE app_public_id = ? AND spin_id = ?
     LIMIT 1`
  ).bind(ctx.publicId, spinId).first();

  if (!redeem){
    let code = '';
    for (let i=0;i<5;i++){
      code = randomRedeemCode(10);
      try{
        const ins = await db.prepare(
          `INSERT INTO wheel_redeems
           (app_id, app_public_id, tg_id, spin_id, prize_code, prize_title, redeem_code, status, issued_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'issued', datetime('now'))`
        ).bind(
          ctx.appId, ctx.publicId, String(tg.id),
          spinId,
          String(lastWon.prize_code||''),
          String(lastWon.prize_title||''),
          String(code)
        ).run();

        redeem = { id: Number(ins?.meta?.last_row_id || ins?.lastInsertRowid || 0), redeem_code: code, status: 'issued' };
        break;
      }catch(e){
        const msg = String(e?.message || e);
        if (!/unique|constraint/i.test(msg)) throw e;
      }
    }
    if (!redeem) return json({ ok:false, error:'REDEEM_CREATE_FAILED' }, 500, request);
  }

  try{
    await db.prepare(
      `UPDATE wheel_spins
       SET status='issued', redeem_id=?, ts_issued=datetime('now')
       WHERE id=? AND status='won'`
    ).bind(Number(redeem.id), spinId).run();
  }catch(_){}

  // bot username
  let botUsername = '';
  try{
    const b = await env.DB.prepare(
      `SELECT username FROM bots
       WHERE app_public_id = ? AND status='active'
       ORDER BY id DESC LIMIT 1`
    ).bind(ctx.publicId).first();
    botUsername = (b && b.username) ? String(b.username).replace(/^@/,'').trim() : '';
  }catch(_){ botUsername = ''; }

  const redeem_code = String(redeem.redeem_code || '');
  const deep_link = botUsername ? `https://t.me/${botUsername}?start=redeem_${encodeURIComponent(redeem_code)}` : '';
  const qr_text = deep_link || (`redeem:${redeem_code}`);

  // отправка в бот
  const botToken = await getBotTokenForApp(ctx.publicId, env, ctx.appId).catch(()=>null);
  if (botToken){
    const txt =
      `🎁 Ваш приз: <b>${String(lastWon.prize_title||'Бонус')}</b>\n\n` +
      `✅ Код выдачи: <code>${redeem_code}</code>\n` +
      (deep_link ? `Откройте ссылку:\n${deep_link}` : `Покажите код кассиру.`);

    try{
      await tgSendMessage(env, botToken, String(tg.id), txt, {}, { appPublicId: ctx.publicId, tgUserId: String(tg.id) });
    }catch(e){
      console.error('[wheel.claim] tgSendMessage failed', e);
    }
  }

  // fresh_state: выключаем кнопку (приз уже выдан кодом)
  let fresh = null;
  try { fresh = await buildState(db, ctx.appId, ctx.publicId, tg.id, cfg); } catch(_){ fresh = {}; }
  fresh = (fresh && typeof fresh === 'object') ? fresh : {};
  fresh.wheel = (fresh.wheel && typeof fresh.wheel === 'object') ? fresh.wheel : {};

  fresh.wheel.has_unclaimed = false;
  fresh.wheel.claim_cooldown_left_ms = 0;
  fresh.wheel.last_prize_code = String(lastWon.prize_code || '');
  fresh.wheel.last_prize_title = String(lastWon.prize_title || '');
  fresh.wheel.spin_id = 0;

  return json({
    ok:true,
    prize: { code: lastWon.prize_code || '', title: lastWon.prize_title || '' },
    redeem: { code: redeem_code, status: redeem.status || 'issued', deep_link, qr_text },
    spin_id: spinId,
    fresh_state: fresh
  }, 200, request);
}





  if (type === 'quiz_finish') {
    const res = await quizFinish(db, ctx.appId, ctx.publicId, tg.id, payload||{});
    return json(res, 200, request);
    
  }











  // ===================== PASSPORT: collect style (with optional PIN) =====================
if (type === 'style.collect' || type === 'style_collect') {
  const styleId = String((payload && (payload.style_id || payload.styleId || payload.code)) || '').trim();
  const pin     = String((payload && payload.pin) || '').trim();
  

  if (!styleId) return json({ ok:false, error:'NO_STYLE_ID' }, 400, request);

  // load runtime config (passport.require_pin)
  const appObj = await env.APPS.get('app:' + ctx.appId, 'json').catch(()=>null);
  const cfg = (appObj && (appObj.app_config ?? appObj.runtime_config ?? appObj.config)) || {};
  const requirePin = !!(cfg && cfg.passport && cfg.passport.require_pin);

  if (requirePin) {
    const pres = await useOneTimePin(db, ctx.publicId, tg.id, pin, styleId);
    if (!pres || !pres.ok) return json(pres || { ok:false, error:'pin_invalid' }, 400, request);
  }
  

  // upsert into styles_user without relying on UNIQUE constraints
  const up = await db.prepare(
    `UPDATE styles_user
     SET status='collected', ts=datetime('now')
     WHERE app_public_id=? AND tg_id=? AND style_id=?`
  ).bind(ctx.publicId, String(tg.id), styleId).run();

  if (!up || !up.meta || !up.meta.changes) {
    await db.prepare(
      `INSERT INTO styles_user (app_id, app_public_id, tg_id, style_id, status, ts)
       VALUES (?, ?, ?, ?, 'collected', datetime('now'))`
    ).bind(ctx.appId, ctx.publicId, String(tg.id), styleId).run();
  }





    // ===== reward on completion (optional)
    try{
      const rwd = await passportIssueRewardIfCompleted(db, env, ctx, tg.id, cfg);
      // можно положить в fresh_state, чтобы UI тоже мог показать (не обязательно)
      // но buildState мы тоже улучшим ниже
    }catch(e){
      console.error('[passport.reward] failed', e);
    }
  

  const fresh = await buildState(db, ctx.appId, ctx.publicId, tg.id, cfg);
  return json({ ok:true, style_id: styleId, fresh_state: fresh }, 200, request);
}


  if (type === 'pin_use') {
    const { pin, style_id } = payload;
    const res = await useOneTimePin(db, ctx.publicId, tg.id, pin, style_id);
    return json(res, res.ok?200:400, request);
  }

  if (type === 'game_submit' || type === 'game.submit') {
    const gameId = String((payload && (payload.game_id || payload.game)) || 'flappy');
    const mode   = String((payload && payload.mode) || 'daily');
    const score  = Number((payload && payload.score) || 0);
    const dur    = Number((payload && payload.duration_ms) || 0);

    const dateStr = new Date().toISOString().slice(0,10);

    const ex = await db.prepare(
      `SELECT id, best_score, plays, duration_ms_total
       FROM games_results_daily
       WHERE app_public_id = ? AND tg_id = ? AND date = ? AND mode = ?`
    ).bind(ctx.publicId, String(tg.id), dateStr, mode).first();

    let best = score, plays = 1, durTotal = dur;

    if (!ex) {
      await db.prepare(
        `INSERT INTO games_results_daily
           (app_id, app_public_id, date, mode, tg_id, best_score, plays, duration_ms_total, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, 1, ?, datetime('now'))`
      ).bind(ctx.appId, ctx.publicId, dateStr, mode, String(tg.id), best, dur).run();
    } else {
      best = Math.max(Number(ex.best_score||0), score);
      plays = Number(ex.plays||0) + 1;
      durTotal = Number(ex.duration_ms_total||0) + dur;

      await db.prepare(
        `UPDATE games_results_daily
         SET best_score = ?, plays = ?, duration_ms_total = ?, updated_at = datetime('now')
         WHERE id = ?`
      ).bind(best, plays, durTotal, ex.id).run();
    }

    // вернуть свежий state
    const appObj = await env.APPS.get('app:' + ctx.appId, 'json').catch(()=>null);
    const cfg = (appObj && (appObj.app_config ?? appObj.runtime_config ?? appObj.config)) || {};
    const fresh = await buildState(db, ctx.appId, ctx.publicId, tg.id, cfg);

    return json({ ok:true, game:gameId, best_score: best, plays, fresh_state: fresh }, 200, request);
  }

    /* ======== CALENDAR (D1-backed) ======== */
    if (type === 'calendar.free_slots' || type === 'calendar_free_slots') {
      const db = env.DB;
      const p = (body && body.payload) || {};
      const date = (p.date && /^\d{4}-\d{2}-\d{2}$/.test(p.date)) ? p.date : new Date().toISOString().slice(0,10);
      const reqDur = Number(p.duration_min || 60);        // <- длительность, которую просим показать
    
      // helper'ы
      const toMin = (hhmm) => { const [h,m] = String(hhmm).split(':').map(n=>+n); return h*60+m; };
      const fmt   = (m)    => String(Math.floor(m/60)).padStart(2,'0') + ':' + String(m%60).padStart(2,'0');
    
      const w = (new Date(date+'T00:00:00')).getDay();
    
      // cfg (weekday приоритетнее NULL)
      const cfg = await db.prepare(
        `SELECT work_start_min AS ws, work_end_min AS we, slot_step_min AS step, capacity_per_slot AS cap
           FROM calendar_cfg
          WHERE app_public_id = ? AND (weekday = ? OR weekday IS NULL)
       ORDER BY (weekday IS NULL) ASC LIMIT 1`
      ).bind(ctx.publicId, w).first();
      if (!cfg) return json({ ok:true, date, slots: [] }, 200, request);
    
      const ws   = Number(cfg.ws || 600);
      const we   = Number(cfg.we || 1080);
      const step = Number(cfg.step || 30);
      const cap  = Number(cfg.cap  || 1);
    
      // Брони на дату
      const booked = await db.prepare(
        `SELECT time, duration_min FROM cal_bookings
          WHERE app_public_id = ? AND date = ? AND status = 'new'`
      ).bind(ctx.publicId, date).all();
    
      // Действующие холды других пользователей
      const holds = await db.prepare(
        `SELECT time, duration_min FROM cal_holds
          WHERE app_public_id = ? AND date = ? AND expires_at > datetime('now') AND tg_id <> ?`
      ).bind(ctx.publicId, date, String(tg.id)).all();
    
      // Счётчик занятости каждого под-слота (start, start+step, …)
      const busy = new Map(); // key: минутная отметка старта под-слота, value: занятость (брони+холды)
      function addBusy(startMin, durMin){
        for (let t = startMin; t < startMin + durMin; t += step) {
          busy.set(t, (busy.get(t) || 0) + 1);
        }
      }
      for (const r of (booked.results || [])) addBusy(toMin(r.time), Number(r.duration_min||step));
      for (const r of (holds.results  || [])) addBusy(toMin(r.time), Number(r.duration_min||step));
    
      // Стартовые слоты: берём те, где ВСЕ под-слоты под duration_min свободны (< cap)
      const slots = [];
      const maxStart = we - reqDur;
      for (let start = ws; start <= maxStart; start += step) {
        let ok = true;
        for (let t = start; t < start + reqDur; t += step) {
          if ((busy.get(t) || 0) >= cap) { ok = false; break; }
        }
        if (ok) slots.push(fmt(start));
      }
    
      return json({ ok:true, date, slots }, 200, request);
    }
    

    if (type === 'calendar.hold' || type === 'calendar_hold') {
      const db = env.DB;
      const p = (body && body.payload) || {};
      const { date, time } = p;
      const reqDur = Number(p.duration_min || 60);
      if (!date || !time) return json({ ok:false, error:'bad_params' }, 400, request);
    
      const toMin = (hhmm) => { const [h,m]=hhmm.split(':').map(Number); return h*60+m; };
      const w = (new Date(date+'T00:00:00')).getDay();
    
      // cfg + cap/step
      const cfg = await db.prepare(
        `SELECT slot_step_min AS step, capacity_per_slot AS cap
           FROM calendar_cfg
          WHERE app_public_id = ? AND (weekday = ? OR weekday IS NULL)
       ORDER BY (weekday IS NULL) ASC LIMIT 1`
      ).bind(ctx.publicId, w).first();
      if (!cfg) return json({ ok:false, error:'no_cfg' }, 400, request);
    
      const step = Number(cfg.step || 30);
      const cap  = Number(cfg.cap  || 1);
      const startMin = toMin(time);
    
      // занятость (брони + холды ДРУГИХ)
      const rows = await db.prepare(
        `SELECT time, duration_min, 'B' AS src FROM cal_bookings WHERE app_public_id = ? AND date = ? AND status = 'new'
         UNION ALL
         SELECT time, duration_min, 'H' AS src FROM cal_holds    WHERE app_public_id = ? AND date = ? AND expires_at > datetime('now') AND tg_id <> ?`
      ).bind(ctx.publicId, date, ctx.publicId, date, String(tg.id)).all();
    
      const busy = new Map();
      for (const r of (rows.results || [])) {
        const s = toMin(r.time), d = Number(r.duration_min||step);
        for (let t=s; t<s+d; t+=step) busy.set(t, (busy.get(t)||0)+1);
      }
    
      // проверяем всю «полосу»
      for (let t = startMin; t < startMin + reqDur; t += step) {
        if ((busy.get(t)||0) >= cap) return json({ ok:false, error:'slot_full' }, 409, request);
      }
    
      // вставляем hold на 5 минут
      await db.prepare(
        `INSERT INTO cal_holds(app_id, app_public_id, date, time, duration_min, tg_id, expires_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?, datetime('now','+5 minutes'), datetime('now'))`
      ).bind(ctx.appId, ctx.publicId, date, time, reqDur, String(tg.id)).run();
    
      return json({ ok:true, hold_id:`hold_${Date.now()}`, expires_at:new Date(Date.now()+5*60*1000).toISOString() }, 200, request);
    }
    

    if (type === 'calendar.book' || type === 'calendar_book') {
      const db = env.DB;
      const p = (body && body.payload) || {};
      const { date, time, contact = '' } = p;
      const reqDur = Number(p.duration_min || 60);
      if (!date || !time) return json({ ok:false, error:'bad_params' }, 400, request);
    
      const toMin = (hhmm) => { const [h,m]=hhmm.split(':').map(Number); return h*60+m; };
      const startMin = toMin(time);
      const w = (new Date(date+'T00:00:00')).getDay();
    
      const cfg = await db.prepare(
        `SELECT slot_step_min AS step, capacity_per_slot AS cap
           FROM calendar_cfg
          WHERE app_public_id = ? AND (weekday = ? OR weekday IS NULL)
       ORDER BY (weekday IS NULL) ASC LIMIT 1`
      ).bind(ctx.publicId, w).first();
      if (!cfg) return json({ ok:false, error:'no_cfg' }, 400, request);
    
      const step = Number(cfg.step || 30);
      const cap  = Number(cfg.cap  || 1);
    
      // занятость (брони + холды ДРУГИХ) — свой hold НЕ считаем
      const rows = await db.prepare(
        `SELECT time, duration_min, 'B' AS src FROM cal_bookings WHERE app_public_id = ? AND date = ? AND status = 'new'
         UNION ALL
         SELECT time, duration_min, 'H' AS src FROM cal_holds    WHERE app_public_id = ? AND date = ? AND expires_at > datetime('now') AND tg_id <> ?`
      ).bind(ctx.publicId, date, ctx.publicId, date, String(tg.id)).all();
    
      const busy = new Map();
      for (const r of (rows.results || [])) {
        const s = toMin(r.time), d = Number(r.duration_min||step);
        for (let t=s; t<s+d; t+=step) busy.set(t, (busy.get(t)||0)+1);
      }
    
      for (let t = startMin; t < startMin + reqDur; t += step) {
        if ((busy.get(t)||0) >= cap) return json({ ok:false, error:'slot_full' }, 409, request);
      }
    
      const bookingId = 'bk_' + (crypto?.randomUUID?.() || Date.now());
    
      await db.prepare(
        `INSERT INTO cal_bookings
         (booking_id, app_id, app_public_id, date, time, duration_min, format, contact, user_id, status, created_at)
         VALUES (?, ?, ?, ?, ?, ?, 'tg_call', ?, ?, 'new', datetime('now'))`
      ).bind(
        bookingId, ctx.appId, ctx.publicId, date, time, reqDur,
        String(contact||''), String(tg.id)
      ).run();
    
      // чистим свой hold (если был)
      try {
        await db.prepare(
          `DELETE FROM cal_holds WHERE app_public_id = ? AND date = ? AND time = ? AND tg_id = ?`
        ).bind(ctx.publicId, date, time, String(tg.id)).run();
      } catch(_){}
    
      return json({ ok:true, booking_id: bookingId }, 200, request);
    }
    


  


  return json({ ok:false, error:'UNKNOWN_TYPE' }, 400, request);
}


function randomRedeemCode(len = 10){
  // Читабельный код без 0/O/I/l
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  let s = '';
  for (let i=0;i<len;i++){
    s += alphabet[bytes[i] % alphabet.length];
  }
  // формат типа SG-XXXX-XXXX
  return 'SG-' + s.slice(0,4) + '-' + s.slice(4,8) + (len>8 ? '-' + s.slice(8) : '');
}



// ================== SALES TOKEN (QR) ==================
function randomSaleToken(lenBytes = 16){
  const b = crypto.getRandomValues(new Uint8Array(lenBytes));
  return btoa(String.fromCharCode(...b))
    .replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
}

function parseInitDataUser(initData){
  try{
    const p = new URLSearchParams(initData || '');
    const userRaw = p.get('user');
    if (!userRaw) return null;
    const u = JSON.parse(userRaw);
    if (!u || !u.id) return null;
    return u;
  }catch(_){
    return null;
  }
}

function saleTokKey(token){ return `sale_tok:${token}`; }



// /sales/token должен брать ttl и список кассиров из D1 (а не из body)
async function getSalesSettingsFromD1(publicId, env){
  try{
    const row = await env.DB.prepare(`
      SELECT cashback_percent, ttl_sec,
             cashier1_tg_id, cashier2_tg_id, cashier3_tg_id, cashier4_tg_id, cashier5_tg_id
      FROM sales_settings
      WHERE app_public_id = ?
      LIMIT 1
    `).bind(publicId).first();

    if (!row) return null;

    const ids = [row.cashier1_tg_id, row.cashier2_tg_id, row.cashier3_tg_id, row.cashier4_tg_id, row.cashier5_tg_id]
      .map(x => (x ? String(x) : ''))
      .filter(Boolean);

    return {
      cashback_percent: Number(row.cashback_percent ?? 10),
      ttl_sec: Number(row.ttl_sec ?? 300),
      cashier_ids: ids
    };
  }catch(e){
    console.error('[sales_settings] read failed', e);
    return null;
  }
}


async function handleSalesToken(publicId, request, env){
  // ожидаем JSON: { init_data: "...", ttl_sec?: 300 }
  let body = {};
  try{ body = await request.json(); }catch(_){}

  const initData = body.init_data || body.initData || '';
    // ttl/cashiers/cashback берем из D1 (истина), а не из body
    const ss = await getSalesSettingsFromD1(publicId, env);
    const ttl = Math.max(60, Math.min(600, Number(ss?.ttl_sec || 300)));
  

  // 1) resolve app by publicId
  const ctx = await resolveAppContextByPublicId(publicId, env);
  if (!ctx || !ctx.ok){
    return json({ ok:false, error:'APP_NOT_FOUND' }, 404, request);
  }

  // 2) get bot token for this app (у тебя уже есть KV + decrypt)
  // ВАЖНО: у разных версий твоего воркера сигнатура getBotTokenForApp могла быть разная
  // поэтому пробуем максимально совместимо:
  let botToken = null;
  try{
    botToken = await getBotTokenForApp(ctx.appId, env);
  }catch(_){}
  if (!botToken){
    try{
      botToken = await getBotTokenForApp(ctx.publicId || publicId, env, ctx.appId);
    }catch(_){}
  }
  if (!botToken){
    return json({ ok:false, error:'BOT_TOKEN_MISSING' }, 400, request);
  }

  // 3) verify initData signature
  const okSig = await verifyInitDataSignature(initData, botToken);
  if (!okSig){
    return json({ ok:false, error:'BAD_INITDATA' }, 403, request);
  }

  // 4) extract user id from initData
  const u = parseInitDataUser(initData);
  if (!u){
    return json({ ok:false, error:'NO_USER' }, 400, request);
  }

  // 5) create one-time token and store in KV
  if (!env.BOT_SECRETS){
    return json({ ok:false, error:'KV_MISSING(BOT_SECRETS)' }, 500, request);
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
      createdAt: Date.now()
    }),
    { expirationTtl: ttl }
  );


// 6) bot username (active) -> deep link
let botUsername = '';
try{
  const pid = String(publicId || (ctx && ctx.publicId) || '').trim();
  console.log('[sales/token] pid=', pid);

  const b = await env.DB.prepare(`
    SELECT username FROM bots
    WHERE app_public_id = ? AND status = 'active'
    ORDER BY id DESC LIMIT 1
  `).bind(pid).first();

  botUsername = (b && b.username) ? String(b.username).replace(/^@/,'').trim() : '';
  console.log('[sales/token] botUsername=', botUsername);
}catch(e){
  console.log('[sales/token] bot lookup error', e);
  botUsername = '';
}

const deep_link = botUsername ? `https://t.me/${botUsername}?start=sale_${token}` : '';
console.log('[sales/token] deep_link=', deep_link);

// ✅ ВАЖНО: логируем ТОЧНО то, что отдаём наружу
const resp = { ok:true, token, ttl_sec: ttl, bot_username: botUsername, deep_link };
console.log('[sales/token] RESPONSE JSON=', JSON.stringify(resp));

return json(resp, 200, request);

}


function addDaysIso(dateStr, days){
  // dateStr: 'YYYY-MM-DD'
  // делаем через UTC чтобы не плясало
  const d = new Date(dateStr + 'T00:00:00Z');
  if (Number.isNaN(d.getTime())) return null;
  d.setUTCDate(d.getUTCDate() + days);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

async function handleCabinetWheelStats(appId, request, env, ownerId){
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
  // если нет дат — просто очень широкий диапазон
  const fromTs = fromOk ? `${fromOk} 00:00:00` : '1970-01-01 00:00:00';
  const toTs   = (toPlus1 ? `${toPlus1} 00:00:00` : '2999-12-31 00:00:00');

  // канонический public_id нужен для wheel_prizes (у тебя wheel_prizes чистится по app_public_id)
  const appPublicId = await getCanonicalPublicIdForApp(appId, env);
  if (!appPublicId) {
    return json({ ok:false, error:'APP_PUBLIC_ID_NOT_FOUND' }, 404, request);
  }

  const db = env.DB;

  // ВАЖНО: wins считаем по wheel_redeems (это факт выигрыша/выдачи redeem_code)
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
      p.active AS active
    FROM wheel_prizes p
    LEFT JOIN agg a ON a.code = p.code
    WHERE (p.app_id = ? OR p.app_public_id = ?)
    ORDER BY COALESCE(a.wins,0) DESC, p.code ASC
  `).bind(
    appId, appPublicId, fromTs, toTs,
    appId, appPublicId
  ).all();

  const items = (rows && rows.results ? rows.results : []).map(r => ({
    prize_code: String(r.prize_code || ''),
    title: String(r.title || ''),
    wins: Number(r.wins || 0),
    redeemed: Number(r.redeemed || 0),
    weight: Number(r.weight ?? 0),
    active: Number(r.active ?? 0) ? 1 : 0,
  }));

  return json({ ok:true, items }, 200, request);
}


function toInt(v, d=0){
  const n = Number(v);
  if (!Number.isFinite(n)) return d;
  return Math.trunc(n);
}

async function resolveWheelAppPublicId(appId, env){
  try{
    if (typeof getCanonicalPublicIdForApp === 'function'){
      const pid = await getCanonicalPublicIdForApp(appId, env);
      if (pid) return String(pid);
    }
  }catch(_){}

  try{
    const r = await env.DB.prepare(`
      SELECT app_public_id
      FROM wheel_redeems
      WHERE app_id = ?
        AND app_public_id IS NOT NULL
        AND app_public_id != ''
      ORDER BY id DESC
      LIMIT 1
    `).bind(appId).first();
    if (r && r.app_public_id) return String(r.app_public_id);
  }catch(_){}

  try{
    const r = await env.DB.prepare(`
      SELECT app_public_id
      FROM wheel_spins
      WHERE app_id = ?
        AND app_public_id IS NOT NULL
        AND app_public_id != ''
      ORDER BY id DESC
      LIMIT 1
    `).bind(appId).first();
    if (r && r.app_public_id) return String(r.app_public_id);
  }catch(_){}

  return null;
}

async function handleCabinetSummary(appId, request, env, ownerId){
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

async function handleCabinetActivity(appId, request, env, ownerId){
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

async function handleCabinetCustomers(appId, request, env, ownerId){
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

async function handleCabinetSalesStats(appId, request, env, ownerId){
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

async function handleCabinetPassportStats(appId, request, env, ownerId){
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

async function handleCabinetCalendarBookings(appId, request, env, ownerId){
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

async function handleCabinetProfitReport(appId, request, env, ownerId){
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



// ===== Overview / Profit (for sg-cabinet-react charts) =====

function _parseRangeOrDefault(url){
  let from = url.searchParams.get('from') || '';
  let to   = url.searchParams.get('to') || '';
  // default: last 7 days inclusive
  if (!from || !to){
    const now = new Date();
    const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    const start = new Date(end.getTime() - 6*24*3600*1000);
    from = start.toISOString().slice(0,10);
    to   = end.toISOString().slice(0,10);
  }
  return { from, to };
}

function _daysBetweenInclusive(from, to){
  const out=[];
  const a=new Date(from+'T00:00:00Z');
  const b=new Date(to+'T00:00:00Z');
  for(let d=new Date(a); d<=b; d=new Date(d.getTime()+24*3600*1000)){
    out.push(d.toISOString().slice(0,10));
  }
  return out;
}

async function handleCabinetOverview(appId, request, env){
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

async function handleCabinetProfit(appId, request, env){
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

async function handleCabinetWheelPrizesGet(appId, request, env, ownerId){
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

async function handleCabinetWheelPrizesUpdate(appId, request, env, ownerId){
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









// ===== CABINET (React panel) =====

// ===== CABINET: dialogs (for Customers messenger) =====

// GET /api/cabinet/apps/:appId/dialogs?range=today|7d|30d|all&q=...
const cabDialogsMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/dialogs$/);
if (cabDialogsMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabDialogsMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  return listDialogs(appId, env, s.uid, request);
}

// GET|POST /api/cabinet/apps/:appId/dialog/:tgUserId
const cabDialogMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/dialog\/([^/]+)$/);
if (cabDialogMatch) {
  const appId = decodeURIComponent(cabDialogMatch[1]);
  const tgUserId = decodeURIComponent(cabDialogMatch[2]);

  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  if (request.method === 'GET')  return getDialogMessages(appId, tgUserId, env, s.uid, request);
  if (request.method === 'POST') return sendDialogMessage(appId, tgUserId, env, s.uid, request);

  return json({ ok:false, error:'METHOD_NOT_ALLOWED' }, 405, request);
}


// ===== CABINET: broadcasts (minimal) =====

// GET /api/cabinet/apps/:appId/broadcasts/campaigns
const cabBcListMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/broadcasts\/campaigns$/);
if (cabBcListMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabBcListMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  return listBroadcasts(appId, env, s.uid, request);
}

// POST /api/cabinet/apps/:appId/broadcasts/send
const cabBcSendMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/broadcasts\/send$/);
if (cabBcSendMatch && request.method === 'POST') {
  const appId = decodeURIComponent(cabBcSendMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  return createAndSendBroadcast(appId, env, s.uid, request);
}


// GET /api/cabinet/apps/:appId/overview?from=YYYY-MM-DD&to=YYYY-MM-DD&under=...&metric=sales|customers|loyalty|qr
const cabOverviewMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/overview$/);
if (cabOverviewMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabOverviewMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status || 403, request);
  return handleCabinetOverview(appId, request, env, s.uid);
}

// GET /api/cabinet/apps/:appId/profit?from=YYYY-MM-DD&to=YYYY-MM-DD&under=...&metric=net|gross|revenue|reward|liability
const cabProfitMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/profit$/);
if (cabProfitMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabProfitMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status || 403, request);
  return handleCabinetProfit(appId, request, env, s.uid);
}

// GET /api/cabinet/apps/:appId/wheel/stats?from=YYYY-MM-DD&to=YYYY-MM-DD&tz=...
const cabWheelStatsMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/wheel\/stats$/);
if (cabWheelStatsMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabWheelStatsMatch[1]);

  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);

  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  return handleCabinetWheelStats(appId, request, env, s.uid);
}


// GET /api/cabinet/apps/:appId/summary
const cabSummaryMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/summary$/);
if (cabSummaryMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabSummaryMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);
  return handleCabinetSummary(appId, request, env, s.uid);
}

// GET /api/cabinet/apps/:appId/activity
const cabActivityMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/activity$/);
if (cabActivityMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabActivityMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);
  return handleCabinetActivity(appId, request, env, s.uid);
}

// GET /api/cabinet/apps/:appId/customers
const cabCustomersMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/customers$/);
if (cabCustomersMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabCustomersMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);
  return handleCabinetCustomers(appId, request, env, s.uid);
}

// GET /api/cabinet/apps/:appId/sales/stats
const cabSalesStatsMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/sales\/stats$/);
if (cabSalesStatsMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabSalesStatsMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);
  return handleCabinetSalesStats(appId, request, env, s.uid);
}

// GET /api/cabinet/apps/:appId/passport/stats
const cabPassportStatsMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/passport\/stats$/);
if (cabPassportStatsMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabPassportStatsMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);
  return handleCabinetPassportStats(appId, request, env, s.uid);
}

// GET /api/cabinet/apps/:appId/calendar/bookings
const cabCalBookingsMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/calendar\/bookings$/);
if (cabCalBookingsMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabCalBookingsMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);
  return handleCabinetCalendarBookings(appId, request, env, s.uid);
}

// GET /api/cabinet/apps/:appId/profit/report
const cabProfitReportMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/profit\/report$/);
if (cabProfitReportMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabProfitReportMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);
  return handleCabinetProfitReport(appId, request, env, s.uid);
}

// GET /api/cabinet/apps/:appId/wheel/prizes
const cabWheelPrizesGetMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/wheel\/prizes$/);
if (cabWheelPrizesGetMatch && request.method === 'GET') {
  const appId = decodeURIComponent(cabWheelPrizesGetMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);
  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);
  return handleCabinetWheelPrizesGet(appId, request, env, s.uid);
}


// PUT /api/cabinet/apps/:appId/wheel/prizes
const cabWheelPrizesMatch = pathname.match(/^\/api\/cabinet\/apps\/([^/]+)\/wheel\/prizes$/);
if (cabWheelPrizesMatch && request.method === 'PUT') {
  const appId = decodeURIComponent(cabWheelPrizesMatch[1]);

  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);

  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  return handleCabinetWheelPrizesUpdate(appId, request, env, s.uid);
}



      // GET /api/public/app/:publicId/config
      const publicMatch = pathname.match(/^\/api\/public\/app\/([^/]+)\/config$/);
      if (publicMatch && request.method === 'GET') {
        const publicId = decodeURIComponent(publicMatch[1]);
        return getPublicConfig(publicId, env, request);
      }

// /api/app/:id/config  GET/PUT  (Blueprint for constructor)
const appCfgMatch = pathname.match(/^\/api\/app\/([^/]+)\/config$/);
if (appCfgMatch) {
  const appId = decodeURIComponent(appCfgMatch[1]);
  const s = await requireSession(request, env);
  if (!s) return json({ ok:false, error:'UNAUTHORIZED' }, 401, request);

  const ownerCheck = await ensureAppOwner(appId, s.uid, env);
  if (!ownerCheck.ok) return json({ ok:false, error:'FORBIDDEN' }, ownerCheck.status, request);

  const key = 'app:' + appId;

  if (request.method === 'GET') {
    const appObj = await env.APPS.get(key, 'json') || {};
    // ВАЖНО: конструктор ждёт Blueprint (routes/nav)
    return json({ ok:true, config: (appObj.config ?? null) }, 200, request);
  }

  if (request.method === 'PUT') {
    const body = await request.json().catch(()=> ({}));
    const bp = body?.config || body?.blueprint || body?.bp || null;

    const appObj = await env.APPS.get(key, 'json') || {};

    // 1) сохраняем Blueprint (как редактирует конструктор)
    if (bp && typeof bp === 'object') {
      appObj.config = bp;
    } else {
      appObj.config = appObj.config || {};
    }

    // 2) derive runtime config (для miniapp)
    try {
      appObj.app_config = extractRuntimeConfigFromBlueprint(appObj.config || {});
    } catch(e) {
      // если упало — не ломаем сохранение BP
      appObj.app_config = appObj.app_config || {};
    }

    await env.APPS.put(key, JSON.stringify(appObj));
    return json({ ok:true }, 200, request);
  }

  return json({ ok:false, error:'METHOD_NOT_ALLOWED' }, 405, request);
}



      return new Response('Not found', { status: 404, headers: corsHeaders(request) });
    } catch (e) {
      console.error(e);
      return new Response('Server error', { status: 500, headers: corsHeaders(request) });
    }
  },
};
