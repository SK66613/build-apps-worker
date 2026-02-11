// src/routes/mini.ts
import type { Env } from "../index";
import { json } from "../utils/http";
import { tgSendMessage } from "../services/telegramSend";
import { decryptToken } from "../services/crypto";
import { getCanonicalPublicIdForApp } from "../services/apps";


function safeJson(obj, maxLen = 8000) {
  try {
    const s = JSON.stringify(obj);
    return s.length > maxLen ? s.slice(0, maxLen) : s;
  } catch (_) {
    return null;
  }
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

async function resolveAppContextByPublicId(publicId, env) {
  const map = await env.APPS.get('app:by_public:' + publicId, 'json');
  if (!map || !map.appId) return { ok:false, status:404, error:'UNKNOWN_PUBLIC_ID' };
  const appId = map.appId;
  const canonicalPublicId = (await getCanonicalPublicIdForApp(appId, env)) || publicId;
  return { ok:true, appId, publicId: canonicalPublicId };
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

function addBusy(startMin, durMin){
        for (let t = startMin; t < startMin + durMin; t += step) {
          busy.set(t, (busy.get(t) || 0) + 1);
        }
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


export async function handleMiniApi(request: Request, env: Env, url: URL){
  const db = env.DB;
  const publicId = url.searchParams.get('public_id') || url.pathname.split('/').pop();

  if (request.method === 'OPTIONS') {
    return new Response('', { status:204, headers: /*legacyCorsHeaders*/(request) });
  }

  // читаем JSON
  let body = {};
  try { body = await request.json(); } catch(_){}

  // tg + подпись
  const initDataRaw = body.init_data || body.initData || null;
  const tg = body.tg_user || {};
  if (!tg || !tg.id) return json({ ok:false, error:'NO_TG_USER_ID' }, 400);

  // проверка приложения + подписи
  const ctx = await requireTgAndVerify(publicId, initDataRaw, env);
  if (!ctx.ok) return json({ ok:false, error: ctx.error || 'AUTH_FAILED' }, ctx.status||403);

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
  return json({ ok:true, state }, 200);
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
    }, 200);
  }

  // 1) создаём "черновик" спина
  const ins = await db.prepare(
    `INSERT INTO wheel_spins (app_id, app_public_id, tg_id, status, prize_code, prize_title, spin_cost)
     VALUES (?, ?, ?, 'new', '', '', ?)`
  ).bind(ctx.appId, ctx.publicId, String(tg.id), spinCost).run();

  const spinId = Number(ins?.meta?.last_row_id || ins?.lastInsertRowid || 0);
  if (!spinId){
    return json({ ok:false, error:'SPIN_CREATE_FAILED' }, 500);
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
      return json({ ok:false, error: spend.error, have: spend.have, need: spend.need }, 409);
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
    return json({ ok:false, error:'NO_PRIZES' }, 400);
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
  }, 200);
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

  if (!lastWon) return json({ ok:false, error:'NOTHING_TO_CLAIM' }, 400);

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
    }, 200);
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
    if (!redeem) return json({ ok:false, error:'REDEEM_CREATE_FAILED' }, 500);
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
  }, 200);
}





  if (type === 'quiz_finish') {
    const res = await quizFinish(db, ctx.appId, ctx.publicId, tg.id, payload||{});
    return json(res, 200);
    
  }











  // ===================== PASSPORT: collect style (with optional PIN) =====================
if (type === 'style.collect' || type === 'style_collect') {
  const styleId = String((payload && (payload.style_id || payload.styleId || payload.code)) || '').trim();
  const pin     = String((payload && payload.pin) || '').trim();
  

  if (!styleId) return json({ ok:false, error:'NO_STYLE_ID' }, 400);

  // load runtime config (passport.require_pin)
  const appObj = await env.APPS.get('app:' + ctx.appId, 'json').catch(()=>null);
  const cfg = (appObj && (appObj.app_config ?? appObj.runtime_config ?? appObj.config)) || {};
  const requirePin = !!(cfg && cfg.passport && cfg.passport.require_pin);

  if (requirePin) {
    const pres = await useOneTimePin(db, ctx.publicId, tg.id, pin, styleId);
    if (!pres || !pres.ok) return json(pres || { ok:false, error:'pin_invalid' }, 400);
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
  return json({ ok:true, style_id: styleId, fresh_state: fresh }, 200);
}


  if (type === 'pin_use') {
    const { pin, style_id } = payload;
    const res = await useOneTimePin(db, ctx.publicId, tg.id, pin, style_id);
    return json(res, res.ok?200:400);
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

    return json({ ok:true, game:gameId, best_score: best, plays, fresh_state: fresh }, 200);
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
      if (!cfg) return json({ ok:true, date, slots: [] }, 200);
    
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
    
      return json({ ok:true, date, slots }, 200);
    }
    

    if (type === 'calendar.hold' || type === 'calendar_hold') {
      const db = env.DB;
      const p = (body && body.payload) || {};
      const { date, time } = p;
      const reqDur = Number(p.duration_min || 60);
      if (!date || !time) return json({ ok:false, error:'bad_params' }, 400);
    
      const toMin = (hhmm) => { const [h,m]=hhmm.split(':').map(Number); return h*60+m; };
      const w = (new Date(date+'T00:00:00')).getDay();
    
      // cfg + cap/step
      const cfg = await db.prepare(
        `SELECT slot_step_min AS step, capacity_per_slot AS cap
           FROM calendar_cfg
          WHERE app_public_id = ? AND (weekday = ? OR weekday IS NULL)
       ORDER BY (weekday IS NULL) ASC LIMIT 1`
      ).bind(ctx.publicId, w).first();
      if (!cfg) return json({ ok:false, error:'no_cfg' }, 400);
    
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
        if ((busy.get(t)||0) >= cap) return json({ ok:false, error:'slot_full' }, 409);
      }
    
      // вставляем hold на 5 минут
      await db.prepare(
        `INSERT INTO cal_holds(app_id, app_public_id, date, time, duration_min, tg_id, expires_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?, datetime('now','+5 minutes'), datetime('now'))`
      ).bind(ctx.appId, ctx.publicId, date, time, reqDur, String(tg.id)).run();
    
      return json({ ok:true, hold_id:`hold_${Date.now()}`, expires_at:new Date(Date.now()+5*60*1000).toISOString() }, 200);
    }
    

    if (type === 'calendar.book' || type === 'calendar_book') {
      const db = env.DB;
      const p = (body && body.payload) || {};
      const { date, time, contact = '' } = p;
      const reqDur = Number(p.duration_min || 60);
      if (!date || !time) return json({ ok:false, error:'bad_params' }, 400);
    
      const toMin = (hhmm) => { const [h,m]=hhmm.split(':').map(Number); return h*60+m; };
      const startMin = toMin(time);
      const w = (new Date(date+'T00:00:00')).getDay();
    
      const cfg = await db.prepare(
        `SELECT slot_step_min AS step, capacity_per_slot AS cap
           FROM calendar_cfg
          WHERE app_public_id = ? AND (weekday = ? OR weekday IS NULL)
       ORDER BY (weekday IS NULL) ASC LIMIT 1`
      ).bind(ctx.publicId, w).first();
      if (!cfg) return json({ ok:false, error:'no_cfg' }, 400);
    
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
        if ((busy.get(t)||0) >= cap) return json({ ok:false, error:'slot_full' }, 409);
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
    
      return json({ ok:true, booking_id: bookingId }, 200);
    }
    


  


  return json({ ok:false, error:'UNKNOWN_TYPE' }, 400);
}

export async function routeMiniApi(request: Request, env: Env, url: URL): Promise<Response> {
  return await handleMiniApi(request, env, url);
}
