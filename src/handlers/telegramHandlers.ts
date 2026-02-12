// src/handlers/telegramHandlers.ts
// Telegram webhook handlers (migrated from _legacyImpl; real implementation).

import type { Env } from "../index";
import { getBotWebhookSecretForPublicId, timingSafeEqual } from "../services/bots";
import { getBotTokenForApp } from "../services/botToken";
import { resolveAppContextByPublicId } from "../services/apps";
import { tgAnswerPreCheckoutQuery } from "../services/telegramApi";
import { tgSendMessage } from "../services/telegramSend";

async function getSalesSettings(db: any, appPublicId: string){
  try{
    const row: any = await db.prepare(
    `SELECT cashier1_tg_id, cashier2_tg_id, cashier3_tg_id, cashier4_tg_id, cashier5_tg_id,
            cashback_percent, ttl_sec
     FROM sales_settings
     WHERE app_public_id = ? LIMIT 1`
  ).bind(String(appPublicId)).first();

  const cashiers = [row?.cashier1_tg_id, row?.cashier2_tg_id, row?.cashier3_tg_id, row?.cashier4_tg_id, row?.cashier5_tg_id]
    .map((x: any) => (x ? String(x).trim() : ''))
    .filter(Boolean);

    return {
      cashiers,
      cashback_percent: row ? Number(row.cashback_percent || 10) : 10,
      ttl_sec: row ? Number(row.ttl_sec || 300) : 300
    };
  }catch(e){
    // if table missing or any DB error — fail-open with defaults
    console.warn('[tg] getSalesSettings failed', String((e as any)?.message || e));
    return { cashiers: [], cashback_percent: 10, ttl_sec: 300 };
  }
}

function parseAmountToCents(s: any){
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
export async function handleTelegramWebhook(publicId: string, request: Request, env: Env): Promise<Response> {
  // 1) check secret from query (?s=...)
  const url = new URL(request.url);
  const s = url.searchParams.get('s') || '';
  const expected = await getBotWebhookSecretForPublicId(publicId, env);

  if (!expected || !timingSafeEqual(s, expected)) {
    return new Response('FORBIDDEN', { status: 403 });
  }

  try {
  // 2) parse update (always return 200 to Telegram)
  let upd;
  try {
    upd = await request.json();
  } catch (e) {
    return new Response('OK', { status: 200 });
  }

  // 3) dedupe update_id (KV TTL)
  const updateId = upd && upd.update_id != null ? String(upd.update_id) : '';
  if (env.BOT_SECRETS && updateId) {
    const k = `tg_upd:public:${publicId}:${updateId}`;
    const seen = await env.BOT_SECRETS.get(k);
    if (seen) return new Response('OK', { status: 200 });
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
        return new Response('OK', { status: 200 });
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

        return new Response('OK', { status: 200 });
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
    return new Response('OK', { status: 200 });
  }

  // 5) get bot token from KV
  const botToken = await getBotTokenForApp(publicId, env, null);



  




  if (!botToken) {
    return new Response('OK', { status: 200 });
  }

  // 6) resolve canonical ctx (for appId + canonical publicId)
  const ctx = await resolveAppContextByPublicId(publicId, env);
  if (!ctx || !ctx.ok) {
    return new Response('OK', { status: 200 });
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
      return new Response('OK', { status: 200 });
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
    return new Response('OK', { status: 200 });
  }

  // 2) PIN MENU (choose stamp/day)
  if (data.startsWith('pin_menu:')){
    const saleId = data.slice('pin_menu:'.length).trim();
    const act = await loadSaleAction(saleId);

    if (!act || !act.customerTgId){
      await tgAnswerCallbackQuery(botToken, cqId, 'Контекст продажи не найден (истёк).', true);
      return new Response('OK', { status: 200 });
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
      return new Response('OK', { status: 200 });
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
    return new Response('OK', { status: 200 });
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
      return new Response('OK', { status: 200 });
    }
    if (!styleId){
      await tgAnswerCallbackQuery(botToken, cqId, 'Нет style_id', true);
      return new Response('OK', { status: 200 });
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
      return new Response('OK', { status: 200 });
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
    return new Response('OK', { status: 200 });
  }

  // unknown callback
  await tgAnswerCallbackQuery(botToken, cqId, 'Неизвестное действие', false);
  return new Response('OK', { status: 200 });
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
    return new Response('OK', { status: 200 });
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
    return new Response('OK', { status: 200 });
  }

  if (String(pr.status) === 'redeemed'){
    await tgSendMessage(env, botToken, chatId,
      'ℹ️ Этот приз уже отмечен как полученный.',
      {}, { appPublicId, tgUserId: from.id }
    );
    return new Response('OK', { status: 200 });
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
    return new Response('OK', { status: 200 });
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

  return new Response('OK', { status: 200 });
}





  if (!r){
    await tgSendMessage(env, botToken, chatId,
      '⛔️ Код недействителен или приз не найден.',
      {}, { appPublicId, tgUserId: from.id }
    );
    return new Response('OK', { status: 200 });
  }

  if (String(r.status) === 'redeemed'){
    await tgSendMessage(env, botToken, chatId,
      'ℹ️ Этот приз уже отмечен как полученный.',
      {}, { appPublicId, tgUserId: from.id }
    );
    return new Response('OK', { status: 200 });
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

  return new Response('OK', { status: 200 });
}




    // === SALE FLOW: /start sale_<token>
    if (payload.startsWith('sale_')) {
      const token = payload.slice(5).trim();

      // 1) token -> KV
      const rawTok = env.BOT_SECRETS ? await env.BOT_SECRETS.get(saleTokKey(token)) : null;

      if (!rawTok){
        await tgSendMessage(env, botToken, chatId, '⛔️ Этот QR устарел. Попросите клиента обновить QR.', {}, { appPublicId, tgUserId: from.id });
        return new Response('OK', { status: 200 });
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
        return new Response('OK', { status: 200 });
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

      return new Response('OK', { status: 200 });
    }

    // обычный старт
    await tgSendMessage(env, botToken, chatId, 'Привет! Я бот этого мини-аппа ✅\nКоманда: /profile', {}, { appPublicId, tgUserId: from.id });
    return new Response('OK', { status: 200 });
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
        return new Response('OK', { status: 200 });
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

      return new Response('OK', { status: 200 });
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

    return new Response('OK', { status: 200 });
  }

  // default
  await tgSendMessage(env, botToken, chatId, 'Принял ✅\nКоманда: /profile', {}, { appPublicId, tgUserId: from.id });
  return new Response('OK', { status: 200 });
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


  } catch (e: any) {
    // Telegram expects 200 OK; never fail webhook with 5xx
    console.error('TG_WEBHOOK_ERROR', { publicId, msg: String(e?.message || e), stack: e?.stack || null });
    return new Response('OK', { status: 200 });
  }

