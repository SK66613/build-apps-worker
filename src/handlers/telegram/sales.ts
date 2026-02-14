// src/handlers/telegram/sales.ts
import type { Env } from "../../index";
import { tgSendMessage } from "../../services/telegramSend";
import { awardCoins } from "../../services/coinsLedger";

type SalesArgs = {
  env: Env;
  db: any; // env.DB
  ctx: { appId: any; publicId: string };
  botToken: string;
  upd: any;
};

// ================== helpers ==================

function parseAmountToCents(s: any) {
  const raw = String(s || "").trim().replace(",", ".");
  if (!raw) return null;
  if (!/^\d+(\.\d{1,2})?$/.test(raw)) return null;
  const parts = raw.split(".");
  const rub = Number(parts[0] || "0");
  const kop = Number((parts[1] || "").padEnd(2, "0"));
  if (!Number.isFinite(rub) || !Number.isFinite(kop)) return null;
  return rub * 100 + kop;
}

function parseIntCoins(s: any) {
  const raw = String(s ?? "").trim().replace(",", ".");
  if (!raw) return null;
  const n = Number(raw);
  if (!Number.isFinite(n)) return null;
  return Math.max(0, Math.floor(n));
}

function clamp(n: number, a: number, b: number) {
  return Math.max(a, Math.min(b, n));
}

function saleTokKey(token: string) {
  return `sale_tok:${String(token || "").trim()}`;
}

// KV keys — namespace = webhook ctx.publicId
function salePendKey(appPublicId: string, cashierTgId: string) {
  return `sale_pending:${String(appPublicId)}:${String(cashierTgId)}`;
}
function saleDraftKey(appPublicId: string, cashierTgId: string) {
  return `sale_draft:${String(appPublicId)}:${String(cashierTgId)}`;
}
function saleActionKey(appPublicId: string, saleId: string, cashierTgId: string) {
  return `sale_action:${String(appPublicId)}:${String(saleId)}:${String(cashierTgId)}`;
}
function pinActionKey(appPublicId: string, pin: string, cashierTgId: string) {
  return `pin_action:${String(appPublicId)}:${String(pin)}:${String(cashierTgId)}`;
}
function saleRedeemWaitKey(appPublicId: string, cashierTgId: string) {
  return `sale_redeem_wait:${String(appPublicId)}:${String(cashierTgId)}`;
}

// One “live message” session per cashier
function saleFlowKey(appPublicId: string, cashierTgId: string) {
  return `sale_flow:${String(appPublicId)}:${String(cashierTgId)}`;
}

type FlowStage =
  | "amount"        // ждём сумму покупки
  | "draft"         // черновик (record / reenter / redeem)
  | "redeem_input"  // ждём ввод монет для списания
  | "recorded"      // продажа записана, кнопки confirm/cancel + pin
  | "pin_menu"      // выбор стиля для PIN
  | "pin_issued";   // pin выдан (кнопка отменить pin + назад)

type FlowState = {
  stage: FlowStage;
  // UI message (one per flow)
  ui_chat_id: string;
  ui_message_id: number;

  // sale context
  customerTgId: string;
  token: string;
  cashback_percent: number;

  amount_cents: number;
  cashbackCoins: number;
  redeemCoins: number;

  // redeem constraints while input
  maxRedeem?: number;
  balance?: number;

  // recorded
  saleId?: string;
};

// ================== KV JSON helpers ==================

async function kvGetJson(env: Env, key: string) {
  const raw = (env as any).BOT_SECRETS ? await (env as any).BOT_SECRETS.get(key) : null;
  if (!raw) return null;
  try { return JSON.parse(raw); } catch { return null; }
}

async function kvPutJson(env: Env, key: string, obj: any, ttlSec: number) {
  if (!(env as any).BOT_SECRETS) return;
  await (env as any).BOT_SECRETS.put(key, JSON.stringify(obj ?? {}), { expirationTtl: Number(ttlSec || 600) }).catch(() => {});
}

async function kvDel(env: Env, key: string) {
  if (!(env as any).BOT_SECRETS) return;
  await (env as any).BOT_SECRETS.delete(key).catch(() => {});
}

// ================== Telegram helpers ==================

async function tgAnswerCallbackQuery(botToken: string, callbackQueryId: string, text = "", showAlert = false) {
  try {
    const url = `https://api.telegram.org/bot${botToken}/answerCallbackQuery`;
    await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ callback_query_id: callbackQueryId, text: text || "", show_alert: !!showAlert }),
    });
  } catch (_) {}
}

async function tgEditMessage(botToken: string, chatId: string, messageId: number, text: string, replyMarkup?: any) {
  try {
    const url = `https://api.telegram.org/bot${botToken}/editMessageText`;
    await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        message_id: messageId,
        text,
        parse_mode: "HTML",
        reply_markup: replyMarkup ? replyMarkup : undefined,
      }),
    });
  } catch (_) {}
}

// ================== settings / coins ==================

async function getSalesSettings(db: any, appPublicId: string) {
  try {
    const row: any = await db
      .prepare(
        `SELECT cashier1_tg_id, cashier2_tg_id, cashier3_tg_id, cashier4_tg_id, cashier5_tg_id,
                cashback_percent, ttl_sec
         FROM sales_settings
         WHERE app_public_id = ? LIMIT 1`
      )
      .bind(String(appPublicId))
      .first();

    const cashiers = [
      row?.cashier1_tg_id,
      row?.cashier2_tg_id,
      row?.cashier3_tg_id,
      row?.cashier4_tg_id,
      row?.cashier5_tg_id,
    ].map((x: any) => (x ? String(x).trim() : "")).filter(Boolean);

    return {
      cashiers,
      cashback_percent: row ? Number(row.cashback_percent || 10) : 10,
      ttl_sec: row ? Number(row.ttl_sec || 300) : 300,
    };
  } catch (_) {
    return { cashiers: [], cashback_percent: 10, ttl_sec: 300 };
  }
}

async function getUserCoinsFast(db: any, appPublicId: string, tgId: string): Promise<number> {
  try {
    const r: any = await db
      .prepare(`SELECT coins FROM app_users WHERE app_public_id=? AND tg_user_id=? LIMIT 1`)
      .bind(String(appPublicId), String(tgId))
      .first();
    return r ? Math.max(0, Math.floor(Number(r.coins || 0))) : 0;
  } catch (_) {
    return 0;
  }
}

// ================== coins_ledger idempotency helpers ==================

async function ledgerHasEvent(db: any, eventId: string): Promise<boolean> {
  if (!eventId) return false;
  try {
    const r = await db.prepare(`SELECT event_id FROM coins_ledger WHERE event_id=? LIMIT 1`).bind(String(eventId)).first();
    return !!r;
  } catch (_) {
    return false;
  }
}

// ================== atomic redeem ==================

async function spendCoinsIfEnoughAtomic(
  db: any,
  appId: any,
  appPublicId: string,
  tgId: string,
  cost: number,
  src: string,
  ref_id: string,
  note: string,
  event_id: string
): Promise<{ ok: boolean; spent?: number; balance?: number; reused?: boolean; error?: string; have?: number; need?: number; message?: string }> {
  cost = Math.max(0, Math.floor(Number(cost || 0)));
  if (cost <= 0) {
    const bal = await getUserCoinsFast(db, appPublicId, tgId);
    return { ok: true, spent: 0, balance: bal };
  }

  if (event_id) {
    try {
      const ex: any = await db.prepare(`SELECT balance_after FROM coins_ledger WHERE event_id=? LIMIT 1`).bind(String(event_id)).first();
      if (ex) return { ok: true, reused: true, spent: cost, balance: Number(ex.balance_after || 0) };
    } catch (_) {}
  }

  try {
    const stmts = [
      db
        .prepare(
          `UPDATE app_users
           SET coins = coins - ?
           WHERE app_public_id=? AND tg_user_id=? AND coins >= ?`
        )
        .bind(cost, String(appPublicId), String(tgId), cost),

      db
        .prepare(
          `INSERT INTO coins_ledger (app_id, app_public_id, tg_id, event_id, src, ref_id, delta, balance_after, note)
           SELECT ?, ?, ?, ?, ?, ?, ?, 
                  (SELECT coins FROM app_users WHERE app_public_id=? AND tg_user_id=? LIMIT 1),
                  ?
           WHERE changes() > 0`
        )
        .bind(
          String(appId || ""),
          String(appPublicId),
          String(tgId),
          event_id || null,
          String(src || ""),
          String(ref_id || ""),
          -cost,
          String(appPublicId),
          String(tgId),
          String(note || "")
        ),
    ];

    const resArr = await db.batch(stmts);
    const updRes = resArr && resArr[0] ? resArr[0] : null;
    const changed = Number((updRes as any)?.meta?.changes || 0);

    if (!changed) {
      const have = await getUserCoinsFast(db, appPublicId, tgId);
      return { ok: false, error: "NOT_ENOUGH_COINS", have, need: cost };
    }

    const bal = await getUserCoinsFast(db, appPublicId, tgId);
    return { ok: true, spent: cost, balance: bal };
  } catch (e: any) {
    const msg = String(e?.message || e);

    if (/unique|constraint/i.test(msg) && event_id) {
      try {
        const ex: any = await db.prepare(`SELECT balance_after FROM coins_ledger WHERE event_id=? LIMIT 1`).bind(String(event_id)).first();
        if (ex) return { ok: true, reused: true, spent: cost, balance: Number(ex.balance_after || 0) };
      } catch (_) {}
    }

    try { console.log("[sale.redeem.atomic.fail]", JSON.stringify({ appPublicId, tgId: String(tgId), cost, event_id, msg })); } catch (_) {}

    return { ok: false, error: "DB_ERROR", message: msg };
  }
}

// ================== PINs ==================

function randomPin4() {
  return String(Math.floor(1000 + Math.random() * 9000));
}

async function issuePinToCustomer(db: any, appPublicId: string, cashierTgId: string, customerTgId: string, styleId: string) {
  let pin = "";
  for (let i = 0; i < 12; i++) {
    pin = randomPin4();
    try {
      await db
        .prepare(
          `INSERT INTO pins_pool (app_public_id, pin, target_tg_id, style_id, issued_by_tg, issued_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))`
        )
        .bind(String(appPublicId), String(pin), String(customerTgId), String(styleId), String(cashierTgId))
        .run();
      return { ok: true, pin };
    } catch (e: any) {
      const msg = String(e?.message || e);
      if (/unique|constraint/i.test(msg)) continue;
      return { ok: false, error: "PIN_DB_ERROR" };
    }
  }
  return { ok: false, error: "PIN_CREATE_FAILED" };
}

async function voidPin(db: any, appPublicId: string, pin: string) {
  const row: any = await db
    .prepare(`SELECT id, used_at FROM pins_pool WHERE app_public_id=? AND pin=? LIMIT 1`)
    .bind(String(appPublicId), String(pin))
    .first();

  if (!row) return { ok: false, error: "PIN_NOT_FOUND" };
  if (row.used_at) return { ok: true, already: true };

  await db
    .prepare(`UPDATE pins_pool SET used_at = datetime('now') WHERE id=? AND used_at IS NULL`)
    .bind(Number(row.id))
    .run();

  return { ok: true, voided: true };
}

// ================== UI render (single live message) ==================

function kb(rows: any[][]) {
  return { reply_markup: { inline_keyboard: rows } };
}

async function render(env: Env, botToken: string, st: FlowState, text: string, keyboardRows: any[][]) {
  await tgEditMessage(botToken, st.ui_chat_id, st.ui_message_id, text, kb(keyboardRows).reply_markup);
}

// Draft text (как ты хотел — с балансом и maxRedeem)
async function draftText(db: any, appPublicId: string, st: FlowState) {
  const cents = Number(st.amount_cents || 0);
  const cb = Number(st.cashbackCoins || 0);
  const rd = Number(st.redeemCoins || 0);
  const bal = st.customerTgId ? await getUserCoinsFast(db, appPublicId, st.customerTgId) : 0;
  const maxByCheck = Math.floor(cents / 100);
  const maxRedeemNow = Math.max(0, Math.min(bal, maxByCheck));

  return (
    `❓ Записать продажу?\n` +
    `Сумма: <b>${(cents / 100).toFixed(2)}</b>\n` +
    `Кэшбэк к выдаче: <b>${cb}</b> монет\n` +
    `Списание монет: <b>${rd}</b> монет\n` +
    `Баланс клиента: <b>${bal}</b> монет\n` +
    `Макс. списание сейчас: <b>${maxRedeemNow}</b>\n` +
    `Клиент: <code>${st.customerTgId}</code>`
  );
}

async function recordedState(db: any, appPublicId: string, saleId: string) {
  const cbC = await ledgerHasEvent(db, `sale_confirm:${appPublicId}:${saleId}`);
  const cbX = await ledgerHasEvent(db, `sale_cancel:${appPublicId}:${saleId}`);
  const rdC = await ledgerHasEvent(db, `sale_redeem_confirm:${appPublicId}:${saleId}`);
  const rdX = await ledgerHasEvent(db, `sale_redeem_cancel:${appPublicId}:${saleId}`);

  return {
    cashback: cbX ? "canceled" : cbC ? "confirmed" : "pending",
    redeem: rdX ? "canceled" : rdC ? "confirmed" : "pending",
  } as const;
}

async function recordedText(db: any, appPublicId: string, st: FlowState) {
  const saleId = String(st.saleId || "");
  const amount = (Number(st.amount_cents || 0) / 100).toFixed(2);
  const cb = Math.max(0, Math.floor(Number(st.cashbackCoins || 0)));
  const rd = Math.max(0, Math.floor(Number(st.redeemCoins || 0)));

  const s = await recordedState(db, appPublicId, saleId);

  const cbMark = s.cashback === "confirmed" ? " ✅" : s.cashback === "canceled" ? " ↩️" : "";
  const rdMark = rd > 0 ? (s.redeem === "confirmed" ? " ✅" : s.redeem === "canceled" ? " ↩️" : "") : "";

  return (
    `✅ Продажа записана.\n` +
    `Sale #${saleId}\n` +
    `Сумма: <b>${amount}</b>\n` +
    `Кэшбэк: <b>${cb}</b>${cbMark}\n` +
    `Списание: <b>${rd}</b>${rdMark}\n` +
    `Клиент: <code>${st.customerTgId}</code>`
  );
}

async function recordedKeyboard(db: any, appPublicId: string, st: FlowState) {
  const saleId = String(st.saleId || "");
  const cb = Math.max(0, Math.floor(Number(st.cashbackCoins || 0)));
  const rd = Math.max(0, Math.floor(Number(st.redeemCoins || 0)));
  const s = await recordedState(db, appPublicId, saleId);

  const row1: any[] = [];
  if (cb > 0) {
    if (s.cashback === "pending") row1.push({ text: "✅ Подтвердить кэшбэк", callback_data: `sale_confirm:${saleId}` });
    else if (s.cashback === "confirmed") row1.push({ text: "❌ Отменить кэшбэк", callback_data: `sale_cancel:${saleId}` });
  }
  if (rd > 0) {
    if (s.redeem === "pending") row1.push({ text: "🪙 Подтвердить списание", callback_data: `sale_redeem_confirm:${saleId}` });
    else if (s.redeem === "confirmed") row1.push({ text: "↩️ Отменить списание", callback_data: `sale_redeem_cancel:${saleId}` });
  }

  const rows: any[][] = [];
  if (row1.length) rows.push(row1);
  rows.push([{ text: "🔑 Выдать PIN", callback_data: `pin_menu:${saleId}` }]);
  rows.push([{ text: "🏠 Меню", callback_data: `sale_menu` }]); // на будущее (если захочешь)
  return rows;
}

// “Меню” — просто возвращает на recorded экран (или draft/amount по желанию)
async function goMenu(env: Env, db: any, botToken: string, appPublicId: string, st: FlowState) {
  if (st.saleId) {
    st.stage = "recorded";
    await kvPutJson(env, saleFlowKey(appPublicId, String(st.ui_chat_id)), st, 3600).catch(() => {});
    await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
  } else {
    st.stage = "amount";
    await render(
      env,
      botToken,
      st,
      `✅ Клиент: <code>${st.customerTgId}</code>\nВведите сумму покупки (например 350 или 350.50):`,
      [[{ text: "⛔️ Отменить", callback_data: "sale_drop" }]]
    );
  }
}

// ================== MAIN ==================

export async function handleSalesFlow(args: SalesArgs): Promise<boolean> {
  const { env, db, botToken, upd } = args;
  const appId = args.ctx.appId;
  const appPublicId = String(args.ctx.publicId || "");

  // ---------- CALLBACKS ----------
  if (upd?.callback_query?.data) {
    const cq = upd.callback_query;
    const data = String(cq.data || "");
    const cqId = String(cq.id || "");
    const from = cq.from || null;
    const cashierTgId = from ? String(from.id) : "";
    const chatId = String(cq?.message?.chat?.id || (from ? from.id : ""));
    const msgId = cq?.message?.message_id != null ? Number(cq.message.message_id) : 0;

    // load flow
    const flowKey = saleFlowKey(appPublicId, cashierTgId);
    let st = (await kvGetJson(env, flowKey)) as FlowState | null;

    // If no flow state, we can’t safely edit anything
    if (!st || !st.ui_chat_id || !st.ui_message_id) {
      await tgAnswerCallbackQuery(botToken, cqId, "Сессия истекла. Сканируй QR заново.", true);
      return true;
    }

    // safety: if callback comes from same message, override ids (когда KV устарел)
    if (chatId && msgId) {
      st.ui_chat_id = String(chatId);
      st.ui_message_id = Number(msgId);
    }

    // sale_drop (cancel flow)
    if (data === "sale_drop") {
      await kvDel(env, salePendKey(appPublicId, cashierTgId));
      await kvDel(env, saleDraftKey(appPublicId, cashierTgId));
      await kvDel(env, saleRedeemWaitKey(appPublicId, cashierTgId));
      await kvDel(env, flowKey);

      await tgEditMessage(botToken, st.ui_chat_id, st.ui_message_id, `⛔️ Продажа отменена.`, { inline_keyboard: [] });
      await tgAnswerCallbackQuery(botToken, cqId, "Отменено", false);
      return true;
    }

    // reenter amount
    if (data === "sale_reenter") {
      st.stage = "amount";
      st.amount_cents = 0;
      st.cashbackCoins = 0;
      st.redeemCoins = 0;
      st.maxRedeem = undefined;
      st.balance = undefined;

      await kvPutJson(env, flowKey, st, 900);

      await render(
        env,
        botToken,
        st,
        `✅ Клиент: <code>${st.customerTgId}</code>\nВведите сумму покупки (например 350 или 350.50):`,
        [[{ text: "⛔️ Отменить", callback_data: "sale_drop" }]]
      );

      await tgAnswerCallbackQuery(botToken, cqId, "Ок", false);
      return true;
    }

    // redeem enter
    if (data === "sale_redeem_enter") {
      // only from draft
      const cents = Number(st.amount_cents || 0);
      const bal = await getUserCoinsFast(db, appPublicId, st.customerTgId);
      const maxByCheck = Math.floor(cents / 100);
      const maxRedeem = Math.max(0, Math.min(bal, maxByCheck));

      st.stage = "redeem_input";
      st.balance = bal;
      st.maxRedeem = maxRedeem;
      await kvPutJson(env, flowKey, st, 600);

      await render(
        env,
        botToken,
        st,
        `🪙 Введите сколько монет списать (целым числом).\n0 — не списывать.\nБаланс клиента: <b>${bal}</b>\nМаксимум к списанию по чеку: <b>${maxRedeem}</b>`,
        [
          [{ text: "⬅️ Назад", callback_data: "sale_back_to_draft" }],
          [{ text: "⛔️ Отменить", callback_data: "sale_drop" }],
        ]
      );

      await tgAnswerCallbackQuery(botToken, cqId, "Жду сумму…", false);
      return true;
    }

    if (data === "sale_back_to_draft") {
      st.stage = "draft";
      await kvPutJson(env, flowKey, st, 900);
      await render(env, botToken, st, await draftText(db, appPublicId, st), draftKeyboardRows(st));
      await tgAnswerCallbackQuery(botToken, cqId, "Ок", false);
      return true;
    }

    // record sale (from draft)
    if (data === "sale_record") {
      if (st.stage !== "draft") {
        await tgAnswerCallbackQuery(botToken, cqId, "Не тот шаг", false);
        return true;
      }

      const redeemCoins = Math.max(0, Math.floor(Number(st.redeemCoins || 0)));

      // INSERT (with redeem_coins if exists)
      let saleId = "";
      try {
        const ins = await db
          .prepare(
            `INSERT INTO sales (app_id, app_public_id, customer_tg_id, cashier_tg_id, amount_cents, cashback_coins, redeem_coins, token, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`
          )
          .bind(
            String(appId || ""),
            String(appPublicId),
            String(st.customerTgId || ""),
            String(cashierTgId),
            Number(st.amount_cents || 0),
            Number(st.cashbackCoins || 0),
            Number(redeemCoins || 0),
            String(st.token || "")
          )
          .run();
        saleId = (ins as any)?.meta?.last_row_id ? String((ins as any).meta.last_row_id) : "";
      } catch (_) {
        const ins2 = await db
          .prepare(
            `INSERT INTO sales (app_id, app_public_id, customer_tg_id, cashier_tg_id, amount_cents, cashback_coins, token, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))`
          )
          .bind(
            String(appId || ""),
            String(appPublicId),
            String(st.customerTgId || ""),
            String(cashierTgId),
            Number(st.amount_cents || 0),
            Number(st.cashbackCoins || 0),
            String(st.token || "")
          )
          .run();
        saleId = (ins2 as any)?.meta?.last_row_id ? String((ins2 as any).meta.last_row_id) : "";
      }

      // save action payload for confirm/cancel + pin
      const act = {
        appPublicId,
        saleId,
        customerTgId: st.customerTgId,
        cashbackCoins: Number(st.cashbackCoins || 0),
        cashback_percent: Number(st.cashback_percent || 0),
        amount_cents: Number(st.amount_cents || 0),
        redeemCoins: Number(redeemCoins || 0),
      };
      await kvPutJson(env, saleActionKey(appPublicId, saleId, cashierTgId), act, 3600);

      // clean old pending/token
      await kvDel(env, salePendKey(appPublicId, cashierTgId));
      await kvDel(env, saleDraftKey(appPublicId, cashierTgId));
      await kvDel(env, saleRedeemWaitKey(appPublicId, cashierTgId));

      st.stage = "recorded";
      st.saleId = saleId;
      await kvPutJson(env, flowKey, st, 3600);

      await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
      await tgAnswerCallbackQuery(botToken, cqId, "Записано ✅", false);
      return true;
    }

    // menu (future)
    if (data === "sale_menu") {
      await goMenu(env, db, botToken, appPublicId, st);
      await tgAnswerCallbackQuery(botToken, cqId, "Ок", false);
      return true;
    }

    // ===== Recorded actions: cashback / redeem confirm/cancel =====

    if (data.startsWith("sale_confirm:")) {
      const saleId = data.slice("sale_confirm:".length).trim();
      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст истёк.", true);
        return true;
      }

      const cashbackCoins = Math.max(0, Math.floor(Number(act.cashbackCoins || 0)));
      const cbp = clamp(Number(act.cashback_percent || 0), 0, 100);
      const eventId = `sale_confirm:${appPublicId}:${String(act.saleId || saleId)}`;

      if (await ledgerHasEvent(db, eventId)) {
        await tgAnswerCallbackQuery(botToken, cqId, "Уже начислено", false);
        // refresh UI
        st.saleId = saleId;
        st.stage = "recorded";
        await kvPutJson(env, flowKey, st, 3600);
        await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
        return true;
      }

      if (act.customerTgId && cashbackCoins > 0) {
        const rr: any = await awardCoins(
          db,
          appId,
          appPublicId,
          String(act.customerTgId),
          cashbackCoins,
          "sale_cashback_confirmed",
          String(act.saleId || saleId),
          `Кэшбэк ${cbp}% за покупку`,
          eventId
        );

        if (!rr?.reused) {
          try {
            await tgSendMessage(
              env,
              botToken,
              String(act.customerTgId),
              `🎉 Кэшбэк подтвержден!\nНачислено <b>${cashbackCoins}</b> монет ✅`,
              {},
              { appPublicId, tgUserId: String(act.customerTgId) }
            );
          } catch (_) {}
        }
      }

      st.saleId = saleId;
      st.stage = "recorded";
      await kvPutJson(env, flowKey, st, 3600);
      await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
      await tgAnswerCallbackQuery(botToken, cqId, "Готово ✅", false);
      return true;
    }

    if (data.startsWith("sale_cancel:")) {
      const saleId = data.slice("sale_cancel:".length).trim();
      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст истёк.", true);
        return true;
      }

      const cancelEventId = `sale_cancel:${appPublicId}:${String(act.saleId || saleId)}`;
      const coinsToCancel = Math.max(0, Math.floor(Number(act.cashbackCoins || 0)));

      if (await ledgerHasEvent(db, cancelEventId)) {
        await tgAnswerCallbackQuery(botToken, cqId, "Уже отменено", false);
        st.saleId = saleId;
        st.stage = "recorded";
        await kvPutJson(env, flowKey, st, 3600);
        await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
        return true;
      }

      const confirmEventId = `sale_confirm:${appPublicId}:${String(act.saleId || saleId)}`;
      const wasConfirmed = await ledgerHasEvent(db, confirmEventId);
      if (!wasConfirmed) {
        await tgAnswerCallbackQuery(botToken, cqId, "Ещё не начисляли", false);
        st.saleId = saleId;
        st.stage = "recorded";
        await kvPutJson(env, flowKey, st, 3600);
        await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
        return true;
      }

      let rr: any = null;
      if (coinsToCancel > 0) {
        rr = await awardCoins(
          db,
          appId,
          appPublicId,
          String(act.customerTgId),
          -Math.abs(coinsToCancel),
          "sale_cancel",
          String(act.saleId || saleId),
          "cancel cashback",
          cancelEventId
        );
      }

      if (!rr?.reused) {
        try {
          await tgSendMessage(env, botToken, String(act.customerTgId), `↩️ Кэшбэк по покупке отменён кассиром.`, {}, { appPublicId, tgUserId: String(act.customerTgId) });
        } catch (_) {}
      }

      st.saleId = saleId;
      st.stage = "recorded";
      await kvPutJson(env, flowKey, st, 3600);
      await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
      await tgAnswerCallbackQuery(botToken, cqId, "Отменено ✅", false);
      return true;
    }

    if (data.startsWith("sale_redeem_confirm:")) {
      const saleId = data.slice("sale_redeem_confirm:".length).trim();
      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст истёк.", true);
        return true;
      }

      const redeemCoins = Math.max(0, Math.floor(Number(act.redeemCoins || 0)));
      if (redeemCoins <= 0) {
        await tgAnswerCallbackQuery(botToken, cqId, "Списания нет", false);
        return true;
      }

      const eventId = `sale_redeem_confirm:${appPublicId}:${String(act.saleId || saleId)}`;

      if (await ledgerHasEvent(db, eventId)) {
        await tgAnswerCallbackQuery(botToken, cqId, "Уже списано", false);
        st.saleId = saleId;
        st.stage = "recorded";
        await kvPutJson(env, flowKey, st, 3600);
        await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
        return true;
      }

      const res = await spendCoinsIfEnoughAtomic(
        db,
        appId,
        appPublicId,
        String(act.customerTgId),
        redeemCoins,
        "sale_redeem_confirm",
        String(act.saleId || saleId),
        `Списание монет за покупку (Sale #${String(act.saleId || saleId)})`,
        eventId
      );

      if (!res.ok) {
        if (res.error === "NOT_ENOUGH_COINS") {
          await tgAnswerCallbackQuery(botToken, cqId, "Не хватает монет", true);
          // UI: просто оставим как есть
          st.saleId = saleId;
          st.stage = "recorded";
          await kvPutJson(env, flowKey, st, 3600);
          await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
          return true;
        }
        await tgAnswerCallbackQuery(botToken, cqId, "Ошибка списания", true);
        st.saleId = saleId;
        st.stage = "recorded";
        await kvPutJson(env, flowKey, st, 3600);
        await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
        return true;
      }

      // sales status (best effort)
      try {
        await db
          .prepare(
            `UPDATE sales
             SET redeem_status='confirmed',
                 redeem_confirmed_at=datetime('now')
             WHERE id=? AND app_public_id=?`
          )
          .bind(Number(act.saleId || saleId), String(appPublicId))
          .run();
      } catch (_) {}

      if (!res.reused) {
        try {
          await tgSendMessage(
            env,
            botToken,
            String(act.customerTgId),
            `🪙 Списано <b>${redeemCoins}</b> монет по вашей покупке.\nБаланс: <b>${Number(res.balance || 0)}</b>`,
            {},
            { appPublicId, tgUserId: String(act.customerTgId) }
          );
        } catch (_) {}
      }

      st.saleId = saleId;
      st.stage = "recorded";
      await kvPutJson(env, flowKey, st, 3600);
      await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
      await tgAnswerCallbackQuery(botToken, cqId, "Списано ✅", false);
      return true;
    }

    if (data.startsWith("sale_redeem_cancel:")) {
      const saleId = data.slice("sale_redeem_cancel:".length).trim();
      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст истёк.", true);
        return true;
      }

      const redeemCoins = Math.max(0, Math.floor(Number(act.redeemCoins || 0)));
      if (redeemCoins <= 0) {
        await tgAnswerCallbackQuery(botToken, cqId, "Списания нет", false);
        return true;
      }

      const confirmEventId = `sale_redeem_confirm:${appPublicId}:${String(act.saleId || saleId)}`;
      const wasConfirmed = await ledgerHasEvent(db, confirmEventId);
      if (!wasConfirmed) {
        await tgAnswerCallbackQuery(botToken, cqId, "Ещё не списывали", false);
        st.saleId = saleId;
        st.stage = "recorded";
        await kvPutJson(env, flowKey, st, 3600);
        await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
        return true;
      }

      const cancelEventId = `sale_redeem_cancel:${appPublicId}:${String(act.saleId || saleId)}`;
      if (await ledgerHasEvent(db, cancelEventId)) {
        await tgAnswerCallbackQuery(botToken, cqId, "Уже отменено", false);
        st.saleId = saleId;
        st.stage = "recorded";
        await kvPutJson(env, flowKey, st, 3600);
        await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
        return true;
      }

      const rr: any = await awardCoins(
        db,
        appId,
        appPublicId,
        String(act.customerTgId),
        Math.abs(redeemCoins),
        "sale_redeem_cancel",
        String(act.saleId || saleId),
        `Возврат монет (отмена списания) Sale #${String(act.saleId || saleId)}`,
        cancelEventId
      );

      try {
        await db
          .prepare(
            `UPDATE sales
             SET redeem_status='canceled',
                 redeem_canceled_at=datetime('now')
             WHERE id=? AND app_public_id=?`
          )
          .bind(Number(act.saleId || saleId), String(appPublicId))
          .run();
      } catch (_) {}

      if (!rr?.reused) {
        try {
          await tgSendMessage(
            env,
            botToken,
            String(act.customerTgId),
            `↩️ Отмена списания: возвращено <b>${redeemCoins}</b> монет.\nБаланс: <b>${Number(rr?.balance ?? 0)}</b>`,
            {},
            { appPublicId, tgUserId: String(act.customerTgId) }
          );
        } catch (_) {}
      }

      st.saleId = saleId;
      st.stage = "recorded";
      await kvPutJson(env, flowKey, st, 3600);
      await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
      await tgAnswerCallbackQuery(botToken, cqId, "Отменено ✅", false);
      return true;
    }

    // ===== PIN menu in the same message =====

    if (data.startsWith("pin_menu:")) {
      const saleId = data.slice("pin_menu:".length).trim();
      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст истёк.", true);
        return true;
      }

      const rows = await db
        .prepare(
          `SELECT style_id, title
           FROM styles_dict
           WHERE app_public_id = ?
           ORDER BY id ASC`
        )
        .bind(String(appPublicId))
        .all();

      const items = rows && (rows as any).results ? (rows as any).results : [];
      if (!items.length) {
        await tgAnswerCallbackQuery(botToken, cqId, "Нет карточек", true);
        return true;
      }

      st.stage = "pin_menu";
      st.saleId = saleId;
      await kvPutJson(env, flowKey, st, 3600);

      const kbRows: any[][] = [];
      for (let i = 0; i < items.length; i += 2) {
        const a = items[i];
        const b = items[i + 1];
        const row: any[] = [];
        row.push({ text: String(a.title || a.style_id), callback_data: `pin_make:${saleId}:${String(a.style_id)}` });
        if (b) row.push({ text: String(b.title || b.style_id), callback_data: `pin_make:${saleId}:${String(b.style_id)}` });
        kbRows.push(row);
      }
      kbRows.push([{ text: "⬅️ Назад", callback_data: "pin_back" }]);

      await render(env, botToken, st, `🔑 Выбери штамп/день — PIN уйдёт клиенту <code>${String(act.customerTgId)}</code>`, kbRows);
      await tgAnswerCallbackQuery(botToken, cqId, "Выбери", false);
      return true;
    }

    if (data === "pin_back") {
      st.stage = "recorded";
      await kvPutJson(env, flowKey, st, 3600);
      await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
      await tgAnswerCallbackQuery(botToken, cqId, "Ок", false);
      return true;
    }

    if (data.startsWith("pin_make:")) {
      const rest = data.slice("pin_make:".length);
      const [saleIdRaw, styleIdRaw] = rest.split(":");
      const saleId = String(saleIdRaw || "").trim();
      const styleId = String(styleIdRaw || "").trim();

      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId || !styleId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст истёк.", true);
        return true;
      }

      let stTitle = "";
      try {
        const r = await db.prepare(`SELECT title FROM styles_dict WHERE app_public_id=? AND style_id=? LIMIT 1`).bind(appPublicId, styleId).first();
        stTitle = r ? String((r as any).title || "") : "";
      } catch (_) {}

      const pinRes = await issuePinToCustomer(db, appPublicId, cashierTgId, String(act.customerTgId), styleId);
      if (!pinRes || !pinRes.ok) {
        await tgAnswerCallbackQuery(botToken, cqId, "Не удалось создать PIN", true);
        return true;
      }

      try {
        await kvPutJson(env, pinActionKey(appPublicId, String(pinRes.pin), cashierTgId), { appPublicId, pin: String(pinRes.pin), customerTgId: String(act.customerTgId), styleId }, 3600);
      } catch (_) {}

      // клиенту
      try {
        await tgSendMessage(
          env,
          botToken,
          String(act.customerTgId),
          `🔑 Ваш PIN для отметки штампа${stTitle ? ` “${stTitle}”` : ""}:\n<code>${String(pinRes.pin)}</code>\n\n(одноразовый)`,
          {},
          { appPublicId, tgUserId: String(act.customerTgId) }
        );
      } catch (_) {}

      // UI -> PIN issued in same message
      st.stage = "pin_issued";
      st.saleId = saleId;
      await kvPutJson(env, flowKey, st, 3600);

      await render(
        env,
        botToken,
        st,
        `✅ PIN отправлен клиенту <code>${String(act.customerTgId)}</code> для ${stTitle ? `“${stTitle}”` : `<code>${styleId}</code>`}.\nPIN: <code>${String(pinRes.pin)}</code>`,
        [
          [{ text: "⛔️ Отменить PIN", callback_data: `pin_void:${String(pinRes.pin)}` }],
          [{ text: "⬅️ Назад", callback_data: "pin_back" }],
        ]
      );

      await tgAnswerCallbackQuery(botToken, cqId, "PIN отправлен ✅", false);
      return true;
    }

    if (data.startsWith("pin_void:")) {
      const pin = data.slice("pin_void:".length).trim();
      const act = await kvGetJson(env, pinActionKey(appPublicId, pin, cashierTgId));
      const res = await voidPin(db, appPublicId, pin);

      if (!res.ok) {
        await tgAnswerCallbackQuery(botToken, cqId, "PIN не найден", true);
        return true;
      }

      try { await kvDel(env, pinActionKey(appPublicId, pin, cashierTgId)); } catch (_) {}

      const customerTgId = act && (act as any).customerTgId ? String((act as any).customerTgId) : "";
      if (customerTgId) {
        try {
          await tgSendMessage(env, botToken, customerTgId, `⛔️ PIN был отменён кассиром.`, {}, { appPublicId, tgUserId: customerTgId });
        } catch (_) {}
      }

      // update UI in same message
      await render(
        env,
        botToken,
        st,
        `⛔️ PIN отменён.\nPIN: <code>${pin}</code>`,
        [[{ text: "⬅️ Назад", callback_data: "pin_back" }]]
      );

      await tgAnswerCallbackQuery(botToken, cqId, "Отменено", false);
      return true;
    }

    // default: refresh UI if recorded
    if (st.stage === "recorded" && st.saleId) {
      await render(env, botToken, st, await recordedText(db, appPublicId, st), await recordedKeyboard(db, appPublicId, st));
      await tgAnswerCallbackQuery(botToken, cqId, "Ок", false);
      return true;
    }

    return false;
  }

  // ---------- MESSAGES ----------
  const text = (upd?.message && upd.message.text) || (upd?.edited_message && upd.edited_message.text) || "";
  const t = String(text || "").trim();

  const msg = upd?.message || upd?.edited_message || null;
  const from = msg?.from || null;
  const fromId = from ? String(from.id) : "";
  const chatId = msg?.chat?.id != null ? String(msg.chat.id) : fromId;

  if (!fromId || !chatId) return false;

  const flowKey = saleFlowKey(appPublicId, fromId);
  let st = (await kvGetJson(env, flowKey)) as FlowState | null;

  // /start sale_
  if (t === "/start" || t.startsWith("/start ")) {
    const payload = t.startsWith("/start ") ? t.slice(7).trim() : "";
    if (!payload.startsWith("sale_")) return false;

    const token = payload.slice(5).trim();

    const rawTok = (env as any).BOT_SECRETS ? await (env as any).BOT_SECRETS.get(saleTokKey(token)) : null;
    if (!rawTok) {
      await tgSendMessage(env, botToken, chatId, "⛔️ Этот QR устарел. Попросите клиента обновить QR.", {}, { appPublicId, tgUserId: fromId });
      return true;
    }

    let tokObj: any = null;
    try { tokObj = JSON.parse(rawTok); } catch (_) {}

    const customerTgId = tokObj && tokObj.customerTgId ? String(tokObj.customerTgId) : "";
    const tokenAppPublicId = tokObj && tokObj.appPublicId ? String(tokObj.appPublicId) : "";

    if (tokenAppPublicId && tokenAppPublicId !== appPublicId) {
      await tgSendMessage(env, botToken, chatId, "⛔️ Этот QR относится к другому проекту/боту. Откройте QR в правильном боте.", {}, { appPublicId, tgUserId: fromId });
      return true;
    }

    const ss = await getSalesSettings(db, appPublicId);
    const isCashier = ss.cashiers.includes(String(fromId));
    if (!isCashier) {
      await tgSendMessage(env, botToken, chatId, "⛔️ Вы не зарегистрированы как кассир для этого проекта.", {}, { appPublicId, tgUserId: fromId });
      return true;
    }

    // pend context (optional)
    await kvPutJson(env, salePendKey(appPublicId, String(fromId)), { appPublicId, customerTgId, token, cashback_percent: ss.cashback_percent }, 600);
    await kvDel(env, saleTokKey(token));

    // create one UI message (start of flow)
    const sent: any = await tgSendMessage(
      env,
      botToken,
      chatId,
      `✅ Клиент: <code>${customerTgId}</code>\nВведите сумму покупки (например 350 или 350.50):`,
      kb([[{ text: "⛔️ Отменить", callback_data: "sale_drop" }]]),
      { appPublicId, tgUserId: fromId }
    );

    const uiMid = sent?.result?.message_id ? Number(sent.result.message_id) : 0;

    st = {
      stage: "amount",
      ui_chat_id: String(chatId),
      ui_message_id: uiMid,
      customerTgId,
      token,
      cashback_percent: Number(ss.cashback_percent || 10),
      amount_cents: 0,
      cashbackCoins: 0,
      redeemCoins: 0,
    };

    await kvPutJson(env, flowKey, st, 900);
    return true;
  }

  // if no flow — не наше
  if (!st || !st.ui_chat_id || !st.ui_message_id) return false;

  // redeem input step (text)
  if (st.stage === "redeem_input") {
    const coins = parseIntCoins(t);
    if (coins == null) {
      await render(
        env,
        botToken,
        st,
        `🪙 Введите целое число монет.\n0 — не списывать.\nМаксимум: <b>${Number(st.maxRedeem || 0)}</b>`,
        [
          [{ text: "⬅️ Назад", callback_data: "sale_back_to_draft" }],
          [{ text: "⛔️ Отменить", callback_data: "sale_drop" }],
        ]
      );
      return true;
    }

    const maxRedeem = Math.max(0, Math.floor(Number(st.maxRedeem || 0)));
    if (coins > maxRedeem) {
      await render(
        env,
        botToken,
        st,
        `Слишком много.\nМаксимум: <b>${maxRedeem}</b>\nВведите другое число:`,
        [
          [{ text: "⬅️ Назад", callback_data: "sale_back_to_draft" }],
          [{ text: "⛔️ Отменить", callback_data: "sale_drop" }],
        ]
      );
      return true;
    }

    st.redeemCoins = coins;
    st.stage = "draft";
    st.maxRedeem = undefined;
    st.balance = undefined;

    await kvPutJson(env, flowKey, st, 900);
    await render(env, botToken, st, await draftText(db, appPublicId, st), draftKeyboardRows(st));
    return true;
  }

  // amount step (text)
  if (st.stage === "amount") {
    const cents = parseAmountToCents(t);
    if (cents == null) {
      await render(env, botToken, st, `Введите сумму числом (например 350 или 350.50):`, [[{ text: "⛔️ Отменить", callback_data: "sale_drop" }]]);
      return true;
    }

    const cbp = clamp(Number(st.cashback_percent || 10), 0, 100);
    const cashbackCoins = Math.max(0, Math.floor((cents / 100) * (cbp / 100)));

    st.amount_cents = Number(cents);
    st.cashbackCoins = Number(cashbackCoins);
    st.redeemCoins = 0;
    st.stage = "draft";

    await kvPutJson(env, flowKey, st, 900);
    await render(env, botToken, st, await draftText(db, appPublicId, st), draftKeyboardRows(st));
    return true;
  }

  // other stages: ignore cashier text (чтобы не ломать UI)
  return true;
}

// draft keyboard rows (button “redeem” only when redeemCoins===0)
function draftKeyboardRows(st: FlowState) {
  const rc = Math.max(0, Math.floor(Number(st.redeemCoins || 0)));
  const rows: any[][] = [];
  if (rc === 0) rows.push([{ text: "🪙 Списать монеты", callback_data: "sale_redeem_enter" }]);
  rows.push([
    { text: "✅ Да, записать", callback_data: "sale_record" },
    { text: "✏️ Ввести заново", callback_data: "sale_reenter" },
  ]);
  rows.push([{ text: "⛔️ Отменить", callback_data: "sale_drop" }]);
  return rows;
}
