// src/handlers/telegram/sales.ts
import type { Env } from "../../index";
import { tgSendMessage } from "../../services/telegramSend";
import { awardCoins } from "../../services/coinsLedger";

type SalesArgs = {
  env: Env;
  db: any; // env.DB
  ctx: { appId: any; publicId: string }; // ctx.publicId = appPublicId текущего вебхука
  botToken: string;
  upd: any;
};

// ================== helpers (как в монолите) ==================

function safeStr(v: any) {
  return String(v ?? "").trim();
}

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

// must match token creator
function saleTokKey(token: string) {
  return `sale_tok:${String(token || "").trim()}`;
}

// KV keys — namespace всегда = webhook ctx.publicId
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
    ]
      .map((x: any) => (x ? String(x).trim() : ""))
      .filter(Boolean);

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


// атомарное списание без db.exec(): через D1 batch (транзакция)
// Требование: желательно UNIQUE INDEX на coins_ledger(event_id)
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

  // 0) идемпотентность (быстро)
  if (event_id) {
    try {
      const ex: any = await db
        .prepare(`SELECT balance_after FROM coins_ledger WHERE event_id=? LIMIT 1`)
        .bind(String(event_id))
        .first();
      if (ex) return { ok: true, reused: true, spent: cost, balance: Number(ex.balance_after || 0) };
    } catch (_) {}
  }

  try {
    // 1) Транзакция: UPDATE (условно) + INSERT ledger только если UPDATE изменил строку
    //    Вставка ledger использует changes()>0, чтобы не писать ledger при недостатке монет.
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

    // результат первого UPDATE
    const updRes = resArr && resArr[0] ? resArr[0] : null;
    const changed = Number((updRes as any)?.meta?.changes || 0);

    if (!changed) {
      const have = await getUserCoinsFast(db, appPublicId, tgId);
      return { ok: false, error: "NOT_ENOUGH_COINS", have, need: cost };
    }

    // если UPDATE прошёл — берём текущий баланс из app_users (уже после списания)
    const bal = await getUserCoinsFast(db, appPublicId, tgId);
    return { ok: true, spent: cost, balance: bal };
  } catch (e: any) {
    const msg = String(e?.message || e);

    // если UNIQUE(event_id) сработал — значит списание уже было (транзакция откатилась)
    if (/unique|constraint/i.test(msg) && event_id) {
      try {
        const ex: any = await db
          .prepare(`SELECT balance_after FROM coins_ledger WHERE event_id=? LIMIT 1`)
          .bind(String(event_id))
          .first();
        if (ex) return { ok: true, reused: true, spent: cost, balance: Number(ex.balance_after || 0) };
      } catch (_) {}
    }

    try {
      console.log("[sale.redeem.atomic.fail]", JSON.stringify({ appPublicId, tgId: String(tgId), cost, event_id, msg }));
    } catch (_) {}

    return { ok: false, error: "DB_ERROR", message: msg };
  }
}


// ================== PINs (MONOLITH COMPAT) ==================

function randomPin4() {
  return String(Math.floor(1000 + Math.random() * 9000)); // 1000..9999
}

// 1:1 как в монолите (pins_pool: target_tg_id + issued_by_tg + issued_at)
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

// Отмена PIN = пометить used_at (чтобы он больше не работал)
async function voidPin(db: any, appPublicId: string, pin: string) {
  const row: any = await db
    .prepare(
      `SELECT id, used_at
       FROM pins_pool
       WHERE app_public_id=? AND pin=?
       LIMIT 1`
    )
    .bind(String(appPublicId), String(pin))
    .first();

  if (!row) return { ok: false, error: "PIN_NOT_FOUND" };
  if (row.used_at) return { ok: true, already: true };

  await db
    .prepare(
      `UPDATE pins_pool
       SET used_at = datetime('now')
       WHERE id=? AND used_at IS NULL`
    )
    .bind(Number(row.id))
    .run();

  return { ok: true, voided: true };
}

// ================== Telegram helper ==================

async function tgAnswerCallbackQuery(botToken: string, callbackQueryId: string, text = "", showAlert = false) {
  try {
    const url = `https://api.telegram.org/bot${botToken}/answerCallbackQuery`;
    await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        callback_query_id: callbackQueryId,
        text: text || "",
        show_alert: !!showAlert,
      }),
    });
  } catch (_) {}
}

// ================== KV (raw string JSON как в монолите) ==================

async function kvGetJson(env: Env, key: string) {
  const raw = (env as any).BOT_SECRETS ? await (env as any).BOT_SECRETS.get(key) : null;
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (_) {
    return null;
  }
}
async function kvPutJson(env: Env, key: string, obj: any, ttlSec: number) {
  if (!(env as any).BOT_SECRETS) return;
  await (env as any).BOT_SECRETS
    .put(key, JSON.stringify(obj ?? {}), {
      expirationTtl: Number(ttlSec || 600),
    })
    .catch(() => {});
}
async function kvDel(env: Env, key: string) {
  if (!(env as any).BOT_SECRETS) return;
  await (env as any).BOT_SECRETS.delete(key).catch(() => {});
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

// ================== UI builders ==================

async function buildDraftText(db: any, appPublicId: string, draft: any) {
  const cents = Number(draft?.amount_cents || 0);
  const cashbackCoins = Number(draft?.cashbackCoins || 0);
  const redeemCoins = Number(draft?.redeemCoins || 0);

  const customerTgId = String(draft?.customerTgId || "");
  const balance = customerTgId ? await getUserCoinsFast(db, appPublicId, customerTgId) : 0;

  const amountCoinsMax = Math.floor(cents / 100); // 1 монета = 1 рубль
  const maxRedeemNow = Math.max(0, Math.min(balance, amountCoinsMax));

  return (
    `❓ Записать продажу?\n` +
    `Сумма: <b>${(cents / 100).toFixed(2)}</b>\n` +
    `Кэшбэк к выдаче: <b>${cashbackCoins}</b> монет\n` +
    `Списание монет: <b>${redeemCoins}</b> монет\n` +
    `Баланс клиента: <b>${balance}</b> монет\n` +
    `Макс. списание сейчас: <b>${maxRedeemNow}</b>\n` +
    `Клиент: <code>${customerTgId}</code>`
  );
}

function buildDraftKeyboard(redeemCoins: number) {
  const rc = Math.max(0, Math.floor(Number(redeemCoins || 0)));

  const kb: any[] = [];

  // ✅ показываем "Списать монеты" только если redeemCoins === 0
  if (rc === 0) {
    kb.push([{ text: "🪙 Списать монеты", callback_data: "sale_redeem_enter" }]);
  }

  kb.push([
    { text: "✅ Да, записать", callback_data: "sale_record" },
    { text: "✏️ Ввести заново", callback_data: "sale_reenter" },
  ]);

  kb.push([{ text: "⛔️ Отменить", callback_data: "sale_drop" }]);

  return { reply_markup: { inline_keyboard: kb } };
}


function buildAfterRecordKeyboard(saleId: string, redeemCoins: number) {
  const rc = Math.max(0, Math.floor(Number(redeemCoins || 0)));

  const kb: any[] = [
    [{ text: "✅ Подтвердить кэшбэк", callback_data: `sale_confirm:${saleId}` }],
  ];

  if (rc > 0) {
    kb.push([{ text: "🪙 Подтвердить списание", callback_data: `sale_redeem_confirm:${saleId}` }]);
  }

  kb.push([{ text: "🔑 Выдать PIN", callback_data: `pin_menu:${saleId}` }]);

  return { reply_markup: { inline_keyboard: kb } };
}


// ================== MAIN: handleSalesFlow ==================

export async function handleSalesFlow(args: SalesArgs): Promise<boolean> {
  const { env, db, botToken, upd } = args;
  const appId = args.ctx.appId;
  const appPublicId = String(args.ctx.publicId || ""); // ✅ namespace KV всегда = publicId вебхука

  try {
  const u = upd?.update_id;
  const kind = upd?.callback_query ? "callback" : (upd?.message ? "message" : (upd?.edited_message ? "edited" : "other"));
  console.log("[sales] in", JSON.stringify({ appPublicId, update_id: u, kind }));
} catch (_) {}


  // ---------- CALLBACKS ----------
  if (upd?.callback_query?.data) {
    const cq = upd.callback_query;
    const data = String(cq.data || "");
    const cqId = String(cq.id || "");
    const from = cq.from || null;
    const cashierTgId = from ? String(from.id) : "";
    const chatId = String(cq?.message?.chat?.id || (from ? from.id : ""));

    // sale_reenter
    if (data === "sale_reenter") {
      await kvDel(env, saleDraftKey(appPublicId, cashierTgId));
      await kvDel(env, saleRedeemWaitKey(appPublicId, cashierTgId));

      const pend = await kvGetJson(env, salePendKey(appPublicId, cashierTgId));
      if (!pend || !pend.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
        return true;
      }
      await tgSendMessage(env, botToken, String(chatId), "Введите сумму заново:", {}, { appPublicId, tgUserId: cashierTgId });
      await tgAnswerCallbackQuery(botToken, cqId, "Ок", false);
      return true;
    }

    // sale_drop
    if (data === "sale_drop") {
      await kvDel(env, saleDraftKey(appPublicId, cashierTgId));
      await kvDel(env, salePendKey(appPublicId, cashierTgId));
      await kvDel(env, saleRedeemWaitKey(appPublicId, cashierTgId));
      await tgSendMessage(env, botToken, String(chatId), "⛔️ Продажа отменена.", {}, { appPublicId, tgUserId: cashierTgId });
      await tgAnswerCallbackQuery(botToken, cqId, "Отменено", false);
      return true;
    }



    // sale_redeem_enter — перейти в режим ввода монет
    if (data === "sale_redeem_enter") {
      const draft = await kvGetJson(env, saleDraftKey(appPublicId, cashierTgId));
      if (!draft || !draft.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Черновик не найден (истёк).", true);
        return true;
      }

      const balance = await getUserCoinsFast(db, appPublicId, String(draft.customerTgId));
      const amountCoinsMax = Math.floor(Number(draft.amount_cents || 0) / 100); // 1 монета = 1 рубль
      const maxRedeem = Math.max(0, Math.min(balance, amountCoinsMax));

      await kvPutJson(env, saleRedeemWaitKey(appPublicId, cashierTgId), { maxRedeem }, 300);

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `🪙 Введите сколько монет списать (целым числом).\n0 — не списывать.\nБаланс клиента: <b>${balance}</b>\nМаксимум к списанию по чеку: <b>${maxRedeem}</b>`,
        {},
        { appPublicId, tgUserId: cashierTgId }
      );

      await tgAnswerCallbackQuery(botToken, cqId, "Жду сумму списания…", false);
      return true;
    }

    // sale_record
    if (data === "sale_record") {
      const draft = await kvGetJson(env, saleDraftKey(appPublicId, cashierTgId));
      if (!draft || !draft.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Черновик продажи не найден (истёк).", true);
        return true;
      }

      const redeemCoins = Math.max(0, Math.floor(Number(draft.redeemCoins || 0)));

      // INSERT (с redeem_coins) — если колонок ещё нет, fallback на старую схему
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
            String(draft.customerTgId || ""),
            String(cashierTgId),
            Number(draft.amount_cents || 0),
            Number(draft.cashbackCoins || 0),
            Number(redeemCoins || 0),
            String(draft.token || "")
          )
          .run();

        saleId = (ins as any)?.meta?.last_row_id ? String((ins as any).meta.last_row_id) : "";
      } catch (e: any) {
        const ins2 = await db
          .prepare(
            `INSERT INTO sales (app_id, app_public_id, customer_tg_id, cashier_tg_id, amount_cents, cashback_coins, token, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))`
          )
          .bind(
            String(appId || ""),
            String(appPublicId),
            String(draft.customerTgId || ""),
            String(cashierTgId),
            Number(draft.amount_cents || 0),
            Number(draft.cashbackCoins || 0),
            String(draft.token || "")
          )
          .run();

        saleId = (ins2 as any)?.meta?.last_row_id ? String((ins2 as any).meta.last_row_id) : "";
      }

      // удалить draft + pend + wait
      await kvDel(env, saleDraftKey(appPublicId, cashierTgId));
      await kvDel(env, salePendKey(appPublicId, cashierTgId));
      await kvDel(env, saleRedeemWaitKey(appPublicId, cashierTgId));

      // сохранить action на 1 час
      const actionPayload = {
        appPublicId: String(appPublicId),
        saleId,
        customerTgId: String(draft.customerTgId || ""),
        cashbackCoins: Number(draft.cashbackCoins || 0),
        cashback_percent: Number(draft.cashback_percent || 0),
        amount_cents: Number(draft.amount_cents || 0),
        redeemCoins: Number(redeemCoins || 0),
      };
      if (saleId) {
        await kvPutJson(env, saleActionKey(appPublicId, saleId, cashierTgId), actionPayload, 3600);
      }

      const msgText =
        `✅ Продажа записана.\n` +
        `Сумма: ${(Number(actionPayload.amount_cents) / 100).toFixed(2)}\n` +
        `Кэшбэк к выдаче: ${Number(actionPayload.cashbackCoins)} монет\n` +
        `Списание монет: ${Number(actionPayload.redeemCoins)} монет\n` +
        `Sale #${saleId}`;

      await tgSendMessage(env, botToken, String(chatId), msgText, buildAfterRecordKeyboard(saleId, redeemCoins), {
        appPublicId,
        tgUserId: cashierTgId,
      });

      await tgAnswerCallbackQuery(botToken, cqId, "Записано ✅", false);
      return true;
    }

    // sale_confirm:<id> — подтверждение кэшбэка
    if (data.startsWith("sale_confirm:")) {
      const saleId = data.slice("sale_confirm:".length).trim();
      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));

      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const cashbackCoins = Math.max(0, Math.floor(Number(act.cashbackCoins || 0)));
      const cbp = Math.max(0, Math.min(100, Number(act.cashback_percent || 0)));

      const eventId = `sale_confirm:${appPublicId}:${String(act.saleId || saleId)}`;

      if (await ledgerHasEvent(db, eventId)) {
        await tgAnswerCallbackQuery(botToken, cqId, "Уже начислено", false);
        await tgSendMessage(env, botToken, String(chatId), `ℹ️ Кэшбэк уже начислен ранее.\nSale #${String(act.saleId || saleId)}`, {}, { appPublicId, tgUserId: cashierTgId });
        return true;
      }

      if (act.customerTgId && cashbackCoins > 0) {
        await awardCoins(db, appId, appPublicId, String(act.customerTgId), cashbackCoins, "sale_cashback_confirmed", String(act.saleId || saleId), `Кэшбэк ${cbp}% за покупку`, eventId);
        try {
          await tgSendMessage(env, botToken, String(act.customerTgId), `🎉 Кассир подтвердил кэшбэк!\nНачислено <b>${cashbackCoins}</b> монет ✅`, {}, { appPublicId, tgUserId: String(act.customerTgId) });
        } catch (_) {}
      }

      await tgSendMessage(
  env,
  botToken,
  String(chatId),
  `✅ Кэшбэк подтверждён.\nSale #${String(act.saleId || saleId)}\nКэшбэк: ${cashbackCoins} монет`,
  {
    reply_markup: {
      inline_keyboard: [[{ text: "❌ Отменить кэшбэк", callback_data: `sale_cancel:${String(act.saleId || saleId)}` }]],
    },
  },
  { appPublicId, tgUserId: cashierTgId }
);

      await tgAnswerCallbackQuery(botToken, cqId, "Подтверждено ✅", false);
      return true;
    }

    // sale_cancel:<id> — отмена кэшбэка
    if (data.startsWith("sale_cancel:")) {
      const saleId = data.slice("sale_cancel:".length).trim();
      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));

      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const cancelEventId = `sale_cancel:${appPublicId}:${String(act.saleId || saleId)}`;
      const coinsToCancel = Math.max(0, Math.floor(Number(act.cashbackCoins || 0)));

      if (await ledgerHasEvent(db, cancelEventId)) {
        await tgAnswerCallbackQuery(botToken, cqId, "Уже отменено", false);
        await tgSendMessage(env, botToken, String(chatId), `ℹ️ Кэшбэк уже был отменён ранее.\nSale #${String(act.saleId || saleId)}`, {}, { appPublicId, tgUserId: cashierTgId });
        return true;
      }

      const confirmEventId = `sale_confirm:${appPublicId}:${String(act.saleId || saleId)}`;
      const wasConfirmed = await ledgerHasEvent(db, confirmEventId);

      if (!wasConfirmed) {
        await tgAnswerCallbackQuery(botToken, cqId, "Ещё не начисляли", false);
        await tgSendMessage(env, botToken, String(chatId), `ℹ️ Нельзя отменить: кэшбэк ещё не начислялся.\nSale #${String(act.saleId || saleId)}`, {}, { appPublicId, tgUserId: cashierTgId });
        return true;
      }

      if (coinsToCancel > 0) {
        await awardCoins(db, appId, appPublicId, String(act.customerTgId), -Math.abs(coinsToCancel), "sale_cancel", String(act.saleId || saleId), "cancel cashback", cancelEventId);
      }

      await tgSendMessage(env, botToken, String(chatId), `↩️ Кэшбэк отменён. Sale #${String(act.saleId || saleId)}.`, {}, { appPublicId, tgUserId: cashierTgId });
      try {
        await tgSendMessage(env, botToken, String(act.customerTgId), `↩️ Кэшбэк по покупке отменён кассиром.`, {}, { appPublicId, tgUserId: String(act.customerTgId) });
      } catch (_) {}

      await tgAnswerCallbackQuery(botToken, cqId, "Готово ✅", false);
      return true;
    }

    // sale_redeem_confirm:<saleId> — подтверждение списания монет
    if (data.startsWith("sale_redeem_confirm:")) {
      const saleId = data.slice("sale_redeem_confirm:".length).trim();
      try {
  console.log("[sale.redeem.confirm.click]", JSON.stringify({ appPublicId, saleId, cashierTgId }));
} catch (_) {}

      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));

      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const redeemCoins = Math.max(0, Math.floor(Number(act.redeemCoins || 0)));
      if (redeemCoins <= 0) {
        await tgAnswerCallbackQuery(botToken, cqId, "Списания нет", false);
        return true;
      }

      const eventId = `sale_redeem_confirm:${appPublicId}:${String(act.saleId || saleId)}`;

      const res = await spendCoinsIfEnoughAtomic(db, appId, appPublicId, String(act.customerTgId), redeemCoins, "sale_redeem_confirm", String(act.saleId || saleId), `Списание монет за покупку (Sale #${String(act.saleId || saleId)})`, eventId);

      if (!res.ok) {
        if (res.error === "NOT_ENOUGH_COINS") {
          await tgAnswerCallbackQuery(botToken, cqId, "Не хватает монет", true);
          await tgSendMessage(env, botToken, String(chatId), `⛔️ Недостаточно монет у клиента.\nНужно: <b>${redeemCoins}</b>\nЕсть: <b>${Number(res.have || 0)}</b>`, {}, { appPublicId, tgUserId: cashierTgId });
          return true;
        }
        await tgAnswerCallbackQuery(botToken, cqId, "Ошибка списания", true);
        await tgSendMessage(env, botToken, String(chatId), `⛔️ Ошибка БД при списании.`, {}, { appPublicId, tgUserId: cashierTgId });
        return true;
      }

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

 await tgSendMessage(
  env,
  botToken,
  String(chatId),
  `✅ Списание подтверждено.\nSale #${String(act.saleId || saleId)}\nСписано: <b>${redeemCoins}</b>\nБаланс клиента: <b>${Number(res.balance || 0)}</b>`,
  {
    reply_markup: {
      inline_keyboard: [[{ text: "↩️ Отменить списание", callback_data: `sale_redeem_cancel:${String(act.saleId || saleId)}` }]],
    },
  },
  { appPublicId, tgUserId: cashierTgId }
);


      try {
        await tgSendMessage(env, botToken, String(act.customerTgId), `🪙 Списано <b>${redeemCoins}</b> монет по вашей покупке.\nБаланс: <b>${Number(res.balance || 0)}</b>`, {}, { appPublicId, tgUserId: String(act.customerTgId) });
      } catch (_) {}

      await tgAnswerCallbackQuery(botToken, cqId, res.reused ? "Уже списано" : "Списано ✅", false);
      return true;
    }

    // sale_redeem_cancel:<saleId> — отмена списания (возврат монет)
    if (data.startsWith("sale_redeem_cancel:")) {
      const saleId = data.slice("sale_redeem_cancel:".length).trim();
      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));

      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
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
        await tgSendMessage(env, botToken, String(chatId), `ℹ️ Нельзя отменить: списание ещё не подтверждалось.\nSale #${String(act.saleId || saleId)}`, {}, { appPublicId, tgUserId: cashierTgId });
        return true;
      }

      const cancelEventId = `sale_redeem_cancel:${appPublicId}:${String(act.saleId || saleId)}`;
      if (await ledgerHasEvent(db, cancelEventId)) {
        await tgAnswerCallbackQuery(botToken, cqId, "Уже отменено", false);
        return true;
      }

      const rr: any = await awardCoins(db, appId, appPublicId, String(act.customerTgId), Math.abs(redeemCoins), "sale_redeem_cancel", String(act.saleId || saleId), `Возврат монет (отмена списания) Sale #${String(act.saleId || saleId)}`, cancelEventId);

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

      await tgSendMessage(env, botToken, String(chatId), `↩️ Списание отменено. Sale #${String(act.saleId || saleId)}.\nВозвращено: <b>${redeemCoins}</b>\nБаланс: <b>${Number(rr?.balance ?? 0)}</b>`, {}, { appPublicId, tgUserId: cashierTgId });

      try {
        await tgSendMessage(env, botToken, String(act.customerTgId), `↩️ Отмена списания: возвращено <b>${redeemCoins}</b> монет.\nБаланс: <b>${Number(rr?.balance ?? 0)}</b>`, {}, { appPublicId, tgUserId: String(act.customerTgId) });
      } catch (_) {}

      await tgAnswerCallbackQuery(botToken, cqId, "Отменено ✅", false);
      return true;
    }

    // pin_menu:<saleId>
    if (data.startsWith("pin_menu:")) {
      const saleId = data.slice("pin_menu:".length).trim();
      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));

      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
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
        await tgSendMessage(env, botToken, String(chatId), `Нет карточек в styles_dict — нечего выдавать.`, {}, { appPublicId, tgUserId: cashierTgId });
        await tgAnswerCallbackQuery(botToken, cqId, "Нет стилей", true);
        return true;
      }

      const kb: any[] = [];
      for (let i = 0; i < items.length; i += 2) {
        const a = items[i];
        const b = items[i + 1];
        const row: any[] = [];
        row.push({ text: String(a.title || a.style_id), callback_data: `pin_make:${saleId}:${String(a.style_id)}` });
        if (b) row.push({ text: String(b.title || b.style_id), callback_data: `pin_make:${saleId}:${String(b.style_id)}` });
        kb.push(row);
      }

      await tgSendMessage(env, botToken, String(chatId), `Выбери штамп/день — PIN уйдёт клиенту (клиент: ${String(act.customerTgId)})`, { reply_markup: { inline_keyboard: kb } }, { appPublicId, tgUserId: cashierTgId });

      await tgAnswerCallbackQuery(botToken, cqId, "Выбери стиль", false);
      return true;
    }

    // pin_make:<saleId>:<styleId>
    if (data.startsWith("pin_make:")) {
      const rest = data.slice("pin_make:".length);
      const [saleIdRaw, styleIdRaw] = rest.split(":");
      const saleId = String(saleIdRaw || "").trim();
      const styleId = String(styleIdRaw || "").trim();

      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
        return true;
      }
      if (!styleId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Нет style_id", true);
        return true;
      }

      let stTitle = "";
      try {
        const r = await db.prepare(`SELECT title FROM styles_dict WHERE app_public_id=? AND style_id=? LIMIT 1`).bind(appPublicId, styleId).first();
        stTitle = r ? String((r as any).title || "") : "";
      } catch (_) {}

      const pinRes = await issuePinToCustomer(db, appPublicId, cashierTgId, String(act.customerTgId), styleId);
      if (!pinRes || !pinRes.ok) {
        await tgAnswerCallbackQuery(botToken, cqId, "Не удалось создать PIN (см. логи).", true);
        return true;
      }

      try {
        await kvPutJson(env, pinActionKey(appPublicId, String(pinRes.pin), cashierTgId), { appPublicId, pin: String(pinRes.pin), customerTgId: String(act.customerTgId), styleId }, 3600);
      } catch (_) {}

      try {
        await tgSendMessage(env, botToken, String(act.customerTgId), `🔑 Ваш PIN для отметки штампа${stTitle ? ` “${stTitle}”` : ""}:\n<code>${String(pinRes.pin)}</code>\n\n(одноразовый)`, {}, { appPublicId, tgUserId: String(act.customerTgId) });
      } catch (_) {}

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `✅ PIN отправлен клиенту ${String(act.customerTgId)} для ${stTitle ? `“${stTitle}”` : styleId}.\nPIN: <code>${String(pinRes.pin)}</code>`,
        { reply_markup: { inline_keyboard: [[{ text: "⛔️ Отменить PIN", callback_data: `pin_void:${String(pinRes.pin)}` }]] } },
        { appPublicId, tgUserId: cashierTgId }
      );

      await tgAnswerCallbackQuery(botToken, cqId, "PIN отправлен ✅", false);
      return true;
    }

    // pin_void:<pin>
    if (data.startsWith("pin_void:")) {
      const pin = data.slice("pin_void:".length).trim();
      const act = await kvGetJson(env, pinActionKey(appPublicId, pin, cashierTgId));
      const res = await voidPin(db, appPublicId, pin);

      if (!res.ok) {
        await tgAnswerCallbackQuery(botToken, cqId, "PIN не найден", true);
        return true;
      }

      try {
        await kvDel(env, pinActionKey(appPublicId, pin, cashierTgId));
      } catch (_) {}

      await tgSendMessage(env, botToken, String(chatId), `⛔️ PIN отменён.\nPIN: <code>${pin}</code>`, {}, { appPublicId, tgUserId: cashierTgId });

      const customerTgId = act && (act as any).customerTgId ? String((act as any).customerTgId) : "";
      if (customerTgId) {
        try {
          await tgSendMessage(env, botToken, customerTgId, `⛔️ PIN был отменён кассиром.`, {}, { appPublicId, tgUserId: customerTgId });
        } catch (_) {}
      }

      await tgAnswerCallbackQuery(botToken, cqId, "Отменено", false);
      return true;
    }

    // не наше
    return false;
  }

  // ---------- MESSAGES (/start sale_... + redeem input step + amount step) ----------
  const text = (upd?.message && upd.message.text) || (upd?.edited_message && upd.edited_message.text) || "";
  const t = String(text || "").trim();

  const msg = upd?.message || upd?.edited_message || null;
  const from = msg?.from || null;
  const fromId = from ? String(from.id) : "";
  const chatId = msg?.chat?.id != null ? String(msg.chat.id) : fromId;

  if (!fromId || !chatId) return false;

  // /start sale_
  if (t === "/start" || t.startsWith("/start ")) {
    const payload = t.startsWith("/start ") ? t.slice(7).trim() : "";

    if (payload.startsWith("sale_")) {
      const token = payload.slice(5).trim();

      const rawTok = (env as any).BOT_SECRETS ? await (env as any).BOT_SECRETS.get(saleTokKey(token)) : null;
      if (!rawTok) {
        await tgSendMessage(env, botToken, chatId, "⛔️ Этот QR устарел. Попросите клиента обновить QR.", {}, { appPublicId, tgUserId: fromId });
        return true;
      }

      let tokObj: any = null;
      try {
        tokObj = JSON.parse(rawTok);
      } catch (_) {}

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

      const pend = { appPublicId, customerTgId, token, cashback_percent: ss.cashback_percent };

      await kvPutJson(env, salePendKey(appPublicId, String(fromId)), pend, 600);
      await kvDel(env, saleTokKey(token));

      await kvDel(env, saleDraftKey(appPublicId, String(fromId)));
      await kvDel(env, saleRedeemWaitKey(appPublicId, String(fromId)));

      await tgSendMessage(env, botToken, chatId, `✅ Клиент: ${customerTgId}\nВведите сумму покупки (например 350 или 350.50):`, {}, { appPublicId, tgUserId: fromId });
      return true;
    }

    return false;
  }

  // ===== redeem amount input step (кассир вводит сколько списать) =====
  try {
    const wait = await kvGetJson(env, saleRedeemWaitKey(appPublicId, String(fromId)));
    if (wait) {
      const draft = await kvGetJson(env, saleDraftKey(appPublicId, String(fromId)));
      if (!draft || !draft.customerTgId) {
        await kvDel(env, saleRedeemWaitKey(appPublicId, String(fromId)));
        return true;
      }

      const coins = parseIntCoins(t);
      if (coins == null) {
        await tgSendMessage(env, botToken, chatId, "Введите целое число монет (например 0 или 120).", {}, { appPublicId, tgUserId: fromId });
        return true;
      }

      const maxRedeem = Math.max(0, Math.floor(Number(wait.maxRedeem || 0)));
      if (coins > maxRedeem) {
        await tgSendMessage(env, botToken, chatId, `Слишком много. Максимум: <b>${maxRedeem}</b>`, {}, { appPublicId, tgUserId: fromId });
        return true;
      }

      draft.redeemCoins = coins;
      await kvPutJson(env, saleDraftKey(appPublicId, String(fromId)), draft, 600);
      await kvDel(env, saleRedeemWaitKey(appPublicId, String(fromId)));

      await tgSendMessage(env, botToken, chatId, await buildDraftText(db, appPublicId, draft), buildDraftKeyboard(Number(draft?.redeemCoins || 0)), { appPublicId, tgUserId: fromId });
      return true;
    }
  } catch (_) {}

  // amount step (draft + ask confirm)
  try {
    const pend = await kvGetJson(env, salePendKey(appPublicId, String(fromId)));
    if (pend) {
      const cents = parseAmountToCents(t);
      if (cents == null) {
        await tgSendMessage(env, botToken, chatId, "Введите сумму числом (например 350 или 350.50)", {}, { appPublicId, tgUserId: fromId });
        return true;
      }

      const cbp = Math.max(0, Math.min(100, Number((pend as any)?.cashback_percent ?? 10)));
      const cashbackCoins = Math.max(0, Math.floor((cents / 100) * (cbp / 100)));

      const draft = {
        appPublicId,
        customerTgId: String((pend as any).customerTgId || ""),
        token: String((pend as any).token || ""),
        amount_cents: Number(cents),
        cashbackCoins: Number(cashbackCoins),
        cashback_percent: Number(cbp),
        redeemCoins: 0,
      };

      await kvPutJson(env, saleDraftKey(appPublicId, String(fromId)), draft, 600);

      await tgSendMessage(env, botToken, chatId, await buildDraftText(db, appPublicId, draft), buildDraftKeyboard(Number(draft?.redeemCoins || 0)), { appPublicId, tgUserId: fromId });
      return true;
    }
  } catch (_) {}

  return false;
}
