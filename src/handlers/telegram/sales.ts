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

// must match token creator
function saleTokKey(token: string) {
  return `sale_tok:${String(token || "").trim()}`;
}

// KV keys — ВАЖНО: namespace всегда = webhook ctx.publicId
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

// ================== PINs (MONOLITH COMPAT) ==================

function randomPin4() {
  return String(Math.floor(1000 + Math.random() * 9000)); // 1000..9999
}

// 1:1 как в монолите (pins_pool: target_tg_id + issued_by_tg + issued_at)
async function issuePinToCustomer(
  db: any,
  appPublicId: string,
  cashierTgId: string,
  customerTgId: string,
  styleId: string
) {
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
    // если таблицы нет/ошибка — не блокируем (как в монолите)
    return false;
  }
}

// ================== MAIN: handleSalesFlow ==================

export async function handleSalesFlow(args: SalesArgs): Promise<boolean> {
  const { env, db, botToken, upd } = args;
  const appId = args.ctx.appId;
  const appPublicId = String(args.ctx.publicId || ""); // ✅ namespace KV всегда = publicId вебхука

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
      await tgSendMessage(env, botToken, String(chatId), "⛔️ Продажа отменена.", {}, { appPublicId, tgUserId: cashierTgId });
      await tgAnswerCallbackQuery(botToken, cqId, "Отменено", false);
      return true;
    }

    // sale_record
    if (data === "sale_record") {
      const draft = await kvGetJson(env, saleDraftKey(appPublicId, cashierTgId));
      if (!draft || !draft.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Черновик продажи не найден (истёк).", true);
        return true;
      }

      // INSERT как в монолите
      const ins = await db
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

      const saleId = (ins as any)?.meta?.last_row_id ? String((ins as any).meta.last_row_id) : "";

      // удалить draft + pend (как в монолите)
      await kvDel(env, saleDraftKey(appPublicId, cashierTgId));
      await kvDel(env, salePendKey(appPublicId, cashierTgId));

      // сохранить action на 1 час (как в монолите)
      const actionPayload = {
        appPublicId: String(appPublicId),
        saleId,
        customerTgId: String(draft.customerTgId || ""),
        cashbackCoins: Number(draft.cashbackCoins || 0),
        cashback_percent: Number(draft.cashback_percent || 0),
        amount_cents: Number(draft.amount_cents || 0),
      };
      if (saleId) {
        await kvPutJson(env, saleActionKey(appPublicId, saleId, cashierTgId), actionPayload, 3600);
      }

      // ✅ как в оригинале: "Подтвердить" + "Отменить" + PIN меню
      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `✅ Продажа записана.\nСумма: ${(Number(actionPayload.amount_cents) / 100).toFixed(2)}\nКэшбэк к выдаче: ${Number(actionPayload.cashbackCoins)} монет\nSale #${saleId}`,
        {
          reply_markup: {
            inline_keyboard: [
              [
                { text: "✅ Подтвердить кэшбэк", callback_data: `sale_confirm:${saleId}` },
                { text: "❌ Отменить", callback_data: `sale_cancel:${saleId}` },
              ],
              [{ text: "🔑 Выдать PIN", callback_data: `pin_menu:${saleId}` }],
            ],
          },
        },
        { appPublicId, tgUserId: cashierTgId }
      );

      await tgAnswerCallbackQuery(botToken, cqId, "Записано ✅", false);
      return true;
    }

    // sale_confirm:<id>
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

      // ✅ защита от повторного нажатия
      if (await ledgerHasEvent(db, eventId)) {
        await tgAnswerCallbackQuery(botToken, cqId, "Уже начислено", false);

        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `ℹ️ Кэшбэк уже начислен ранее.\nSale #${String(act.saleId || saleId)}`,
          {},
          { appPublicId, tgUserId: cashierTgId }
        );

        return true;
      }

      if (act.customerTgId && cashbackCoins > 0) {
        await awardCoins(
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

        try {
          await tgSendMessage(
            env,
            botToken,
            String(act.customerTgId),
            `🎉 Кассир подтвердил кэшбэк!\nНачислено <b>${cashbackCoins}</b> монет ✅`,
            {},
            { appPublicId, tgUserId: String(act.customerTgId) }
          );
        } catch (_) {}
      }

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `✅ Кэшбэк подтверждён.\nSale #${String(act.saleId || saleId)}\nКэшбэк: ${cashbackCoins} монет`,
        {},
        { appPublicId, tgUserId: cashierTgId }
      );

      await tgAnswerCallbackQuery(botToken, cqId, "Подтверждено ✅", false);
      return true;
    }

    // sale_cancel:<id>
    if (data.startsWith("sale_cancel:")) {
      const saleId = data.slice("sale_cancel:".length).trim();
      const act = await kvGetJson(env, saleActionKey(appPublicId, saleId, cashierTgId));

      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const cancelEventId = `sale_cancel:${appPublicId}:${String(act.saleId || saleId)}`;
      const coinsToCancel = Math.max(0, Math.floor(Number(act.cashbackCoins || 0)));

      // ✅ защита от повторного нажатия "отменить"
      if (await ledgerHasEvent(db, cancelEventId)) {
        await tgAnswerCallbackQuery(botToken, cqId, "Уже отменено", false);

        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `ℹ️ Кэшбэк уже был отменён ранее.\nSale #${String(act.saleId || saleId)}`,
          {},
          { appPublicId, tgUserId: cashierTgId }
        );

        return true;
      }

      // отмена имеет смысл только если подтверждение уже было
      const confirmEventId = `sale_confirm:${appPublicId}:${String(act.saleId || saleId)}`;
      const wasConfirmed = await ledgerHasEvent(db, confirmEventId);

      if (!wasConfirmed) {
        await tgAnswerCallbackQuery(botToken, cqId, "Ещё не начисляли", false);

        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `ℹ️ Нельзя отменить: кэшбэк ещё не начислялся.\nSale #${String(act.saleId || saleId)}`,
          {},
          { appPublicId, tgUserId: cashierTgId }
        );

        return true;
      }

      if (coinsToCancel > 0) {
        await awardCoins(
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

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `↩️ Кэшбэк отменён. Sale #${String(act.saleId || saleId)}.`,
        {},
        { appPublicId, tgUserId: cashierTgId }
      );

      try {
        await tgSendMessage(
          env,
          botToken,
          String(act.customerTgId),
          `↩️ Кэшбэк по покупке отменён кассиром.`,
          {},
          { appPublicId, tgUserId: String(act.customerTgId) }
        );
      } catch (_) {}

      await tgAnswerCallbackQuery(botToken, cqId, "Готово ✅", false);
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

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `Выбери штамп/день — PIN уйдёт клиенту (клиент: ${String(act.customerTgId)})`,
        { reply_markup: { inline_keyboard: kb } },
        { appPublicId, tgUserId: cashierTgId }
      );

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
        const r = await db
          .prepare(`SELECT title FROM styles_dict WHERE app_public_id=? AND style_id=? LIMIT 1`)
          .bind(appPublicId, styleId)
          .first();
        stTitle = r ? String((r as any).title || "") : "";
      } catch (_) {}

      const pinRes = await issuePinToCustomer(db, appPublicId, cashierTgId, String(act.customerTgId), styleId);
      if (!pinRes || !pinRes.ok) {
        await tgAnswerCallbackQuery(botToken, cqId, "Не удалось создать PIN (см. логи).", true);
        return true;
      }

      // сохранить контекст PIN (для отмены) — namespace = webhook publicId
      try {
        await kvPutJson(
          env,
          pinActionKey(appPublicId, String(pinRes.pin), cashierTgId),
          { appPublicId, pin: String(pinRes.pin), customerTgId: String(act.customerTgId), styleId },
          3600
        );
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

      // кассиру
      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `✅ PIN отправлен клиенту ${String(act.customerTgId)} для ${stTitle ? `“${stTitle}”` : styleId}.\nPIN: <code>${String(pinRes.pin)}</code>`,
        {
          reply_markup: {
            inline_keyboard: [[{ text: "⛔️ Отменить PIN", callback_data: `pin_void:${String(pinRes.pin)}` }]],
          },
        },
        { appPublicId, tgUserId: cashierTgId }
      );

      await tgAnswerCallbackQuery(botToken, cqId, "PIN отправлен ✅", false);
      return true;
    }

    // pin_void:<pin>
    if (data.startsWith("pin_void:")) {
      const pin = data.slice("pin_void:".length).trim();

      // читаем контекст PIN (если есть) — но namespace всё равно webhook publicId
      const act = await kvGetJson(env, pinActionKey(appPublicId, pin, cashierTgId));
      const res = await voidPin(db, appPublicId, pin);

      if (!res.ok) {
        await tgAnswerCallbackQuery(botToken, cqId, "PIN не найден", true);
        return true;
      }

      // можно удалить контекст PIN, чтобы не мусорить
      try {
        await kvDel(env, pinActionKey(appPublicId, pin, cashierTgId));
      } catch (_) {}

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `⛔️ PIN отменён.\nPIN: <code>${pin}</code>`,
        {},
        { appPublicId, tgUserId: cashierTgId }
      );

      // если знаем клиента — сообщим (опционально)
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

  // ---------- MESSAGES (/start sale_... + amount step) ----------
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

      // ✅ FIX: токен должен принадлежать ЭТОМУ publicId (namespace = webhook)
      if (tokenAppPublicId && tokenAppPublicId !== appPublicId) {
        await tgSendMessage(
          env,
          botToken,
          chatId,
          "⛔️ Этот QR относится к другому проекту/боту. Откройте QR в правильном боте.",
          {},
          { appPublicId, tgUserId: fromId }
        );
        return true;
      }

      const ss = await getSalesSettings(db, appPublicId);
      const isCashier = ss.cashiers.includes(String(fromId));
      if (!isCashier) {
        await tgSendMessage(env, botToken, chatId, "⛔️ Вы не зарегистрированы как кассир для этого проекта.", {}, { appPublicId, tgUserId: fromId });
        return true;
      }

      const pend = {
        appPublicId,
        customerTgId,
        token,
        cashback_percent: ss.cashback_percent,
      };

      await kvPutJson(env, salePendKey(appPublicId, String(fromId)), pend, 600);

      // удалить одноразовый token
      await kvDel(env, saleTokKey(token));

      await tgSendMessage(
        env,
        botToken,
        chatId,
        `✅ Клиент: ${customerTgId}\nВведите сумму покупки (например 350 или 350.50):`,
        {},
        { appPublicId, tgUserId: fromId }
      );

      return true;
    }

    return false;
  }

  // amount step (draft + ask confirm) — ищем pend по webhook publicId (namespace стабильный)
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
      };

      await kvPutJson(env, saleDraftKey(appPublicId, String(fromId)), draft, 600);

      await tgSendMessage(
        env,
        botToken,
        chatId,
        `❓ Записать продажу?\nСумма: <b>${(cents / 100).toFixed(2)}</b>\nКэшбэк к выдаче: <b>${cashbackCoins}</b> монет\nКлиент: <code>${String((pend as any).customerTgId || "")}</code>`,
        {
          reply_markup: {
            inline_keyboard: [
              [
                { text: "✅ Да, записать", callback_data: "sale_record" },
                { text: "✏️ Ввести заново", callback_data: "sale_reenter" },
              ],
              [{ text: "⛔️ Отменить", callback_data: "sale_drop" }],
            ],
          },
        },
        { appPublicId, tgUserId: fromId }
      );

      return true;
    }
  } catch (_) {
    // тихо как в монолите — не валим вебхук
  }

  return false;
}
