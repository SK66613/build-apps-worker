// src/handlers/telegram/sales.ts
import type { Env } from "../../index";
import { tgSendMessage } from "../../services/telegramSend";
import { awardCoins } from "../../services/coinsLedger";

type SalesArgs = {
  env: Env;
  db: any;
  ctx: { appId: any; publicId: string };
  botToken: string;
  upd: any;
};

function safeStr(v: any) {
  return String(v ?? "").trim();
}
function escHtml(s: string) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

async function tgAnswerCallbackQuery(botToken: string, callbackQueryId: string, text?: string, showAlert = false) {
  const url = `https://api.telegram.org/bot${botToken}/answerCallbackQuery`;
  await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      callback_query_id: callbackQueryId,
      text: text || "",
      show_alert: !!showAlert,
    }),
  }).catch(() => null);
}

// ===== KV (как у тебя: BOT_SECRETS) =====
function kv(env: Env): KVNamespace | null {
  return (env as any)?.BOT_SECRETS || null;
}
async function loadKV(env: Env, key: string) {
  const k = kv(env);
  if (!k) return null;
  return await k.get(key, "json").catch(() => null);
}
async function saveKV(env: Env, key: string, obj: any, ttlSec: number) {
  const k = kv(env);
  if (!k) return;
  await k.put(key, JSON.stringify(obj ?? {}), { expirationTtl: Math.max(60, Number(ttlSec || 600)) }).catch(() => null);
}
async function delKV(env: Env, key: string) {
  const k = kv(env);
  if (!k) return;
  await k.delete(key).catch(() => null);
}

// ===== KV keys (как в оригинале) =====
function saleTokKey(tok: string) {
  return `sale_tok:${tok}`; // без appPublicId
}
function salePendingKey(appPublicId: string, cashierTgId: string) {
  return `sale_pending:${appPublicId}:${cashierTgId}`;
}
function saleDraftKey(appPublicId: string, cashierTgId: string) {
  return `sale_draft:${appPublicId}:${cashierTgId}`;
}
function saleActionKey(appPublicId: string, saleId: string, cashierTgId: string) {
  return `sale_action:${appPublicId}:${saleId}:${cashierTgId}`;
}
function pinActionKey(appPublicId: string, pin: string, cashierTgId: string) {
  return `pin_action:${appPublicId}:${pin}:${cashierTgId}`;
}

// ===== settings =====
async function getSalesSettings(db: any, appPublicId: string) {
  const row: any = await db
    .prepare(
      `SELECT cashier1_tg_id, cashier2_tg_id, cashier3_tg_id, cashier4_tg_id, cashier5_tg_id,
              cashback_percent, ttl_sec
       FROM sales_settings
       WHERE app_public_id = ? LIMIT 1`
    )
    .bind(String(appPublicId))
    .first();

  const cashiers = [row?.cashier1_tg_id, row?.cashier2_tg_id, row?.cashier3_tg_id, row?.cashier4_tg_id, row?.cashier5_tg_id]
    .filter(Boolean)
    .map((x: any) => String(x));

  return {
    cashiers,
    cashback_percent: Number(row?.cashback_percent || 0),
    ttl_sec: Number(row?.ttl_sec || 600),
  };
}
function isCashier(settings: any, tgId: any) {
  const id = String(tgId || "");
  return !!id && Array.isArray(settings?.cashiers) && settings.cashiers.includes(id);
}

function parseAmountToCents(text: string): number | null {
  const t = safeStr(text).replace(",", ".");
  if (!t) return null;
  if (!/^(\d+)(\.\d{1,2})?$/.test(t)) return null;
  const n = Number(t);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.round(n * 100);
}

// ===== pins_pool (как у тебя) =====
async function issuePinToCustomer(db: any, appPublicId: string, cashierTgId: string, targetTgId: string, styleId: string) {
  const pin = String(Math.floor(100000 + Math.random() * 900000));
  await db
    .prepare(
      `INSERT INTO pins_pool (app_public_id, pin, target_tg_id, style_id, issued_by_tg, created_at)
       VALUES (?, ?, ?, ?, ?, datetime('now'))`
    )
    .bind(String(appPublicId), String(pin), String(targetTgId), String(styleId), String(cashierTgId))
    .run();
  return { ok: true, pin };
}
async function voidPin(db: any, appPublicId: string, pin: string) {
  const upd = await db
    .prepare(
      `UPDATE pins_pool
       SET used_at=datetime('now')
       WHERE app_public_id=? AND pin=? AND used_at IS NULL`
    )
    .bind(String(appPublicId), String(pin))
    .run();
  return { ok: Number(upd?.meta?.changes || 0) > 0 };
}

// ===== safe D1 helpers: try query, ignore schema errors =====
function isNoSuchColumnErr(e: any) {
  const msg = String(e?.message || e || "");
  return msg.includes("no column named");
}
async function d1Try(db: any, sql: string, binds: any[]) {
  try {
    return await db.prepare(sql).bind(...binds).run();
  } catch (e: any) {
    // schema mismatch — бросаем наверх только если это НЕ про колонку
    if (isNoSuchColumnErr(e)) return null;
    throw e;
  }
}

export async function handleSalesFlow(args: SalesArgs): Promise<boolean> {
  const { env, db, botToken, upd } = args;

  const cbId = safeStr(upd?.callback_query?.id);
  const data = safeStr(upd?.callback_query?.data);
  const chatId = String(upd?.callback_query?.message?.chat?.id || upd?.callback_query?.from?.id || "");
  const cashierTgId = String(upd?.callback_query?.from?.id || "");

  // ================= CALLBACKS =================
  if (cbId && data) {
    // --- sale_record
    if (data === "sale_record") {
      const last = await loadKV(env, `sale_last_app:${cashierTgId}`);
      const appPublicId = String(last?.appPublicId || "");
      if (!appPublicId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const settings = await getSalesSettings(db, appPublicId).catch(() => ({ cashiers: [], cashback_percent: 0, ttl_sec: 600 }));
      if (!isCashier(settings, cashierTgId)) {
        await tgAnswerCallbackQuery(botToken, cbId, "Только кассир может это сделать.", true);
        return true;
      }

      const draft = await loadKV(env, saleDraftKey(appPublicId, cashierTgId));
      if (!draft || !draft.customerTgId || !draft.amountCents) {
        await tgAnswerCallbackQuery(botToken, cbId, "Черновик продажи не найден (истёк).", true);
        return true;
      }

      // ✅ INSERT БЕЗ status
      const ins = await db
        .prepare(
          `INSERT INTO sales (app_public_id, customer_tg_id, cashier_tg_id, amount_cents, cashback_coins, created_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))`
        )
        .bind(String(appPublicId), String(draft.customerTgId), String(cashierTgId), Number(draft.amountCents), Number(draft.cashbackCoins || 0))
        .run();

      const saleId = String((ins as any)?.meta?.last_row_id || (ins as any)?.lastInsertRowid || "");

      await saveKV(
        env,
        saleActionKey(appPublicId, saleId, cashierTgId),
        {
          saleId,
          appPublicId,
          customerTgId: String(draft.customerTgId),
          amountCents: Number(draft.amountCents),
          cashbackCoins: Number(draft.cashbackCoins || 0),
        },
        3600
      );

      await delKV(env, saleDraftKey(appPublicId, cashierTgId));

      const buttons = {
        inline_keyboard: [
          [
            { text: "✅ Подтвердить", callback_data: `sale_confirm:${saleId}` },
            { text: "❌ Отклонить", callback_data: `sale_decline:${saleId}` },
          ],
          [{ text: "🔐 Выдать PIN", callback_data: `pin_menu:${saleId}` }],
        ],
      };

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `🧾 Продажа #${escHtml(saleId)}\nКлиент: <code>${escHtml(String(draft.customerTgId))}</code>\nСумма: <b>${(Number(draft.amountCents) / 100).toFixed(2)}</b>\nКэшбэк: <b>${Number(draft.cashbackCoins || 0)}</b> мон.\n\nПодтверди/отклони:`,
        { reply_markup: buttons },
        { appPublicId, tgUserId: cashierTgId }
      ).catch(() => null);

      await tgAnswerCallbackQuery(botToken, cbId, "Ок", false);
      return true;
    }

    // --- reenter / drop
    if (data === "sale_reenter" || data === "sale_drop") {
      const last = await loadKV(env, `sale_last_app:${cashierTgId}`);
      const appPublicId = String(last?.appPublicId || "");
      if (!appPublicId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const settings = await getSalesSettings(db, appPublicId).catch(() => ({ cashiers: [], cashback_percent: 0, ttl_sec: 600 }));
      if (!isCashier(settings, cashierTgId)) {
        await tgAnswerCallbackQuery(botToken, cbId, "Только кассир может это сделать.", true);
        return true;
      }

      if (data === "sale_drop") {
        await delKV(env, salePendingKey(appPublicId, cashierTgId));
        await delKV(env, saleDraftKey(appPublicId, cashierTgId));
        await tgSendMessage(env, botToken, String(chatId), "🗑️ Ок, сбросил. Сканируй QR заново.", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
        await tgAnswerCallbackQuery(botToken, cbId, "Сброшено", false);
        return true;
      }

      await tgSendMessage(env, botToken, String(chatId), "✍️ Введите сумму покупки (например: 450 или 450.50).", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
      await tgAnswerCallbackQuery(botToken, cbId, "Ок", false);
      return true;
    }

    // --- confirm / decline
    if (data.startsWith("sale_confirm:") || data.startsWith("sale_decline:")) {
      const saleId = data.split(":")[1] || "";
      const okConfirm = data.startsWith("sale_confirm:");

      const last = await loadKV(env, `sale_last_app:${cashierTgId}`);
      const appPublicId = String(last?.appPublicId || "");
      if (!appPublicId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const settings = await getSalesSettings(db, appPublicId).catch(() => ({ cashiers: [], cashback_percent: 0, ttl_sec: 600 }));
      if (!isCashier(settings, cashierTgId)) {
        await tgAnswerCallbackQuery(botToken, cbId, "Только кассир может это сделать.", true);
        return true;
      }

      const act = await loadKV(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      // ✅ UPDATE БЕЗ status: ставим recorded_at или declined_at (если declined_at нет — тихо пропускаем)
      if (okConfirm) {
        await d1Try(
          db,
          `UPDATE sales SET recorded_at=datetime('now') WHERE app_public_id=? AND id=?`,
          [String(appPublicId), Number(saleId)]
        );
      } else {
        const r = await d1Try(
          db,
          `UPDATE sales SET declined_at=datetime('now') WHERE app_public_id=? AND id=?`,
          [String(appPublicId), Number(saleId)]
        );
        // если declined_at колонки нет — просто ничего, но логика “отклонено” всё равно отработает
        void r;
      }

      if (okConfirm) {
        const coins = Math.max(0, Math.floor(Number(act.cashbackCoins || 0)));

        if (coins > 0) {
          await awardCoins(
            db,
            args.ctx.appId,
            String(appPublicId),
            String(act.customerTgId),
            coins,
            "sale_cashback",
            String(saleId),
            "cashback",
            `sale_cashback:${String(appPublicId)}:${String(act.customerTgId)}:${String(saleId)}:${String(coins)}`
          );
        }

        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `✅ Подтверждено.\nSale #${escHtml(String(saleId))}\n🪙 Начислено клиенту: <b>${coins}</b> мон.`,
          { reply_markup: { inline_keyboard: [[{ text: "↩️ Отменить кэшбэк", callback_data: `sale_cancel:${saleId}` }]] } },
          { appPublicId, tgUserId: cashierTgId }
        ).catch(() => null);

        try {
          await tgSendMessage(
            env,
            botToken,
            String(act.customerTgId),
            `✅ Покупка подтверждена кассиром.\n🪙 Начислено <b>${coins}</b> монет.`,
            {},
            { appPublicId, tgUserId: String(act.customerTgId) }
          );
        } catch (_) {}

        await tgAnswerCallbackQuery(botToken, cbId, "Подтверждено ✅", false);
        return true;
      }

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `❌ Отклонено.\nSale #${escHtml(String(saleId))}`,
        {},
        { appPublicId, tgUserId: cashierTgId }
      ).catch(() => null);

      await tgAnswerCallbackQuery(botToken, cbId, "Отклонено", false);
      return true;
    }

    // --- cancel cashback
    if (data.startsWith("sale_cancel:")) {
      const saleId = data.slice("sale_cancel:".length).trim();

      const last = await loadKV(env, `sale_last_app:${cashierTgId}`);
      const appPublicId = String(last?.appPublicId || "");
      if (!appPublicId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const settings = await getSalesSettings(db, appPublicId).catch(() => ({ cashiers: [], cashback_percent: 0, ttl_sec: 600 }));
      if (!isCashier(settings, cashierTgId)) {
        await tgAnswerCallbackQuery(botToken, cbId, "Только кассир может это сделать.", true);
        return true;
      }

      const act = await loadKV(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const coins = Math.max(0, Math.floor(Number(act.cashbackCoins || 0)));
      if (coins > 0) {
        await awardCoins(
          db,
          args.ctx.appId,
          String(appPublicId),
          String(act.customerTgId),
          -Math.abs(coins),
          "sale_cancel",
          String(saleId),
          "cancel cashback",
          `sale_cancel:${String(appPublicId)}:${String(saleId)}`
        );
      }

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `↩️ Кэшбэк отменён. Sale #${escHtml(String(saleId))}.`,
        {},
        { appPublicId, tgUserId: cashierTgId }
      ).catch(() => null);

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

      await tgAnswerCallbackQuery(botToken, cbId, "Готово ✅", false);
      return true;
    }

    // --- PIN menu / make / void (оставил как было у тебя)
    if (data.startsWith("pin_menu:")) {
      const saleId = data.slice("pin_menu:".length).trim();

      const last = await loadKV(env, `sale_last_app:${cashierTgId}`);
      const appPublicId = String(last?.appPublicId || "");
      if (!appPublicId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const settings = await getSalesSettings(db, appPublicId).catch(() => ({ cashiers: [], cashback_percent: 0, ttl_sec: 600 }));
      if (!isCashier(settings, cashierTgId)) {
        await tgAnswerCallbackQuery(botToken, cbId, "Только кассир может это сделать.", true);
        return true;
      }

      const act = await loadKV(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const rows = await db
        .prepare(`SELECT style_id, title FROM styles_dict WHERE app_public_id = ? ORDER BY id ASC`)
        .bind(String(appPublicId))
        .all();

      const items = rows?.results || [];
      if (!items.length) {
        await tgSendMessage(env, botToken, String(chatId), `Нет карточек в styles_dict — нечего выдавать.`, {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
        await tgAnswerCallbackQuery(botToken, cbId, "Нет стилей", true);
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
      ).catch(() => null);

      await tgAnswerCallbackQuery(botToken, cbId, "Выбери стиль", false);
      return true;
    }

    if (data.startsWith("pin_make:")) {
      const rest = data.slice("pin_make:".length);
      const [saleIdRaw, styleIdRaw] = rest.split(":");
      const saleId = String(saleIdRaw || "").trim();
      const styleId = String(styleIdRaw || "").trim();

      const last = await loadKV(env, `sale_last_app:${cashierTgId}`);
      const appPublicId = String(last?.appPublicId || "");
      if (!appPublicId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст продажи не найден (истёк).", true);
        return true;
      }

      const settings = await getSalesSettings(db, appPublicId).catch(() => ({ cashiers: [], cashback_percent: 0, ttl_sec: 600 }));
      if (!isCashier(settings, cashierTgId)) {
        await tgAnswerCallbackQuery(botToken, cbId, "Только кассир может это сделать.", true);
        return true;
      }

      const act = await loadKV(env, saleActionKey(appPublicId, saleId, cashierTgId));
      if (!act || !act.customerTgId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст продажи не найден (истёк).", true);
        return true;
      }
      if (!styleId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Нет style_id", true);
        return true;
      }

      let stTitle = "";
      try {
        const r = await db.prepare(`SELECT title FROM styles_dict WHERE app_public_id=? AND style_id=? LIMIT 1`)
          .bind(String(appPublicId), String(styleId))
          .first();
        stTitle = r ? String((r as any).title || "") : "";
      } catch (_) {}

      const pinRes = await issuePinToCustomer(db, String(appPublicId), cashierTgId, String(act.customerTgId), styleId).catch(() => null);
      if (!pinRes?.ok) {
        await tgAnswerCallbackQuery(botToken, cbId, "Не удалось создать PIN (см. логи).", true);
        return true;
      }

      await saveKV(env, pinActionKey(String(appPublicId), String(pinRes.pin), cashierTgId), {
        appPublicId: String(appPublicId),
        pin: String(pinRes.pin),
        customerTgId: String(act.customerTgId),
        styleId,
      }, 3600);

      try {
        await tgSendMessage(
          env,
          botToken,
          String(act.customerTgId),
          `🔑 Ваш PIN для отметки штампа${stTitle ? ` “${escHtml(stTitle)}”` : ""}:\n<code>${escHtml(String(pinRes.pin))}</code>\n\n(одноразовый)`,
          {},
          { appPublicId: String(appPublicId), tgUserId: String(act.customerTgId) }
        );
      } catch (_) {}

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `✅ PIN отправлен клиенту ${String(act.customerTgId)} для ${stTitle ? `“${escHtml(stTitle)}”` : escHtml(styleId)}.\nPIN: <code>${escHtml(String(pinRes.pin))}</code>`,
        { reply_markup: { inline_keyboard: [[{ text: "⛔️ Отменить PIN", callback_data: `pin_void:${String(pinRes.pin)}` }]] } },
        { appPublicId: String(appPublicId), tgUserId: cashierTgId }
      ).catch(() => null);

      await tgAnswerCallbackQuery(botToken, cbId, "PIN отправлен ✅", false);
      return true;
    }

    if (data.startsWith("pin_void:")) {
      const pin = data.slice("pin_void:".length).trim();

      const last = await loadKV(env, `sale_last_app:${cashierTgId}`);
      const appPublicIdFallback = String(last?.appPublicId || "");

      const act = await loadKV(env, pinActionKey(appPublicIdFallback, pin, cashierTgId));
      const appPublicId = String(act?.appPublicId || appPublicIdFallback || "");

      if (!appPublicId) {
        await tgAnswerCallbackQuery(botToken, cbId, "Контекст не найден.", true);
        return true;
      }

      const settings = await getSalesSettings(db, appPublicId).catch(() => ({ cashiers: [], cashback_percent: 0, ttl_sec: 600 }));
      if (!isCashier(settings, cashierTgId)) {
        await tgAnswerCallbackQuery(botToken, cbId, "Только кассир может это сделать.", true);
        return true;
      }

      const res = await voidPin(db, appPublicId, pin).catch(() => ({ ok: false }));
      if (!res?.ok) {
        await tgAnswerCallbackQuery(botToken, cbId, "PIN не найден", true);
        return true;
      }

      await tgSendMessage(env, botToken, String(chatId), `⛔️ PIN отменён.\nPIN: <code>${escHtml(pin)}</code>`, {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
      await tgAnswerCallbackQuery(botToken, cbId, "Отменено", false);
      return true;
    }

    return false;
  }

  // ================= /start sale_<TOK> =================
  const text = safeStr(upd?.message?.text || "");
  const fromId = String(upd?.message?.from?.id || "");
  const msgChatId = String(upd?.message?.chat?.id || fromId || "");

  if (text.startsWith("/start") && text.includes("sale_")) {
    const m = text.match(/sale_([a-zA-Z0-9\-\_]+)/);
    const tok = m ? String(m[1]) : "";
    if (!tok) return true;

    const saleTok = await loadKV(env, saleTokKey(tok));
    if (!saleTok) {
      await tgSendMessage(env, botToken, msgChatId, "❌ Токен продажи истёк или неверный.", {}, { appPublicId: args.ctx.publicId, tgUserId: fromId }).catch(() => null);
      return true;
    }

    const tokenAppPublicId = String((saleTok as any).appPublicId || (saleTok as any).app_public_id || "");
    const customerTgId = String((saleTok as any).tg_id || (saleTok as any).tgId || (saleTok as any).customerTgId || "");
    if (!tokenAppPublicId || !customerTgId) {
      await tgSendMessage(env, botToken, msgChatId, "❌ Токен продажи повреждён (нет appPublicId/tg_id).", {}, { appPublicId: args.ctx.publicId, tgUserId: fromId }).catch(() => null);
      return true;
    }

    const settings = await getSalesSettings(db, tokenAppPublicId).catch(() => ({ cashiers: [], cashback_percent: 0, ttl_sec: 600 }));
    if (!isCashier(settings, fromId)) {
      await tgSendMessage(env, botToken, msgChatId, "❌ Только кассир может записывать продажи.", {}, { appPublicId: tokenAppPublicId, tgUserId: fromId }).catch(() => null);
      return true;
    }

    await saveKV(env, salePendingKey(tokenAppPublicId, fromId), { appPublicId: tokenAppPublicId, customerTgId, createdAt: Date.now() }, Number(settings.ttl_sec || 600));
    await saveKV(env, `sale_last_app:${fromId}`, { appPublicId: tokenAppPublicId }, 24 * 3600);

    await delKV(env, saleTokKey(tok)); // одноразовый

    await tgSendMessage(env, botToken, msgChatId, "✍️ Введите сумму покупки (например: 450 или 450.50).", {}, { appPublicId: tokenAppPublicId, tgUserId: fromId }).catch(() => null);
    return true;
  }

  // ================= cashier typed amount =================
  if (text) {
    const last = await loadKV(env, `sale_last_app:${fromId}`);
    const appPublicId = String(last?.appPublicId || "");
    if (!appPublicId) return false;

    const settings = await getSalesSettings(db, appPublicId).catch(() => ({ cashiers: [], cashback_percent: 0, ttl_sec: 600 }));
    if (!isCashier(settings, fromId)) return false;

    const pend = await loadKV(env, salePendingKey(appPublicId, fromId));
    if (!pend || !pend.customerTgId) return false;

    const cents = parseAmountToCents(text);
    if (cents == null) {
      await tgSendMessage(env, botToken, msgChatId, "❌ Введите сумму числом (например: 450 или 450.50).", {}, { appPublicId, tgUserId: fromId }).catch(() => null);
      return true;
    }

    const percent = Math.max(0, Number(settings.cashback_percent || 0));
    const cashbackCoins = Math.floor((cents / 100) * (percent / 100));

    await saveKV(env, saleDraftKey(appPublicId, fromId), { appPublicId, customerTgId: String(pend.customerTgId), amountCents: cents, cashbackCoins, ts: Date.now() }, Number(settings.ttl_sec || 600));

    const buttons = { inline_keyboard: [[{ text: "✅ Записать", callback_data: "sale_record" }], [{ text: "✍️ Ввести заново", callback_data: "sale_reenter" }], [{ text: "🗑️ Сбросить", callback_data: "sale_drop" }]] };

    await tgSendMessage(
      env,
      botToken,
      msgChatId,
      `🧾 Черновик продажи\nКлиент: <code>${escHtml(String(pend.customerTgId))}</code>\nСумма: <b>${(cents / 100).toFixed(2)}</b>\nКэшбэк: <b>${cashbackCoins}</b> мон.\n\nЗаписать?`,
      { reply_markup: buttons },
      { appPublicId, tgUserId: fromId }
    ).catch(() => null);

    return true;
  }

  return false;
}
