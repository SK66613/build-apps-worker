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

function parseAmountToCents(text: string): number | null {
  const t = safeStr(text).replace(",", ".");
  if (!t) return null;
  if (!/^(\d+)(\.\d{1,2})?$/.test(t)) return null;
  const n = Number(t);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.round(n * 100);
}

async function tgAnswerCallbackQuery(env: Env, botToken: string, callbackQueryId: string, text?: string) {
  const url = `https://api.telegram.org/bot${botToken}/answerCallbackQuery`;
  await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      callback_query_id: callbackQueryId,
      text: text || "",
      show_alert: false,
    }),
  }).catch(() => null);
}

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

  const cashiers = [
    row?.cashier1_tg_id,
    row?.cashier2_tg_id,
    row?.cashier3_tg_id,
    row?.cashier4_tg_id,
    row?.cashier5_tg_id,
  ]
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

// KV keys (как в монолите)
function saleTokKey(appPublicId: string, tok: string) {
  return `sale_tok:${appPublicId}:${tok}`;
}
function salePendKey(appPublicId: string, cashierTgId: string) {
  return `sale_pend:${appPublicId}:${cashierTgId}`;
}
function saleLockKey(appPublicId: string, saleId: string) {
  return `sale_lock:${appPublicId}:${saleId}`;
}

async function kvGetJson(env: Env, key: string) {
  if (!env.BOT_SECRETS) return null;
  return await env.BOT_SECRETS.get(key, "json");
}
async function kvPutJson(env: Env, key: string, obj: any, ttlSec: number) {
  if (!env.BOT_SECRETS) return;
  await env.BOT_SECRETS.put(key, JSON.stringify(obj || {}), { expirationTtl: Math.max(60, Number(ttlSec || 600)) });
}
async function kvDel(env: Env, key: string) {
  if (!env.BOT_SECRETS) return;
  await env.BOT_SECRETS.delete(key);
}

async function upsertAppUserFromBot(db: any, { appId, appPublicId, tgUserId, tgUsername = null }: any) {
  await db
    .prepare(
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
    )
    .bind(String(appId || ""), String(appPublicId), String(tgUserId), tgUsername ? String(tgUsername) : null)
    .run();
}

// PIN issue/void (как у тебя по смыслу; если у тебя в монолите другие таблицы — поменяй тут 2 SQL)
async function issuePinToCustomer(db: any, ctx: any, targetTgId: string, styleId: string | null, ttlSec: number) {
  // генерируем pin 6 цифр
  const pin = String(Math.floor(100000 + Math.random() * 900000));

  await db
    .prepare(
      `INSERT INTO pins_pool (app_id, app_public_id, pin, target_tg_id, style_id, created_at)
       VALUES (?, ?, ?, ?, ?, datetime('now'))`
    )
    .bind(String(ctx.appId), String(ctx.publicId), pin, String(targetTgId), styleId ? String(styleId) : null)
    .run();

  return { ok: true, pin, ttl_sec: ttlSec };
}

async function voidPin(db: any, ctx: any, pin: string, cashierTgId: string) {
  // помечаем used_at, чтобы “аннулировать”
  const upd = await db
    .prepare(
      `UPDATE pins_pool
       SET used_at=datetime('now')
       WHERE app_public_id=? AND pin=? AND used_at IS NULL`
    )
    .bind(String(ctx.publicId), String(pin))
    .run();

  return { ok: true, changes: Number(upd?.meta?.changes || 0), by: cashierTgId };
}

/**
 * Flow продаж/пинов:
 * - /start sale_<TOK>  -> создаём pend для кассира (KV)
 * - кассир вводит сумму -> создаём sale и кнопки "Записать / Пере-ввести / Сбросить"
 * - sale_record -> начисление cashback монет
 * - pin_make -> выдача pin клиенту
 * - pin_void -> аннулировать pin
 */
export async function handleSalesFlow(args: SalesArgs): Promise<boolean> {
  const { env, db, ctx, botToken, upd } = args;

  const settings = await getSalesSettings(db, ctx.publicId).catch(() => ({ cashiers: [], cashback_percent: 0, ttl_sec: 600 }));

  // ===== callbacks
  const cbId = safeStr(upd?.callback_query?.id);
  const cb = safeStr(upd?.callback_query?.data);
  if (cbId && cb) {
    const from = upd?.callback_query?.from;
    const fromId = String(from?.id || "");
    const chatId = String(upd?.callback_query?.message?.chat?.id || fromId || "");

    if (!isCashier(settings, fromId)) {
      try {
        await tgAnswerCallbackQuery(env, botToken, cbId, "Только кассир может это сделать.");
      } catch (_) {}
      return true;
    }

    // sale_record:<saleId>
    if (cb.startsWith("sale_record:")) {
      await tgAnswerCallbackQuery(env, botToken, cbId, "Записываю…").catch(() => null);

      const saleId = cb.split(":")[1] || "";
      if (!saleId) return true;

      // lock (KV) чтобы не кликали дважды
      const lockK = saleLockKey(ctx.publicId, saleId);
      const locked = await kvGetJson(env, lockK);
      if (locked) {
        await tgSendMessage(env, botToken, chatId, "ℹ️ Уже обрабатывается.", {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
        return true;
      }
      await kvPutJson(env, lockK, { ts: Date.now(), by: fromId }, 30).catch(() => null);

      try {
        const sale: any = await db
          .prepare(
            `SELECT id, tg_id, amount_cents, status
             FROM sales
             WHERE app_public_id=? AND id=? LIMIT 1`
          )
          .bind(ctx.publicId, Number(saleId))
          .first();

        if (!sale) {
          await tgSendMessage(env, botToken, chatId, "❌ Продажа не найдена.", {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
          await kvDel(env, lockK).catch(() => null);
          return true;
        }
        if (String(sale.status || "") === "recorded") {
          await tgSendMessage(env, botToken, chatId, "ℹ️ Уже записано.", {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
          await kvDel(env, lockK).catch(() => null);
          return true;
        }

        const amountCents = Number(sale.amount_cents || 0);
        const percent = Math.max(0, Number(settings.cashback_percent || 0));
        const coins = Math.floor((amountCents / 100) * (percent / 100));

        // отмечаем recorded
        await db
          .prepare(
            `UPDATE sales
             SET status='recorded', recorded_at=datetime('now'), recorded_by_tg=?
             WHERE id=? AND app_public_id=?`
          )
          .bind(fromId, Number(sale.id), ctx.publicId)
          .run();

        // upsert user (чтобы app_users точно был)
        await upsertAppUserFromBot(db, {
          appId: ctx.appId,
          appPublicId: ctx.publicId,
          tgUserId: String(sale.tg_id),
          tgUsername: null,
        }).catch(() => null);

        if (coins > 0) {
          await awardCoins(
            db,
            ctx.appId,
            ctx.publicId,
            String(sale.tg_id),
            coins,
            "sale_cashback",
            String(sale.id),
            `cashback ${percent}%`,
            `sale:cashback:${ctx.publicId}:${sale.tg_id}:${sale.id}:${coins}`
          );
        }

        await tgSendMessage(
          env,
          botToken,
          chatId,
          `✅ Записано.\nСумма: <b>${(amountCents / 100).toFixed(2)}</b>\nКэшбек: <b>${coins}</b> мон.`,
          {},
          { appPublicId: ctx.publicId, tgUserId: fromId }
        ).catch(() => null);

      } catch (e: any) {
        await tgSendMessage(env, botToken, chatId, `❌ Ошибка записи продажи: ${escHtml(e?.message || String(e))}`, {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
      } finally {
        await kvDel(env, lockK).catch(() => null);
      }

      return true;
    }

    // sale_reenter:<cashierId>  (как у тебя: просим ввести сумму заново)
    if (cb.startsWith("sale_reenter:")) {
      await tgAnswerCallbackQuery(env, botToken, cbId, "Ок, введи сумму заново.").catch(() => null);

      // pend уже есть (мы его не трогаем) — просто подсказка
      await tgSendMessage(
        env,
        botToken,
        chatId,
        "✍️ Введите сумму покупки (например: 450 или 450.50).",
        {},
        { appPublicId: ctx.publicId, tgUserId: fromId }
      ).catch(() => null);

      return true;
    }

    // sale_drop:<cashierId> (сброс pend)
    if (cb.startsWith("sale_drop:")) {
      await tgAnswerCallbackQuery(env, botToken, cbId, "Сбрасываю.").catch(() => null);

      const k = salePendKey(ctx.publicId, fromId);
      await kvDel(env, k).catch(() => null);

      await tgSendMessage(env, botToken, chatId, "🗑️ Ок, сбросил. Сканируй QR заново.", {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
      return true;
    }

    // pin_make:<saleId>:<styleId>
    if (cb.startsWith("pin_make:")) {
      await tgAnswerCallbackQuery(env, botToken, cbId, "Генерирую PIN…").catch(() => null);

      const parts = cb.split(":");
      const saleId = parts[1] || "";
      const styleId = parts[2] || null;

      if (!saleId) return true;

      const sale: any = await db
        .prepare(`SELECT id, tg_id FROM sales WHERE app_public_id=? AND id=? LIMIT 1`)
        .bind(ctx.publicId, Number(saleId))
        .first();

      if (!sale) {
        await tgSendMessage(env, botToken, chatId, "❌ Продажа не найдена.", {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
        return true;
      }

      const res = await issuePinToCustomer(db, ctx, String(sale.tg_id), styleId, Number(settings.ttl_sec || 600)).catch((e: any) => ({ ok: false, error: e?.message || String(e) }));
      if (!(res as any).ok) {
        await tgSendMessage(env, botToken, chatId, `❌ Ошибка PIN: ${escHtml((res as any).error || "FAIL")}`, {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
        return true;
      }

      // сообщаем кассиру + (опционально) клиенту
      await tgSendMessage(
        env,
        botToken,
        chatId,
        `✅ PIN создан: <code>${escHtml((res as any).pin)}</code>\nTTL: ${Number((res as any).ttl_sec || 0)} сек.`,
        {},
        { appPublicId: ctx.publicId, tgUserId: fromId }
      ).catch(() => null);

      try {
        await tgSendMessage(
          env,
          botToken,
          String(sale.tg_id),
          `🔐 Ваш PIN: <code>${escHtml((res as any).pin)}</code>\nПокажите кассиру или используйте в мини-апп.`,
          {},
          { appPublicId: ctx.publicId, tgUserId: String(sale.tg_id) }
        );
      } catch (_) {}

      return true;
    }

    // pin_void:<PIN>
    if (cb.startsWith("pin_void:")) {
      await tgAnswerCallbackQuery(env, botToken, cbId, "Аннулирую…").catch(() => null);

      const pin = cb.split(":")[1] || "";
      if (!pin) return true;

      const res = await voidPin(db, ctx, pin, fromId).catch((e: any) => ({ ok: false, error: e?.message || String(e) }));
      if (!(res as any).ok) {
        await tgSendMessage(env, botToken, chatId, `❌ Ошибка: ${escHtml((res as any).error || "FAIL")}`, {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
        return true;
      }

      await tgSendMessage(env, botToken, chatId, `🗑️ PIN аннулирован: <code>${escHtml(pin)}</code>`, {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
      return true;
    }

    return false;
  }

  // ===== /start sale_<TOK>
  const text = safeStr(upd?.message?.text || "");
  const from = upd?.message?.from;
  const fromId = String(from?.id || "");
  const chatId = String(upd?.message?.chat?.id || fromId || "");

  if (text.startsWith("/start") && text.includes("sale_")) {
    if (!isCashier(settings, fromId)) {
      await tgSendMessage(env, botToken, chatId, "❌ Только кассир может записывать продажи.", {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
      return true;
    }

    const m = text.match(/sale_([a-zA-Z0-9\-\_]+)/);
    const tok = m ? String(m[1]) : "";
    if (!tok) return true;

    const saleTok = await kvGetJson(env, saleTokKey(ctx.publicId, tok));
    if (!saleTok) {
      await tgSendMessage(env, botToken, chatId, "❌ Токен продажи истёк или неверный.", {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
      return true;
    }

    // pend для кассира: { tg_id клиента, style_id? }
    await kvPutJson(env, salePendKey(ctx.publicId, fromId), saleTok, Number(settings.ttl_sec || 600)).catch(() => null);

    await tgSendMessage(
      env,
      botToken,
      chatId,
      "✍️ Введите сумму покупки (например: 450 или 450.50).",
      {},
      { appPublicId: ctx.publicId, tgUserId: fromId }
    ).catch(() => null);

    return true;
  }

  // ===== кассир ввёл сумму (если есть pend в KV)
  if (text) {
    if (!isCashier(settings, fromId)) return false;

    const pend = await kvGetJson(env, salePendKey(ctx.publicId, fromId));
    if (!pend) return false;

    const cents = parseAmountToCents(text);
    if (cents == null) {
      await tgSendMessage(env, botToken, chatId, "❌ Введите сумму числом (например: 450 или 450.50).", {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
      return true;
    }

    const targetTgId = String((pend as any).tg_id || (pend as any).tgId || "");
    const styleId = (pend as any).style_id ? String((pend as any).style_id) : null;

    if (!targetTgId) {
      await tgSendMessage(env, botToken, chatId, "❌ Нет tg_id клиента в pend. Пересканируй QR.", {}, { appPublicId: ctx.publicId, tgUserId: fromId }).catch(() => null);
      return true;
    }

    // создаём sale
    const ins = await db
      .prepare(
        `INSERT INTO sales (app_id, app_public_id, tg_id, amount_cents, status, created_at, created_by_tg)
         VALUES (?, ?, ?, ?, 'new', datetime('now'), ?)`
      )
      .bind(String(ctx.appId), String(ctx.publicId), targetTgId, Number(cents), fromId)
      .run();

    const saleId = Number((ins as any)?.meta?.last_row_id || (ins as any)?.lastInsertRowid || 0);

    const buttons = {
      inline_keyboard: [
        [
          { text: "✅ Записать", callback_data: `sale_record:${saleId}` },
          { text: "✍️ Ввести заново", callback_data: `sale_reenter:${fromId}` },
          { text: "🗑️ Сбросить", callback_data: `sale_drop:${fromId}` },
        ],
        [
          { text: "🔐 Выдать PIN", callback_data: `pin_make:${saleId}:${styleId || ""}` },
        ],
      ],
    };

    await tgSendMessage(
      env,
      botToken,
      chatId,
      `🧾 Продажа #${saleId}\nКлиент: <code>${escHtml(targetTgId)}</code>\nСумма: <b>${(cents / 100).toFixed(2)}</b>\n\nЧто делаем?`,
      { reply_markup: buttons },
      { appPublicId: ctx.publicId, tgUserId: fromId }
    ).catch(() => null);

    return true;
  }

  return false;
}
