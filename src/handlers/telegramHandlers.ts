// src/handlers/telegramHandlers.ts
// Telegram webhook handlers (safe + verbose logging; always 200 to Telegram).

import type { Env } from "../index";
import { getBotWebhookSecretForPublicId, timingSafeEqual } from "../services/bots";
import { getBotTokenForApp } from "../services/botToken";
import { resolveAppContextByPublicId } from "../services/apps";
import { tgAnswerPreCheckoutQuery } from "../services/telegramApi";
import { tgSendMessage } from "../services/telegramSend";
import { awardCoins } from "../services/coinsLedger";

// ================== LOGGING HELPERS ==================

function safeJson(obj: any, maxLen = 8000) {
  try {
    const s = JSON.stringify(obj);
    return s.length > maxLen ? s.slice(0, maxLen) : s;
  } catch (_) {
    return null;
  }
}

function errObj(e: any) {
  if (!e) return { message: "unknown" };
  return {
    name: String(e?.name || "Error"),
    message: String(e?.message || e),
    stack: e?.stack ? String(e.stack) : null,
    cause: e?.cause ? String(e.cause) : null,
  };
}

function logEvt(level: "info" | "warn" | "error", tag: string, data: any) {
  const payload = { level, tag, ...data };
  const line = safeJson(payload, 16000);
  if (level === "error") console.error(line);
  else if (level === "warn") console.warn(line);
  else console.log(line);
}

function pickUpdateType(upd: any) {
  if (!upd) return "unknown";
  if (upd.pre_checkout_query) return "pre_checkout_query";
  if (upd?.message?.successful_payment) return "successful_payment";
  if (upd.callback_query) return "callback_query";
  if (upd.edited_message) return "edited_message";
  if (upd.message) return "message";
  return "unknown";
}

function pickMsgType(upd: any) {
  if (upd && upd.callback_query) return "callback";
  const txt =
    (upd.message && upd.message.text) ||
    (upd.edited_message && upd.edited_message.text) ||
    "";
  if (txt && String(txt).trim().startsWith("/")) return "command";
  return "text";
}

// ================== SALES / PINS HELPERS (LOCAL) ==================

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
  } catch (e) {
    logEvt("warn", "tg.getSalesSettings_failed", { err: errObj(e), appPublicId });
    return { cashiers: [], cashback_percent: 10, ttl_sec: 300 };
  }
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

// must match token creator (where you generate sale token)
function saleTokKey(token: string) {
  return `sale_tok:${String(token || "").trim()}`;
}

// KV keys
function salePendKey(appPublicId: string, cashierTgId: string) {
  return `sale_pending:${String(appPublicId)}:${String(cashierTgId)}`;
}
function saleDraftKey(appPublicId: string, cashierTgId: string) {
  return `sale_draft:${String(appPublicId)}:${String(cashierTgId)}`;
}
function saleActionKey(appPublicId: string, saleId: string, cashierTgId: string) {
  return `sale_action:${String(appPublicId)}:${String(saleId)}:${String(cashierTgId)}`;
}
function redeemActionKey(appPublicId: string, redeemCode: string, cashierTgId: string) {
  return `redeem_action:${String(appPublicId)}:${String(redeemCode)}:${String(cashierTgId)}`;
}
function pinActionKey(appPublicId: string, pin: string, cashierTgId: string) {
  return `pin_action:${String(appPublicId)}:${String(pin)}:${String(cashierTgId)}`;
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

      logEvt("error", "pin.issue_failed", {
        err: errObj(e),
        appPublicId,
        cashierTgId,
        customerTgId,
        styleId,
      });
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

// ================== TELEGRAM API HELPERS (LOCAL) ==================

async function tgAnswerCallbackQuery(botToken: string, callbackQueryId: string, text = "", showAlert = false) {
  try {
    const url = `https://api.telegram.org/bot${botToken}/answerCallbackQuery`;
    const body = {
      callback_query_id: callbackQueryId,
      text: text || "",
      show_alert: !!showAlert,
    };
    await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (e) {
    logEvt("warn", "tg.answerCallbackQuery_failed", { err: errObj(e) });
  }
}

// ================== BOT LOGGING + SYNC (D1) ==================

async function logBotMessage(
  db: any,
  {
    appPublicId,
    tgUserId,
    direction,
    msgType,
    text = null,
    chatId = null,
    tgMessageId = null,
    payload = null,
  }: any
) {
  try {
    await db
      .prepare(
        `INSERT INTO bot_messages
          (app_public_id, tg_user_id, direction, msg_type, text, tg_message_id, chat_id, payload_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .bind(
        String(appPublicId),
        String(tgUserId),
        String(direction),
        String(msgType),
        text != null ? String(text) : null,
        tgMessageId != null ? Number(tgMessageId) : null,
        chatId != null ? String(chatId) : null,
        payload ? safeJson(payload) : null
      )
      .run();
  } catch (e) {
    logEvt("warn", "bot.log_in_failed", { err: errObj(e), appPublicId, tgUserId });
  }
}

async function upsertAppUserFromBot(db: any, { appId, appPublicId, tgUserId, tgUsername = null }: any) {
  try {
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
  } catch (e) {
    logEvt("warn", "bot.upsert_user_failed", { err: errObj(e), appPublicId, tgUserId });
  }
}

// Minimal state for /profile
async function buildStateLite(db: any, appId: any, appPublicId: string, tgId: string, cfg: any = {}) {
  const out: any = {
    coins: 0,
    styles_count: 0,
    styles_total: 0,
    game_today_best: 0,
    ref_total: 0,
  };

  try {
    const u = await db
      .prepare(`SELECT coins FROM app_users WHERE app_public_id = ? AND tg_user_id = ? LIMIT 1`)
      .bind(String(appPublicId), String(tgId))
      .first();
    out.coins = u ? Number((u as any).coins || 0) : 0;
  } catch (e) {
    logEvt("warn", "state.coins_failed", { err: errObj(e), appPublicId, tgId });
  }

  try {
    const r = await db
      .prepare(
        `SELECT COUNT(DISTINCT style_id) AS c
         FROM styles_user
         WHERE app_public_id = ? AND tg_id = ? AND status = 'collected'`
      )
      .bind(String(appPublicId), String(tgId))
      .first();
    out.styles_count = r ? Number((r as any).c || 0) : 0;
  } catch (e) {
    logEvt("warn", "state.styles_count_failed", { err: errObj(e), appPublicId, tgId });
  }

  try {
    const r = await db.prepare(`SELECT COUNT(*) AS c FROM styles_dict WHERE app_public_id = ?`).bind(String(appPublicId)).first();
    out.styles_total = r ? Number((r as any).c || 0) : 0;
  } catch (e) {
    logEvt("warn", "state.styles_total_failed", { err: errObj(e), appPublicId });
  }

  try {
    const today = new Date().toISOString().slice(0, 10);
    const g = await db
      .prepare(
        `SELECT best_score
         FROM games_results_daily
         WHERE app_public_id = ? AND date = ? AND mode = 'daily' AND tg_id = ?
         ORDER BY id DESC LIMIT 1`
      )
      .bind(String(appPublicId), String(today), String(tgId))
      .first();
    out.game_today_best = g ? Number((g as any).best_score || 0) : 0;
  } catch (e) {
    logEvt("warn", "state.game_failed", { err: errObj(e), appPublicId, tgId });
  }

  try {
    const r = await db
      .prepare(`SELECT COUNT(*) AS c FROM referrals WHERE app_public_id = ? AND referrer_tg_id = ?`)
      .bind(String(appPublicId), String(tgId))
      .first();
    out.ref_total = r ? Number((r as any).c || 0) : 0;
  } catch (_) {
    out.ref_total = 0;
  }

  out.config = cfg || {};
  return out;
}

// ================== MAIN HANDLER ==================

export async function handleTelegramWebhook(publicId: string, request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const s = url.searchParams.get("s") || "";

  let upd: any = null;
  let updateId = "";
  let fromId: any = null;
  let chatId: any = null;

  try {
    const expected = await getBotWebhookSecretForPublicId(publicId, env);
    if (!expected || !timingSafeEqual(s, expected)) {
      return new Response("FORBIDDEN", { status: 403 });
    }

    try {
      upd = await request.json();
      updateId = upd && upd.update_id != null ? String(upd.update_id) : "";
    } catch (_) {
      return new Response("OK", { status: 200 });
    }

    const updType = pickUpdateType(upd);
    const msg =
      upd.message || upd.edited_message || (upd.callback_query ? upd.callback_query.message : null);

    const from =
      (upd.message && upd.message.from) ||
      (upd.edited_message && upd.edited_message.from) ||
      (upd.callback_query && upd.callback_query.from) ||
      null;

    fromId = from ? from.id : null;
    chatId = msg && msg.chat ? msg.chat.id : (from ? from.id : null);

    logEvt("info", "tg.webhook_in", {
      publicId,
      updateId,
      updType,
      fromId,
      chatId,
      cfRay: request.headers.get("cf-ray") || null,
      len: request.headers.get("content-length") || null,
    });

    if (env.BOT_SECRETS && updateId) {
      const k = `tg_upd:public:${publicId}:${updateId}`;
      const seen = await env.BOT_SECRETS.get(k);
      if (seen) return new Response("OK", { status: 200 });
      await env.BOT_SECRETS.put(k, "1", { expirationTtl: 3600 });
    }

    // ===== STARS =====
    try {
      const botTokenEarly = await getBotTokenForApp(publicId, env, null);
      if (botTokenEarly) {
        if (upd && upd.pre_checkout_query) {
          const pcq = upd.pre_checkout_query;
          const invPayload = String(pcq.invoice_payload || "");
          const orderId = invPayload.startsWith("order:") ? invPayload.slice(6) : "";

          let ok = true;
          let err = "";

          if (!orderId) {
            ok = false;
            err = "Bad payload";
          } else {
            const row = await env.DB.prepare(
              `SELECT id, status, total_stars
               FROM stars_orders
               WHERE id = ? AND app_public_id = ?
               LIMIT 1`
            ).bind(orderId, publicId).first();

            if (!row) { ok = false; err = "Order not found"; }
            else if (String((row as any).status) !== "created") { ok = false; err = "Order already processed"; }
          }

          await tgAnswerPreCheckoutQuery(botTokenEarly, pcq.id, ok, err);
          return new Response("OK", { status: 200 });
        }

        const sp = upd?.message?.successful_payment;
        if (sp) {
          const invPayload = String(sp.invoice_payload || "");
          const orderId = invPayload.startsWith("order:") ? invPayload.slice(6) : "";

          if (orderId) {
            await env.DB.prepare(
              `UPDATE stars_orders
               SET status = 'paid',
                   paid_at = datetime('now'),
                   telegram_payment_charge_id = ?,
                   provider_payment_charge_id = ?,
                   paid_total_amount = ?
               WHERE id = ? AND app_public_id = ?`
            ).bind(
              String(sp.telegram_payment_charge_id || ""),
              String(sp.provider_payment_charge_id || ""),
              Number(sp.total_amount || 0),
              orderId,
              publicId
            ).run();
          }

          return new Response("OK", { status: 200 });
        }
      }
    } catch (e) {
      logEvt("error", "stars.handler_failed", { publicId, updateId, err: errObj(e) });
    }

    if (!chatId || !from) return new Response("OK", { status: 200 });

    const botToken = await getBotTokenForApp(publicId, env, null);
    if (!botToken) return new Response("OK", { status: 200 });

    const ctx = await resolveAppContextByPublicId(publicId, env);
    if (!ctx || !ctx.ok) return new Response("OK", { status: 200 });

    const appPublicId = ctx.publicId || publicId;
    const appId = ctx.appId;

    // ================= CALLBACKS =================
    if (upd?.callback_query?.data) {
      const cq = upd.callback_query;
      const data = String(cq.data || "");
      const cqId = String(cq.id || "");
      const cashierTgId = String(from.id);

      const loadKV = async (k: string) => {
        const raw = env.BOT_SECRETS ? await env.BOT_SECRETS.get(k) : null;
        if (!raw) return null;
        try { return JSON.parse(raw); } catch (_) { return null; }
      };

      // ---------- SALE: confirm record ----------
      if (data === "sale_reenter") {
        // вернуть кассира на ввод суммы
        const pend = await loadKV(salePendKey(appPublicId, cashierTgId));
        if (!pend || !pend.customerTgId) {
          await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
          return new Response("OK", { status: 200 });
        }
        await tgSendMessage(env, botToken, String(chatId), "Введите сумму заново:", {}, { appPublicId, tgUserId: cashierTgId });
        await tgAnswerCallbackQuery(botToken, cqId, "Ок", false);
        return new Response("OK", { status: 200 });
      }

      if (data === "sale_drop") {
        // отменить вообще
        if (env.BOT_SECRETS) {
          await env.BOT_SECRETS.delete(saleDraftKey(appPublicId, cashierTgId)).catch(()=>{});
          await env.BOT_SECRETS.delete(salePendKey(appPublicId, cashierTgId)).catch(()=>{});
        }
        await tgSendMessage(env, botToken, String(chatId), "⛔️ Продажа отменена.", {}, { appPublicId, tgUserId: cashierTgId });
        await tgAnswerCallbackQuery(botToken, cqId, "Отменено", false);
        return new Response("OK", { status: 200 });
      }

      if (data === "sale_record") {
        // записать sale в БД из draft
        const draft = await loadKV(saleDraftKey(appPublicId, cashierTgId));
        if (!draft || !draft.customerTgId) {
          await tgAnswerCallbackQuery(botToken, cqId, "Черновик продажи не найден (истёк).", true);
          return new Response("OK", { status: 200 });
        }

        // запись sale
        const ins = await env.DB.prepare(
          `INSERT INTO sales (app_id, app_public_id, customer_tg_id, cashier_tg_id, amount_cents, cashback_coins, token, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))`
        ).bind(
          String(appId || ""),
          String(draft.appPublicId || appPublicId),
          String(draft.customerTgId || ""),
          String(cashierTgId),
          Number(draft.amount_cents || 0),
          Number(draft.cashbackCoins || 0),
          String(draft.token || "")
        ).run();

        const saleId = (ins as any)?.meta?.last_row_id ? String((ins as any).meta.last_row_id) : "";

        // удалить draft, оставить pend (чтобы pin_menu работал от sale_action)
        if (env.BOT_SECRETS) {
          await env.BOT_SECRETS.delete(saleDraftKey(appPublicId, cashierTgId)).catch(()=>{});
          await env.BOT_SECRETS.delete(salePendKey(appPublicId, cashierTgId)).catch(()=>{});
        }

        // сохранить action контекст на 1 час
        const actionPayload = {
          appPublicId: String(draft.appPublicId || appPublicId),
          saleId,
          customerTgId: String(draft.customerTgId || ""),
          cashbackCoins: Number(draft.cashbackCoins || 0),
          cashback_percent: Number(draft.cashback_percent || 0),
          amount_cents: Number(draft.amount_cents || 0),
        };
        if (env.BOT_SECRETS && saleId) {
          await env.BOT_SECRETS.put(saleActionKey(actionPayload.appPublicId, saleId, cashierTgId), JSON.stringify(actionPayload), { expirationTtl: 3600 });
        }

        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `✅ Продажа записана.\nСумма: ${(Number(actionPayload.amount_cents)/100).toFixed(2)}\nКэшбэк к выдаче: ${Number(actionPayload.cashbackCoins)} монет\nSale #${saleId}`,
          {
            reply_markup: {
              inline_keyboard: [
                [
                  { text: "✅ Подтвердить кэшбэк", callback_data: `sale_confirm:${saleId}` },
                  { text: "⛔️ Не выдавать", callback_data: `sale_decline:${saleId}` },
                ],
                [
                  { text: "🔑 Выдать PIN", callback_data: `pin_menu:${saleId}` },
                ],
                [
                  { text: "↩️ Отменить кэшбэк", callback_data: `sale_cancel:${saleId}` },
                ],
              ],
            },
          },
          { appPublicId: actionPayload.appPublicId, tgUserId: cashierTgId }
        );

        await tgAnswerCallbackQuery(botToken, cqId, "Записано ✅", false);
        return new Response("OK", { status: 200 });
      }

      // ---------- SALE CONFIRM CASHBACK ----------
      if (data.startsWith("sale_confirm:")) {
        const saleId = data.slice("sale_confirm:".length).trim();
        const act = await loadKV(saleActionKey(appPublicId, saleId, cashierTgId));

        if (!act || !act.customerTgId) {
          await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
          return new Response("OK", { status: 200 });
        }

        const actAppPublicId = String(act.appPublicId || appPublicId);
        const cashbackCoins = Math.max(0, Math.floor(Number(act.cashbackCoins || 0)));
        const cbp = Math.max(0, Math.min(100, Number(act.cashback_percent || 0)));

        if (act.customerTgId && cashbackCoins > 0) {
          await awardCoins(
            env.DB,
            appId,
            actAppPublicId,
            String(act.customerTgId),
            cashbackCoins,
            "sale_cashback_confirmed",
            String(act.saleId || saleId),
            `Кэшбэк ${cbp}% за покупку`,
            `sale_confirm:${actAppPublicId}:${String(act.saleId || saleId)}`
          );

          try {
            await tgSendMessage(
              env,
              botToken,
              String(act.customerTgId),
              `🎉 Кассир подтвердил кэшбэк!\nНачислено <b>${cashbackCoins}</b> монет ✅`,
              {},
              { appPublicId: actAppPublicId, tgUserId: String(act.customerTgId) }
            );
          } catch (_) {}
        }

        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `✅ Кэшбэк подтверждён.\nSale #${String(act.saleId || saleId)}\nКэшбэк: ${cashbackCoins} монет`,
          {},
          { appPublicId: actAppPublicId, tgUserId: cashierTgId }
        );

        await tgAnswerCallbackQuery(botToken, cqId, "Подтверждено ✅", false);
        return new Response("OK", { status: 200 });
      }

      if (data.startsWith("sale_decline:")) {
        const saleId = data.slice("sale_decline:".length).trim();
        const act = await loadKV(saleActionKey(appPublicId, saleId, cashierTgId));

        if (!act || !act.customerTgId) {
          await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
          return new Response("OK", { status: 200 });
        }

        const actAppPublicId = String(act.appPublicId || appPublicId);

        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `⛔️ Кэшбэк НЕ выдан (отменено).\nSale #${String(act.saleId || saleId)}.`,
          {},
          { appPublicId: actAppPublicId, tgUserId: cashierTgId }
        );

        try {
          await tgSendMessage(
            env,
            botToken,
            String(act.customerTgId),
            `ℹ️ Кэшбэк по покупке не подтверждён кассиром.`,
            {},
            { appPublicId: actAppPublicId, tgUserId: String(act.customerTgId) }
          );
        } catch (_) {}

        await tgAnswerCallbackQuery(botToken, cqId, "Ок", false);
        return new Response("OK", { status: 200 });
      }

      // ---------- REDEEM CONFIRM/DECLINE ----------
      if (data.startsWith("redeem_confirm:")) {
        const redeemCode = data.slice("redeem_confirm:".length).trim();
        const act = await loadKV(redeemActionKey(appPublicId, redeemCode, cashierTgId));

        if (!act || !act.redeemCode) {
          await tgAnswerCallbackQuery(botToken, cqId, "Контекст приза не найден (истёк).", true);
          return new Response("OK", { status: 200 });
        }

        // WHEEL
        if (act.kind === "wheel") {
          const r: any = await env.DB.prepare(
            `SELECT id, tg_id, prize_code, prize_title, status
             FROM wheel_redeems
             WHERE app_public_id=? AND redeem_code=?
             LIMIT 1`
          ).bind(appPublicId, redeemCode).first();

          if (!r) {
            await tgSendMessage(env, botToken, String(chatId), "⛔️ Код недействителен или приз не найден.", {}, { appPublicId, tgUserId: cashierTgId });
            return new Response("OK", { status: 200 });
          }
          if (String(r.status) === "redeemed") {
            await tgSendMessage(env, botToken, String(chatId), "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: cashierTgId });
            return new Response("OK", { status: 200 });
          }

          // coins по wheel_prizes
          let coins = 0;
          try {
            const pr: any = await env.DB.prepare(
              `SELECT coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`
            ).bind(appPublicId, String(r.prize_code || "")).first();
            coins = Math.max(0, Math.floor(Number(pr?.coins || 0)));
          } catch (_) {}

          if (coins > 0) {
            await awardCoins(
              env.DB,
              appId,
              appPublicId,
              String(r.tg_id),
              coins,
              "wheel_redeem_confirmed",
              String(redeemCode),
              String(r.prize_title || "Wheel prize"),
              `wheel:redeem:${appPublicId}:${String(r.tg_id)}:${String(r.id)}:${coins}`
            );
          }

          await env.DB.prepare(
            `UPDATE wheel_redeems
             SET status='redeemed', redeemed_at=datetime('now'), redeemed_by_tg=?
             WHERE id=? AND status='issued'`
          ).bind(String(cashierTgId), Number(r.id)).run();

          try {
            await env.DB.prepare(
              `UPDATE wheel_spins
               SET status='redeemed', ts_redeemed=datetime('now'), redeemed_by_tg=?
               WHERE app_public_id=? AND redeem_id=?`
            ).bind(String(cashierTgId), appPublicId, Number(r.id)).run();
          } catch (_) {}

          await tgSendMessage(
            env,
            botToken,
            String(chatId),
            `✅ Выдача подтверждена.\nКод: <code>${redeemCode}</code>\nПриз: <b>${String(r.prize_title || "")}</b>` +
              (coins > 0 ? `\n🪙 Начислено: <b>${coins}</b>` : ""),
            {},
            { appPublicId, tgUserId: cashierTgId }
          );

          try {
            await tgSendMessage(
              env,
              botToken,
              String(r.tg_id),
              `🎉 Кассир подтвердил выдачу!\n<b>${String(r.prize_title || "")}</b>` +
                (coins > 0 ? `\n🪙 Начислено <b>${coins}</b> монет.` : ""),
              {},
              { appPublicId, tgUserId: String(r.tg_id) }
            );
          } catch (_) {}

          await tgAnswerCallbackQuery(botToken, cqId, "Подтверждено ✅", false);
          return new Response("OK", { status: 200 });
        }

        // PASSPORT
        if (act.kind === "passport") {
          const pr: any = await env.DB.prepare(
            `SELECT id, tg_id, prize_code, prize_title, coins, status
             FROM passport_rewards
             WHERE app_public_id=? AND redeem_code=?
             ORDER BY id DESC
             LIMIT 1`
          ).bind(appPublicId, redeemCode).first();

          if (!pr) {
            await tgSendMessage(env, botToken, String(chatId), "⛔️ Код недействителен или приз не найден.", {}, { appPublicId, tgUserId: cashierTgId });
            return new Response("OK", { status: 200 });
          }

          if (String(pr.status) === "redeemed") {
            await tgSendMessage(env, botToken, String(chatId), "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: cashierTgId });
            return new Response("OK", { status: 200 });
          }

          const updRes = await env.DB.prepare(
            `UPDATE passport_rewards
             SET status='redeemed',
                 redeemed_at=datetime('now'),
                 redeemed_by_tg=?
             WHERE id=? AND status='issued'`
          ).bind(String(cashierTgId), Number(pr.id)).run();

          if (!updRes || !(updRes as any).meta || !(updRes as any).meta.changes) {
            await tgSendMessage(env, botToken, String(chatId), "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: cashierTgId });
            return new Response("OK", { status: 200 });
          }

          const coins = Math.max(0, Math.floor(Number(pr.coins || 0)));
          if (coins > 0) {
            await awardCoins(
              env.DB,
              appId,
              appPublicId,
              String(pr.tg_id),
              coins,
              "passport_complete_redeemed",
              String(pr.prize_code || ""),
              String(pr.prize_title || "Паспорт: приз"),
              `passport:redeem:${appPublicId}:${String(pr.tg_id)}:${String(pr.id)}:${coins}`
            );
          }

          // Сброс круга паспорта (статистика rewards остаётся!)
          try {
            await env.DB.prepare(`DELETE FROM styles_user WHERE app_public_id=? AND tg_id=?`)
              .bind(appPublicId, String(pr.tg_id))
              .run();
          } catch (_) {}

          // отметить в passport_bonus тоже (если есть строка)
          try {
            await env.DB.prepare(
              `UPDATE passport_bonus
               SET status='redeemed', redeemed_at=datetime('now'), redeemed_by_tg=?
               WHERE app_public_id=? AND redeem_code=? AND status='issued'`
            ).bind(String(cashierTgId), appPublicId, redeemCode).run();
          } catch (_) {}

          await tgSendMessage(
            env,
            botToken,
            String(chatId),
            `✅ Выдача подтверждена.\nКод: <code>${redeemCode}</code>\nПриз: <b>${String(pr.prize_title || "")}</b>` +
              (coins > 0 ? `\n🪙 Начислено: <b>${coins}</b>` : ""),
            {},
            { appPublicId, tgUserId: cashierTgId }
          );

          try {
            await tgSendMessage(
              env,
              botToken,
              String(pr.tg_id),
              `🎉 Кассир подтвердил выдачу!\n<b>${String(pr.prize_title || "")}</b>` +
                (coins > 0 ? `\n🪙 Начислено <b>${coins}</b> монет.` : ""),
              {},
              { appPublicId, tgUserId: String(pr.tg_id) }
            );
          } catch (_) {}

          await tgAnswerCallbackQuery(botToken, cqId, "Подтверждено ✅", false);
          return new Response("OK", { status: 200 });
        }

        await tgAnswerCallbackQuery(botToken, cqId, "Неизвестный тип приза", true);
        return new Response("OK", { status: 200 });
      }

      if (data.startsWith("redeem_decline:")) {
        const redeemCode = data.slice("redeem_decline:".length).trim();
        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `⛔️ Выдача отменена.\nКод: <code>${redeemCode}</code>`,
          {},
          { appPublicId, tgUserId: cashierTgId }
        );
        await tgAnswerCallbackQuery(botToken, cqId, "Отменено", false);
        return new Response("OK", { status: 200 });
      }

      // ---------- CANCEL CASHBACK (rollback) ----------
      if (data.startsWith("sale_cancel:")) {
        const saleId = data.slice("sale_cancel:".length).trim();
        const act = await loadKV(saleActionKey(appPublicId, saleId, cashierTgId));

        if (!act || !act.customerTgId) {
          await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
          return new Response("OK", { status: 200 });
        }

        if (Number(act.cashbackCoins) > 0) {
          await awardCoins(
            env.DB,
            appId,
            String(act.appPublicId || appPublicId),
            String(act.customerTgId),
            -Math.abs(Number(act.cashbackCoins)),
            "sale_cancel",
            String(act.saleId || saleId),
            "cancel cashback",
            `sale_cancel:${String(act.appPublicId || appPublicId)}:${String(act.saleId || saleId)}`
          );
        }

        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `↩️ Кэшбэк отменён. Sale #${String(act.saleId || saleId)}.`,
          {},
          { appPublicId: String(act.appPublicId || appPublicId), tgUserId: cashierTgId }
        );

        try {
          await tgSendMessage(
            env,
            botToken,
            String(act.customerTgId),
            `↩️ Кэшбэк по покупке отменён кассиром.`,
            {},
            { appPublicId: String(act.appPublicId || appPublicId), tgUserId: String(act.customerTgId) }
          );
        } catch (_) {}

        await tgAnswerCallbackQuery(botToken, cqId, "Готово ✅", false);
        return new Response("OK", { status: 200 });
      }

      // ---------- PIN MENU ----------
      if (data.startsWith("pin_menu:")) {
        const saleId = data.slice("pin_menu:".length).trim();
        const act = await loadKV(saleActionKey(appPublicId, saleId, cashierTgId));

        if (!act || !act.customerTgId) {
          await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
          return new Response("OK", { status: 200 });
        }

        const rows = await env.DB.prepare(
          `SELECT style_id, title
           FROM styles_dict
           WHERE app_public_id = ?
           ORDER BY id ASC`
        ).bind(String(act.appPublicId || appPublicId)).all();

        const items = rows && (rows as any).results ? (rows as any).results : [];
        if (!items.length) {
          await tgSendMessage(
            env,
            botToken,
            String(chatId),
            `Нет карточек в styles_dict — нечего выдавать.`,
            {},
            { appPublicId: String(act.appPublicId || appPublicId), tgUserId: cashierTgId }
          );
          await tgAnswerCallbackQuery(botToken, cqId, "Нет стилей", true);
          return new Response("OK", { status: 200 });
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
          { appPublicId: String(act.appPublicId || appPublicId), tgUserId: cashierTgId }
        );

        await tgAnswerCallbackQuery(botToken, cqId, "Выбери стиль", false);
        return new Response("OK", { status: 200 });
      }

      // ---------- PIN MAKE ----------
      if (data.startsWith("pin_make:")) {
        const rest = data.slice("pin_make:".length);
        const [saleIdRaw, styleIdRaw] = rest.split(":");
        const saleId = String(saleIdRaw || "").trim();
        const styleId = String(styleIdRaw || "").trim();

        const act = await loadKV(saleActionKey(appPublicId, saleId, cashierTgId));
        if (!act || !act.customerTgId) {
          await tgAnswerCallbackQuery(botToken, cqId, "Контекст продажи не найден (истёк).", true);
          return new Response("OK", { status: 200 });
        }
        if (!styleId) {
          await tgAnswerCallbackQuery(botToken, cqId, "Нет style_id", true);
          return new Response("OK", { status: 200 });
        }

        const actAppPublicId = String(act.appPublicId || appPublicId);

        let stTitle = "";
        try {
          const r = await env.DB.prepare(
            `SELECT title FROM styles_dict WHERE app_public_id=? AND style_id=? LIMIT 1`
          ).bind(actAppPublicId, styleId).first();
          stTitle = r ? String((r as any).title || "") : "";
        } catch (_) {}

        const pinRes = await issuePinToCustomer(env.DB, actAppPublicId, cashierTgId, String(act.customerTgId), styleId);
        if (!pinRes || !pinRes.ok) {
          await tgAnswerCallbackQuery(botToken, cqId, "Не удалось создать PIN (см. логи).", true);
          return new Response("OK", { status: 200 });
        }

        // сохранить контекст PIN (для отмены)
        try {
          if (env.BOT_SECRETS) {
            await env.BOT_SECRETS.put(
              pinActionKey(actAppPublicId, String(pinRes.pin), cashierTgId),
              JSON.stringify({ appPublicId: actAppPublicId, pin: String(pinRes.pin), customerTgId: String(act.customerTgId), styleId }),
              { expirationTtl: 3600 }
            );
          }
        } catch (_) {}

        try {
          await tgSendMessage(
            env,
            botToken,
            String(act.customerTgId),
            `🔑 Ваш PIN для отметки штампа${stTitle ? ` “${stTitle}”` : ""}:\n<code>${String(pinRes.pin)}</code>\n\n(одноразовый)`,
            {},
            { appPublicId: actAppPublicId, tgUserId: String(act.customerTgId) }
          );
        } catch (e) {
          logEvt("error", "pin.send_to_customer_failed", {
            err: errObj(e),
            appPublicId: actAppPublicId,
            customerTgId: String(act.customerTgId),
          });
        }

        // кассиру: подтверждение + кнопка отмены PIN
        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `✅ PIN отправлен клиенту ${String(act.customerTgId)} для ${stTitle ? `“${stTitle}”` : styleId}.\nPIN: <code>${String(pinRes.pin)}</code>`,
          {
            reply_markup: {
              inline_keyboard: [
                [{ text: "⛔️ Отменить PIN", callback_data: `pin_void:${String(pinRes.pin)}` }],
              ],
            },
          },
          { appPublicId: actAppPublicId, tgUserId: cashierTgId }
        );

        await tgAnswerCallbackQuery(botToken, cqId, "PIN отправлен ✅", false);
        return new Response("OK", { status: 200 });
      }

      // ---------- PIN VOID ----------
      if (data.startsWith("pin_void:")) {
        const pin = data.slice("pin_void:".length).trim();
        const act = await loadKV(pinActionKey(appPublicId, pin, cashierTgId));
        const actAppPublicId = String(act?.appPublicId || appPublicId);

        const res = await voidPin(env.DB, actAppPublicId, pin);

        if (!res.ok) {
          await tgAnswerCallbackQuery(botToken, cqId, "PIN не найден", true);
          return new Response("OK", { status: 200 });
        }

        await tgSendMessage(
          env,
          botToken,
          String(chatId),
          `⛔️ PIN отменён.\nPIN: <code>${pin}</code>`,
          {},
          { appPublicId: actAppPublicId, tgUserId: cashierTgId }
        );

        await tgAnswerCallbackQuery(botToken, cqId, "Отменено", false);
        return new Response("OK", { status: 200 });
      }

      await tgAnswerCallbackQuery(botToken, cqId, "Неизвестное действие", false);
      return new Response("OK", { status: 200 });
    }

    // ================= SYNC + LOG IN =================
    await upsertAppUserFromBot(env.DB, {
      appId,
      appPublicId,
      tgUserId: from.id,
      tgUsername: from.username || null,
    });

    await logBotMessage(env.DB, {
      appPublicId,
      tgUserId: from.id,
      direction: "in",
      msgType: pickMsgType(upd),
      text:
        (upd.message && upd.message.text) ||
        (upd.edited_message && upd.edited_message.text) ||
        (upd.callback_query && upd.callback_query.data) ||
        null,
      chatId,
      tgMessageId: msg && msg.message_id ? msg.message_id : null,
      payload: { update: upd },
    });

    const text =
      (upd.message && upd.message.text) ||
      (upd.edited_message && upd.edited_message.text) ||
      (upd.callback_query && upd.callback_query.data) ||
      "";
    const t = String(text || "").trim();

    // ================= /start =================
    if (t === "/start" || t.startsWith("/start ")) {
      const payload = t.startsWith("/start ") ? t.slice(7).trim() : "";

      // ----- redeem flow -----
      if (payload.startsWith("redeem_")) {
        const redeemCode = payload.slice(7).trim();

        const ss = await getSalesSettings(env.DB, appPublicId);
        const isCashier = ss.cashiers.includes(String(from.id));
        if (!isCashier) {
          await tgSendMessage(env, botToken, chatId, "⛔️ Вы не зарегистрированы как кассир для этого проекта.", {}, { appPublicId, tgUserId: from.id });
          return new Response("OK", { status: 200 });
        }

        const r: any = await env.DB.prepare(
          `SELECT id, tg_id, prize_code, prize_title, status
           FROM wheel_redeems
           WHERE app_public_id = ? AND redeem_code = ?
           LIMIT 1`
        ).bind(appPublicId, redeemCode).first();

        if (!r) {
          const pr: any = await env.DB.prepare(
            `SELECT id, tg_id, prize_code, prize_title, coins, passport_key, status
             FROM passport_rewards
             WHERE app_public_id = ? AND redeem_code = ?
             ORDER BY id DESC
             LIMIT 1`
          ).bind(appPublicId, redeemCode).first();

          if (!pr) {
            await tgSendMessage(env, botToken, chatId, "⛔️ Код недействителен или приз не найден.", {}, { appPublicId, tgUserId: from.id });
            return new Response("OK", { status: 200 });
          }
          if (String(pr.status) === "redeemed") {
            await tgSendMessage(env, botToken, chatId, "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: from.id });
            return new Response("OK", { status: 200 });
          }

          const coins = Math.max(0, Math.floor(Number(pr.coins || 0)));

          try {
            if (env.BOT_SECRETS) {
              await env.BOT_SECRETS.put(
                redeemActionKey(appPublicId, redeemCode, String(from.id)),
                JSON.stringify({ kind: "passport", redeemCode }),
                { expirationTtl: 3600 }
              );
            }
          } catch (_) {}

          await tgSendMessage(
            env,
            botToken,
            chatId,
            `❓ Подтвердить выдачу приза по паспорту?\nКод: <code>${redeemCode}</code>\nПриз: <b>${String(pr.prize_title || "")}</b>` +
              (coins > 0 ? `\n🪙 Монеты: <b>${coins}</b>` : ""),
            {
              reply_markup: {
                inline_keyboard: [[
                  { text: "✅ Да, выдать", callback_data: `redeem_confirm:${redeemCode}` },
                  { text: "⛔️ Нет", callback_data: `redeem_decline:${redeemCode}` },
                ]],
              },
            },
            { appPublicId, tgUserId: from.id }
          );

          return new Response("OK", { status: 200 });
        }

        // wheel redeem -> ask confirm
        if (String(r.status) === "redeemed") {
          await tgSendMessage(env, botToken, chatId, "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: from.id });
          return new Response("OK", { status: 200 });
        }

        let coins = 0;
        try {
          const pr2: any = await env.DB.prepare(
            `SELECT coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`
          ).bind(appPublicId, String(r.prize_code || "")).first();
          coins = Math.max(0, Math.floor(Number(pr2?.coins || 0)));
        } catch (_) {}

        try {
          if (env.BOT_SECRETS) {
            await env.BOT_SECRETS.put(
              redeemActionKey(appPublicId, redeemCode, String(from.id)),
              JSON.stringify({ kind: "wheel", redeemCode }),
              { expirationTtl: 3600 }
            );
          }
        } catch (_) {}

        await tgSendMessage(
          env,
          botToken,
          chatId,
          `❓ Подтвердить выдачу приза?\nКод: <code>${redeemCode}</code>\nПриз: <b>${String(r.prize_title || "")}</b>` +
            (coins > 0 ? `\n🪙 Монеты: <b>${coins}</b>` : ""),
          {
            reply_markup: {
              inline_keyboard: [[
                { text: "✅ Да, выдать", callback_data: `redeem_confirm:${redeemCode}` },
                { text: "⛔️ Нет", callback_data: `redeem_decline:${redeemCode}` },
              ]],
            },
          },
          { appPublicId, tgUserId: from.id }
        );

        return new Response("OK", { status: 200 });
      }

      // ----- sale flow -----
      if (payload.startsWith("sale_")) {
        const token = payload.slice(5).trim();

        const rawTok = env.BOT_SECRETS ? await env.BOT_SECRETS.get(saleTokKey(token)) : null;
        if (!rawTok) {
          await tgSendMessage(env, botToken, chatId, "⛔️ Этот QR устарел. Попросите клиента обновить QR.", {}, { appPublicId, tgUserId: from.id });
          return new Response("OK", { status: 200 });
        }

        let tokObj: any = null;
        try { tokObj = JSON.parse(rawTok); } catch (_) {}

        const customerTgId = tokObj && tokObj.customerTgId ? String(tokObj.customerTgId) : "";
        const tokenAppPublicId = tokObj && tokObj.appPublicId ? String(tokObj.appPublicId) : appPublicId;

        const ss = await getSalesSettings(env.DB, tokenAppPublicId);
        const isCashier = ss.cashiers.includes(String(from.id));
        if (!isCashier) {
          await tgSendMessage(env, botToken, chatId, "⛔️ Вы не зарегистрированы как кассир для этого проекта.", {}, { appPublicId, tgUserId: from.id });
          return new Response("OK", { status: 200 });
        }

        const pend = {
          appPublicId: tokenAppPublicId,
          customerTgId,
          token,
          cashback_percent: ss.cashback_percent,
        };

        if (env.BOT_SECRETS) {
          await env.BOT_SECRETS.put(salePendKey(tokenAppPublicId, String(from.id)), JSON.stringify(pend), { expirationTtl: 600 });
          try { await env.BOT_SECRETS.delete(saleTokKey(token)); } catch (_) {}
        }

        await tgSendMessage(
          env,
          botToken,
          chatId,
          `✅ Клиент: ${customerTgId}\nВведите сумму покупки (например 350 или 350.50):`,
          {},
          { appPublicId: tokenAppPublicId, tgUserId: from.id }
        );

        return new Response("OK", { status: 200 });
      }

      await tgSendMessage(env, botToken, chatId, "Привет! Я бот этого мини-аппа ✅\nКоманда: /profile", {}, { appPublicId, tgUserId: from.id });
      return new Response("OK", { status: 200 });
    }

    // ================= AMOUNT STEP: draft + ask confirm =================
    try {
      // сначала ищем pend по appPublicId текущего вебхука
      const pendRaw = env.BOT_SECRETS ? await env.BOT_SECRETS.get(salePendKey(appPublicId, String(from.id))) : null;

      if (pendRaw) {
        let pend: any = null;
        try { pend = JSON.parse(pendRaw); } catch (_) {}

        const cents = parseAmountToCents(t);
        if (cents == null) {
          await tgSendMessage(env, botToken, chatId, "Введите сумму числом (например 350 или 350.50)", {}, { appPublicId, tgUserId: from.id });
          return new Response("OK", { status: 200 });
        }

        const cbp = Math.max(0, Math.min(100, Number(pend?.cashback_percent ?? 10)));
        const cashbackCoins = Math.max(0, Math.floor((cents / 100) * (cbp / 100)));

        const draft = {
          appPublicId: String(pend.appPublicId || appPublicId),
          customerTgId: String(pend.customerTgId || ""),
          token: String(pend.token || ""),
          amount_cents: Number(cents),
          cashbackCoins: Number(cashbackCoins),
          cashback_percent: Number(cbp),
        };

        if (env.BOT_SECRETS) {
          await env.BOT_SECRETS.put(saleDraftKey(String(pend.appPublicId || appPublicId), String(from.id)), JSON.stringify(draft), { expirationTtl: 600 });
        }

        await tgSendMessage(
          env,
          botToken,
          chatId,
          `❓ Записать продажу?\nСумма: <b>${(cents/100).toFixed(2)}</b>\nКэшбэк к выдаче: <b>${cashbackCoins}</b> монет\nКлиент: <code>${String(pend.customerTgId || "")}</code>`,
          {
            reply_markup: {
              inline_keyboard: [
                [
                  { text: "✅ Да, записать", callback_data: "sale_record" },
                  { text: "✏️ Ввести заново", callback_data: "sale_reenter" },
                ],
                [
                  { text: "⛔️ Отменить", callback_data: "sale_drop" },
                ],
              ],
            },
          },
          { appPublicId: String(pend.appPublicId || appPublicId), tgUserId: from.id }
        );

        return new Response("OK", { status: 200 });
      }
    } catch (e) {
      logEvt("error", "sale.amount_step_failed", { err: errObj(e), appPublicId, fromId });
    }

    // ================= /profile =================
    if (t === "/profile") {
      try {
        const appObj = await env.APPS.get("app:" + ctx.appId, "json").catch(() => null);
        const cfg = (appObj && ((appObj as any).app_config ?? (appObj as any).runtime_config ?? {})) || {};

        const state = await buildStateLite(env.DB, ctx.appId, appPublicId, String(from.id), cfg || {});
        const lines = [
          `👤 ${from.username ? "@" + from.username : (from.first_name || "Пользователь")}`,
          `🪙 Монеты: ${Number(state.coins || 0)}`,
          `🎨 Стили: ${Number(state.styles_count || 0)}/${Number(state.styles_total || 0)}`,
          `🎮 Лучший сегодня: ${Number(state.game_today_best || 0)}`,
          `🎟 Рефералы: ${Number(state.ref_total || 0)}`,
        ];

        await tgSendMessage(env, botToken, chatId, lines.join("\n"), {}, { appPublicId, tgUserId: from.id });
      } catch (e) {
        logEvt("error", "tg.profile_failed", { err: errObj(e), appPublicId, fromId });
        await tgSendMessage(env, botToken, chatId, "Ошибка при получении профиля 😕", {}, { appPublicId, tgUserId: from.id });
      }

      return new Response("OK", { status: 200 });
    }

    // default
    await tgSendMessage(env, botToken, chatId, "Принял ✅\nКоманда: /profile", {}, { appPublicId, tgUserId: from.id });
    return new Response("OK", { status: 200 });
  } catch (e: any) {
    logEvt("error", "tg.webhook_fatal", {
      publicId,
      updateId,
      fromId,
      chatId,
      err: errObj(e),
      updType: pickUpdateType(upd),
      upd: upd ? safeJson(upd, 8000) : null,
    });
    return new Response("OK", { status: 200 });
  }
}
