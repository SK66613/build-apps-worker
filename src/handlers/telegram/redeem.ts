// src/handlers/telegram/redeem.ts
import type { Env } from "../../index";
import { tgSendMessage } from "../../services/telegramSend";
import { awardCoins } from "../../services/coinsLedger";

function logRedeemEvent(event: {
  code: string;
  msg: string;
  appPublicId: string;
  tgUserId: string;
  route: string;
  extra?: Record<string, any>;
}) {
  try {
    console.log(JSON.stringify(event));
  } catch (_) {}
}

function safeStr(v: any) {
  return String(v ?? "").trim();
}
function escHtml(s: string) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

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

// ===== sales_settings -> cashiers (как в монолите) =====
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
    .map((x: any) => (x ? String(x).trim() : ""))
    .filter(Boolean);

  return { cashiers };
}

// KV key (как в монолите)
function redeemActionKey(appPublicId: string, redeemCode: string, cashierTgId: string) {
  return `redeem_action:${String(appPublicId)}:${String(redeemCode)}:${String(cashierTgId)}`;
}

async function kvGetJson(env: Env, key: string) {
  const raw = (env as any).BOT_SECRETS ? await (env as any).BOT_SECRETS.get(key) : null;
  if (!raw) return null;
  try { return JSON.parse(raw); } catch (_) { return null; }
}
async function kvPutJson(env: Env, key: string, obj: any, ttlSec: number) {
  if (!(env as any).BOT_SECRETS) return;
  await (env as any).BOT_SECRETS.put(key, JSON.stringify(obj ?? {}), { expirationTtl: Number(ttlSec || 3600) }).catch(() => {});
}

export async function handleRedeem(args: {
  env: Env;
  db: any;
  ctx: { appId: any; publicId: string }; // appPublicId
  botToken: string;
  upd: any;
}): Promise<boolean> {
  const { env, db, ctx, botToken, upd } = args;
  const appPublicId = String(ctx.publicId || "");
  const appId = ctx.appId;

  // ================= callback confirm/decline =================
  const cbId = safeStr(upd?.callback_query?.id);
  const data = safeStr(upd?.callback_query?.data);
  const chatId = String(upd?.callback_query?.message?.chat?.id || upd?.callback_query?.from?.id || "");
  const from = upd?.callback_query?.from;
  const cashierTgId = String(from?.id || "");

  if (cbId && (data.startsWith("redeem_confirm:") || data.startsWith("redeem_decline:"))) {
    const route = "wheel.cashier.confirm";
    const redeemCode = data.split(":").slice(1).join(":").trim();
    if (!redeemCode) {
      logRedeemEvent({
        code: "mini.wheel.cashier.confirm.fail.reward_not_found",
        msg: "Empty redeem code",
        appPublicId,
        tgUserId: cashierTgId,
        route,
      });
      await tgAnswerCallbackQuery(botToken, cbId, "Неверный код", true);
      return true;
    }

    // cashier check (как в /start в оригинале)
    const ss = await getSalesSettings(db, appPublicId).catch(() => ({ cashiers: [] }));
    if (!ss?.cashiers?.includes(String(cashierTgId))) {
      logRedeemEvent({
        code: "mini.wheel.cashier.confirm.fail.unauthorized",
        msg: "Cashier is not authorized",
        appPublicId,
        tgUserId: cashierTgId,
        route,
      });
      await tgSendMessage(env, botToken, chatId, "⛔️ Вы не зарегистрированы как кассир для этого проекта.", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
      await tgAnswerCallbackQuery(botToken, cbId, "Нет прав", true);
      return true;
    }

    // decline (как в оригинале: просто сообщение)
    if (data.startsWith("redeem_decline:")) {
      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `⛔️ Выдача отменена.\nКод: <code>${escHtml(redeemCode)}</code>`,
        {},
        { appPublicId, tgUserId: cashierTgId }
      ).catch(() => null);

      await tgAnswerCallbackQuery(botToken, cbId, "Отменено", false);
      return true;
    }

    // confirm: act из KV (как в оригинале)
    const act = await kvGetJson(env, redeemActionKey(appPublicId, redeemCode, cashierTgId));
    if (!act || !act.redeemCode) {
      logRedeemEvent({
        code: "mini.wheel.cashier.confirm.fail.reward_not_found",
        msg: "Redeem action context is missing",
        appPublicId,
        tgUserId: cashierTgId,
        route,
        extra: { redeemCode },
      });
      await tgAnswerCallbackQuery(botToken, cbId, "Контекст приза не найден (истёк).", true);
      return true;
    }

    // ---------- WHEEL ----------
    if (act.kind === "wheel") {
      let r: any;
      try {
        r = await db.prepare(
          `SELECT id, tg_id, prize_code, prize_title, status
           FROM wheel_redeems
           WHERE app_public_id=? AND redeem_code=?
           LIMIT 1`
        ).bind(appPublicId, redeemCode).first();
      } catch (e: any) {
        logRedeemEvent({
          code: "mini.wheel.cashier.confirm.fail.db_error",
          msg: "Failed to fetch wheel reward",
          appPublicId,
          tgUserId: cashierTgId,
          route,
          extra: { redeemCode, error: String(e?.message || e) },
        });
        await tgSendMessage(env, botToken, String(chatId), "⛔️ Ошибка БД, попробуйте ещё раз.", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
        return true;
      }

      if (!r) {
        logRedeemEvent({
          code: "mini.wheel.cashier.confirm.fail.reward_not_found",
          msg: "Reward not found by redeem_code",
          appPublicId,
          tgUserId: cashierTgId,
          route,
          extra: { redeemCode },
        });
        await tgSendMessage(env, botToken, String(chatId), "⛔️ Код недействителен или приз не найден.", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
        return true;
      }
      if (String(r.status) === "redeemed") {
        logRedeemEvent({
          code: "mini.wheel.cashier.confirm.fail.reward_not_found",
          msg: "Reward already redeemed",
          appPublicId,
          tgUserId: cashierTgId,
          route,
          extra: { redeemCode, rewardId: Number(r.id) },
        });
        await tgSendMessage(env, botToken, String(chatId), "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
        return true;
      }

      // coins по wheel_prizes (как в оригинале)
      let coins = 0;
      try {
        const pr: any = await db.prepare(
          `SELECT coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`
        ).bind(appPublicId, String(r.prize_code || "")).first();
        coins = Math.max(0, Math.floor(Number(pr?.coins || 0)));
      } catch (_) {}

      let updRes: any;
      try {
        updRes = await db.prepare(
          `UPDATE wheel_redeems
           SET status='redeemed', redeemed_at=datetime('now'), redeemed_by_tg=?
           WHERE id=? AND status='issued'`
        ).bind(String(cashierTgId), Number(r.id)).run();
      } catch (e: any) {
        logRedeemEvent({
          code: "mini.wheel.cashier.confirm.fail.db_error",
          msg: "Failed to update wheel reward",
          appPublicId,
          tgUserId: cashierTgId,
          route,
          extra: { redeemCode, rewardId: Number(r.id), error: String(e?.message || e) },
        });
        await tgSendMessage(env, botToken, String(chatId), "⛔️ Ошибка БД, попробуйте ещё раз.", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
        return true;
      }

      if (!updRes || !(updRes as any).meta || !(updRes as any).meta.changes) {
        logRedeemEvent({
          code: "mini.wheel.cashier.confirm.fail.reward_not_found",
          msg: "Reward status is not issued",
          appPublicId,
          tgUserId: cashierTgId,
          route,
          extra: { redeemCode, rewardId: Number(r.id) },
        });
        await tgSendMessage(env, botToken, String(chatId), "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
        return true;
      }

      if (coins > 0) {
        await awardCoins(
          db,
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

      // как в оригинале: wheel_spins по redeem_id (если у тебя так)
      try{
        await db.prepare(
          `UPDATE wheel_spins
           SET status='redeemed', ts_redeemed=datetime('now'), redeemed_by_tg=?
           WHERE app_public_id=? AND redeem_id=?`
        ).bind(String(cashierTgId), appPublicId, Number(r.id)).run();
      } catch (_) {}

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `✅ Выдача подтверждена.\nКод: <code>${escHtml(redeemCode)}</code>\nПриз: <b>${escHtml(String(r.prize_title || ""))}</b>` +
          (coins > 0 ? `\n🪙 Начислено: <b>${coins}</b>` : ""),
        {},
        { appPublicId, tgUserId: cashierTgId }
      ).catch(() => null);

      try {
        await tgSendMessage(
          env,
          botToken,
          String(r.tg_id),
          `🎉 Кассир подтвердил выдачу!\n<b>${escHtml(String(r.prize_title || ""))}</b>` +
            (coins > 0 ? `\n🪙 Начислено <b>${coins}</b> монет.` : ""),
          {},
          { appPublicId, tgUserId: String(r.tg_id) }
        );
      } catch (_) {}

      logRedeemEvent({
        code: "mini.wheel.cashier.confirm.ok",
        msg: "Cashier confirmed wheel reward",
        appPublicId,
        tgUserId: cashierTgId,
        route,
        extra: { redeemCode, rewardId: Number(r.id), coins },
      });

      await tgAnswerCallbackQuery(botToken, cbId, "Подтверждено ✅", false);
      return true;
    }

    // ---------- PASSPORT ----------
    if (act.kind === "passport") {
      const pr: any = await db.prepare(
        `SELECT id, tg_id, prize_code, prize_title, coins, status
         FROM passport_rewards
         WHERE app_public_id=? AND redeem_code=?
         ORDER BY id DESC
         LIMIT 1`
      ).bind(appPublicId, redeemCode).first();

      if (!pr) {
        await tgSendMessage(env, botToken, String(chatId), "⛔️ Код недействителен или приз не найден.", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
        return true;
      }
      if (String(pr.status) === "redeemed") {
        await tgSendMessage(env, botToken, String(chatId), "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
        return true;
      }

      const updRes = await db.prepare(
        `UPDATE passport_rewards
         SET status='redeemed',
             redeemed_at=datetime('now'),
             redeemed_by_tg=?
         WHERE id=? AND status='issued'`
      ).bind(String(cashierTgId), Number(pr.id)).run();

      if (!updRes || !(updRes as any).meta || !(updRes as any).meta.changes) {
        await tgSendMessage(env, botToken, String(chatId), "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: cashierTgId }).catch(() => null);
        return true;
      }

      const coins = Math.max(0, Math.floor(Number(pr.coins || 0)));
      if (coins > 0) {
        await awardCoins(
          db,
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

      // как в оригинале: сброс круга паспорта
      try {
        await db.prepare(`DELETE FROM styles_user WHERE app_public_id=? AND tg_id=?`)
          .bind(appPublicId, String(pr.tg_id))
          .run();
      } catch (_) {}

      // как в оригинале: если есть passport_bonus — тоже помечаем
      try {
        await db.prepare(
          `UPDATE passport_bonus
           SET status='redeemed', redeemed_at=datetime('now'), redeemed_by_tg=?
           WHERE app_public_id=? AND redeem_code=? AND status='issued'`
        ).bind(String(cashierTgId), appPublicId, redeemCode).run();
      } catch (_) {}

      await tgSendMessage(
        env,
        botToken,
        String(chatId),
        `✅ Выдача подтверждена.\nКод: <code>${escHtml(redeemCode)}</code>\nПриз: <b>${escHtml(String(pr.prize_title || ""))}</b>` +
          (coins > 0 ? `\n🪙 Начислено: <b>${coins}</b>` : ""),
        {},
        { appPublicId, tgUserId: cashierTgId }
      ).catch(() => null);

      try {
        await tgSendMessage(
          env,
          botToken,
          String(pr.tg_id),
          `🎉 Кассир подтвердил выдачу!\n<b>${escHtml(String(pr.prize_title || ""))}</b>` +
            (coins > 0 ? `\n🪙 Начислено <b>${coins}</b> монет.` : ""),
          {},
          { appPublicId, tgUserId: String(pr.tg_id) }
        );
      } catch (_) {}

      await tgAnswerCallbackQuery(botToken, cbId, "Подтверждено ✅", false);
      return true;
    }

    await tgAnswerCallbackQuery(botToken, cbId, "Неизвестный тип приза", true);
    return true;
  }

  // ================= /start redeem_<CODE> =================
  const text = safeStr(upd?.message?.text || "");
  const msgChatId = String(upd?.message?.chat?.id || upd?.message?.from?.id || "");
  const from2 = upd?.message?.from;
  const from2Id = String(from2?.id || "");

  if (text.startsWith("/start") && text.includes("redeem_")) {
    const m = text.match(/redeem_([A-Z0-9\-]+)/i);
    const redeemCode = m ? String(m[1]) : "";
    if (!redeemCode) return false;

    // cashier check (как в оригинале)
    const ss = await getSalesSettings(db, appPublicId).catch(() => ({ cashiers: [] }));
    const isCashier = ss.cashiers.includes(String(from2Id));
    if (!isCashier) {
      await tgSendMessage(env, botToken, msgChatId, "⛔️ Вы не зарегистрированы как кассир для этого проекта.", {}, { appPublicId, tgUserId: from2Id }).catch(() => null);
      return true;
    }

    // определяем kind как в оригинале: сначала wheel_redeems, иначе passport_rewards
    const r: any = await db.prepare(
      `SELECT id, tg_id, prize_code, prize_title, status
       FROM wheel_redeems
       WHERE app_public_id = ? AND redeem_code = ?
       LIMIT 1`
    ).bind(appPublicId, redeemCode).first();

    if (!r) {
      const pr: any = await db.prepare(
        `SELECT id, tg_id, prize_code, prize_title, coins, status
         FROM passport_rewards
         WHERE app_public_id = ? AND redeem_code = ?
         ORDER BY id DESC
         LIMIT 1`
      ).bind(appPublicId, redeemCode).first();

      if (!pr) {
        await tgSendMessage(env, botToken, msgChatId, "⛔️ Код недействителен или приз не найден.", {}, { appPublicId, tgUserId: from2Id }).catch(() => null);
        return true;
      }
      if (String(pr.status) === "redeemed") {
        await tgSendMessage(env, botToken, msgChatId, "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: from2Id }).catch(() => null);
        return true;
      }

      // KV act (passport)
      await kvPutJson(
        env,
        redeemActionKey(appPublicId, redeemCode, String(from2Id)),
        { kind: "passport", redeemCode },
        3600
      );

      const coins = Math.max(0, Math.floor(Number(pr.coins || 0)));

      await tgSendMessage(
        env,
        botToken,
        msgChatId,
        `❓ Подтвердить выдачу приза по паспорту?\nКод: <code>${escHtml(redeemCode)}</code>\nПриз: <b>${escHtml(String(pr.prize_title || ""))}</b>` +
          (coins > 0 ? `\n🪙 Монеты: <b>${coins}</b>` : ""),
        {
          reply_markup: {
            inline_keyboard: [[
              { text: "✅ Да, выдать", callback_data: `redeem_confirm:${redeemCode}` },
              { text: "⛔️ Нет", callback_data: `redeem_decline:${redeemCode}` },
            ]],
          },
        },
        { appPublicId, tgUserId: from2Id }
      ).catch(() => null);

      return true;
    }

    // wheel
    if (String(r.status) === "redeemed") {
      await tgSendMessage(env, botToken, msgChatId, "ℹ️ Этот приз уже отмечен как полученный.", {}, { appPublicId, tgUserId: from2Id }).catch(() => null);
      return true;
    }

    let coins = 0;
    try {
      const pr2: any = await db.prepare(
        `SELECT coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`
      ).bind(appPublicId, String(r.prize_code || "")).first();
      coins = Math.max(0, Math.floor(Number(pr2?.coins || 0)));
    } catch (_) {}

    // KV act (wheel)
    await kvPutJson(
      env,
      redeemActionKey(appPublicId, redeemCode, String(from2Id)),
      { kind: "wheel", redeemCode },
      3600
    );

    await tgSendMessage(
      env,
      botToken,
      msgChatId,
      `❓ Подтвердить выдачу приза?\nКод: <code>${escHtml(redeemCode)}</code>\nПриз: <b>${escHtml(String(r.prize_title || ""))}</b>` +
        (coins > 0 ? `\n🪙 Монеты: <b>${coins}</b>` : ""),
      {
        reply_markup: {
          inline_keyboard: [[
            { text: "✅ Да, выдать", callback_data: `redeem_confirm:${redeemCode}` },
            { text: "⛔️ Нет", callback_data: `redeem_decline:${redeemCode}` },
          ]],
        },
      },
      { appPublicId, tgUserId: from2Id }
    ).catch(() => null);

    return true;
  }

  return false;
}
