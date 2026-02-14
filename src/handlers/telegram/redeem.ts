// src/handlers/telegram/redeem.ts
import type { Env } from "../../index";
import { tgSendMessage } from "../../services/telegramSend";
import { awardCoins } from "../../services/coinsLedger";

function safeStr(v: any) {
  return String(v ?? "").trim();
}
function escHtml(s: string) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
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

async function notifyUser(env: Env, botToken: string, userTgId: string, txt: string, publicId: string) {
  if (!userTgId) return;
  try {
    await tgSendMessage(
      env,
      botToken,
      String(userTgId),
      txt,
      {},
      { appPublicId: publicId, tgUserId: String(userTgId) }
    );
  } catch (_) {}
}

// обработка:
// - callback_data: redeem_confirm:<CODE> / redeem_decline:<CODE>
// - /start redeem_<CODE>
export async function handleRedeem(args: {
  env: Env;
  db: any;
  ctx: { appId: any; publicId: string };
  botToken: string;
  upd: any;
}): Promise<boolean> {
  const { env, db, ctx, botToken, upd } = args;

  const cbId = safeStr(upd?.callback_query?.id);
  const cb = safeStr(upd?.callback_query?.data);
  const chatId = String(upd?.callback_query?.message?.chat?.id || upd?.callback_query?.from?.id || "");
  const from = upd?.callback_query?.from;

  // ===== callback confirm/decline
  if (cbId && (cb.startsWith("redeem_confirm:") || cb.startsWith("redeem_decline:"))) {
    const action = cb.startsWith("redeem_confirm:") ? "confirm" : "decline";
    const redeemCode = cb.split(":").slice(1).join(":").trim();

    await tgAnswerCallbackQuery(env, botToken, cbId, action === "confirm" ? "Подтверждаю…" : "Отменяю…").catch(() => null);

    if (!redeemCode) {
      await tgSendMessage(env, botToken, chatId, "❌ Неверный код.", {}, { appPublicId: ctx.publicId, tgUserId: String(from?.id || "") }).catch(() => null);
      return true;
    }

    // 1) wheel redeem?
    const wr: any = await db
      .prepare(
        `SELECT id, tg_id, spin_id, prize_code, prize_title, status
         FROM wheel_redeems
         WHERE app_public_id=? AND redeem_code=? LIMIT 1`
      )
      .bind(ctx.publicId, redeemCode)
      .first();

    if (wr) {
      const userTgId = String(wr.tg_id || "");

      if (action === "decline") {
        await db
          .prepare(
            `UPDATE wheel_redeems
             SET status='declined', declined_at=datetime('now'), declined_by_tg=?
             WHERE id=? AND status='issued'`
          )
          .bind(String(from?.id || ""), Number(wr.id))
          .run();

        await tgSendMessage(env, botToken, chatId, "🚫 Выдача отменена.", {}, { appPublicId: ctx.publicId, tgUserId: String(from?.id || "") }).catch(() => null);

        // ✅ уведомляем пользователя
        await notifyUser(
          env,
          botToken,
          userTgId,
          `🚫 Кассир отменил выдачу приза.\nКод: <code>${escHtml(redeemCode)}</code>`,
          ctx.publicId
        );

        return true;
      }

      // confirm
      const updRes = await db
        .prepare(
          `UPDATE wheel_redeems
           SET status='redeemed', redeemed_at=datetime('now'), redeemed_by_tg=?
           WHERE id=? AND status='issued'`
        )
        .bind(String(from?.id || ""), Number(wr.id))
        .run();

      if (!updRes?.meta?.changes) {
        await tgSendMessage(env, botToken, chatId, "ℹ️ Уже обработано или статус не issued.", {}, { appPublicId: ctx.publicId, tgUserId: String(from?.id || "") }).catch(() => null);
        return true;
      }

      // начисление монет по wheel_prizes (истина)
      let coins = 0;
      try {
        const pr: any = await db
          .prepare(`SELECT coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`)
          .bind(ctx.publicId, String(wr.prize_code || ""))
          .first();
        coins = Math.max(0, Math.floor(Number(pr?.coins || 0)));
      } catch (_) {
        coins = 0;
      }

      if (coins > 0) {
        await awardCoins(
          db,
          ctx.appId,
          ctx.publicId,
          String(wr.tg_id),
          coins,
          "wheel_redeem_confirm",
          String(wr.prize_code || ""),
          String(wr.prize_title || ""),
          `wheel:redeem:${ctx.publicId}:${wr.tg_id}:${wr.spin_id}:${redeemCode}:${coins}`
        );
      }

      // wheel_spins -> redeemed
      await db
        .prepare(
          `UPDATE wheel_spins
           SET status='redeemed', ts_redeemed=datetime('now')
           WHERE app_public_id=? AND id=?`
        )
        .bind(ctx.publicId, Number(wr.spin_id))
        .run()
        .catch(() => null);

      // ✅ сообщение кассиру
      await tgSendMessage(
        env,
        botToken,
        chatId,
        `✅ Подтверждено.\n${coins > 0 ? `🪙 Начислено монет: <b>${coins}</b>` : ""}`,
        {},
        { appPublicId: ctx.publicId, tgUserId: String(from?.id || "") }
      ).catch(() => null);

      // ✅ сообщение пользователю (ЭТО И БЫЛО ПОТЕРЯНО)
      await notifyUser(
        env,
        botToken,
        userTgId,
        `✅ Кассир подтвердил выдачу приза.\n🎁 Приз: <b>${escHtml(String(wr.prize_title || wr.prize_code || ""))}</b>\n` +
          (coins > 0 ? `🪙 Начислено монет: <b>${coins}</b>` : ""),
        ctx.publicId
      );

      return true;
    }

    // 2) passport reward?
    const prw: any = await db
      .prepare(
        `SELECT id, tg_id, prize_code, prize_title, coins, status
         FROM passport_rewards
         WHERE app_public_id=? AND redeem_code=? LIMIT 1`
      )
      .bind(ctx.publicId, redeemCode)
      .first();

    if (!prw) {
      await tgSendMessage(env, botToken, chatId, "❌ Код не найден.", {}, { appPublicId: ctx.publicId, tgUserId: String(from?.id || "") }).catch(() => null);
      return true;
    }

    const userTgId = String(prw.tg_id || "");

    if (action === "decline") {
      await db
        .prepare(
          `UPDATE passport_rewards
           SET status='declined', declined_at=datetime('now'), declined_by_tg=?
           WHERE id=? AND status='issued'`
        )
        .bind(String(from?.id || ""), Number(prw.id))
        .run();

      await tgSendMessage(env, botToken, chatId, "🚫 Выдача отменена.", {}, { appPublicId: ctx.publicId, tgUserId: String(from?.id || "") }).catch(() => null);

      // ✅ уведомляем пользователя
      await notifyUser(
        env,
        botToken,
        userTgId,
        `🚫 Кассир отменил выдачу приза паспорта.\nКод: <code>${escHtml(redeemCode)}</code>`,
        ctx.publicId
      );

      return true;
    }

    // confirm passport
    const upd2 = await db
      .prepare(
        `UPDATE passport_rewards
         SET status='redeemed', redeemed_at=datetime('now'), redeemed_by_tg=?
         WHERE id=? AND status='issued'`
      )
      .bind(String(from?.id || ""), Number(prw.id))
      .run();

    if (!upd2?.meta?.changes) {
      await tgSendMessage(env, botToken, chatId, "ℹ️ Уже обработано или статус не issued.", {}, { appPublicId: ctx.publicId, tgUserId: String(from?.id || "") }).catch(() => null);
      return true;
    }

    const coins = Math.max(0, Math.floor(Number(prw.coins || 0)));
    if (coins > 0) {
      await awardCoins(
        db,
        ctx.appId,
        ctx.publicId,
        String(prw.tg_id),
        coins,
        "passport_redeem_confirm",
        String(prw.prize_code || ""),
        String(prw.prize_title || ""),
        `passport:redeem:${ctx.publicId}:${prw.tg_id}:${redeemCode}:${coins}`
      );
    }

    // совместимость: если есть passport_bonus — тоже помечаем redeemed
    await db
      .prepare(
        `UPDATE passport_bonus
         SET status='redeemed', redeemed_at=datetime('now'), redeemed_by_tg=?
         WHERE app_public_id=? AND tg_id=? AND redeem_code=? AND status='issued'`
      )
      .bind(String(from?.id || ""), ctx.publicId, String(prw.tg_id), redeemCode)
      .run()
      .catch(() => null);

    // ✅ кассиру
    await tgSendMessage(
      env,
      botToken,
      chatId,
      `✅ Подтверждено.\n🎁 Приз: <b>${escHtml(String(prw.prize_title || prw.prize_code || ""))}</b>\n${coins > 0 ? `🪙 Начислено монет: <b>${coins}</b>` : ""}`,
      {},
      { appPublicId: ctx.publicId, tgUserId: String(from?.id || "") }
    ).catch(() => null);

    // ✅ пользователю (ЭТО И БЫЛО ПОТЕРЯНО)
    await notifyUser(
      env,
      botToken,
      userTgId,
      `✅ Кассир подтвердил выдачу приза паспорта.\n🎁 Приз: <b>${escHtml(String(prw.prize_title || prw.prize_code || ""))}</b>\n` +
        (coins > 0 ? `🪙 Начислено монет: <b>${coins}</b>` : ""),
      ctx.publicId
    );

    return true;
  }

  // ===== /start redeem_CODE (кассир/админ нажал deep-link)
  const text = safeStr(upd?.message?.text || "");
  const msgChatId = String(upd?.message?.chat?.id || upd?.message?.from?.id || "");
  const from2 = upd?.message?.from;

  if (text.startsWith("/start") && text.includes("redeem_")) {
    const m = text.match(/redeem_([A-Z0-9\-]+)/i);
    const redeemCode = m ? String(m[1]) : "";
    if (!redeemCode) return false;

    const buttons = {
      inline_keyboard: [
        [
          { text: "✅ Подтвердить", callback_data: `redeem_confirm:${redeemCode}` },
          { text: "❌ Отменить", callback_data: `redeem_decline:${redeemCode}` },
        ],
      ],
    };

    await tgSendMessage(
      env,
      botToken,
      msgChatId,
      `🔐 Код выдачи: <code>${escHtml(redeemCode)}</code>\n\nПодтверди или отмени выдачу:`,
      { reply_markup: buttons },
      { appPublicId: ctx.publicId, tgUserId: String(from2?.id || "") }
    ).catch(() => null);

    return true;
  }

  return false;
}
