// src/handlers/telegram/stars.ts
import type { Env } from "../../index";
import { tgSendMessage } from "../../services/telegramSend";

// минимальный вызов TG API (как в монолите по смыслу)
async function tgApi(env: Env, botToken: string, method: string, payload: any) {
  const url = `https://api.telegram.org/bot${botToken}/${method}`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  return res.json().catch(() => ({}));
}

export async function handleStarsHooks(args: {
  env: Env;
  botToken: string;
  publicId: string;
  upd: any;
}): Promise<Response | null> {
  const { env, botToken, publicId, upd } = args;

  // pre_checkout_query: answer OK
  if (upd?.pre_checkout_query?.id) {
    try {
      await tgApi(env, botToken, "answerPreCheckoutQuery", {
        pre_checkout_query_id: upd.pre_checkout_query.id,
        ok: true,
      });
    } catch (_) {}
    return new Response("OK", { status: 200 });
  }

  // successful_payment: благодарим
  const sp = upd?.message?.successful_payment;
  if (sp && upd?.message?.chat?.id) {
    const chatId = String(upd.message.chat.id);
    const total = Number(sp.total_amount || 0) / 100;

    try {
      await tgSendMessage(
        env,
        botToken,
        chatId,
        `✅ Оплата получена!\nСумма: <b>${total.toFixed(2)}</b>\nСпасибо ❤️`,
        {},
        { appPublicId: publicId, tgUserId: String(upd?.message?.from?.id || "") }
      );
    } catch (_) {}

    return new Response("OK", { status: 200 });
  }

  return null;
}
