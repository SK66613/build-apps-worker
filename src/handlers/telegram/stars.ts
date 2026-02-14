// src/handlers/telegram/stars.ts
import type { Env } from "../../index";
import { tgSendMessage } from "../../services/telegramSend";

async function tgApi(env: Env, botToken: string, method: string, payload: any) {
  const url = `https://api.telegram.org/bot${botToken}/${method}`;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload || {}),
    });

    // Telegram обычно JSON, но на сетевых/edge проблемах может быть иначе
    const text = await res.text().catch(() => "");
    try {
      return JSON.parse(text || "{}");
    } catch (_) {
      return { ok: false, status: res.status, raw: text };
    }
  } catch (e: any) {
    return { ok: false, error: String(e?.message || e) };
  }
}

function formatPayment(sp: any) {
  const currency = String(sp?.currency || "").toUpperCase();
  const totalAmount = Number(sp?.total_amount || 0);

  // Telegram Stars (часто currency XTR)
  if (currency === "XTR") {
    // обычно total_amount это кол-во Stars (или минимальная единица Stars)
    // если вдруг придёт scale — учтём
    const exp = Number(sp?.currency_exp || sp?.currency_exponent || 0);
    const val = exp ? totalAmount / Math.pow(10, exp) : totalAmount;
    return { label: "Stars", value: String(val) };
  }

  // обычные валюты (как раньше): total_amount в копейках/центах
  const val = totalAmount / 100;
  return { label: currency || "Amount", value: val.toFixed(2) };
}

export async function handleStarsHooks(args: {
  env: Env;
  botToken: string;
  publicId: string;
  upd: any;
}): Promise<Response | null> {
  const { env, botToken, publicId, upd } = args;

  // 1) pre_checkout_query: всегда отвечаем ok:true
  const pcq = upd?.pre_checkout_query;
  if (pcq?.id) {
    const r = await tgApi(env, botToken, "answerPreCheckoutQuery", {
      pre_checkout_query_id: pcq.id,
      ok: true,
    });

    // лог полезен, но не ломаем webhook
    if (!r?.ok) {
      console.warn("[stars] answerPreCheckoutQuery failed", r);
    }

    return new Response("OK", { status: 200 });
  }

  // 2) successful_payment: благодарим
  const msg = upd?.message;
  const sp = msg?.successful_payment;

  if (sp && msg?.chat?.id) {
    const chatId = String(msg.chat.id);
    const fromId = String(msg?.from?.id || "");
    const p = formatPayment(sp);

    try {
      await tgSendMessage(
        env,
        botToken,
        chatId,
        `✅ Оплата получена!\n${p.label}: <b>${p.value}</b>\nСпасибо ❤️`,
        {},
        { appPublicId: publicId, tgUserId: fromId }
      );
    } catch (e) {
      console.warn("[stars] tgSendMessage failed", e);
    }

    return new Response("OK", { status: 200 });
  }

  return null;
}
