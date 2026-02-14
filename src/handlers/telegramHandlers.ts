// src/handlers/telegramHandlers.ts
import type { Env } from "../index";
import { timingSafeEqual, getBotWebhookSecretForPublicId } from "../services/bots";
import { getBotTokenForApp } from "../services/botToken";
import { resolveAppContextByPublicId } from "../services/apps";
import { tgSendMessage } from "../services/telegramSend";

import { handleStarsHooks, handleRedeem, handleSalesFlow } from "./telegram"; // из index.ts

function corsHeaders(req: Request) {
  return {
    "access-control-allow-origin": req.headers.get("origin") || "*",
    "access-control-allow-methods": "POST,OPTIONS",
    "access-control-allow-headers": "content-type",
  };
}

export async function handleTelegramWebhook(request: Request, env: Env): Promise<Response> {
  if (request.method === "OPTIONS") return new Response("OK", { status: 200, headers: corsHeaders(request) });

  // 1) parse body (Telegram)
  let upd: any = null;
  try {
    upd = await request.json();
  } catch (_) {
    return new Response("OK", { status: 200, headers: corsHeaders(request) });
  }

  // 2) public_id из query (как у тебя)
  const u = new URL(request.url);
  const publicId = String(u.searchParams.get("public_id") || "");

  if (!publicId) {
    // всегда 200 для Telegram
    return new Response("OK", { status: 200, headers: corsHeaders(request) });
  }

  // 3) verify secret (как у тебя было)
  try {
    const got = request.headers.get("x-telegram-bot-api-secret-token") || "";
    const want = await getBotWebhookSecretForPublicId(env, publicId);
    if (want && !timingSafeEqual(got, want)) {
      return new Response("OK", { status: 200, headers: corsHeaders(request) });
    }
  } catch (_) {
    // безопасно: не роняем
    return new Response("OK", { status: 200, headers: corsHeaders(request) });
  }

  // 4) resolve ctx
  const ctx = await resolveAppContextByPublicId(env, publicId).catch(() => null);
  if (!ctx?.appId) return new Response("OK", { status: 200, headers: corsHeaders(request) });

  // 5) bot token
  const botToken = await getBotTokenForApp(env, publicId, ctx.appId).catch(() => null);
  if (!botToken) return new Response("OK", { status: 200, headers: corsHeaders(request) });

  const db = env.DB;

  // ===== ROUTING (важен порядок) =====

  // A) Stars hooks (pre_checkout / successful_payment)
  const starsRes = await handleStarsHooks({ env, botToken, publicId, upd });
  if (starsRes) return new Response("OK", { status: 200, headers: corsHeaders(request) });

  // B) Redeem confirm/decline + /start redeem_
  const redeemed = await handleRedeem({ env, db, ctx: { appId: ctx.appId, publicId }, botToken, upd });
  if (redeemed) return new Response("OK", { status: 200, headers: corsHeaders(request) });

  // C) Sales / pins flow
  const sales = await handleSalesFlow({ env, db, ctx: { appId: ctx.appId, publicId }, botToken, upd });
  if (sales) return new Response("OK", { status: 200, headers: corsHeaders(request) });

  // D) TODO: сюда оставляешь твой “остаток монолита”:
  // - passport сообщения/команды кассира (если у тебя есть отдельные callback_data)
  // - /start general
  // - /profile /help и т.п.
  // Важно: не ломаем боевую — если не распознали, просто молчим (200).

  return new Response("OK", { status: 200, headers: corsHeaders(request) });
}
