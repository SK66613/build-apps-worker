// src/handlers/telegramHandlers.ts
import type { Env } from "../index";
import { getBotWebhookSecretForPublicId, timingSafeEqual } from "../services/bots";
import { getBotTokenForApp } from "../services/botToken";
import { resolveAppContextByPublicId } from "../services/apps";

import { handleStarsHooks, handleRedeem, handleSalesFlow } from "./telegram";

function corsHeaders(req: Request) {
  return {
    "access-control-allow-origin": req.headers.get("origin") || "*",
    "access-control-allow-methods": "POST,OPTIONS",
    "access-control-allow-headers": "content-type",
  };
}

/**
 * ВАЖНО: сигнатура должна остаться как в старом коде,
 * потому что src/routes/telegram.ts вызывает именно так:
 * handleTelegramWebhook(publicId, request, env)
 */
export async function handleTelegramWebhook(publicId: string, request: Request, env: Env): Promise<Response> {
  // Telegram должен всегда получить 200
  const ok = () => new Response("OK", { status: 200, headers: corsHeaders(request) });

  try {
    if (request.method === "OPTIONS") return ok();
    if (request.method !== "POST") return ok();

    const pid = String(publicId || "").trim();
    if (!pid) return ok();

    // ===== verify secret (как было у тебя) =====
    // У тебя в бою URL: /api/tg/webhook/:publicId?s=...
    // Плюс можно поддержать header secret-token (не мешает).
    try {
      const url = new URL(request.url);
      const gotQuery = String(url.searchParams.get("s") || "");
      const gotHeader = String(request.headers.get("x-telegram-bot-api-secret-token") || "");
      const got = gotHeader || gotQuery;

      const want = await getBotWebhookSecretForPublicId(pid, env);
      if (want && !timingSafeEqual(got, want)) return ok();
    } catch (_) {
      return ok();
    }

    // ===== parse update =====
    let upd: any = null;
    try {
      upd = await request.json();
    } catch (_) {
      return ok();
    }
    if (!upd) return ok();

    // ===== resolve ctx =====
    const ctx = await resolveAppContextByPublicId(pid, env).catch(() => null);
    if (!ctx || !(ctx as any).appId) return ok();

    const appId = (ctx as any).appId;
    const canonicalPublicId = String((ctx as any).publicId || pid);

    // ===== bot token =====
    const botToken = await getBotTokenForApp(canonicalPublicId, env, appId).catch(() => null);
    if (!botToken) return ok();

    const db: any = env.DB;

    // ===== ROUTING (порядок важен) =====

    // A) Stars hooks (pre_checkout / successful_payment)
    if (await handleStarsHooks({ env, botToken, publicId: canonicalPublicId, upd })) {
      return ok();
    }

    // B) Redeem confirm/decline + /start redeem_
    if (await handleRedeem({ env, db, ctx: { appId, publicId: canonicalPublicId }, botToken, upd })) {
      return ok();
    }

    // C) Sales / pins flow
    if (await handleSalesFlow({ env, db, ctx: { appId, publicId: canonicalPublicId }, botToken, upd })) {
      return ok();
    }

    // D) Остаток “монолита”:
    // сюда позже вернёшь: паспорт/команды/прочие callback_data.
    return ok();
  } catch (e) {
    // Никаких 500 в телегу
    console.error("[tg.webhook] UNHANDLED", e);
    return new Response("OK", { status: 200, headers: corsHeaders(request) });
  }
}
