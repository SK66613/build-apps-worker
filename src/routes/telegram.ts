// src/routes/telegram.ts
import type { Env } from "../index";
import { handleTelegramWebhook } from "../handlers/telegramHandlers";

export async function routeTelegram(request: Request, env: Env, url: URL): Promise<Response | null> {
  const m = url.pathname.match(/^\/api\/tg\/webhook\/([^/]+)$/);
  if (m && request.method === "POST") {
    const publicId = decodeURIComponent(m[1]);
    return handleTelegramWebhook(publicId, request as any, env as any);
  }
  return null;
}
