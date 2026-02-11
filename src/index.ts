import { withCors, handleOptions } from "./middleware/cors";
import { json } from "./utils/http";
import { legacyFetch } from "./legacy/legacyFetch";

import { handleBlocksProxy } from "./routes/blocksProxy";
import { handleHealth, handleVersion } from "./routes/health";
import { routeAuth } from "./routes/auth";
import { routePublic } from "./routes/public";
import { routeTelegram } from "./routes/telegram";
import { enforceSameOriginForMutations } from "./middleware/security";

import { routeMiniApi } from "./routes/mini";

export interface Env {
  DB: D1Database;
  APPS: KVNamespace;
  BOT_SECRETS: KVNamespace;
  BOT_TOKEN_KEY: string;   // secret в Cloudflare UI (Variables & Secrets)
  SESSION_SECRET: string;  // secret в Cloudflare UI (Variables & Secrets)
  GITHUB_SHA?: string;     // set at deploy time (non-secret)
  DEBUG_ERRORS?: string;   // "1" чтобы отдавать stack в ответ (временно)
  // RATE?: KVNamespace;    // опционально потом
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    // общий try/catch, чтобы никогда не было "вечного спиннера"
    try {
      if (request.method === "OPTIONS") return handleOptions(request);

      const url = new URL(request.url);

      // Mini API (published mini-app runtime)
// Важно: раньше legacy, чтобы legacy вообще не касался /api/mini/*
if (url.pathname.startsWith("/api/mini/")) {
  // CSRF allowlist уже проверяется выше (у тебя стоит на /api/*)
  const r = await routeMiniApi(request, env, url);
  return withCors(request, r);
}


      // Health / Version
      if (url.pathname === "/_health") return withCors(request, handleHealth());
      if (url.pathname === "/_version") return withCors(request, handleVersion(env));

      // CSRF protection for cookie-auth mutations (API endpoints)
      // ВАЖНО: /api/mini/* часто вызывается из mini.salesgenius.ru и это норм.
      if (url.pathname.startsWith("/api/")) {
        const csrf = enforceSameOriginForMutations(request);
        if (csrf) return withCors(request, csrf);
      }

      
      // Auth API (cabinet)
      const authResp = await routeAuth(request, env, url);
      if (authResp) return withCors(request, authResp);

      // Public API (events, stars, sales token)
      const pubResp = await routePublic(request, env, url);
      if (pubResp) return withCors(request, pubResp);

      // Telegram webhook
      const tgResp = await routeTelegram(request, env, url);
      if (tgResp) return withCors(request, tgResp);

      // Blocks proxy (fast path)
      if (url.pathname.startsWith("/blocks/")) {
        if (request.method !== "GET") {
          return withCors(request, json({ ok: false, error: "METHOD_NOT_ALLOWED" }, 405, request));
        }
        const r = await handleBlocksProxy(request);
        return withCors(request, r);
      }

      // Legacy router (everything else)
      const resp = await legacyFetch(request, env, ctx);
      if (resp) return withCors(request, resp);

      return withCors(request, json({ ok: false, error: "NOT_FOUND" }, 404, request));
    } catch (e: any) {
      // Лог в Observability (тут будет stack)
      try {
        const u = new URL(request.url);
        console.error("UNHANDLED", {
          method: request.method,
          path: u.pathname,
          search: u.search,
          msg: String(e?.message || e),
          stack: e?.stack || null,
        });
      } catch (_) {}

      const debug = String(env?.DEBUG_ERRORS || "0") === "1";
      const payload: any = {
        ok: false,
        error: "UNHANDLED",
        msg: String(e?.message || e),
      };
      if (debug) payload.stack = String(e?.stack || "");

      return withCors(request, json(payload, 500, request));
    }
  },
};