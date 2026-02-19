import { withCors, handleOptions } from "./middleware/cors";
import { json } from "./utils/http";

import { handleBlocksProxy } from "./routes/blocksProxy";
import { handleHealth, handleVersion } from "./routes/health";
import { enforceSameOriginForMutations } from "./middleware/security";

import { routeMiniApi } from "./routes/mini";
import { routeAuth } from "./routes/auth";
import { routePublic } from "./routes/public";
import { routeTelegram } from "./routes/telegram";
import { routeCabinet } from "./routes/cabinet";
import { routeTemplates } from "./routes/templates";
import { routeAnalytics } from "./routes/analytics";



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

if (url.pathname.startsWith("/api/mini/")) {
  const res = await routeMiniApi(request, env);
  return withCors(request, res);
}





      // /app/:publicId и /m/:publicId — редирект на публичный runtime
      if (url.pathname.startsWith("/app/") || url.pathname.startsWith("/m/")) {
        const parts = url.pathname.split("/").filter(Boolean); // ['app','<id>'] или ['m','<id>']
        const publicId = parts[1] || "";
        const target = "https://mini.salesgenius.ru/m/" + encodeURIComponent(publicId);
        return withCors(request, Response.redirect(target, 302));
      }

      // New routers (thin wrappers around legacy handlers for now)
      const rAuth = await routeAuth(request, env, url);
      if (rAuth) return withCors(request, rAuth);

      const rPub = await routePublic(request, env, url);
      if (rPub) return withCors(request, rPub);

      const rTg = await routeTelegram(request, env, url);
      if (rTg) return withCors(request, rTg);

      // Health / Version
      if (url.pathname === "/_health") return withCors(request, handleHealth());
      if (url.pathname === "/_version") return withCors(request, handleVersion(env));

      // CSRF protection for cookie-auth mutations (API endpoints)
      // ВАЖНО: /api/mini/* часто вызывается из mini.salesgenius.ru и это норм.
      if (url.pathname.startsWith("/api/")) {
        // CSRF protection applies only to cookie-auth кабинета.
        // Server-to-server endpoints (tg webhook) and public miniapp endpoints must work without Origin.
        const skip =
          url.pathname.startsWith("/api/mini/") ||
          url.pathname.startsWith("/api/public/") ||
          url.pathname.startsWith("/api/tg/");
        if (!skip) {
          const csrf = enforceSameOriginForMutations(request);
          if (csrf) return withCors(request, csrf);
        }
      }

      // Templates (cookie session GET)
      const rTpl = await routeTemplates(request, env, url);
      if (rTpl) return withCors(request, rTpl);

      // Cabinet legacy-compat analytics (/api/cabinet/apps/*)
      const rAn = await routeAnalytics(request, env, url);
      if (rAn) return withCors(request, rAn);

      // Cabinet routes (cookie session)
      const rCab = await routeCabinet(request, env, url);
      if (rCab) return withCors(request, rCab);

      // Blocks proxy (fast path)
      if (url.pathname.startsWith("/blocks/")) {
        if (request.method !== "GET") {
          return withCors(request, json({ ok: false, error: "METHOD_NOT_ALLOWED" }, 405, request));
        }
        const r = await handleBlocksProxy(request);
        return withCors(request, r);
      }

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
