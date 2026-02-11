import { withCors, handleOptions } from "./middleware/cors";
import { json } from "./utils/http";
import { legacyFetch } from "./legacy/legacyFetch";

import { handleBlocksProxy } from "./routes/blocksProxy";
import { handleHealth, handleVersion } from "./routes/health";
import { enforceSameOriginForMutations } from "./middleware/security";

export interface Env {
  DB: D1Database;
  APPS: KVNamespace;
  BOT_SECRETS: KVNamespace;
  BOT_TOKEN_KEY: string;   // secret в Cloudflare UI (Variables & Secrets)
  SESSION_SECRET: string;  // secret в Cloudflare UI (Variables & Secrets)
  GITHUB_SHA?: string;     // set at deploy time (non-secret)
  // RATE?: KVNamespace; // опционально потом
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try{
      if (request.method === "OPTIONS") return handleOptions(request);

      const url = new URL(request.url);

      // Health / Version
      if (url.pathname === "/_health") return withCors(request, handleHealth());
      if (url.pathname === "/_version") return withCors(request, handleVersion(env));

      // CSRF protection for cookie-auth mutations (API endpoints)
      if (url.pathname.startsWith("/api/")) {
        const csrf = enforceSameOriginForMutations(request);
        if (csrf) return withCors(request, csrf);
      }

      // Blocks proxy (fast path)
      if (url.pathname.startsWith("/blocks/")) {
        if (request.method !== "GET") return withCors(request, json({ ok:false, error:"METHOD_NOT_ALLOWED" }, 405));
        const r = await handleBlocksProxy(request);
        return withCors(request, r);
      }

      // Legacy router (everything else)
      const resp = await legacyFetch(request, env, ctx);
      if (resp) return withCors(request, resp);

      return withCors(request, json({ ok: false, error: "NOT_FOUND" }, 404));
    }catch(e:any){
      // fail-safe JSON so UI doesn't hang
      return withCors(request, json({ ok:false, error:"UNHANDLED", msg:String(e?.message||e) }, 500));
    }
  },
};
