import { withCors, handleOptions } from "./middleware/cors";
import { json } from "./utils/http";
import { legacyFetch } from "./legacy/legacyFetch";

import { handleBlocksProxy } from "./routes/blocksProxy";


export interface Env {
  DB: D1Database;
  APPS: KVNamespace;
  BOT_SECRETS: KVNamespace;
  BOT_TOKEN_KEY: string;   // secret в Cloudflare UI (Variables & Secrets)
  SESSION_SECRET: string;  // secret в Cloudflare UI (Variables & Secrets)
  // RATE?: KVNamespace; // опционально потом
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (request.method === "OPTIONS") return handleOptions(request);

    const url = new URL(request.url);

if (url.pathname.startsWith("/blocks/")) {
  if (request.method === "OPTIONS") return handleOptions(request);
  if (request.method !== "GET") return withCors(request, json({ ok:false, error:"METHOD_NOT_ALLOWED" }, 405));
  const r = await handleBlocksProxy(request);
  return withCors(request, r);
}


    // TODO: когда начнём выносить модули — сюда добавим новые роуты
    const resp = await legacyFetch(request, env, ctx);
    if (resp) return withCors(request, resp);

    return withCors(request, json({ ok: false, error: "NOT_FOUND" }, 404));
  },
};
