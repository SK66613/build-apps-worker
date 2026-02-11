import { withCors, handleOptions } from "./middleware/cors";
import { json } from "./utils/http";
import { legacyFetch } from "./legacy/legacyFetch";

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

    // TODO: когда начнём выносить модули — сюда добавим новые роуты
    const resp = await legacyFetch(request, env, ctx);
    if (resp) return withCors(request, resp);

    return withCors(request, json({ ok: false, error: "NOT_FOUND" }, 404));
  },
};
