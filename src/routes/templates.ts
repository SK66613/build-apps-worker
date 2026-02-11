// src/routes/templates.ts
// Templates catalog (cookie session) endpoint.

import type { Env } from "../index";
import { requireSession } from "../handlers/cabinetApiHandlers";
import { TEMPLATE_CATALOG } from "../services/templates";
import { json } from "../utils/http";

export async function routeTemplates(request: Request, env: Env, url: URL): Promise<Response | null> {
  const p = url.pathname;
  if (p !== "/api/templates") return null;
  if (request.method !== "GET") return json({ ok: false, error: "METHOD_NOT_ALLOWED" }, 405, request);

  const s = await requireSession(request as any, env as any);
  if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);

  return json({ ok: true, items: TEMPLATE_CATALOG }, 200, request);
}
