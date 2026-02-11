import { ALLOW_ORIGINS_SET } from "../config/origins";
import { json } from "../utils/http";

function isStateChanging(method: string): boolean {
  const m = (method || "GET").toUpperCase();
  return m === "POST" || m === "PUT" || m === "PATCH" || m === "DELETE";
}

function getReqOriginOrRefererOrigin(request: Request): string {
  const o = request.headers.get("Origin");
  if (o) return o;
  const r = request.headers.get("Referer");
  if (!r) return "";
  try {
    return new URL(r).origin;
  } catch {
    return "";
  }
}

/**
 * CSRF protection for cookie-auth APIs.
 * If a request changes state, require Origin/Referer origin to be allowlisted.
 */
export function enforceSameOriginForMutations(request: Request): Response | null {
  if (!isStateChanging(request.method)) return null;

  const origin = getReqOriginOrRefererOrigin(request);

  // No origin/referer => likely CSRF form submission / script
  if (!origin) {
    return json({ ok: false, error: "CSRF_ORIGIN_MISSING" }, 403);
  }

  if (!ALLOW_ORIGINS_SET.has(origin)) {
    return json({ ok: false, error: "CSRF_ORIGIN_FORBIDDEN", origin }, 403);
  }

  return null;
}
