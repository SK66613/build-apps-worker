// src/middleware/cors.ts
import { ALLOW_ORIGINS_SET } from "../config/origins";

export function corsHeaders(request: Request): Record<string, string> {
  const origin = request.headers.get("Origin") || "";

  // No Origin => not a CORS request (same-origin or server-to-server)
  if (!origin) return { Vary: "Origin" };

  // Strict allowlist
  if (!ALLOW_ORIGINS_SET.has(origin)) return { Vary: "Origin" };

  const reqHeaders =
    request.headers.get("Access-Control-Request-Headers") ||
    "Content-Type, Authorization";

  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": reqHeaders,
    Vary: "Origin",
  };
}

export function withCors(request: Request, response: Response): Response {
  if (!response) return response;
  const h = new Headers(response.headers);
  const ch = corsHeaders(request);
  for (const [k, v] of Object.entries(ch)) h.set(k, v);
  return new Response(response.body, { status: response.status, headers: h });
}

export function handleOptions(request: Request): Response {
  return new Response("", { status: 204, headers: corsHeaders(request) });
}
