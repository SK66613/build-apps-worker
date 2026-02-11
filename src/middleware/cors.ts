const CORS_ALLOW = new Set<string>([
  "https://app.salesgenius.ru",
  "https://mini.salesgenius.ru",
  "https://blocks.salesgenius.ru",
  "https://ru.salesgenius.ru",
  "https://apps.salesgenius.ru",
  "https://web.telegram.org",
  "https://web.telegram.org/k",
  "https://web.telegram.org/z",
]);

export function corsHeaders(request: Request): Record<string, string> {
  const origin = request.headers.get("Origin") || "";
  if (!origin) return { "Vary": "Origin" };
  if (!CORS_ALLOW.has(origin)) return { "Vary": "Origin" };

  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    "Access-Control-Allow-Headers":
      request.headers.get("Access-Control-Request-Headers") || "Content-Type, Authorization",
    "Vary": "Origin",
  };
}

export function withCors(request: Request, response: Response): Response {
  const h = new Headers(response.headers);
  const ch = corsHeaders(request);
  for (const [k, v] of Object.entries(ch)) h.set(k, v);
  return new Response(response.body, { status: response.status, headers: h });
}

export function handleOptions(request: Request): Response {
  return new Response("", { status: 204, headers: corsHeaders(request) });
}
