// src/services/cors.ts
export function corsHeaders(req: Request) {
  const origin = req.headers.get("Origin") || "";

  // Разрешаем только наши фронты
  const allow = new Set([
    "https://mini.salesgenius.ru",
    "https://ru.salesgenius.ru",
    "https://app.salesgenius.ru",
    // добавь сюда второй домен зеркала если есть:
    // "https://<your-mirror-domain>",
  ]);

  const h = new Headers();

  if (allow.has(origin)) {
    h.set("Access-Control-Allow-Origin", origin);
    h.set("Vary", "Origin");
    h.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    h.set("Access-Control-Allow-Headers", "Content-Type");
    h.set("Access-Control-Max-Age", "86400");
  }

  return h;
}

export function withCors(req: Request, res: Response) {
  const cors = corsHeaders(req);
  if (!cors.has("Access-Control-Allow-Origin")) return res;

  const out = new Headers(res.headers);
  for (const [k, v] of cors.entries()) out.set(k, v);

  return new Response(res.body, { status: res.status, headers: out });
}
