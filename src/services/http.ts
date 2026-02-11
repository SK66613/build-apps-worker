// src/services/http.ts
// Small response helpers. CORS is applied centrally in src/index.ts via withCors().

function varyHeaders(): Record<string, string> {
  return { Vary: 'Origin' };
}

export function jsonResponse(obj: any, status = 200, _request: Request | null = null): Response {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      'Content-Type': 'application/json; charset=UTF-8',
      ...varyHeaders(),
    },
  });
}
