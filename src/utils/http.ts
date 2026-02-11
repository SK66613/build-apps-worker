export function json(obj: unknown, status = 200, extraHeaders: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "Content-Type": "application/json; charset=UTF-8",
      ...extraHeaders,
    },
  });
}
