// src/services/analyticsRange.ts
// Small helpers for analytics date ranges.

export function parseRangeOrDefault(url: URL): { from: string; to: string } {
  let from = url.searchParams.get("from") || "";
  let to = url.searchParams.get("to") || "";

  // default: last 7 days inclusive (UTC)
  if (!from || !to) {
    const now = new Date();
    const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    const start = new Date(end.getTime() - 6 * 24 * 3600 * 1000);
    from = start.toISOString().slice(0, 10);
    to = end.toISOString().slice(0, 10);
  }
  return { from, to };
}

export function daysBetweenInclusive(from: string, to: string): string[] {
  const out: string[] = [];
  const a = new Date(from + "T00:00:00Z");
  const b = new Date(to + "T00:00:00Z");
  for (let d = new Date(a); d <= b; d = new Date(d.getTime() + 24 * 3600 * 1000)) {
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}
