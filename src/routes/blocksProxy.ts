// src/routes/blocksProxy.ts
import { corsHeaders } from "../middleware/cors";

const BLOCKS_UPSTREAMS = [
  // ЕДИНСТВЕННЫЙ корректный апстрим для блоков/манифестов
  "https://blocks.salesgenius.ru/sg-blocks/",
];

// ВРЕМЕННО: выключаем кэш для /blocks/*
const BLOCKS_CACHE_DISABLED = true;

// небольшая подмога: пробуем несколько апстримов по очереди
async function fetchFromUpstreams(pathRel: string, request: Request): Promise<Response> {
  let lastErr: any = null;

  const rel = String(pathRel || "").replace(/^\//, "");

  const isManifest =
    rel.endsWith("index.json") ||
    rel.includes("/index.json") ||
    rel.endsWith("manifest.json") ||
    rel.includes("/manifest.json");

  // TTL: манифест короткий, ассеты — длинный
  const EDGE_TTL_OK = isManifest ? 60 : 86400;
  const BROWSER_TTL_OK = isManifest ? 60 : 86400;

  for (const base of BLOCKS_UPSTREAMS) {
    try {
      const u = new URL(rel, base);

      // Edge cache key (только GET)
      const cacheKey = new Request(u.toString(), { method: "GET" });
      const cache = caches.default;

      // 1) try cache (только если кэш НЕ отключён)
      if (!BLOCKS_CACHE_DISABLED) {
        const cached = await cache.match(cacheKey);
        if (cached) {
          const resp = new Response(cached.body, cached);
          resp.headers.set("X-SG-Upstream", base);
          resp.headers.set("X-SG-Cache", "HIT");
          return resp;
        }
      }

      // 2) fetch
      const r = await fetch(u.toString(), {
        method: "GET",
        headers: {
          Accept: request.headers.get("Accept") || "*/*",
          ...(BLOCKS_CACHE_DISABLED ? { "Cache-Control": "no-cache" } : {}),
        },
        cf: BLOCKS_CACHE_DISABLED
          ? {
              cacheEverything: false,
              cacheTtl: 0,
              cacheTtlByStatus: {
                "200-299": 0,
                "404": 0,
                "500-599": 0,
              },
            }
          : {
              cacheEverything: true,
              cacheTtl: EDGE_TTL_OK,
              cacheTtlByStatus: {
                "200-299": EDGE_TTL_OK,
                "404": 60,
                "500-599": 0,
              },
            },
      } as any);

      // 404: сразу отдаём
      if (r.status === 404) {
        const resp404 = new Response(r.body, r);
        resp404.headers.set("X-SG-Upstream", base);
        resp404.headers.set("X-SG-Cache", BLOCKS_CACHE_DISABLED ? "BYPASS" : "MISS");
        if (BLOCKS_CACHE_DISABLED) {
          resp404.headers.set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
          resp404.headers.set("Pragma", "no-cache");
          resp404.headers.set("Expires", "0");
        }
        return resp404;
      }

      if (r.ok) {
        // защита от “поймали HTML вместо JSON”
        if (isManifest) {
          const ct = (r.headers.get("content-type") || "").toLowerCase();
          if (ct && !ct.includes("application/json") && !ct.includes("text/json")) {
            lastErr = new Error(`Manifest looks non-JSON: ${ct} from ${base}`);
            continue;
          }
        }

        const resp = new Response(r.body, r);

        if (BLOCKS_CACHE_DISABLED) {
          resp.headers.set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
          resp.headers.set("Pragma", "no-cache");
          resp.headers.set("Expires", "0");
          resp.headers.set("X-SG-Cache", "BYPASS");
        } else {
          resp.headers.set("Cache-Control", `public, max-age=${BROWSER_TTL_OK}`);
          resp.headers.set("X-SG-Cache", "MISS");
          await cache.put(cacheKey, resp.clone());
        }

        resp.headers.set("X-SG-Upstream", base);
        return resp;
      }

      lastErr = new Error(`Upstream ${base} responded ${r.status}`);
    } catch (e) {
      lastErr = e;
    }
  }

  throw lastErr || new Error("No upstreams");
}

export async function handleBlocksProxy(request: Request): Promise<Response> {
  const url = new URL(request.url);

  // /blocks/<rel>
  let rel = url.pathname.replace(/^\/blocks\/+/, "");
  rel = rel.replace(/^\/+/, "");

  // если фронт случайно прислал /blocks/blocks/blocks/...
  if (rel.startsWith("blocks/blocks/")) {
    rel = rel.replace(/^blocks\//, "");
  }

  // пробуем несколько апстримов по очереди
  const upstreamResp = await fetchFromUpstreams(rel, request);

  // добавим CORS (на всякий)
  const h = new Headers(upstreamResp.headers);
  const ch = corsHeaders(request);
  for (const [k, v] of Object.entries(ch)) h.set(k, v);

  return new Response(upstreamResp.body, { status: upstreamResp.status, headers: h });
}
