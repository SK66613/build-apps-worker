import type { Env } from "../index";

/**
 * СЮДА ВСТАВЛЯЕШЬ КОД ИЗ Cloudflare UI.
 *
 * Как вставлять:
 * 1) Cloudflare Dashboard -> Workers & Pages -> build-apps -> Edit code
 * 2) Найди блок:
 *      export default {
 *        async fetch(request, env, ctx) {
 *          ... ВОТ ЭТО СОДЕРЖИМОЕ ...
 *        }
 *      }
 * 3) Скопируй ТОЛЬКО содержимое внутри fetch(...) { ... } и вставь ниже вместо примера.
 *
 * Важно:
 * - Не вставляй `export default` внутрь этого файла.
 * - Если в твоём коде объявлены функции/константы ВНЕ fetch — перенеси их ниже (после этой функции) как есть.
 */
export async function legacyFetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response | null> {
  // ====== ВРЕМЕННАЯ ЗАГЛУШКА ======
  // Когда вставишь настоящий код — удали эту заглушку.
  const url = new URL(request.url);
  if (url.pathname === "/" && request.method === "GET") {
    return new Response("build-apps worker is alive (ts scaffold)", { status: 200 });
  }

  // Верни null, если роут не обработан.
  return null;
}

// ↓↓↓ сюда можно перенести твои старые helper-функции/константы, если они были в UI выше/ниже fetch
