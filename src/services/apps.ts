// src/services/apps.ts
import type { Env } from "../index";

/**
 * Возвращает канонический public_id для appId.
 * Быстро: KV app:<appId>
 * Фоллбек: D1 apps.public_id
 */
export async function getCanonicalPublicIdForApp(appId: string | number, env: Env): Promise<string | null> {
  // 1) KV app:<appId> (самое быстрое)
  try {
    const raw = await env.APPS.get("app:" + String(appId));
    if (raw) {
      const obj = JSON.parse(raw);
      if (obj && obj.publicId) return String(obj.publicId);
    }
  } catch (_) {}

  // 2) fallback: D1 apps.public_id
  try {
    const row: any = await env.DB
      .prepare("SELECT public_id FROM apps WHERE id = ? LIMIT 1")
      .bind(Number(appId))
      .first();
    if (row && row.public_id) return String(row.public_id);
  } catch (e) {
    console.error("[publicId] getCanonicalPublicIdForApp failed", e);
  }

  return null;
}

// publicId -> { appId, canonical publicId }
export async function resolveAppContextByPublicId(
  publicId: string,
  env: Env
): Promise<{ ok: boolean; appId?: string; publicId?: string; status?: number; error?: string }> {
  const map: any = await env.APPS.get('app:by_public:' + String(publicId), 'json');
  if (!map || !map.appId) return { ok: false, status: 404, error: 'UNKNOWN_PUBLIC_ID' };
  const appId = String(map.appId);
  const canonicalPublicId = (await getCanonicalPublicIdForApp(appId, env)) || String(publicId);
  return { ok: true, appId, publicId: canonicalPublicId };
}
