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
