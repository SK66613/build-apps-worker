// src/services/runtimeSync.ts
// Sync runtime lookup tables (wheel_prizes, styles_dict) from runtime config.

import type { Env } from "../index";

export async function syncRuntimeTablesFromConfig(appId: any, publicId: string, cfg: any, env: Env) {
  const out: any = { wheelInserted: 0, stylesInserted: 0 };
  try {
    const db = env.DB;
    if (!db) {
      out.error = "DB_BINDING_MISSING";
      return out;
    }

    // ---- wheel_prizes ----
    await db.prepare(`DELETE FROM wheel_prizes WHERE app_public_id = ?`).bind(publicId).run();
    const prizes = (cfg && cfg.wheel && Array.isArray(cfg.wheel.prizes)) ? cfg.wheel.prizes : [];
    for (const p of prizes) {
      if (!p) continue;

      const code = String(p.code || "").trim();
      const title = String((p.title || p.name || p.code) || "").trim();
      const wRaw = Number(p.weight);
      const weight = Number.isFinite(wRaw) ? Math.max(0, Math.round(wRaw)) : 1;
      const cRaw = Number(p.coins);
      const coins = Number.isFinite(cRaw) ? Math.max(0, Math.round(cRaw)) : 0;
      const active = (p.active === false) ? 0 : 1;
      if (!code) continue;

      await db
        .prepare(
          `INSERT INTO wheel_prizes (app_id, app_public_id, code, title, weight, coins, active)
           VALUES (?, ?, ?, ?, ?, ?, ?)`
        )
        .bind(appId, publicId, code, title, weight, coins, active)
        .run();

      out.wheelInserted++;
    }

    // ---- styles_dict ----
    await db.prepare(`DELETE FROM styles_dict WHERE app_public_id = ?`).bind(publicId).run();
    const styles = (cfg && cfg.passport && Array.isArray(cfg.passport.styles)) ? cfg.passport.styles : [];
    for (const s of styles) {
      if (!s) continue;
      if (s.active === false) continue;
      await db
        .prepare(
          `INSERT INTO styles_dict (app_id, app_public_id, style_id, title)
           VALUES (?, ?, ?, ?)`
        )
        .bind(appId, publicId, String(s.code || ""), String(s.name || s.code || ""))
        .run();
      out.stylesInserted++;
    }
  } catch (e: any) {
    console.error("[syncRuntimeTablesFromConfig] failed", e);
    out.error = String(e && e.message ? e.message : e);
  }
  return out;
}
