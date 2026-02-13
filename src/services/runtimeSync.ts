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
    const prizes = cfg?.wheel?.prizes && Array.isArray(cfg.wheel.prizes) ? cfg.wheel.prizes : [];
    for (const p of prizes) {
      if (!p) continue;

      const code = String(p.code || "").trim();
      if (!code) continue;

      const title = String((p.title || p.name || p.code) || "").trim();

      const wRaw = Number(p.weight);
      const weight = Number.isFinite(wRaw) ? Math.max(0, Math.round(wRaw)) : 1;

      const cRaw = Number(p.coins);
      const coins = Number.isFinite(cRaw) ? Math.max(0, Math.round(cRaw)) : 0;

      const active = p.active === false ? 0 : 1;
      const img = p.img ? String(p.img) : ""; // если у тебя есть колонка img — оставим; если нет — просто удали

      // ⚠️ если wheel_prizes НЕ имеет img — удали img из INSERT ниже
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
    const styles = cfg?.passport?.styles && Array.isArray(cfg.passport.styles) ? cfg.passport.styles : [];

    let i = 0;
    for (const s of styles) {
      if (!s) continue;

      const styleId = String(s.code || "").trim();
      if (!styleId) continue;

      const title = String(s.name || s.code || "").trim();
      const descr = String(s.desc || "").trim();
      const image = String(s.image || "").trim();
      const active = s.active === false ? 0 : 1;
      const sort = i++;

      // ✅ ВАРИАНТ 1 (рекомендуется): если ты добавил колонки descr/image/active/sort
      // Тогда оставь этот INSERT.
      await db
        .prepare(
          `INSERT INTO styles_dict (app_id, app_public_id, style_id, title, descr, image, active, sort)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .bind(appId, publicId, styleId, title, descr, image, active, sort)
        .run();

      out.stylesInserted++;
    }
  } catch (e: any) {
    console.error("[syncRuntimeTablesFromConfig] failed", e);
    out.error = String(e && e.message ? e.message : e);
  }
  return out;
}

