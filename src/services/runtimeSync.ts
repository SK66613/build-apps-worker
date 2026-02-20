// src/services/runtimeSync.ts
// Sync runtime lookup tables (wheel_prizes, styles_dict) from runtime config.

import type { Env } from "../index";

async function getTableCols(db: any, table: string): Promise<Set<string>> {
  const res: any = await db.prepare(`PRAGMA table_info(${table})`).all();
  const cols = new Set<string>();
  for (const r of (res?.results || [])) cols.add(String(r.name || ""));
  return cols;
}

function pick(obj: any, key: string, fallback: any) {
  const v = obj?.[key];
  return v === undefined ? fallback : v;
}

function toCostCentFromMajor(v: any): number {
  // UI/настройки вводятся в "рублях" (major units) -> храним в копейках (cents)
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.max(0, Math.round(n * 100));
}

export async function syncRuntimeTablesFromConfig(appId: any, publicId: string, cfg: any, env: Env) {
  const out: any = { wheelInserted: 0, stylesInserted: 0 };
  try {
    const db = env.DB;
    if (!db) return { ...out, error: "DB_BINDING_MISSING" };

    const wheelCols = await getTableCols(db, "wheel_prizes");

    // wheel spin cost (store in D1 if column exists)
    const spinCost = Math.max(0, Math.floor(Number(cfg?.wheel?.spin_cost ?? 0)));

    await db.prepare(`DELETE FROM wheel_prizes WHERE app_public_id = ?`).bind(publicId).run();
    const prizes = (cfg?.wheel && Array.isArray(cfg.wheel.prizes)) ? cfg.wheel.prizes : [];


    for (const p of prizes) {
      const code = String(p?.code || "").trim();
      if (!code) continue;

      // базовые поля (они должны быть всегда)
      const row: any = {
        app_id: appId,
        app_public_id: publicId,
        code,
        title: String(p?.title || p?.name || code).trim(),
        weight: Math.max(0, Math.round(Number(p?.weight || 0))),
        coins: Math.max(0, Math.round(Number(p?.coins || 0))),
        active: (p?.active === false) ? 0 : 1,
      };

      // новые поля — только если есть колонка в D1
      if (wheelCols.has("kind")) row.kind = String(p?.kind || "");
      if (wheelCols.has("img")) row.img = p?.img ? String(p.img) : null;

      if (wheelCols.has("cost_cent")) {
  const major = (p?.cost_cent ?? p?.cost ?? 0); // в конфиге это "рубли"
  row.cost_cent = toCostCentFromMajor(major);   // в D1 это "копейки"
}
      if (wheelCols.has("cost_currency")) row.cost_currency = String(p?.cost_currency ?? p?.currency ?? "RUB");
      if (wheelCols.has("cost_currency_custom")) row.cost_currency_custom = String(p?.cost_currency_custom ?? p?.currency_custom ?? "");

      if (wheelCols.has("track_qty")) row.track_qty = (p?.track_qty === true || Number(p?.track_qty || 0) === 1) ? 1 : 0;
      if (wheelCols.has("qty_left")) row.qty_left = Math.max(0, Math.round(Number(p?.qty_left ?? p?.stock_qty ?? 0)));
      if (wheelCols.has("stop_when_zero")) {
        const swz = (p?.stop_when_zero === undefined ? true : !!p.stop_when_zero);
        row.stop_when_zero = swz ? 1 : 0;
      }

      // собрать INSERT динамически
      const keys = Object.keys(row).filter(k => wheelCols.has(k) || ["app_id","app_public_id","code","title","weight","coins","active"].includes(k));
      const cols = keys.join(", ");
      const qs = keys.map(() => "?").join(", ");
      const vals = keys.map(k => row[k]);

      await db.prepare(`INSERT INTO wheel_prizes (${cols}) VALUES (${qs})`).bind(...vals).run();
      out.wheelInserted++;
    }

    // Persist spin_cost into D1 (one value per app; duplicated across rows intentionally)
    if (wheelCols.has("spin_cost")) {
      await db.prepare(
        `UPDATE wheel_prizes SET spin_cost = ? WHERE app_public_id = ?`
      ).bind(spinCost, publicId).run();
    }

    

    // styles_dict — как у тебя было (ок)
    await db.prepare(`DELETE FROM styles_dict WHERE app_public_id = ?`).bind(publicId).run();
    const styles = (cfg?.passport && Array.isArray(cfg.passport.styles)) ? cfg.passport.styles : [];
    for (const s of styles) {
      if (!s || s.active === false) continue;
      await db.prepare(
        `INSERT INTO styles_dict (app_id, app_public_id, style_id, title) VALUES (?, ?, ?, ?)`
      ).bind(appId, publicId, String(s.code || ""), String(s.name || s.code || "")).run();
      out.stylesInserted++;
    }

    return out;
  } catch (e: any) {
    console.error("[syncRuntimeTablesFromConfig] failed", e);
    return { ...out, error: String(e?.message || e) };
  }
}

