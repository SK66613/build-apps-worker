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
  if (!code) continue;

  const title = String((p.title || p.name || p.code) || "").trim();

  const wRaw = Number(p.weight);
  const weight = Number.isFinite(wRaw) ? Math.max(0, Math.round(wRaw)) : 0;

  const kindRaw = String(p.kind || "").toLowerCase();
  const kind =
    kindRaw === "coins" ? "coins" :
    kindRaw === "item" ? "item" :
    kindRaw === "physical" ? "item" :
    (Number(p.coins || 0) > 0 ? "coins" : "item");

  const cRaw = Number(p.coins);
  const coins = (kind === "coins" && Number.isFinite(cRaw)) ? Math.max(0, Math.round(cRaw)) : 0;

  const active = (p.active === false) ? 0 : 1;

  const img = p.img ? String(p.img) : null;

  const costCentRaw = Number(p.cost_cent ?? p.cost ?? 0);
  const cost_cent = (kind === "item" && Number.isFinite(costCentRaw)) ? Math.max(0, Math.round(costCentRaw)) : 0;

  const cost_currency = String(p.cost_currency ?? p.currency ?? "RUB");
  const cost_currency_custom = String(p.cost_currency_custom ?? p.currency_custom ?? "");

  const track_qty =
    (kind === "item")
      ? ((p.track_qty === true) || Number(p.track_qty || 0) === 1 ? 1 : 0)
      : 0;

  const qtyLeftRaw = Number(p.qty_left ?? p.stock_qty ?? 0);
  const qty_left = (kind === "item" && Number.isFinite(qtyLeftRaw)) ? Math.max(0, Math.round(qtyLeftRaw)) : 0;

  const stop_when_zero = (kind === "item")
    ? ((p.stop_when_zero === undefined ? true : !!p.stop_when_zero) ? 1 : 0)
    : 1;

  await db.prepare(
    `INSERT INTO wheel_prizes
      (app_id, app_public_id, code, title, weight, active, coins, kind, img,
       cost_cent, cost_currency, cost_currency_custom,
       track_qty, qty_left, stop_when_zero)
     VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?,
       ?, ?, ?,
       ?, ?, ?)`
  )
  .bind(
    appId, publicId, code, title, weight, active, coins, kind, img,
    cost_cent, cost_currency, cost_currency_custom,
    track_qty, qty_left, stop_when_zero
  )
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
