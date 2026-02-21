// src/services/runtimeSync.ts
// Sync runtime lookup tables (wheel_prizes, styles_dict) from runtime config.
//
// Variant A (compromise):
// - D1 wheel_prizes is source of truth for LIVE fields (active/qty/stop/track...)
// - publish MUST NOT wipe live fields
// - publish updates ONLY structure fields (title/kind/coins/img/weight/cost_coins)
// - new prizes are INSERTed with defaults once (INSERT OR IGNORE)

import type { Env } from "../index";

async function getTableCols(db: any, table: string): Promise<Set<string>> {
  const res: any = await db.prepare(`PRAGMA table_info(${table})`).all();
  const cols = new Set<string>();
  for (const r of (res?.results || [])) cols.add(String(r.name || ""));
  return cols;
}

function toInt(v: any, d = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return d;
  return Math.trunc(n);
}

export async function syncRuntimeTablesFromConfig(
  appId: any,
  publicId: string,
  cfg: any,
  env: Env
) {
  const out: any = { wheelInserted: 0, stylesInserted: 0 };

  try {
    const db = env.DB;
    if (!db) return { ...out, error: "DB_BINDING_MISSING" };

    const wheelCols = await getTableCols(db, "wheel_prizes");

    // wheel spin cost (store in D1 if column exists)
    const spinCost = Math.max(0, toInt(cfg?.wheel?.spin_cost ?? 0, 0));

    // IMPORTANT: NO DELETE here (publish must not reset live fields)
    const prizes = cfg?.wheel && Array.isArray(cfg.wheel.prizes) ? cfg.wheel.prizes : [];

    for (const p of prizes) {
      const code = String(p?.code || "").trim();
      if (!code) continue;

      // ========= A) INSERT only if missing =========
      // live defaults are set ONLY once when prize first appears
      const insertRow: any = {
        app_id: appId,
        app_public_id: publicId,
        code,
        title: String(p?.title || p?.name || code).trim(),
      };

      // structure-ish columns
      if (wheelCols.has("kind")) insertRow.kind = String(p?.kind || "");
      if (wheelCols.has("coins")) insertRow.coins = Math.max(0, toInt(p?.coins || 0, 0));
      if (wheelCols.has("img")) insertRow.img = p?.img ? String(p.img) : null;

      // ✅ structure (from editor): weight
      if (wheelCols.has("weight")) insertRow.weight = Math.max(0, toInt(p?.weight || 0, 0));

      // ✅ structure (from editor): себестоимость физ. приза в монетах
      // (для coins-приза можно 0 — не используется)
      if (wheelCols.has("cost_coins")) insertRow.cost_coins = Math.max(0, toInt(p?.cost_coins ?? 0, 0));

      // live defaults (ONLY on first insert; publish will not update them later)
      if (wheelCols.has("active")) insertRow.active = p?.active === false ? 0 : 1;

      // inventory fields are LIVE (not overwritten on publish)
      if (wheelCols.has("track_qty"))
        insertRow.track_qty =
          p?.track_qty === true || Number(p?.track_qty || 0) === 1 ? 1 : 0;

      if (wheelCols.has("qty_left")) {
        const q = p?.qty_left ?? p?.stock_qty ?? 0;
        insertRow.qty_left = Math.max(0, toInt(q || 0, 0));
      }

      if (wheelCols.has("stop_when_zero")) {
        const swz = p?.stop_when_zero === undefined ? true : !!p.stop_when_zero;
        insertRow.stop_when_zero = swz ? 1 : 0;
      }

      {
        const baseAlways = new Set(["app_id", "app_public_id", "code", "title"]);
        const keys = Object.keys(insertRow).filter((k) => wheelCols.has(k) || baseAlways.has(k));
        const cols = keys.join(", ");
        const qs = keys.map(() => "?").join(", ");
        const vals = keys.map((k) => insertRow[k]);

        await db
          .prepare(`INSERT OR IGNORE INTO wheel_prizes (${cols}) VALUES (${qs})`)
          .bind(...vals)
          .run();
      }

      // ========= B) UPDATE ONLY STRUCTURE fields =========
      // publish updates only these fields; live fields are not overwritten
      {
        const sets: string[] = [];
        const vals: any[] = [];

        // title is structure
        sets.push(`title = ?`);
        vals.push(String(p?.title || p?.name || code).trim());

        // keep app_id up to date
        if (wheelCols.has("app_id")) {
          sets.push(`app_id = ?`);
          vals.push(appId);
        }

        if (wheelCols.has("kind")) {
          sets.push(`kind = ?`);
          vals.push(String(p?.kind || ""));
        }
        if (wheelCols.has("coins")) {
          sets.push(`coins = ?`);
          vals.push(Math.max(0, toInt(p?.coins || 0, 0)));
        }
        if (wheelCols.has("img")) {
          sets.push(`img = ?`);
          vals.push(p?.img ? String(p.img) : null);
        }

        // ✅ weight from editor (structure)
        if (wheelCols.has("weight")) {
          sets.push(`weight = ?`);
          vals.push(Math.max(0, toInt(p?.weight || 0, 0)));
        }

        // ✅ cost_coins from editor (structure)
        if (wheelCols.has("cost_coins")) {
          sets.push(`cost_coins = ?`);
          vals.push(Math.max(0, toInt(p?.cost_coins ?? 0, 0)));
        }

        vals.push(publicId, code);

        await db
          .prepare(
            `
            UPDATE wheel_prizes
            SET ${sets.join(", ")}
            WHERE app_public_id = ?
              AND code = ?
          `
          )
          .bind(...vals)
          .run();
      }

      out.wheelInserted++;
    }

    // Persist spin_cost into D1 (one value per app; duplicated across rows intentionally)
    if (wheelCols.has("spin_cost")) {
      await db
        .prepare(`UPDATE wheel_prizes SET spin_cost = ? WHERE app_public_id = ?`)
        .bind(spinCost, publicId)
        .run();
    }

    // styles_dict — как у тебя было (ok)
    await db.prepare(`DELETE FROM styles_dict WHERE app_public_id = ?`).bind(publicId).run();
    const styles = cfg?.passport && Array.isArray(cfg.passport.styles) ? cfg.passport.styles : [];
    for (const s of styles) {
      if (!s || s.active === false) continue;
      await db
        .prepare(`INSERT INTO styles_dict (app_id, app_public_id, style_id, title) VALUES (?, ?, ?, ?)`)
        .bind(appId, publicId, String(s.code || ""), String(s.name || s.code || ""))
        .run();
      out.stylesInserted++;
    }

    return out;
  } catch (e: any) {
    console.error("[syncRuntimeTablesFromConfig] failed", e);
    return { ...out, error: String(e?.message || e) };
  }
}
