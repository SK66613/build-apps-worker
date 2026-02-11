// src/services/salesSettings.ts
// Extract + persist sales_qr_one settings from constructor blueprint.

import type { Env } from "../index";

export type SalesSettings = {
  ttl_sec: number;
  cashback_percent: number;
  cashier1_tg_id: string;
  cashier2_tg_id: string;
  cashier3_tg_id: string;
  cashier4_tg_id: string;
  cashier5_tg_id: string;
};

export function extractSalesSettingsFromBlueprint(BP: any): SalesSettings {
  const out: SalesSettings = {
    ttl_sec: 300,
    cashback_percent: 10,
    cashier1_tg_id: "",
    cashier2_tg_id: "",
    cashier3_tg_id: "",
    cashier4_tg_id: "",
    cashier5_tg_id: "",
  };

  const routes = Array.isArray(BP && BP.routes) ? BP.routes : [];
  const blocksDict = (BP && BP.blocks && typeof BP.blocks === "object") ? BP.blocks : {};

  const insts: any[] = [];
  try {
    for (const rt of routes) {
      const blocks = (rt && Array.isArray(rt.blocks)) ? rt.blocks : [];
      for (const b of blocks) insts.push(b);
    }
  } catch (_) {}

  const getProps = (inst: any) => {
    if (!inst) return {};
    if (inst.props && typeof inst.props === "object") return inst.props;
    const id = inst.id != null ? String(inst.id) : "";
    const p = id && (blocksDict as any)[id];
    return (p && typeof p === "object") ? p : {};
  };

  const sq = insts.find((b) => b && (b.key === "sales_qr_one" || b.type === "sales_qr_one"));
  if (!sq) return out;

  const p: any = getProps(sq);
  const pick = (v: any) => String(v ?? "").trim();

  const ttl = Number(p.ttl_sec ?? 300);
  out.ttl_sec = Math.max(60, Math.min(600, Number.isFinite(ttl) ? ttl : 300));

  const cb = Number(p.cashback_percent ?? 10);
  out.cashback_percent = Math.max(0, Math.min(100, Number.isFinite(cb) ? cb : 10));

  out.cashier1_tg_id = pick(p.cashier1_tg_id);
  out.cashier2_tg_id = pick(p.cashier2_tg_id);
  out.cashier3_tg_id = pick(p.cashier3_tg_id);
  out.cashier4_tg_id = pick(p.cashier4_tg_id);
  out.cashier5_tg_id = pick(p.cashier5_tg_id);

  return out;
}

export async function upsertSalesSettings(appId: any, publicId: string, salesCfg: Partial<SalesSettings> | null, env: Env) {
  const db = env.DB;
  if (!db) return { ok: false, error: "DB_BINDING_MISSING" };

  const ttl = Math.max(60, Math.min(600, Number(salesCfg?.ttl_sec ?? 300)));
  const cb = Math.max(0, Math.min(100, Number(salesCfg?.cashback_percent ?? 10)));

  const pick = (v: any) => {
    const s = String(v ?? "").trim();
    return s ? s : null;
  };

  const c1 = pick(salesCfg?.cashier1_tg_id);
  const c2 = pick(salesCfg?.cashier2_tg_id);
  const c3 = pick(salesCfg?.cashier3_tg_id);
  const c4 = pick(salesCfg?.cashier4_tg_id);
  const c5 = pick(salesCfg?.cashier5_tg_id);

  await db
    .prepare(
      `
    INSERT INTO sales_settings
      (app_public_id, cashier1_tg_id, cashier2_tg_id, cashier3_tg_id, cashier4_tg_id, cashier5_tg_id,
       cashback_percent, ttl_sec, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(app_public_id) DO UPDATE SET
      cashier1_tg_id=excluded.cashier1_tg_id,
      cashier2_tg_id=excluded.cashier2_tg_id,
      cashier3_tg_id=excluded.cashier3_tg_id,
      cashier4_tg_id=excluded.cashier4_tg_id,
      cashier5_tg_id=excluded.cashier5_tg_id,
      cashback_percent=excluded.cashback_percent,
      ttl_sec=excluded.ttl_sec,
      updated_at=datetime('now')
  `
    )
    .bind(String(publicId), c1, c2, c3, c4, c5, cb, ttl)
    .run();

  return { ok: true };
}
