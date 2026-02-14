// src/handlers/telegram/stateLite.ts
// Minimal state for /profile (вынесено из монолита 1-в-1 по логике)

export async function buildStateLite(db: any, appId: any, appPublicId: string, tgId: string, cfg: any = {}) {
  const out: any = {
    coins: 0,
    styles_count: 0,
    styles_total: 0,
    game_today_best: 0,
    ref_total: 0,
    config: cfg || {},
  };

  // coins
  try {
    const u = await db
      .prepare(`SELECT coins FROM app_users WHERE app_public_id = ? AND tg_user_id = ? LIMIT 1`)
      .bind(String(appPublicId), String(tgId))
      .first();
    out.coins = u ? Number((u as any).coins || 0) : 0;
  } catch (_) {
    out.coins = 0;
  }

  // styles_count
  try {
    const r = await db
      .prepare(
        `SELECT COUNT(DISTINCT style_id) AS c
         FROM styles_user
         WHERE app_public_id = ? AND tg_id = ? AND status = 'collected'`
      )
      .bind(String(appPublicId), String(tgId))
      .first();
    out.styles_count = r ? Number((r as any).c || 0) : 0;
  } catch (_) {
    out.styles_count = 0;
  }

  // styles_total
  try {
    const r = await db
      .prepare(`SELECT COUNT(*) AS c FROM styles_dict WHERE app_public_id = ?`)
      .bind(String(appPublicId))
      .first();
    out.styles_total = r ? Number((r as any).c || 0) : 0;
  } catch (_) {
    out.styles_total = 0;
  }

  // game_today_best (daily)
  try {
    const today = new Date().toISOString().slice(0, 10);
    const g = await db
      .prepare(
        `SELECT best_score
         FROM games_results_daily
         WHERE app_public_id = ? AND date = ? AND mode = 'daily' AND tg_id = ?
         ORDER BY id DESC LIMIT 1`
      )
      .bind(String(appPublicId), String(today), String(tgId))
      .first();
    out.game_today_best = g ? Number((g as any).best_score || 0) : 0;
  } catch (_) {
    out.game_today_best = 0;
  }

  // ref_total
  try {
    const r = await db
      .prepare(`SELECT COUNT(*) AS c FROM referrals WHERE app_public_id = ? AND referrer_tg_id = ?`)
      .bind(String(appPublicId), String(tgId))
      .first();
    out.ref_total = r ? Number((r as any).c || 0) : 0;
  } catch (_) {
    out.ref_total = 0;
  }

  return out;
}
