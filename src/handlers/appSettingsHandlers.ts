// src/handlers/appSettingsHandlers.ts
import type { Env } from "../index";
import { json } from "../utils/http";

export async function handleGetAppSettings(env: Env, appPublicId: string) {
  const row = await env.DB
    .prepare(`SELECT coin_value_cents, currency
              FROM app_settings
              WHERE app_public_id = ?`)
    .bind(appPublicId)
    .first();

  if (!row) {
    return json({
      ok: true,
      settings: {
        coin_value_cents: 100,
        currency: "RUB",
      },
    });
  }

  return json({
    ok: true,
    settings: row,
  });
}

export async function handleSetAppSettings(
  env: Env,
  appPublicId: string,
  body: any
) {
  const coin = Number(body?.settings?.coin_value_cents);
  const currency = String(body?.settings?.currency || "RUB")
    .toUpperCase()
    .slice(0, 8);

  if (!Number.isFinite(coin) || coin <= 0) {
    return json({ ok: false, error: "INVALID_COIN_VALUE" }, 400);
  }

  await env.DB
    .prepare(`
      INSERT INTO app_settings (app_public_id, coin_value_cents, currency, updated_at)
      VALUES (?, ?, ?, strftime('%s','now'))
      ON CONFLICT(app_public_id)
      DO UPDATE SET
        coin_value_cents = excluded.coin_value_cents,
        currency = excluded.currency,
        updated_at = strftime('%s','now')
    `)
    .bind(appPublicId, Math.floor(coin), currency)
    .run();

  return json({ ok: true });
}
