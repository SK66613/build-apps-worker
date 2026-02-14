// src/handlers/telegram/stars.ts
import type { Env } from "../../index";
import { tgAnswerPreCheckoutQuery } from "../../services/telegramApi";

// 1:1 с оригиналом по смыслу
export async function handleStarsHooks(args: {
  env: Env;
  botToken: string;
  publicId: string; // appPublicId
  upd: any;
}): Promise<boolean> {
  const { env, botToken, publicId, upd } = args;

  // ===== A) pre_checkout_query =====
  const pcq = upd?.pre_checkout_query;
  if (pcq?.id) {
    const invPayload = String(pcq.invoice_payload || "");
    const orderId = invPayload.startsWith("order:") ? invPayload.slice(6) : "";

    let ok = true;
    let err = "";

    if (!orderId) {
      ok = false;
      err = "Bad payload";
    } else {
      try {
        const row = await env.DB.prepare(
          `SELECT id, status, total_stars
           FROM stars_orders
           WHERE id = ? AND app_public_id = ?
           LIMIT 1`
        ).bind(orderId, publicId).first();

        if (!row) { ok = false; err = "Order not found"; }
        else if (String((row as any).status) !== "created") { ok = false; err = "Order already processed"; }
      } catch (e) {
        ok = false;
        err = "DB error";
        console.error("[stars] pre_checkout_query db error", e);
      }
    }

    // Важно: отвечаем Telegram’у
    try {
      await tgAnswerPreCheckoutQuery(botToken, pcq.id, ok, err);
    } catch (e) {
      console.error("[stars] answerPreCheckoutQuery failed", e);
    }

    return true;
  }

  // ===== B) successful_payment =====
  const sp = upd?.message?.successful_payment;
  if (sp) {
    const invPayload = String(sp.invoice_payload || "");
    const orderId = invPayload.startsWith("order:") ? invPayload.slice(6) : "";

    if (orderId) {
      try {
        await env.DB.prepare(
          `UPDATE stars_orders
           SET status = 'paid',
               paid_at = datetime('now'),
               telegram_payment_charge_id = ?,
               provider_payment_charge_id = ?,
               paid_total_amount = ?
           WHERE id = ? AND app_public_id = ?`
        ).bind(
          String(sp.telegram_payment_charge_id || ""),
          String(sp.provider_payment_charge_id || ""),
          Number(sp.total_amount || 0),
          orderId,
          publicId
        ).run();
      } catch (e) {
        console.error("[stars] successful_payment db update failed", e);
        // Telegram уже прислал successful_payment — просто логируем и не падаем
      }
    }

    return true;
  }

  return false;
}
