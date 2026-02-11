// src/services/telegramApi.ts

export async function tgApi(botToken: string, method: string, payload: any): Promise<any> {
  const url = `https://api.telegram.org/bot${botToken}/${method}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const j = await r.json().catch(() => null);
  if (!r.ok || !j || !j.ok) {
    throw new Error(`[tgApi] ${method} failed: HTTP ${r.status} ` + (j ? JSON.stringify(j) : 'nojson'));
  }
  return j.result;
}

export async function tgCreateInvoiceLinkStars(
  botToken: string,
  args: { title?: string; description?: string; payload: string; stars: number; photo_url?: string }
): Promise<string> {
  const data: any = {
    title: String(args.title || 'Покупка'),
    description: String(args.description || ''),
    payload: String(args.payload || ''),
    currency: 'XTR',
    prices: [{ label: 'Итого', amount: Math.max(1, Math.floor(Number(args.stars || 0))) }],
  };
  if (args.photo_url) data.photo_url = String(args.photo_url);
  return await tgApi(botToken, 'createInvoiceLink', data);
}

// Telegram Stars payments: answer pre-checkout query.
// Must be called quickly; Telegram expects an answer.
export async function tgAnswerPreCheckoutQuery(
  botToken: string,
  preCheckoutQueryId: string,
  ok: boolean,
  errorMessage: string = ''
): Promise<void> {
  const payload: any = { pre_checkout_query_id: String(preCheckoutQueryId || ''), ok: !!ok };
  if (!ok) payload.error_message = String(errorMessage || 'Ошибка');
  try {
    await tgApi(botToken, 'answerPreCheckoutQuery', payload);
  } catch (e) {
    // Do not throw: webhook must still respond 200 to Telegram.
    console.error('[tgAnswerPreCheckoutQuery] failed', e);
  }
}
