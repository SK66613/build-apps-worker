// src/services/telegramSend.ts
export async function tgSendMessage(
  env: any,
  botToken: string,
  chatId: string,
  text: string,
  extra: any = {},
  _logCtx: any = null
): Promise<Response> {
  const payload: any = {
    chat_id: chatId,
    text: String(text || ""),
    parse_mode: "HTML",
    disable_web_page_preview: true,
    ...extra,
  };

  return await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}
