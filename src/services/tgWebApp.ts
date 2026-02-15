// src/services/tgWebApp.ts
// Verify Telegram WebApp initData signature (HMAC SHA-256).

function safeHexEqual(a: string, b: string): boolean {
  const aa = String(a || "").toLowerCase();
  const bb = String(b || "").toLowerCase();
  if (aa.length !== bb.length) return false;

  let diff = 0;
  for (let i = 0; i < aa.length; i++) diff |= aa.charCodeAt(i) ^ bb.charCodeAt(i);
  return diff === 0;
}

export async function verifyInitDataSignature(initData: string, botToken: string) {
  if (!initData || !botToken) return false;

  const params = new URLSearchParams(initData);
  const hash = params.get("hash");
  if (!hash) return false;
  params.delete("hash");

  const arr: string[] = [];
  for (const [k, v] of params.entries()) arr.push(`${k}=${v}`);
  arr.sort();
  const dataCheckString = arr.join("\n");

  const enc = new TextEncoder();

  // secret_key = HMAC_SHA256(key="WebAppData", data=bot_token)
  const webAppKey = await crypto.subtle.importKey(
    "raw",
    enc.encode("WebAppData"),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const secretKeyBuf = await crypto.subtle.sign("HMAC", webAppKey, enc.encode(botToken));

  // calc_hash = HMAC_SHA256(key=secret_key, data=data_check_string)
  const secretKey = await crypto.subtle.importKey(
    "raw",
    secretKeyBuf,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const sig = await crypto.subtle.sign("HMAC", secretKey, enc.encode(dataCheckString));

  const calcHash = Array.from(new Uint8Array(sig))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  return safeHexEqual(calcHash, hash);
}
