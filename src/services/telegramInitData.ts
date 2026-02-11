// src/services/telegramInitData.ts
// Telegram WebApp initData verification + helpers.

export async function verifyInitDataSignature(initData: string, botToken: string): Promise<boolean> {
  if (!initData || !botToken) return false;

  const params = new URLSearchParams(String(initData));
  const hash = params.get('hash');
  if (!hash) return false;
  params.delete('hash');

  // data_check_string: key=value lines sorted by key
  const arr: string[] = [];
  for (const [k, v] of params.entries()) arr.push(`${k}=${v}`);
  arr.sort();
  const dataCheckString = arr.join('\n');

  const enc = new TextEncoder();

  // secret_key = HMAC_SHA256(key="WebAppData", data=bot_token)
  const webAppKey = await crypto.subtle.importKey(
    'raw',
    enc.encode('WebAppData'),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );

  const secretKeyBuf = await crypto.subtle.sign('HMAC', webAppKey, enc.encode(botToken));

  // calc_hash = HMAC_SHA256(key=secret_key, data=data_check_string)
  const secretKey = await crypto.subtle.importKey(
    'raw',
    secretKeyBuf,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );

  const sig = await crypto.subtle.sign('HMAC', secretKey, enc.encode(dataCheckString));

  const calcHash = Array.from(new Uint8Array(sig))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');

  return calcHash === String(hash).toLowerCase();
}

export function parseInitDataUser(initData: string): any | null {
  try {
    const p = new URLSearchParams(String(initData || ''));
    const userRaw = p.get('user');
    if (!userRaw) return null;
    const u = JSON.parse(userRaw);
    if (!u || !u.id) return null;
    return u;
  } catch (_e) {
    return null;
  }
}
