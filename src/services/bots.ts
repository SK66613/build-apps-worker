// src/services/bots.ts
import type { Env } from "../index";
import { decryptToken, encryptToken } from "./crypto";

export async function getBotTokenForApp(publicId: string, env: Env, appIdFallback: any = null) {
  if (!env.BOT_SECRETS || !env.BOT_TOKEN_KEY) return null;

  const tryGet = async (key: string) => {
    const raw = await env.BOT_SECRETS.get(key);
    if (!raw) return null;

    let cipher = raw;
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object" && parsed.cipher) cipher = parsed.cipher;
    } catch (_) {}

    try {
      return await decryptToken(cipher, env.BOT_TOKEN_KEY);
    } catch (e) {
      console.error("[botToken] decrypt error for key", key, e);
      return null;
    }
  };

  const tok1 = await tryGet("bot_token:public:" + publicId);
  if (tok1) return tok1;

  if (appIdFallback) {
    const tok2 = await tryGet("bot_token:app:" + appIdFallback);
    if (tok2) return tok2;
  }

  return null;
}

export function timingSafeEqual(a, b) {
  a = String(a || '');
  b = String(b || '');
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i++) out |= (a.charCodeAt(i) ^ b.charCodeAt(i));
  return out === 0;
}

function randomSecret(len = 24) {
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

export async function getBotWebhookSecretForPublicId(publicId, env) {
  if (!env.BOT_SECRETS || !env.BOT_TOKEN_KEY) return null;

  const raw = await env.BOT_SECRETS.get('bot_whsec:public:' + publicId);
  if (!raw) return null;

  let cipher = raw;
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object' && parsed.cipher) cipher = parsed.cipher;
  } catch (_) {}

  try {
    return await decryptToken(cipher, env.BOT_TOKEN_KEY);
  } catch (e) {
    console.error('[botWhSec] decrypt error for publicId', publicId, e);
    return null;
  }
}

export async function ensureBotWebhookSecretForPublicId(publicId, env) {
  const existing = await getBotWebhookSecretForPublicId(publicId, env);
  if (existing) {
    return { secret: existing, created: false, kv_key: 'bot_whsec:public:' + publicId };
  }

  const secretPlain = randomSecret(24);
  const cipher = await encryptToken(secretPlain, env.BOT_TOKEN_KEY);

  const kvKey = 'bot_whsec:public:' + publicId;
  await env.BOT_SECRETS.put(kvKey, JSON.stringify({ cipher }));

  return { secret: secretPlain, created: true, kv_key: kvKey };
}
