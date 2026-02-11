// src/services/botToken.ts

import type { Env } from '../index';
import { decryptToken } from './crypto';

export async function getBotTokenForApp(publicId: string, env: Env, appIdFallback: string | number | null = null): Promise<string | null> {
  if (!env.BOT_SECRETS || !env.BOT_TOKEN_KEY) return null;

  const tryGet = async (key: string): Promise<string | null> => {
    const raw = await env.BOT_SECRETS.get(key);
    if (!raw) return null;

    let cipher = raw;
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object' && parsed.cipher) cipher = parsed.cipher;
    } catch (_e) {}

    try {
      return await decryptToken(cipher, env.BOT_TOKEN_KEY);
    } catch (e) {
      console.error('[botToken] decrypt error for key', key, e);
      return null;
    }
  };

  // Canonical storage
  const tok1 = await tryGet('bot_token:public:' + publicId);
  if (tok1) return tok1;

  // Legacy fallback
  if (appIdFallback) {
    const tok2 = await tryGet('bot_token:app:' + String(appIdFallback));
    if (tok2) return tok2;
  }

  return null;
}
