// src/services/crypto.ts
// AES-GCM (SHA-256 derived key) for secrets in KV
export async function encryptToken(plain: string, masterKey: string): Promise<string> {
  const enc = new TextEncoder();
  const masterBytes = enc.encode(masterKey);
  const keyBytes = await crypto.subtle.digest('SHA-256', masterBytes);

  const key = await crypto.subtle.importKey(
    'raw',
    keyBytes,
    { name: 'AES-GCM' },
    false,
    ['encrypt']
  );

  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv },
    key,
    enc.encode(plain)
  );

  const buf = new Uint8Array(iv.byteLength + ciphertext.byteLength);
  buf.set(iv, 0);
  buf.set(new Uint8Array(ciphertext), iv.byteLength);

  return btoa(String.fromCharCode(...buf));
}

export async function decryptToken(cipherText: string, masterKey: string): Promise<string | null> {
  const raw = Uint8Array.from(atob(cipherText), c => c.charCodeAt(0));
  const iv = raw.slice(0, 12);
  const data = raw.slice(12);

  const enc = new TextEncoder();
  const masterBytes = enc.encode(masterKey);
  const keyBytes = await crypto.subtle.digest('SHA-256', masterBytes);

  const key = await crypto.subtle.importKey(
    'raw',
    keyBytes,
    { name: 'AES-GCM' },
    false,
    ['decrypt']
  );

  const plain = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv },
    key,
    data
  );

  return new TextDecoder().decode(plain);
}