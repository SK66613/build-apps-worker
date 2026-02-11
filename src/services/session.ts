// src/services/session.ts
// Minimal cookie session (JWT HS256)
function base64UrlEncode(bytes: Uint8Array): string {
  // btoa expects binary string
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  const b64 = btoa(bin);
  return b64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlDecode(b64url: string): Uint8Array {
  const b64 = b64url.replace(/-/g, "+").replace(/_/g, "/");
  const pad = b64.length % 4 === 0 ? "" : "=".repeat(4 - (b64.length % 4));
  const bin = atob(b64 + pad);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export async function signToken(payload: any, secret: string): Promise<string> {
  const enc = new TextEncoder();
  const header = { alg: "HS256", typ: "JWT" };
  const headerBytes = enc.encode(JSON.stringify(header));
  const payloadBytes = enc.encode(JSON.stringify(payload));

  const headerB64 = base64UrlEncode(headerBytes);
  const payloadB64 = base64UrlEncode(payloadBytes);

  const data = headerB64 + "." + payloadB64;

  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(data));
  const sigB64 = base64UrlEncode(new Uint8Array(sig));

  return data + "." + sigB64;
}

export async function verifyToken(token: string, secret: string): Promise<any | null> {
  if (!token) return null;
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  const [headerB64, payloadB64, sigB64] = parts;
  const enc = new TextEncoder();
  const data = headerB64 + "." + payloadB64;

  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const expectedSig = await crypto.subtle.sign("HMAC", key, enc.encode(data));
  const expectedSigB64 = base64UrlEncode(new Uint8Array(expectedSig));

  if (expectedSigB64 !== sigB64) return null;

  try {
    const bytes = base64UrlDecode(payloadB64);
    const jsonStr = new TextDecoder().decode(bytes);
    const payload = JSON.parse(jsonStr);
    if (payload.exp && Math.floor(Date.now() / 1000) > payload.exp) {
      return null;
    }
    return payload;
  } catch (e) {
    console.error("[auth] verifyToken JSON error", e);
    return null;
  }
}

function getSessionTokenFromRequest(request) {
  const cookie = request.headers.get("Cookie") || "";
  const m = cookie.match(/(?:^|;\s*)sg_session=([^;]+)/);
  return m ? m[1] : null;
}

export async function getSession(request, env) {
  const secret = env.SESSION_SECRET;
  if (!secret || secret.length < 16) {
    return null;
  }
  const token = getSessionTokenFromRequest(request);
  if (!token) return null;
  try {
    return await verifyToken(token, secret);
  } catch (e) {
    console.error("[auth] getSession verify error", e);
    return null;
  }
}

export async function requireSession(request, env){
  const s = await getSession(request, env);
  if (!s || !s.uid) return null;
  return s;
}

export async function createSessionCookie(user, env) {
  const secret = env.SESSION_SECRET;
  if (!secret || secret.length < 16) {
    console.error("[auth] SESSION_SECRET missing or too short");
    return null;
  }
  const now = Math.floor(Date.now() / 1000);
  const exp = now + 60 * 60 * 24 * 30;
  const payload = {
    uid: user.id,
    email: user.email,
    exp,
  };
  const token = await signToken(payload, secret);
  const maxAge = 60 * 60 * 24 * 30;
  return "sg_session=" + token
  + "; Path=/; Domain=.salesgenius.ru; HttpOnly; Secure; SameSite=None; Max-Age=" + maxAge;

}
