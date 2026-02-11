// src/handlers/authHandlers.ts
import type { Env } from "../index";
import { json } from "../utils/http";
import { createSessionCookie, getSession } from "../services/session";

async function hashPassword(password, salt) {
  const enc = new TextEncoder();
  const data = enc.encode(salt + ":" + password);
  const hash = await crypto.subtle.digest("SHA-256", data);
  const bytes = new Uint8Array(hash);
  let hex = "";
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i].toString(16).padStart(2, "0");
  }
  return hex;
}

function generateSalt(len = 16) {
  const buf = new Uint8Array(len);
  crypto.getRandomValues(buf);
  let hex = "";
  for (let i = 0; i < buf.length; i++) {
    hex += buf[i].toString(16).padStart(2, "0");
  }
  return hex;
}

function randomHex(len = 32) {
  const bytesCount = Math.ceil(len / 2);
  const arr = new Uint8Array(bytesCount);
  crypto.getRandomValues(arr);
  return Array.from(arr, b => b.toString(16).padStart(2, '0')).join('').slice(0, len);
}

// [signToken] moved to src/services/*

// [verifyToken] moved to src/services/*

async function sendVerificationEmail(email, confirmUrl, env) {
  console.log("[auth] sendVerificationEmail to", email, "url:", confirmUrl);
}

// POST /api/auth/register
export async function handleRegister(request, env, url) {
  const db = env.DB;

  let body;
  try {
    body = await request.json();
  } catch (e) {
    return json({ ok: false, error: 'BAD_JSON' }, 400, request);
  }

  const email = String(body.email || '').trim().toLowerCase();
  const password = String(body.password || '').trim();

  if (!email || !email.includes('@') || !password || password.length < 6) {
    return json({ ok: false, error: 'BAD_INPUT' }, 400, request);
  }

  let existing = await db
    .prepare('SELECT id, is_verified FROM users WHERE email = ?')
    .bind(email)
    .first();

  let userId;

  if (!existing) {
    const salt = generateSalt();
    const hash = await hashPassword(password, salt);

    await db
      .prepare(
        `INSERT INTO users (email, password_hash, salt, is_verified, created_at, updated_at)
         VALUES (?, ?, ?, 0, datetime('now'), datetime('now'))`
      )
      .bind(email, hash, salt)
      .run();

    existing = await db
      .prepare('SELECT id, is_verified FROM users WHERE email = ?')
      .bind(email)
      .first();

    if (!existing || !existing.id) {
      console.error('[auth] cannot reselect user after insert for', email, existing);
      return json({ ok: false, error: 'NO_USER_ID' }, 500, request);
    }

    userId = existing.id;
  } else {
    userId = existing.id;
    if (existing.is_verified) {
      return json({ ok: false, error: 'ALREADY_VERIFIED' }, 400, request);
    }
  }

  const token = randomHex(32);

  await db
    .prepare(
      `INSERT INTO email_verifications (user_id, token, created_at, expires_at)
       VALUES (?, ?, datetime('now'), datetime('now', '+1 day'))`
    )
    .bind(userId, token)
    .run();

  const base = url.origin;
  const confirmUrl = `${base}/api/auth/confirm?token=${encodeURIComponent(token)}`;

  await sendVerificationEmail(email, confirmUrl, env);

  return json({ ok: true, needVerify: true }, 200, request);
}

// GET /api/auth/confirm?token=...
export async function handleConfirmEmail(url, env, request) {
  const db = env.DB;
  if (!db) {
    return json({ ok: false, error: "NO_DB" }, 500, request);
  }

  const token = (url.searchParams.get("token") || "").trim();
  if (!token) {
    return json({ ok: false, error: "NO_TOKEN" }, 400, request);
  }

  const row = await db
    .prepare(
      "SELECT ev.id, ev.user_id " +
      "FROM email_verifications ev " +
      "JOIN users u ON u.id = ev.user_id " +
      "WHERE ev.token = ? " +
      "  AND ev.used_at IS NULL " +
      "  AND ev.expires_at > datetime('now') " +
      "ORDER BY ev.created_at DESC " +
      "LIMIT 1"
    )
    .bind(token)
    .first();

  if (!row) {
    return json({ ok: false, error: "TOKEN_INVALID_OR_EXPIRED" }, 400, request);
  }

  await db
    .prepare(
      "UPDATE users SET is_verified = 1, updated_at = datetime('now') WHERE id = ?"
    )
    .bind(row.user_id)
    .run();

  await db
    .prepare(
      "UPDATE email_verifications SET used_at = datetime('now') WHERE id = ?"
    )
    .bind(row.id)
    .run();

  return json({ ok: true }, 200, request);
}

// POST /api/auth/login
export async function handleLogin(request, env) {
  const db = env.DB;
  if (!db) {
    return json({ ok: false, error: "NO_DB" }, 500, request);
  }

  let body;
  try {
    body = await request.json();
  } catch (e) {
    return json({ ok: false, error: "BAD_JSON" }, 400, request);
  }

  let email = (body.email || "").trim().toLowerCase();
  const password = String(body.password || "");

  if (!email || !password) {
    return json({ ok: false, error: "EMAIL_OR_PASSWORD_MISSING" }, 400, request);
  }

  const user = await db
    .prepare(
      "SELECT id, email, password_hash, salt, is_verified " +
      "FROM users WHERE email = ?"
    )
    .bind(email)
    .first();

  if (!user || !user.password_hash || !user.salt) {
    return json({ ok: false, error: "BAD_CREDENTIALS" }, 400, request);
  }

  const hash = await hashPassword(password, user.salt);
  if (hash !== user.password_hash) {
    return json({ ok: false, error: "BAD_CREDENTIALS" }, 400, request);
  }

  // if (!user.is_verified) {
  //   return json({ ok: false, error: "EMAIL_NOT_VERIFIED" }, 400, request);
  // }

  const cookie = await createSessionCookie(
    { id: user.id, email: user.email },
    env
  );
  if (!cookie) {
    return json({ ok: false, error: "NO_SESSION_SECRET" }, 500, request);
  }

  const resp = json({ ok: true, userId: user.id, email: user.email }, 200, request);
  resp.headers.append("Set-Cookie", cookie);
  return resp;
}

// POST /api/auth/logout
export async function handleLogout(request) {
  const resp = json({ ok: true }, 200, request);
  resp.headers.append(
    "Set-Cookie",
    "sg_session=; Path=/; Domain=.salesgenius.ru; HttpOnly; Secure; SameSite=None; Max-Age=0"
  );
  
  return resp;
}

// GET /api/auth/me
export async function handleMe(request, env) {
  const db = env.DB;
  if (!db) {
    return json({ ok: false, error: "NO_DB" }, 500, request);
  }
  const session = await getSession(request, env);
  if (!session) {
    return json({ ok: true, authenticated: false }, 200, request);
  }

  const user = await db
    .prepare("SELECT id, email, is_verified, created_at FROM users WHERE id = ?")
    .bind(session.uid)
    .first();

  if (!user) {
    return json({ ok: true, authenticated: false }, 200, request);
  }

  return json({
    ok: true,
    authenticated: true,
    user: {
      id: user.id,
      email: user.email,
      is_verified: !!user.is_verified,
      created_at: user.created_at,
    },
  }, 200, request);
}

// ================== APPS: D1 + KV ==================

function slugify(input, maxLen = 64){
  const s = String(input || '').trim().toLowerCase();
  const out = s
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, maxLen);
  return out;
}

