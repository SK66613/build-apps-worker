// src/routes/cabinet.ts
// Cabinet (cookie session) API router.

import type { Env } from "../index";
import {
  requireSession,
  ensureAppOwner,
  listMyApps,
  createApp,
  getApp,
  saveApp,
  deleteApp,
  publishApp,
  getBotIntegration,
  saveBotIntegration,
  deleteBotIntegration,
  listBroadcasts,
  createAndSendBroadcast,
  listDialogs,
  getDialogMessages,
  sendDialogMessage,
} from "../handlers/cabinetApiHandlers";
import { json } from "../utils/http";

export async function routeCabinet(request: Request, env: Env, url: URL): Promise<Response | null> {
  const p = url.pathname;

  // ===== constructor blueprint config (DRAFT KV) =====
  // GET/PUT /api/app/:id/config
  const mAppCfg = p.match(/^\/api\/app\/([^/]+)\/config$/);
  if (mAppCfg) {
    const appId = decodeURIComponent(mAppCfg[1]);

    const s = await requireSession(request as any, env as any);
    if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);

    const ownerCheck = await ensureAppOwner(appId, s.uid, env as any);
    if (!ownerCheck.ok)
      return json({ ok: false, error: "FORBIDDEN" }, ownerCheck.status || 403, request);

    const metaKey = "app:" + appId;
    const draftKey = "app:draft:" + appId;

    // ===== GET draft blueprint =====
    if (request.method === "GET") {
      // 1️⃣ сначала пробуем новый draft
      const draft = await env.APPS.get(draftKey, "json");
      if (draft) {
        return json({ ok: true, config: draft }, 200, request);
      }

      // 2️⃣ fallback — старый config (для старых приложений)
      const appObj = (await env.APPS.get(metaKey, "json")) || {};
      return json({ ok: true, config: (appObj as any).config ?? null }, 200, request);
    }

    // ===== SAVE draft blueprint =====
    if (request.method === "PUT") {
      const body: any = await request.json().catch(() => ({}));
      const bp = body?.config || body?.blueprint || body?.bp || null;

      // записываем ТОЛЬКО draft
      await env.APPS.put(draftKey, JSON.stringify(bp));

      // обновляем timestamp meta (не трогаем config!)
      const appObj = (await env.APPS.get(metaKey, "json")) || {};
      (appObj as any).updatedAt = new Date().toISOString();
      await env.APPS.put(metaKey, JSON.stringify(appObj));

      return json({ ok: true }, 200, request);
    }

    return json({ ok: false, error: "METHOD_NOT_ALLOWED" }, 405, request);
  }


  // ===== apps list =====
  if ((p === "/api/my/apps" || p === "/api/apps") && request.method === "GET") {
    const s = await requireSession(request as any, env as any);
    if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);
    return listMyApps(env as any, s.uid, request);
  }

  // ===== create app =====
  if (p === "/api/app" && request.method === "POST") {
    const s = await requireSession(request as any, env as any);
    if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);
    return createApp(request as any, env as any, url as any, s.uid);
  }

  // ===== app CRUD =====
  const mApp = p.match(/^\/api\/app\/([^/]+)$/);
  if (mApp) {
    const appId = decodeURIComponent(mApp[1]);
    const s = await requireSession(request as any, env as any);
    if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);

    const ownerCheck = await ensureAppOwner(appId, s.uid, env as any);
    if (!ownerCheck.ok)
      return json({ ok: false, error: "FORBIDDEN" }, ownerCheck.status, request);

    if (request.method === "GET") return getApp(appId, env as any, request);
    if (request.method === "PUT") return saveApp(appId, request as any, env as any);
    if (request.method === "DELETE") return deleteApp(appId, env as any, request);

    return json({ ok: false, error: "METHOD_NOT_ALLOWED" }, 405, request);
  }

  // ===== publish =====
  const mPub = p.match(/^\/api\/app\/([^/]+)\/publish$/);
  if (mPub && request.method === "POST") {
    const appId = decodeURIComponent(mPub[1]);

    const s = await requireSession(request as any, env as any);
    if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);

    const ownerCheck = await ensureAppOwner(appId, s.uid, env as any);
    if (!ownerCheck.ok)
      return json({ ok: false, error: "FORBIDDEN" }, ownerCheck.status, request);

    // publishApp внутри уже копирует draft -> live
    return publishApp(appId, env as any, url as any, request);
  }

  // ===== bot integration =====
  const mBot = p.match(/^\/api\/app\/([^/]+)\/bot$/);
  if (mBot) {
    const appId = decodeURIComponent(mBot[1]);
    const s = await requireSession(request as any, env as any);
    if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);

    const ownerCheck = await ensureAppOwner(appId, s.uid, env as any);
    if (!ownerCheck.ok)
      return json({ ok: false, error: "FORBIDDEN" }, ownerCheck.status, request);

    if (request.method === "GET")
      return getBotIntegration(appId, env as any, s.uid, request);

    if (request.method === "PUT") {
      const body = await request.json().catch(() => ({}));
      return saveBotIntegration(appId, env as any, body, s.uid, request);
    }

    if (request.method === "DELETE") {
      const body = await request.json().catch(() => ({}));
      if (body.action === "unlink")
        return deleteBotIntegration(appId, env as any, s.uid, request);
      return json({ ok: false, error: "METHOD_NOT_ALLOWED" }, 405, request);
    }

    return json({ ok: false, error: "METHOD_NOT_ALLOWED" }, 405, request);
  }

  // ===== broadcasts =====
  const mBcList = p.match(/^\/api\/app\/([^/]+)\/broadcasts$/);
  if (mBcList && request.method === "GET") {
    const appId = decodeURIComponent(mBcList[1]);
    const s = await requireSession(request as any, env as any);
    if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);

    const ownerCheck = await ensureAppOwner(appId, s.uid, env as any);
    if (!ownerCheck.ok)
      return json({ ok: false, error: "FORBIDDEN" }, ownerCheck.status, request);

    return listBroadcasts(appId, env as any, s.uid, request);
  }

  const mBcSend = p.match(/^\/api\/app\/([^/]+)\/broadcast$/);
  if (mBcSend && request.method === "POST") {
    const appId = decodeURIComponent(mBcSend[1]);
    const s = await requireSession(request as any, env as any);
    if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);

    const ownerCheck = await ensureAppOwner(appId, s.uid, env as any);
    if (!ownerCheck.ok)
      return json({ ok: false, error: "FORBIDDEN" }, ownerCheck.status, request);

    return createAndSendBroadcast(appId, env as any, s.uid, request);
  }

  // ===== dialogs =====
  const mDlgList = p.match(/^\/api\/app\/([^/]+)\/dialogs$/);
  if (mDlgList && request.method === "GET") {
    const appId = decodeURIComponent(mDlgList[1]);
    const s = await requireSession(request as any, env as any);
    if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);

    const ownerCheck = await ensureAppOwner(appId, s.uid, env as any);
    if (!ownerCheck.ok)
      return json({ ok: false, error: "FORBIDDEN" }, ownerCheck.status, request);

    return listDialogs(appId, env as any, s.uid, request);
  }

  const mDlg = p.match(/^\/api\/app\/([^/]+)\/dialog\/([^/]+)$/);
  if (mDlg) {
    const appId = decodeURIComponent(mDlg[1]);
    const tgUserId = decodeURIComponent(mDlg[2]);
    const s = await requireSession(request as any, env as any);
    if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);

    const ownerCheck = await ensureAppOwner(appId, s.uid, env as any);
    if (!ownerCheck.ok)
      return json({ ok: false, error: "FORBIDDEN" }, ownerCheck.status, request);

    if (request.method === "GET")
      return getDialogMessages(appId, tgUserId, env as any, s.uid, request);

    if (request.method === "POST")
      return sendDialogMessage(appId, tgUserId, env as any, s.uid, request);

    return json({ ok: false, error: "METHOD_NOT_ALLOWED" }, 405, request);
  }

  return null;
}
