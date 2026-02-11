// src/routes/analytics.ts
// Back-compat + analytics endpoints under /api/cabinet/apps/:appId/*

import type { Env } from "../index";
import {
  requireSession,
  ensureAppOwner,
  listDialogs,
  getDialogMessages,
  sendDialogMessage,
  listBroadcasts,
  createAndSendBroadcast,
} from "../handlers/cabinetApiHandlers";
import {
  handleCabinetOverview,
  handleCabinetProfit,
  handleCabinetWheelStats,
  handleCabinetSummary,
  handleCabinetActivity,
  handleCabinetCustomers,
  handleCabinetSalesStats,
  handleCabinetPassportStats,
  handleCabinetCalendarBookings,
  handleCabinetProfitReport,
  handleCabinetWheelPrizesGet,
  handleCabinetWheelPrizesUpdate,
} from "../handlers/analyticsHandlers";
import { json } from "../utils/http";

async function mustSession(request: Request, env: Env): Promise<any | null> {
  const s = await requireSession(request as any, env as any);
  return s || null;
}

async function mustOwner(appId: string, uid: string, env: Env, request: Request): Promise<Response | null> {
  const ownerCheck = await ensureAppOwner(appId, uid, env as any);
  if (!ownerCheck.ok) {
    return json({ ok: false, error: "FORBIDDEN" }, ownerCheck.status || 403, request);
  }
  return null;
}

export async function routeAnalytics(request: Request, env: Env, url: URL): Promise<Response | null> {
  const p = url.pathname;

  // /api/cabinet/apps/:appId/*
  const base = p.match(/^\/api\/cabinet\/apps\/([^/]+)(?:\/(.*))?$/);
  if (!base) return null;

  const appId = decodeURIComponent(base[1]);
  const tail = base[2] || "";

  const s = await mustSession(request, env);
  if (!s) return json({ ok: false, error: "UNAUTHORIZED" }, 401, request);

  const forb = await mustOwner(appId, s.uid, env, request);
  if (forb) return forb;

  // ===== dialogs (legacy compat) =====
  // GET /api/cabinet/apps/:appId/dialogs?range=today|7d|30d|all&q=...
  if (tail === "dialogs" && request.method === "GET") {
    return listDialogs(appId, env as any, s.uid, request as any);
  }

  // GET|POST /api/cabinet/apps/:appId/dialog/:tgUserId
  const mDlg = tail.match(/^dialog\/([^/]+)$/);
  if (mDlg) {
    const tgUserId = decodeURIComponent(mDlg[1]);
    if (request.method === "GET") return getDialogMessages(appId, tgUserId, env as any, s.uid, request as any);
    if (request.method === "POST") return sendDialogMessage(appId, tgUserId, env as any, s.uid, request as any);
    return json({ ok: false, error: "METHOD_NOT_ALLOWED" }, 405, request);
  }

  // ===== broadcasts (legacy compat) =====
  // GET /api/cabinet/apps/:appId/broadcasts/campaigns
  if (tail === "broadcasts/campaigns" && request.method === "GET") {
    return listBroadcasts(appId, env as any, s.uid, request as any);
  }

  // POST /api/cabinet/apps/:appId/broadcasts/send
  if (tail === "broadcasts/send" && request.method === "POST") {
    return createAndSendBroadcast(appId, env as any, s.uid, request as any);
  }

  // ===== analytics =====
  // GET /api/cabinet/apps/:appId/overview
  if (tail === "overview" && request.method === "GET") {
    return handleCabinetOverview(appId, request as any, env as any);
  }

  // GET /api/cabinet/apps/:appId/profit
  if (tail === "profit" && request.method === "GET") {
    return handleCabinetProfit(appId, request as any, env as any);
  }

  // GET /api/cabinet/apps/:appId/wheel/stats
  if (tail === "wheel/stats" && request.method === "GET") {
    return handleCabinetWheelStats(appId, request as any, env as any, s.uid);
  }

  // GET /api/cabinet/apps/:appId/summary
  if (tail === "summary" && request.method === "GET") {
    return handleCabinetSummary(appId, request as any, env as any, s.uid);
  }

  // GET /api/cabinet/apps/:appId/activity
  if (tail === "activity" && request.method === "GET") {
    return handleCabinetActivity(appId, request as any, env as any, s.uid);
  }

  // GET /api/cabinet/apps/:appId/customers
  if (tail === "customers" && request.method === "GET") {
    return handleCabinetCustomers(appId, request as any, env as any, s.uid);
  }

  // GET /api/cabinet/apps/:appId/sales/stats
  if (tail === "sales/stats" && request.method === "GET") {
    return handleCabinetSalesStats(appId, request as any, env as any, s.uid);
  }

  // GET /api/cabinet/apps/:appId/passport/stats
  if (tail === "passport/stats" && request.method === "GET") {
    return handleCabinetPassportStats(appId, request as any, env as any, s.uid);
  }

  // GET /api/cabinet/apps/:appId/calendar/bookings
  if (tail === "calendar/bookings" && request.method === "GET") {
    return handleCabinetCalendarBookings(appId, request as any, env as any, s.uid);
  }

  // GET /api/cabinet/apps/:appId/profit/report
  if (tail === "profit/report" && request.method === "GET") {
    return handleCabinetProfitReport(appId, request as any, env as any, s.uid);
  }

  // GET|PUT /api/cabinet/apps/:appId/wheel/prizes
  if (tail === "wheel/prizes") {
    if (request.method === "GET") return handleCabinetWheelPrizesGet(appId, request as any, env as any, s.uid);
    if (request.method === "PUT") return handleCabinetWheelPrizesUpdate(appId, request as any, env as any, s.uid);
    return json({ ok: false, error: "METHOD_NOT_ALLOWED" }, 405, request);
  }

  return null;
}
