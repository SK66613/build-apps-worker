// src/routes/public.ts
import type { Env } from "../index";
import {
  handlePublicEvent,
  handleSalesToken,
  handleStarsCreate,
  handleStarsOrderGet,
} from "../legacy/legacyFetch";

export async function routePublic(request: Request, env: Env, url: URL): Promise<Response | null> {
  const pathname = url.pathname;

  // Public events from miniapp
  const mEvent = pathname.match(/^\/api\/public\/app\/([^/]+)\/event$/);
  if (mEvent && request.method === "POST") {
    const publicId = decodeURIComponent(mEvent[1]);
    return handlePublicEvent(publicId, request as any, env as any);
  }

  // Sales QR token (one-time)
  const mSaleTok = pathname.match(/^\/api\/public\/app\/([^/]+)\/sales\/token$/);
  if (mSaleTok && request.method === "POST") {
    const publicId = decodeURIComponent(mSaleTok[1]);
    return handleSalesToken(publicId, request as any, env as any);
  }

  // Stars: create invoice link
  const mStarsCreate = pathname.match(/^\/api\/public\/app\/([^/]+)\/stars\/create$/);
  if (mStarsCreate && request.method === "POST") {
    const publicId = decodeURIComponent(mStarsCreate[1]);
    return handleStarsCreate(publicId, request as any, env as any);
  }

  // Stars: get order status
  const mStarsGet = pathname.match(/^\/api\/public\/app\/([^/]+)\/stars\/order\/([^/]+)$/);
  if (mStarsGet && request.method === "GET") {
    const publicId = decodeURIComponent(mStarsGet[1]);
    const orderId = decodeURIComponent(mStarsGet[2]);
    return handleStarsOrderGet(publicId, orderId, request as any, env as any);
  }

  return null;
}
