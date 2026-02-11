// src/handlers/cabinetApiHandlers.ts
// Cabinet (cookie session) CRUD + bots + broadcasts + dialogs.
//
// ВАЖНО: без `export { ... } from`.

import type { Env } from "../index";
import { requireSession as requireSessionSvc } from "../services/session";
import * as impl from "./_legacyImpl";

export async function requireSession(request: Request, env: Env) {
  return await requireSessionSvc(request as any, env as any);
}

export async function ensureAppOwner(appId: any, ownerId: any, env: Env) {
  return (impl as any).ensureAppOwner(appId, ownerId, env);
}

export async function listMyApps(request: Request, env: Env) {
  return (impl as any).listMyApps(request, env);
}

export async function createApp(request: Request, env: Env, body: any) {
  return (impl as any).createApp(request, env, body);
}

export async function getApp(appId: any, request: Request, env: Env) {
  return (impl as any).getApp(appId, request, env);
}

export async function saveApp(appId: any, request: Request, env: Env, body: any) {
  return (impl as any).saveApp(appId, request, env, body);
}

export async function deleteApp(appId: any, request: Request, env: Env) {
  return (impl as any).deleteApp(appId, request, env);
}

export async function publishApp(appId: any, request: Request, env: Env) {
  return (impl as any).publishApp(appId, request, env);
}

export async function getBotIntegration(appId: any, request: Request, env: Env) {
  return (impl as any).getBotIntegration(appId, request, env);
}

export async function saveBotIntegration(appId: any, env: Env, body: any, ownerId: any, request: Request) {
  return (impl as any).saveBotIntegration(appId, env, body, ownerId, request);
}

export async function deleteBotIntegration(appId: any, env: Env, body: any, ownerId: any, request: Request) {
  return (impl as any).deleteBotIntegration(appId, env, body, ownerId, request);
}

export async function listBroadcasts(appId: any, request: Request, env: Env) {
  return (impl as any).listBroadcasts(appId, request, env);
}

export async function createAndSendBroadcast(appId: any, request: Request, env: Env, body: any) {
  return (impl as any).createAndSendBroadcast(appId, request, env, body);
}

export async function listDialogs(appId: any, request: Request, env: Env) {
  return (impl as any).listDialogs(appId, request, env);
}

export async function getDialogMessages(appId: any, tgUserId: any, request: Request, env: Env) {
  return (impl as any).getDialogMessages(appId, tgUserId, request, env);
}

export async function sendDialogMessage(appId: any, tgUserId: any, request: Request, env: Env, body: any) {
  return (impl as any).sendDialogMessage(appId, tgUserId, request, env, body);
}
