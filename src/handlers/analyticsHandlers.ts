// src/handlers/analyticsHandlers.ts
// Cabinet analytics handlers.
//
// ВАЖНО: без `export { ... } from`.

import type { Env } from "../index";
import * as impl from "./_legacyImpl";

export async function handleCabinetWheelStats(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetWheelStats(appId, request, env);
}
export async function handleCabinetSummary(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetSummary(appId, request, env);
}
export async function handleCabinetActivity(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetActivity(appId, request, env);
}
export async function handleCabinetCustomers(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetCustomers(appId, request, env);
}
export async function handleCabinetSalesStats(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetSalesStats(appId, request, env);
}
export async function handleCabinetPassportStats(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetPassportStats(appId, request, env);
}
export async function handleCabinetCalendarBookings(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetCalendarBookings(appId, request, env);
}
export async function handleCabinetProfitReport(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetProfitReport(appId, request, env);
}
export async function handleCabinetOverview(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetOverview(appId, request, env);
}
export async function handleCabinetProfit(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetProfit(appId, request, env);
}
export async function handleCabinetWheelPrizesGet(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetWheelPrizesGet(appId, request, env);
}
export async function handleCabinetWheelPrizesUpdate(appId: any, request: Request, env: Env) {
  return (impl as any).handleCabinetWheelPrizesUpdate(appId, request, env);
}
