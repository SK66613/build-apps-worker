// src/routes/mini.ts
import type { Env } from "../index";
import { handleMiniApi } from "../legacy/legacyFetch";

export async function routeMiniApi(
  request: Request,
  env: Env,
  url: URL
): Promise<Response> {
  // handleMiniApi — твоя текущая рабочая логика из legacy
  return await handleMiniApi(request, env as any, url);
}
