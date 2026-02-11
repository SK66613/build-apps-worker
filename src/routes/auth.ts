// src/routes/auth.ts
import type { Env } from "../index";
import {
  handleRegister,
  handleLogin,
  handleLogout,
  handleMe,
  handleConfirmEmail,
} from "../legacy/legacyFetch";

export async function routeAuth(request: Request, env: Env, url: URL): Promise<Response | null> {
  const p = url.pathname;

  if (p === "/api/auth/register" && request.method === "POST") return handleRegister(request as any, env as any, url as any);
  if (p === "/api/auth/login" && request.method === "POST") return handleLogin(request as any, env as any);
  if (p === "/api/auth/logout" && request.method === "POST") return handleLogout(request as any);
  if (p === "/api/auth/me" && request.method === "GET") return handleMe(request as any, env as any);
  if (p === "/api/auth/confirm" && request.method === "GET") return handleConfirmEmail(url as any, env as any, request as any);

  return null;
}
