import { json } from "../utils/http";

export function handleHealth(): Response {
  return new Response("ok", { status: 200, headers: { "Content-Type": "text/plain; charset=UTF-8" } });
}

export function handleVersion(env: any): Response {
  return json({ ok: true, sha: String(env?.GITHUB_SHA || "unknown") }, 200);
}
