# build-apps Worker (TypeScript)

Этот репозиторий — заготовка для миграции твоего Cloudflare Worker `build-apps` из UI в GitHub + CI деплой.

## Что внутри
- `src/index.ts` — вход, роутер (пока вызывает legacy)
- `src/legacy/legacyFetch.ts` — сюда вставишь код из Cloudflare UI (тело fetch)
- `wrangler.toml` — биндинги DB/APPS/BOT_SECRETS (вставь IDs)
- GitHub Actions деплой: `.github/workflows/deploy.yml`

## Как тестить
- После деплоя на прод: открой workers.dev URL и /api/* маршруты как раньше.
