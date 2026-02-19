export const ALLOW_ORIGINS = [
  "https://app.salesgenius.ru",
  "https://mini.salesgenius.ru",
  "https://blocks.salesgenius.ru",
  "https://ru.salesgenius.ru",
  "https://ru.cifrovichkoff.ru",
  "https://apps.salesgenius.ru",
  "https://web.telegram.org",
  "https://web.telegram.org/k",
  "https://web.telegram.org/z",
] as const;

export const ALLOW_ORIGINS_SET = new Set<string>(ALLOW_ORIGINS as unknown as string[]);
