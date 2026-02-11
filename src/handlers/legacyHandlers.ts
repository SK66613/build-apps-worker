// src/handlers/legacyHandlers.ts
// Legacy compatibility barrel.
//
// Раньше здесь был монолит с роутингом/хендлерами.
// Теперь этот файл — только "barrel" экспорты из модулей.

export * from "./authHandlers";
export * from "./publicHandlers";
export * from "./telegramHandlers";
export * from "./cabinetApiHandlers";
export * from "./analyticsHandlers";
