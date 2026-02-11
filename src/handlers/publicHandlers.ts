// src/handlers/publicHandlers.ts
// Public (no cookie session) handlers used by /api/public/*.
//
// NOTE: Реализация пока живёт в handlers/legacyHandlers.ts.
// Этот файл — модульная точка входа, чтобы дальше переносить реализацию по частям.

export {
  handlePublicEvent,
  handleSalesToken,
  handleStarsCreate,
  handleStarsOrderGet,
  getPublicConfig,
} from "./legacyHandlers";
