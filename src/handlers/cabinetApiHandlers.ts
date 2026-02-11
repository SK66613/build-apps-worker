// src/handlers/cabinetApiHandlers.ts
// Cabinet (cookie session) CRUD + bots + broadcasts + dialogs.
//
// NOTE: Реализация пока живёт в handlers/legacyHandlers.ts.

export {
  requireSession,
  ensureAppOwner,
  listMyApps,
  createApp,
  getApp,
  saveApp,
  deleteApp,
  publishApp,
  getBotIntegration,
  saveBotIntegration,
  deleteBotIntegration,
  listBroadcasts,
  createAndSendBroadcast,
  listDialogs,
  getDialogMessages,
  sendDialogMessage,
} from "./legacyHandlers";
