// src/handlers/telegram/sales.ts
import type { Env } from "../../index";
import { tgSendMessage } from "../../services/telegramSend";
import { awardCoins } from "../../services/coinsLedger";

type SalesArgs = {
  env: Env;
  db: any;
  ctx: { appId: any; publicId: string };
  botToken: string;
  upd: any;
};

/* =========================================================
   BASIC HELPERS
========================================================= */

function parseAmountToCents(s: any) {
  const raw = String(s || "").trim().replace(",", ".");
  if (!raw) return null;
  if (!/^\d+(\.\d{1,2})?$/.test(raw)) return null;
  const [r, k] = raw.split(".");
  return Number(r) * 100 + Number((k || "").padEnd(2, "0"));
}

function parseIntCoins(s: any) {
  const n = Number(String(s ?? "").trim().replace(",", "."));
  return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : null;
}

/* =========================================================
   KV
========================================================= */

async function kvGetJson(env: Env, key: string) {
  const raw = await (env as any).BOT_SECRETS?.get(key);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch { return null; }
}

async function kvPutJson(env: Env, key: string, v: any, ttl = 600) {
  await (env as any).BOT_SECRETS?.put(key, JSON.stringify(v ?? {}), { expirationTtl: ttl });
}

async function kvDel(env: Env, key: string) {
  await (env as any).BOT_SECRETS?.delete(key);
}

/* =========================================================
   KEYS
========================================================= */

const pendKey = (p: string, u: string) => `sale_pending:${p}:${u}`;
const draftKey = (p: string, u: string) => `sale_draft:${p}:${u}`;
const actionKey = (p: string, id: string, u: string) => `sale_action:${p}:${id}:${u}`;
const redeemWaitKey = (p: string, u: string) => `sale_redeem_wait:${p}:${u}`;
const msgKey = (p: string, id: string) => `sale_msg:${p}:${id}`;

/* =========================================================
   TELEGRAM
========================================================= */

async function answer(botToken: string, id: string, text = "", alert = false) {
  await fetch(`https://api.telegram.org/bot${botToken}/answerCallbackQuery`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ callback_query_id: id, text, show_alert: alert })
  }).catch(()=>{});
}

async function editMsg(env:Env,botToken:string,chatId:string,msgId:number,text:string,keyboard:any){
  await fetch(`https://api.telegram.org/bot${botToken}/editMessageText`,{
    method:"POST",
    headers:{ "content-type":"application/json"},
    body: JSON.stringify({
      chat_id:chatId,
      message_id:msgId,
      text,
      parse_mode:"HTML",
      reply_markup:keyboard?.reply_markup
    })
  }).catch(()=>{});
}

/* =========================================================
   LEDGER
========================================================= */

async function ledgerHas(db:any,id:string){
  const r=await db.prepare("SELECT 1 FROM coins_ledger WHERE event_id=?").bind(id).first();
  return !!r;
}

async function getCoins(db:any,p:string,tg:string){
  const r:any=await db.prepare(
    "SELECT coins FROM app_users WHERE app_public_id=? AND tg_user_id=?"
  ).bind(p,tg).first();
  return r?Number(r.coins||0):0;
}

async function spend(db:any,appId:any,p:string,tg:string,cost:number,ev:string){
  const b=await getCoins(db,p,tg);
  if(b<cost)return{ok:false,have:b};

  await db.batch([
    db.prepare("UPDATE app_users SET coins=coins-? WHERE app_public_id=? AND tg_user_id=?")
      .bind(cost,p,tg),
    db.prepare(`
      INSERT INTO coins_ledger(app_id,app_public_id,tg_id,event_id,delta,balance_after)
      VALUES(?,?,?,?,?,?)
    `).bind(appId,p,tg,ev,-cost,b-cost)
  ]);
  return{ok:true,balance:b-cost};
}

/* =========================================================
   UI BUILDERS
========================================================= */

function draftKb(r:number){
  const k:any[]=[];
  if(!r)k.push([{text:"🪙 Списать монеты",callback_data:"redeem"}]);
  k.push([
    {text:"✅ Да",callback_data:"record"},
    {text:"✏️ Заново",callback_data:"reenter"}
  ]);
  k.push([{text:"⛔️ Отмена",callback_data:"drop"}]);
  return{reply_markup:{inline_keyboard:k}};
}

function afterKb(id:string,r:number){
  const k:any[]=[[{text:"✅ Подтвердить кэшбэк",callback_data:`cb:${id}`}]];
  if(r)k.push([{text:"🪙 Подтвердить списание",callback_data:`rd:${id}`}]);
  return{reply_markup:{inline_keyboard:k}};
}

/* =========================================================
   MAIN
========================================================= */

export async function handleSalesFlow({env,db,ctx,botToken,upd}:SalesArgs){

const appPublicId=ctx.publicId;

/* ================= CALLBACK ================= */

if(upd.callback_query){

const cq=upd.callback_query;
const data=String(cq.data||"");
const cqId=cq.id;
const tgId=String(cq.from.id);
const chatId=String(cq.message.chat.id);

/* -------- record sale -------- */

if(data==="record"){

const draft=await kvGetJson(env,draftKey(appPublicId,tgId));
if(!draft)return true;

const ins=await db.prepare(`
INSERT INTO sales(app_id,app_public_id,customer_tg_id,cashier_tg_id,amount_cents,cashback_coins,redeem_coins)
VALUES(?,?,?,?,?,?,?)
`).bind(ctx.appId,appPublicId,draft.customerTgId,tgId,draft.amount,draft.cb,draft.rd).run();

const id=String(ins.meta.last_row_id);

await kvPutJson(env,actionKey(appPublicId,id,tgId),draft,3600);

const txt=`✅ Продажа #${id}
Сумма: ${(draft.amount/100).toFixed(2)}
Кэшбэк: ${draft.cb}
Списание: ${draft.rd}`;

const sent:any=await tgSendMessage(env,botToken,chatId,txt,afterKb(id,draft.rd));
await kvPutJson(env,msgKey(appPublicId,id),{chatId,msgId:sent?.result?.message_id},3600);

await answer(botToken,cqId,"OK");
return true;
}

/* -------- cashback confirm -------- */

if(data.startsWith("cb:")){
const id=data.split(":")[1];
const act=await kvGetJson(env,actionKey(appPublicId,id,tgId));
if(!act)return true;

const ev=`cb:${id}`;
if(await ledgerHas(db,ev)){await answer(botToken,cqId,"Уже");return true;}

await awardCoins(db,ctx.appId,appPublicId,act.customerTgId,act.cb,"cashback",id,"",ev);

const ui=await kvGetJson(env,msgKey(appPublicId,id));
if(ui){
await editMsg(env,botToken,ui.chatId,ui.msgId,
`✅ Продажа #${id}
Кэшбэк: ${act.cb} ✅`,
{reply_markup:{inline_keyboard:[
[{text:"❌ Отменить кэшбэк",callback_data:`cbx:${id}`}]
]}}
);
}
await answer(botToken,cqId,"Начислено");
return true;
}

/* -------- cashback cancel -------- */

if(data.startsWith("cbx:")){
const id=data.split(":")[1];
const act=await kvGetJson(env,actionKey(appPublicId,id,tgId));
if(!act)return true;

await awardCoins(db,ctx.appId,appPublicId,act.customerTgId,-act.cb,"cancel",id,"",`cbx:${id}`);

const ui=await kvGetJson(env,msgKey(appPublicId,id));
if(ui){
await editMsg(env,botToken,ui.chatId,ui.msgId,
`↩️ Кэшбэк отменён\nSale #${id}`,{});
}
await answer(botToken,cqId,"Отменено");
return true;
}

/* -------- redeem confirm -------- */

if(data.startsWith("rd:")){
const id=data.split(":")[1];
const act=await kvGetJson(env,actionKey(appPublicId,id,tgId));
if(!act)return true;

const ev=`rd:${id}`;
if(await ledgerHas(db,ev)){await answer(botToken,cqId,"Уже");return true;}

const res=await spend(db,ctx.appId,appPublicId,act.customerTgId,act.rd,ev);
if(!res.ok){await answer(botToken,cqId,"Нет монет",true);return true;}

const ui=await kvGetJson(env,msgKey(appPublicId,id));
if(ui){
await editMsg(env,botToken,ui.chatId,ui.msgId,
`✅ Продажа #${id}
Списано: ${act.rd} ✅
Баланс: ${res.balance}`,
{reply_markup:{inline_keyboard:[
[{text:"↩️ Отменить списание",callback_data:`rdx:${id}`}]
]}}
);
}
await answer(botToken,cqId,"OK");
return true;
}

/* -------- redeem cancel -------- */

if(data.startsWith("rdx:")){
const id=data.split(":")[1];
const act=await kvGetJson(env,actionKey(appPublicId,id,tgId));
if(!act)return true;

await awardCoins(db,ctx.appId,appPublicId,act.customerTgId,act.rd,"refund",id,"",`rdx:${id}`);

const ui=await kvGetJson(env,msgKey(appPublicId,id));
if(ui){
await editMsg(env,botToken,ui.chatId,ui.msgId,
`↩️ Списание отменено\nSale #${id}`,{});
}
await answer(botToken,cqId,"OK");
return true;
}

return false;
}

/* ================= MESSAGE ================= */

const text=String(upd.message?.text||"").trim();
const tgId=String(upd.message?.from?.id||"");
const chatId=String(upd.message?.chat?.id||"");

if(!text||!tgId)return false;

/* amount input */

const pend=await kvGetJson(env,pendKey(appPublicId,tgId));
if(pend){

const cents=parseAmountToCents(text);
if(cents==null){
await tgSendMessage(env,botToken,chatId,"Введите сумму числом");
return true;
}

const cb=Math.floor((cents/100)*(pend.pct/100));
const draft={customerTgId:pend.customer,amount:cents,cb,rd:0};

await kvPutJson(env,draftKey(appPublicId,tgId),draft,600);

await tgSendMessage(
env,botToken,chatId,
`❓ Продажа?
Сумма: ${(cents/100).toFixed(2)}
Кэшбэк: ${cb}
Списание: 0`,
draftKb(0)
);

return true;
}

return false;
}
