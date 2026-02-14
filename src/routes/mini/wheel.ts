// src/routes/mini/wheel.ts
import type { Env } from "../../index";
import { json } from "../../utils/http";
import { tgSendMessage } from "../../services/telegramSend";
import { decryptToken } from "../../services/crypto";

function randomRedeemCodeLocal(len = 10) {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  let s = "";
  for (let i = 0; i < len; i++) s += alphabet[bytes[i] % alphabet.length];
  return "SG-" + s.slice(0, 4) + "-" + s.slice(4, 8) + (len > 8 ? "-" + s.slice(8) : "");
}

// локально, чтобы не тянуть mini.ts (и не получить циклический импорт)
async function getBotTokenForApp(publicId: string, env: Env, appIdFallback: any = null) {
  if (!env.BOT_SECRETS || !env.BOT_TOKEN_KEY) return null;

  const tryGet = async (key: string) => {
    const raw = await env.BOT_SECRETS.get(key);
    if (!raw) return null;

    let cipher = raw;
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object" && parsed.cipher) cipher = parsed.cipher;
    } catch (_) {}

    try {
      return await decryptToken(cipher, env.BOT_TOKEN_KEY);
    } catch (e) {
      console.error("[botToken] decrypt error for key", key, e);
      return null;
    }
  };

  const tok1 = await tryGet("bot_token:public:" + publicId);
  if (tok1) return tok1;

  if (appIdFallback) {
    const tok2 = await tryGet("bot_token:app:" + appIdFallback);
    if (tok2) return tok2;
  }

  return null;
}

async function pickWheelPrize(db: any, appPublicId: string) {
  // ⚠️ ВАЖНО: совместимость со старой схемой, где wheel_prizes не имеет img.
  let rows: any;

  try {
    rows = await db
      .prepare(`SELECT code, title, weight, coins, active, img FROM wheel_prizes WHERE app_public_id = ?`)
      .bind(appPublicId)
      .all();
  } catch (e: any) {
    const msg = String(e?.message || e);
    if (/no such column:\s*img/i.test(msg)) {
      rows = await db
        .prepare(`SELECT code, title, weight, coins, active FROM wheel_prizes WHERE app_public_id = ?`)
        .bind(appPublicId)
        .all();
    } else {
      throw e;
    }
  }

  const hasImg = !!(rows?.results && rows.results.length && Object.prototype.hasOwnProperty.call(rows.results[0], "img"));

  const list = (rows.results || [])
    .filter((r: any) => Number(r.active || 0) && Number(r.weight || 0) > 0)
    .map((r: any) => ({
      code: String(r.code),
      title: String(r.title || r.code),
      weight: Number(r.weight),
      coins: Number(r.coins || 0),
      img: hasImg ? (r as any).img || "" : "",
    }));

  if (!list.length) return null;

  const sum = list.reduce((a: number, b: any) => a + b.weight, 0);
  let rnd = Math.random() * sum;
  let acc = 0;

  for (const it of list) {
    acc += it.weight;
    if (rnd <= acc) return it;
  }

  return list[list.length - 1];
}

type WheelArgs = {
  request: Request;
  env: Env;
  db: any;
  type: string;
  payload: any;
  tg: any;
  ctx: any;

  buildState: (db: any, appId: any, appPublicId: string, tgId: any, cfg: any) => Promise<any>;

  spendCoinsIfEnough: (
    db: any,
    appId: any,
    appPublicId: string,
    tgId: any,
    cost: any,
    src: any,
    ref_id: any,
    note: any,
    event_id: any
  ) => Promise<any>;

  awardCoins: (
    db: any,
    appId: any,
    appPublicId: string,
    tgId: any,
    delta: any,
    src: any,
    ref_id: any,
    note: any,
    event_id: any
  ) => Promise<any>;
};

export async function handleWheelMiniApi(args: WheelArgs): Promise<Response | null> {
  const { request, env, db, type, tg, ctx, buildState, spendCoinsIfEnough, awardCoins } = args;

  // ====== wheel.spin (bonus_wheel_one)
  if (type === "wheel.spin" || type === "wheel_spin" || type === "spin") {
    const appObj = await env.APPS.get("app:" + (ctx as any).appId, "json").catch(() => null);
    const cfg =
      (appObj as any) && ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        ? ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        : {};

    const spinCost = Math.max(0, Math.floor(Number((cfg as any)?.wheel?.spin_cost ?? 0)));

    // 0) если уже есть незабранный выигрыш — запрещаем новый spin
    const unclaimed: any = await db
      .prepare(
        `SELECT id, prize_code, prize_title
         FROM wheel_spins
         WHERE app_public_id=? AND tg_id=? AND status='won'
         ORDER BY id DESC LIMIT 1`
      )
      .bind((ctx as any).publicId, String(tg.id))
      .first();

    if (unclaimed) {
      const pr: any = await db
        .prepare(`SELECT coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`)
        .bind((ctx as any).publicId, String(unclaimed.prize_code || ""))
        .first();

      const prizeCoins = Math.max(0, Math.floor(Number(pr?.coins || 0)));
      const fresh_state = await buildState(db, (ctx as any).appId, (ctx as any).publicId, tg.id, cfg);

      return json(
        {
          ok: true,
          already_won: true,
          spin_id: Number(unclaimed.id),
          spin_cost: spinCost,
          prize: { code: unclaimed.prize_code || "", title: unclaimed.prize_title || "", coins: prizeCoins },
          fresh_state,
        },
        200,
        request
      );
    }

    // 1) создаём спин
    const ins = await db
      .prepare(
        `INSERT INTO wheel_spins (app_id, app_public_id, tg_id, status, prize_code, prize_title, spin_cost)
         VALUES (?, ?, ?, 'new', '', '', ?)`
      )
      .bind((ctx as any).appId, (ctx as any).publicId, String(tg.id), spinCost)
      .run();

    const spinId = Number((ins as any)?.meta?.last_row_id || (ins as any)?.lastInsertRowid || 0);
    if (!spinId) return json({ ok: false, error: "SPIN_CREATE_FAILED" }, 500, request);

    // 2) списываем стоимость
    if (spinCost > 0) {
      const spend: any = await spendCoinsIfEnough(
        db,
        (ctx as any).appId,
        (ctx as any).publicId,
        tg.id,
        spinCost,
        "wheel_spin_cost",
        String(spinId),
        "Spin cost",
        `wheel:cost:${(ctx as any).publicId}:${tg.id}:${spinId}`
      );

      if (!spend?.ok) {
        try {
          await db.prepare(`DELETE FROM wheel_spins WHERE id=?`).bind(spinId).run();
        } catch (_) {}
        return json({ ok: false, error: spend?.error || "NOT_ENOUGH", have: spend?.have, need: spend?.need }, 409, request);
      }
    }

    // 3) выбираем приз
    const prize: any = await pickWheelPrize(db, (ctx as any).publicId);
    if (!prize) {
      // возврат если нет призов
      if (spinCost > 0) {
        await awardCoins(
          db,
          (ctx as any).appId,
          (ctx as any).publicId,
          tg.id,
          spinCost,
          "wheel_refund",
          String(spinId),
          "Refund: no prizes",
          `wheel:refund:${(ctx as any).publicId}:${tg.id}:${spinId}`
        );
      }
      try {
        await db.prepare(`DELETE FROM wheel_spins WHERE id=?`).bind(spinId).run();
      } catch (_) {}
      return json({ ok: false, error: "NO_PRIZES" }, 400, request);
    }

    // 4) фиксируем win
    await db
      .prepare(`UPDATE wheel_spins SET status='won', prize_code=?, prize_title=? WHERE id=?`)
      .bind(String(prize.code || ""), String(prize.title || ""), spinId)
      .run();

    // 5) coins берём из wheel_prizes (истина)
    const pr: any = await db
      .prepare(`SELECT coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`)
      .bind((ctx as any).publicId, String(prize.code || ""))
      .first();

    const prizeCoins = Math.max(0, Math.floor(Number(pr?.coins || 0)));
    const fresh_state = await buildState(db, (ctx as any).appId, (ctx as any).publicId, tg.id, cfg);

    return json(
      {
        ok: true,
        prize: { code: prize.code || "", title: prize.title || "", coins: prizeCoins, img: prize.img || "" },
        spin_cost: spinCost,
        spin_id: spinId,
        fresh_state,
      },
      200,
      request
    );
  }

  // ====== wheel.claim (bonus_wheel_one)
  if (type === "wheel.claim" || type === "wheel_claim" || type === "claim_prize") {
    const appObj = await env.APPS.get("app:" + (ctx as any).appId, "json").catch(() => null);
    const cfg =
      (appObj as any) && ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        ? ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        : {};

    const lastWon: any = await db
      .prepare(
        `SELECT id, prize_code, prize_title
         FROM wheel_spins
         WHERE app_public_id=? AND tg_id=? AND status='won'
         ORDER BY id DESC LIMIT 1`
      )
      .bind((ctx as any).publicId, String(tg.id))
      .first();

    if (!lastWon) return json({ ok: false, error: "NOTHING_TO_CLAIM" }, 400, request);

    const spinId = Number(lastWon.id);

    const pr: any = await db
      .prepare(`SELECT coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`)
      .bind((ctx as any).publicId, String(lastWon.prize_code || ""))
      .first();

    const prizeCoins = Math.max(0, Math.floor(Number(pr?.coins || 0)));

    // любой приз -> wheel_redeems + deep link redeem_
    let redeem: any = await db
      .prepare(
        `SELECT id, redeem_code, status
         FROM wheel_redeems
         WHERE app_public_id=? AND spin_id=?
         LIMIT 1`
      )
      .bind((ctx as any).publicId, spinId)
      .first();

    if (!redeem) {
      let code = "";
      for (let i = 0; i < 5; i++) {
        code = randomRedeemCodeLocal(10);
        try {
          const ins2 = await db
            .prepare(
              `INSERT INTO wheel_redeems
                 (app_id, app_public_id, tg_id, spin_id, prize_code, prize_title, redeem_code, status, issued_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'issued', datetime('now'))`
            )
            .bind(
              (ctx as any).appId,
              (ctx as any).publicId,
              String(tg.id),
              spinId,
              String(lastWon.prize_code || ""),
              String(lastWon.prize_title || ""),
              String(code)
            )
            .run();

          redeem = {
            id: Number((ins2 as any)?.meta?.last_row_id || (ins2 as any)?.lastInsertRowid || 0),
            redeem_code: code,
            status: "issued",
          };
          break;
        } catch (e: any) {
          const msg = String(e?.message || e);
          if (!/unique|constraint/i.test(msg)) throw e;
        }
      }
      if (!redeem) return json({ ok: false, error: "REDEEM_CREATE_FAILED" }, 500, request);
    }

    try {
      await db
        .prepare(`UPDATE wheel_spins SET status='issued', redeem_id=?, ts_issued=datetime('now') WHERE id=? AND status='won'`)
        .bind(Number(redeem.id), spinId)
        .run();
    } catch (_) {}

    let botUsername = "";
    try {
      const b: any = await db
        .prepare(`SELECT username FROM bots WHERE app_public_id=? AND status='active' ORDER BY id DESC LIMIT 1`)
        .bind((ctx as any).publicId)
        .first();
      botUsername = b?.username ? String(b.username).replace(/^@/, "").trim() : "";
    } catch (_) {}

    const redeem_code = String(redeem.redeem_code || "");
    const deep_link = botUsername ? `https://t.me/${botUsername}?start=redeem_${encodeURIComponent(redeem_code)}` : "";

    // сообщение пользователю
    try {
      const botToken = await getBotTokenForApp((ctx as any).publicId, env, (ctx as any).appId);
      if (botToken) {
        const txt =
          `🎁 Ваш приз: <b>${String(lastWon.prize_title || "Бонус")}</b>\n` +
          (prizeCoins > 0 ? `🪙 Монеты: <b>${prizeCoins}</b> (начислятся после подтверждения кассиром)\n` : ``) +
          `\n✅ Код выдачи: <code>${redeem_code}</code>\n` +
          (deep_link ? `Откройте ссылку:\n${deep_link}` : `Покажите код кассиру.`);

        await tgSendMessage(env, botToken, String(tg.id), txt, {}, { appPublicId: (ctx as any).publicId, tgUserId: String(tg.id) });
      }
    } catch (e) {
      console.error("[wheel.claim] tgSendMessage redeem failed", e);
    }

    const fresh_state = await buildState(db, (ctx as any).appId, (ctx as any).publicId, tg.id, cfg);

    return json(
      {
        ok: true,
        issued: true,
        redeem_code,
        deep_link,
        spin_id: spinId,
        prize: { code: lastWon.prize_code || "", title: lastWon.prize_title || "", coins: prizeCoins },
        fresh_state,
      },
      200,
      request
    );
  }

  return null;
}
