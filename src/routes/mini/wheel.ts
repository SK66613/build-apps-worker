// src/routes/mini/wheel.ts
import type { Env } from "../../index";
import { json } from "../../utils/http";

function logWheelEvent(event: {
  code: string;
  msg: string;
  appPublicId: string;
  tgUserId: string;
  route: string;
  extra?: Record<string, any>;
}) {
  try {
    console.log(JSON.stringify(event));
  } catch (_) {}
}

function randomRedeemCodeLocal(len = 10) {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  let s = "";
  for (let i = 0; i < len; i++) s += alphabet[bytes[i] % alphabet.length];
  return "SG-" + s.slice(0, 4) + "-" + s.slice(4, 8) + (len > 8 ? "-" + s.slice(8) : "");
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

async function getWheelPrizeDetails(db: any, appPublicId: string, prizeCode: string) {
  try {
    return await db
      .prepare(`SELECT coins, img, cost_cent, cost_currency FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`)
      .bind(appPublicId, prizeCode)
      .first();
  } catch (e: any) {
    const msg = String(e?.message || e);
    if (/no such column:\s*(img|cost_cent|cost_currency)/i.test(msg)) {
      const fallback: any = await db
        .prepare(`SELECT coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`)
        .bind(appPublicId, prizeCode)
        .first();
      return {
        coins: Number(fallback?.coins || 0),
        img: "",
        cost_cent: 0,
        cost_currency: null,
      };
    }
    throw e;
  }
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
    const route = "wheel.spin";
    const appPublicId = String((ctx as any).publicId || "");
    const tgUserId = String(tg.id || "");
    const appObj = await env.APPS.get("app:" + (ctx as any).appId, "json").catch(() => null);
    const cfg =
      (appObj as any) && ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        ? ((appObj as any).app_config ?? (appObj as any).runtime_config ?? (appObj as any).config)
        : {};

    const spinCost = Math.max(0, Math.floor(Number((cfg as any)?.wheel?.spin_cost ?? 0)));

    // 1) создаём спин
    let ins: any;
    try {
      ins = await db
        .prepare(
          `INSERT INTO wheel_spins (app_id, app_public_id, tg_id, status, prize_code, prize_title, spin_cost)
           VALUES (?, ?, ?, 'new', '', '', ?)`
        )
        .bind((ctx as any).appId, appPublicId, tgUserId, spinCost)
        .run();
    } catch (e: any) {
      logWheelEvent({ code: "mini.wheel.spin.fail.db_error", msg: "Failed to create spin", appPublicId, tgUserId, route, extra: { error: String(e?.message || e) } });
      return json({ ok: false, error: "SPIN_CREATE_FAILED" }, 500, request);
    }

    const spinId = Number((ins as any)?.meta?.last_row_id || (ins as any)?.lastInsertRowid || 0);
    if (!spinId) {
      logWheelEvent({ code: "mini.wheel.spin.fail.db_error", msg: "Spin id was not returned", appPublicId, tgUserId, route });
      return json({ ok: false, error: "SPIN_CREATE_FAILED" }, 500, request);
    }

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
        logWheelEvent({
          code: "mini.wheel.spin.fail.not_enough_coins",
          msg: "Not enough coins for spin",
          appPublicId,
          tgUserId,
          route,
          extra: { spinId, have: spend?.have, need: spend?.need },
        });
        return json({ ok: false, error: spend?.error || "NOT_ENOUGH", have: spend?.have, need: spend?.need }, 409, request);
      }
    }

    // 3) выбираем приз
    let prize: any = null;
    try {
      prize = await pickWheelPrize(db, (ctx as any).publicId);
    } catch (e: any) {
      logWheelEvent({ code: "mini.wheel.spin.fail.db_error", msg: "Failed to load prizes", appPublicId, tgUserId, route, extra: { spinId, error: String(e?.message || e) } });
      return json({ ok: false, error: "DB_ERROR" }, 500, request);
    }
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
      logWheelEvent({ code: "mini.wheel.spin.fail.no_prizes", msg: "No active prizes configured", appPublicId, tgUserId, route, extra: { spinId } });
      return json({ ok: false, error: "NO_PRIZES" }, 400, request);
    }

    let pr: any = null;
    try {
      pr = await getWheelPrizeDetails(db, appPublicId, String(prize.code || ""));
    } catch (e: any) {
      logWheelEvent({ code: "mini.wheel.spin.fail.db_error", msg: "Failed to load prize details", appPublicId, tgUserId, route, extra: { spinId, error: String(e?.message || e) } });
      return json({ ok: false, error: "DB_ERROR" }, 500, request);
    }
    const prizeCoins = Math.max(0, Math.floor(Number(pr?.coins || 0)));
    const prizeImg = String((pr?.img ?? prize?.img ?? "") || "");
    const costCent = Math.max(0, Math.floor(Number(pr?.cost_cent || 0)));
    const costCurrency = pr?.cost_currency ? String(pr.cost_currency) : null;

    let redeem: any = null;
    for (let i = 0; i < 5; i++) {
      const redeemCode = randomRedeemCodeLocal(10);
      try {
        const ins2 = await db
          .prepare(
            `INSERT INTO wheel_redeems
               (app_id, app_public_id, tg_id, spin_id, prize_code, prize_title, redeem_code, status, issued_at, img, expires_at, cost_cent, cost_currency)
             VALUES (?, ?, ?, ?, ?, ?, ?, 'issued', datetime('now'), ?, NULL, ?, ?)`
          )
          .bind(
            (ctx as any).appId,
            appPublicId,
            tgUserId,
            spinId,
            String(prize.code || ""),
            String(prize.title || ""),
            redeemCode,
            prizeImg,
            costCent,
            costCurrency
          )
          .run();
        redeem = {
          id: Number((ins2 as any)?.meta?.last_row_id || (ins2 as any)?.lastInsertRowid || 0),
          redeem_code: redeemCode,
        };
        break;
      } catch (e: any) {
        const msg = String(e?.message || e);
        if (!/unique|constraint/i.test(msg)) {
          logWheelEvent({ code: "mini.wheel.spin.fail.db_error", msg: "Failed to issue reward", appPublicId, tgUserId, route, extra: { spinId, error: msg } });
          return json({ ok: false, error: "REDEEM_CREATE_FAILED" }, 500, request);
        }
      }
    }

    if (!redeem?.id) {
      logWheelEvent({ code: "mini.wheel.spin.fail.db_error", msg: "Could not issue reward code", appPublicId, tgUserId, route, extra: { spinId } });
      return json({ ok: false, error: "REDEEM_CREATE_FAILED" }, 500, request);
    }

    try {
      await db
        .prepare(`UPDATE wheel_spins SET status='issued', prize_code=?, prize_title=?, redeem_id=?, ts_issued=datetime('now') WHERE id=?`)
        .bind(String(prize.code || ""), String(prize.title || ""), Number(redeem.id), spinId)
        .run();
    } catch (e: any) {
      logWheelEvent({ code: "mini.wheel.spin.fail.db_error", msg: "Failed to update spin status", appPublicId, tgUserId, route, extra: { spinId, error: String(e?.message || e) } });
      return json({ ok: false, error: "SPIN_UPDATE_FAILED" }, 500, request);
    }

    let rewardsCountRow: any;
    try {
      rewardsCountRow = await db
        .prepare(`SELECT COUNT(*) AS c FROM wheel_redeems WHERE app_public_id=? AND tg_id=? AND status='issued'`)
        .bind(appPublicId, tgUserId)
        .first();
    } catch (e: any) {
      logWheelEvent({ code: "mini.wheel.spin.fail.db_error", msg: "Failed to count rewards", appPublicId, tgUserId, route, extra: { spinId, error: String(e?.message || e) } });
      return json({ ok: false, error: "DB_ERROR" }, 500, request);
    }
    const rewards_count = Math.max(0, Number(rewardsCountRow?.c || 0));

    const fresh_state = await buildState(db, (ctx as any).appId, (ctx as any).publicId, tg.id, cfg);

    logWheelEvent({ code: "mini.wheel.spin.ok", msg: "Spin completed", appPublicId, tgUserId, route, extra: { spinId, prizeCode: String(prize.code || ""), rewards_count } });

    return json(
      {
        ok: true,
        prize: { code: prize.code || "", title: prize.title || "", coins: prizeCoins, img: prizeImg || "" },
        spin_cost: spinCost,
        spin_id: spinId,
        rewards_count,
        fresh_state,
      },
      200,
      request
    );
  }

  // ====== wheel.claim (bonus_wheel_one)
  if (type === "wheel.claim" || type === "wheel_claim" || type === "claim_prize") {
    return json(
      {
        ok: true,
        deprecated: true,
        msg: "Reward claim is no longer required: rewards are issued automatically after spin.",
      },
      200,
      request
    );
  }

  if (type === "wheel.rewards" || type === "wheel_rewards") {
    const route = "wheel.rewards";
    const appPublicId = String((ctx as any).publicId || "");
    const tgUserId = String(tg.id || "");
    try {
      const rows: any = await db
        .prepare(
          `SELECT id, prize_code, prize_title, redeem_code, status, issued_at, img, expires_at, cost_cent, cost_currency
           FROM wheel_redeems
           WHERE app_public_id=? AND tg_id=? AND status='issued'
           ORDER BY id DESC`
        )
        .bind(appPublicId, tgUserId)
        .all();
      const rewards = (rows?.results || []).map((r: any) => ({
        id: Number(r.id || 0),
        prize_code: String(r.prize_code || ""),
        prize_title: String(r.prize_title || ""),
        redeem_code: String(r.redeem_code || ""),
        status: String(r.status || "issued"),
        issued_at: r.issued_at || null,
        img: r.img || "",
        expires_at: r.expires_at || null,
        cost_cent: Math.max(0, Number(r.cost_cent || 0)),
        cost_currency: r.cost_currency || null,
      }));

      logWheelEvent({ code: "mini.wheel.rewards.ok", msg: "Rewards fetched", appPublicId, tgUserId, route, extra: { rewards_count: rewards.length } });
      return json({ ok: true, rewards, rewards_count: rewards.length }, 200, request);
    } catch (e: any) {
      logWheelEvent({ code: "mini.wheel.rewards.fail.db_error", msg: "Failed to fetch rewards", appPublicId, tgUserId, route, extra: { error: String(e?.message || e) } });
      return json({ ok: false, error: "DB_ERROR" }, 500, request);
    }
  }

  return null;
}
