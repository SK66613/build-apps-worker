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

async function getSpinCostFromDb(db: any, appPublicId: string): Promise<number | null> {
  try {
    const row: any = await db
      .prepare(`SELECT MAX(spin_cost) AS spin_cost FROM wheel_prizes WHERE app_public_id=?`)
      .bind(appPublicId)
      .first();

    const v = Number(row?.spin_cost);
    return Number.isFinite(v) ? Math.max(0, Math.floor(v)) : null;
  } catch (e: any) {
    const msg = String(e?.message || e);
    if (/no such column:\s*spin_cost/i.test(msg)) return null;
    return null;
  }
}

// coin_value_cents (стоимость 1 монеты) — берем снапшотом из app_settings (D1)
async function getCoinValueCentsFromDb(db: any, appPublicId: string): Promise<number | null> {
  try {
    const row: any = await db
      .prepare(`SELECT coin_value_cents FROM app_settings WHERE app_public_id=? LIMIT 1`)
      .bind(appPublicId)
      .first();

    const v = Number(row?.coin_value_cents);
    return Number.isFinite(v) ? Math.max(0, Math.floor(v)) : null;
  } catch (e: any) {
    const msg = String(e?.message || e);
    if (/no such table:\s*app_settings/i.test(msg)) return null;
    if (/no such column:\s*coin_value_cents/i.test(msg)) return null;
    return null;
  }
}

type PrizeRow = {
  code: string;
  title: string;
  weight: number;
  coins: number;
  active: number;

  // optional columns
  img?: string;
  kind?: string;

  // NEW economics (source of truth for item)
  cost_coins?: number;

  // legacy (можешь потом выпилить)
  cost_cent?: number;
  cost_currency?: string;

  track_qty?: number;
  qty_left?: number | null;
  stop_when_zero?: number;
};

function effWeight(p: any) {
  const w = Math.max(0, Math.floor(Number(p?.weight || 0)));
  if (!w) return 0;

  const track = Number(p?.track_qty || 0) === 1;
  const stop = Number(p?.stop_when_zero || 0) === 1;

  const left = p?.qty_left === null || p?.qty_left === undefined ? null : Number(p.qty_left);

  if (track && stop) {
    if (left === null || !Number.isFinite(left) || left <= 0) return 0;
  }
  return w;
}

function pickWeighted(list: PrizeRow[]) {
  const items = list
    .map((p) => ({ p, w: effWeight(p) }))
    .filter((x) => x.w > 0);

  if (!items.length) return null;

  const total = items.reduce((s, x) => s + x.w, 0);
  let r = Math.random() * total;

  for (const it of items) {
    r -= it.w;
    if (r <= 0) return it.p;
  }
  return items[items.length - 1].p;
}

// Load prizes. Backward compatible with older schema.
async function loadWheelPrizes(db: any, appPublicId: string): Promise<PrizeRow[]> {
  try {
    const rows: any = await db
      .prepare(
        `SELECT code, title, weight, coins, active,
                img, kind,
                cost_coins,
                cost_cent, cost_currency,
                track_qty, qty_left, stop_when_zero
         FROM wheel_prizes
         WHERE app_public_id = ?`
      )
      .bind(appPublicId)
      .all();

    return (rows?.results || []).map((r: any) => ({
      code: String(r.code || ""),
      title: String(r.title || r.code || ""),
      weight: Number(r.weight || 0),
      coins: Number(r.coins || 0),
      active: Number(r.active || 0),

      img: (r.img ?? "") || "",
      kind: (r.kind ?? "") || "",

      cost_coins: Number(r.cost_coins ?? 0),

      cost_cent: Number(r.cost_cent ?? 0),
      cost_currency: r.cost_currency ? String(r.cost_currency) : undefined,

      track_qty: Number(r.track_qty || 0),
      qty_left: r.qty_left === null || r.qty_left === undefined ? null : Number(r.qty_left),
      stop_when_zero: Number(r.stop_when_zero || 0),
    }));
  } catch (e: any) {
    const msg = String(e?.message || e);

    // fallback if some columns are missing
    if (
      /no such column:\s*(img|kind|cost_coins|cost_cent|cost_currency|track_qty|qty_left|stop_when_zero)/i.test(msg)
    ) {
      const rows: any = await db
        .prepare(`SELECT code, title, weight, coins, active FROM wheel_prizes WHERE app_public_id = ?`)
        .bind(appPublicId)
        .all();

      return (rows?.results || []).map((r: any) => ({
        code: String(r.code || ""),
        title: String(r.title || r.code || ""),
        weight: Number(r.weight || 0),
        coins: Number(r.coins || 0),
        active: Number(r.active || 0),
      }));
    }

    throw e;
  }
}

// Get prize detail snapshot from wheel_prizes.
// Backward compatible.
async function getWheelPrizeDetails(db: any, appPublicId: string, prizeCode: string) {
  try {
    return await db
      .prepare(
        `SELECT coins, img, kind,
                cost_coins,
                cost_cent, cost_currency,
                track_qty, qty_left, stop_when_zero
         FROM wheel_prizes
         WHERE app_public_id=? AND code=? LIMIT 1`
      )
      .bind(appPublicId, prizeCode)
      .first();
  } catch (e: any) {
    const msg = String(e?.message || e);
    if (
      /no such column:\s*(img|kind|cost_coins|cost_cent|cost_currency|track_qty|qty_left|stop_when_zero)/i.test(msg)
    ) {
      const fallback: any = await db
        .prepare(`SELECT coins FROM wheel_prizes WHERE app_public_id=? AND code=? LIMIT 1`)
        .bind(appPublicId, prizeCode)
        .first();

      return {
        coins: Number(fallback?.coins || 0),
        img: "",
        kind: null,

        cost_coins: 0,
        cost_cent: 0,
        cost_currency: null,

        track_qty: 0,
        qty_left: null,
        stop_when_zero: 1,
      };
    }
    throw e;
  }
}

// Reserve 1 item at "issued".
async function reserveOneIfNeeded(db: any, appPublicId: string, prizeCode: string, prRow: any): Promise<boolean> {
  const track = Number(prRow?.track_qty || 0) === 1;
  if (!track) return true;

  const stop = Number(prRow?.stop_when_zero || 0) === 1;

  try {
    const upd = await db
      .prepare(
        `UPDATE wheel_prizes
         SET qty_left = CASE
           WHEN qty_left IS NULL THEN NULL
           WHEN qty_left > 0 THEN qty_left - 1
           ELSE qty_left
         END
         WHERE app_public_id=?
           AND code=?
           AND track_qty=1
           AND (
             ? = 0
             OR (qty_left IS NOT NULL AND qty_left > 0)
           )`
      )
      .bind(String(appPublicId), String(prizeCode), stop ? 1 : 0)
      .run();

    return !!upd?.meta?.changes;
  } catch (e: any) {
    const msg = String(e?.message || e);
    if (/no such column:\s*(track_qty|qty_left|stop_when_zero)/i.test(msg)) return true;
    throw e;
  }
}

export type WheelArgs = {
  request: Request;
  env: Env;
  db: any;
  type: string;
  payload: any;
  tg: any;
  ctx: { appId: any; publicId: string };
  buildState: (db: any, appId: any, appPublicId: string, tgId: any, cfg: any) => Promise<any>;
  spendCoinsIfEnough: (...args: any[]) => Promise<any>;
  awardCoins: (...args: any[]) => Promise<any>;
};

export async function handleWheelMiniApi(args: WheelArgs): Promise<Response | null> {
  const { request, db, type, payload, tg, ctx, buildState, spendCoinsIfEnough, awardCoins } = args;

  let cfg: any =
    (payload as any)?.cfg ??
    (payload as any)?.config ??
    (payload as any)?.runtime_config ??
    (payload as any)?.state ??
    {};

  // ====== wheel.spin
  if (type === "wheel.spin" || type === "wheel_spin" || type === "spin") {
    const route = "wheel.spin";
    const appPublicId = String((ctx as any).publicId || "");
    const tgUserId = String(tg?.id || "");

    // Spin cost
    const spinCostFromDb = await getSpinCostFromDb(db, appPublicId);
    const spinCostFromCfg = Math.max(
      0,
      Math.floor(Number((cfg as any)?.wheel?.spin_cost ?? (payload?.spin_cost ?? payload?.spin_cost_coins ?? 0)))
    );
    const spinCost = spinCostFromDb !== null ? spinCostFromDb : spinCostFromCfg;

    // coin value cents snapshot (оставляем: полезно для отображения денег, но ROI считаем по prize_cost_coins)
    const coinValueCentsFromDb = await getCoinValueCentsFromDb(db, appPublicId);
    const coinValueCentsFromCfg = Math.max(
      0,
      Math.floor(
        Number(
          (cfg as any)?.app_settings?.coin_value_cents ??
            (cfg as any)?.coins?.coin_value_cents ??
            (cfg as any)?.coins?.value_cents ??
            0
        )
      )
    );
    const coinValueCents = coinValueCentsFromDb !== null ? coinValueCentsFromDb : coinValueCentsFromCfg;

    // 1) create spin row (status new)
    let ins: any;
    try {
      ins = await db
        .prepare(
          `INSERT INTO wheel_spins (app_id, app_public_id, tg_id, status, prize_code, prize_title, spin_cost, prize_coins)
           VALUES (?, ?, ?, 'new', '', '', ?, 0)`
        )
        .bind((ctx as any).appId, appPublicId, tgUserId, spinCost)
        .run();
    } catch (e: any) {
      logWheelEvent({
        code: "mini.wheel.spin.fail.db_error",
        msg: "Failed to create spin",
        appPublicId,
        tgUserId,
        route,
        extra: { error: String(e?.message || e) },
      });
      return json({ ok: false, error: "SPIN_CREATE_FAILED" }, 500, request);
    }

    const spinId = Number((ins as any)?.meta?.last_row_id || (ins as any)?.lastInsertRowid || 0);
    if (!spinId) {
      logWheelEvent({
        code: "mini.wheel.spin.fail.db_error",
        msg: "Spin id was not returned",
        appPublicId,
        tgUserId,
        route,
      });
      return json({ ok: false, error: "SPIN_CREATE_FAILED" }, 500, request);
    }

    // 2) spend coins for spin
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

        return json(
          { ok: false, error: spend?.error || "NOT_ENOUGH", have: spend?.have, need: spend?.need },
          409,
          request
        );
      }
    }

    // 3) load prizes + pick + reserve
    let prizes: PrizeRow[] = [];
    try {
      prizes = await loadWheelPrizes(db, (ctx as any).publicId);
    } catch (e: any) {
      logWheelEvent({
        code: "mini.wheel.spin.fail.db_error",
        msg: "Failed to load prizes",
        appPublicId,
        tgUserId,
        route,
        extra: { spinId, error: String(e?.message || e) },
      });
      return json({ ok: false, error: "DB_ERROR" }, 500, request);
    }

    const baseList = prizes
      .filter((r) => Number(r.active || 0) && Number(r.weight || 0) > 0)
      .map((r) => ({
        ...r,
        code: String(r.code),
        title: String(r.title || r.code),
      }));

    if (!baseList.length) {
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

      logWheelEvent({
        code: "mini.wheel.spin.fail.no_prizes",
        msg: "No active prizes configured",
        appPublicId,
        tgUserId,
        route,
        extra: { spinId },
      });

      return json({ ok: false, error: "NO_PRIZES" }, 400, request);
    }

    // pick & reserve with retries
    let prize: PrizeRow | null = null;
    let prDetails: any = null;

    for (let attempt = 0; attempt < 7; attempt++) {
      const picked = pickWeighted(baseList);
      if (!picked) break;

      prDetails = await getWheelPrizeDetails(db, appPublicId, String(picked.code || ""));
      const okReserve = await reserveOneIfNeeded(db, appPublicId, String(picked.code || ""), prDetails);
      if (!okReserve) {
        prize = null;
        prDetails = null;
        continue;
      }

      prize = picked;
      break;
    }

    if (!prize) {
      if (spinCost > 0) {
        await awardCoins(
          db,
          (ctx as any).appId,
          (ctx as any).publicId,
          tg.id,
          spinCost,
          "wheel_refund",
          String(spinId),
          "Refund: no available prizes",
          `wheel:refund:${(ctx as any).publicId}:${tg.id}:${spinId}`
        );
      }
      try {
        await db.prepare(`DELETE FROM wheel_spins WHERE id=?`).bind(spinId).run();
      } catch (_) {}

      logWheelEvent({
        code: "mini.wheel.spin.fail.no_available_prizes",
        msg: "No available prizes (stock/weights)",
        appPublicId,
        tgUserId,
        route,
        extra: { spinId },
      });

      return json({ ok: false, error: "NO_AVAILABLE_PRIZES" }, 409, request);
    }

    const prizeCoins = Math.max(0, Math.floor(Number(prDetails?.coins ?? prize.coins ?? 0)));
    const prizeImg = String((prDetails?.img ?? prize.img ?? "") || "");

    const kindRaw = String((prDetails?.kind ?? prize.kind ?? "") || "").trim().toLowerCase();
    const kind = kindRaw === "coins" ? "coins" : "item";
    const prizeKind = String(kind || "item");

    // ✅ MAIN: prize_cost_coins snapshot (used for ROI)
    // - coins prize: cost == prizeCoins
    // - item prize: cost == wheel_prizes.cost_coins
    const prizeCostCoins =
      prizeKind === "coins"
        ? Math.max(0, Math.floor(Number(prizeCoins || 0)))
        : Math.max(0, Math.floor(Number(prDetails?.cost_coins ?? (prize as any).cost_coins ?? 0)));

    // legacy (оставляем пока)
    const prizeCostCent =
      prizeKind === "coins"
        ? 0
        : Math.max(0, Math.floor(Number(prDetails?.cost_cent ?? (prize as any).cost_cent ?? 0)));
    const prizeCostCurrencyRaw = prDetails?.cost_currency ?? (prize as any).cost_currency ?? "RUB";
    const prizeCostCurrency = prizeCostCurrencyRaw ? String(prizeCostCurrencyRaw) : "RUB";

    // ==== COINS PRIZE → instant award (no redeem row) ====
    if (kind === "coins" && prizeCoins > 0) {
      const res: any = await awardCoins(
        db,
        (ctx as any).appId,
        appPublicId,
        tg.id,
        prizeCoins,
        "wheel_prize_coins",
        String(spinId),
        String(prize.title || "Wheel coins"),
        `wheel:prize:${appPublicId}:${tg.id}:${spinId}:${prizeCoins}`
      );

      if (!res?.ok) {
        if (spinCost > 0) {
          await awardCoins(
            db,
            (ctx as any).appId,
            appPublicId,
            tg.id,
            spinCost,
            "wheel_refund",
            String(spinId),
            "Refund: coins prize award failed",
            `wheel:refund:${appPublicId}:${tg.id}:${spinId}`
          ).catch(() => null);
        }
        await db.prepare(`DELETE FROM wheel_spins WHERE id=?`).bind(spinId).run().catch(() => null);

        logWheelEvent({
          code: "mini.wheel.spin.fail.award_coins_failed",
          msg: "Failed to award coins prize",
          appPublicId,
          tgUserId,
          route,
          extra: { spinId, err: res?.error || res?.msg || "unknown" },
        });

        return json({ ok: false, error: "AWARD_COINS_FAILED" }, 500, request);
      }

      // write snapshots (best-effort if columns exist)
      await db
        .prepare(
          `UPDATE wheel_spins
           SET status='redeemed',
               prize_code=?,
               prize_title=?,
               prize_coins=?,
               spin_cost=?,

               prize_kind=?,
               prize_cost_cent=?,
               prize_cost_currency=?,
               coin_value_cents=?,
               prize_cost_coins=?,

               ts_issued=datetime('now'),
               ts_redeemed=datetime('now'),
               redeemed_by_tg=?
           WHERE id=?`
        )
        .bind(
          String(prize.code || ""),
          String(prize.title || ""),
          prizeCoins,
          spinCost,

          prizeKind,
          prizeCostCent,
          prizeCostCurrency,
          coinValueCents,
          prizeCostCoins,

          String(tgUserId),
          spinId
        )
        .run()
        .catch(async (e2: any) => {
          const msg2 = String(e2?.message || e2);

          // fallback if new columns missing
          if (/no such column:\s*(prize_cost_coins|coin_value_cents|prize_cost_cent|prize_cost_currency|prize_kind)/i.test(msg2)) {
            await db
              .prepare(
                `UPDATE wheel_spins
                 SET status='redeemed',
                     prize_code=?,
                     prize_title=?,
                     prize_coins=?,
                     spin_cost=?,
                     ts_issued=datetime('now'),
                     ts_redeemed=datetime('now'),
                     redeemed_by_tg=?
                 WHERE id=?`
              )
              .bind(
                String(prize.code || ""),
                String(prize.title || ""),
                prizeCoins,
                spinCost,
                String(tgUserId),
                spinId
              )
              .run()
              .catch(() => null);
            return;
          }

          // unknown error
          throw e2;
        });

      const fresh_state = await buildState(db, (ctx as any).appId, appPublicId, tg.id, cfg);

      return json(
        {
          ok: true,
          prize: { code: prize.code || "", title: prize.title || "", coins: prizeCoins, img: prizeImg || "" },
          spin_cost: spinCost,
          spin_id: spinId,
          rewards_count: 0,
          fresh_state,
          auto_awarded: true,
        },
        200,
        request
      );
    }

    // ===== 5) issue redeem (wheel_redeems) =====
    let redeem: any = null;
    for (let i = 0; i < 5; i++) {
      const redeemCode = randomRedeemCodeLocal(10);
      try {
        // ✅ cost_coins is the main economics
        const ins2 = await db
          .prepare(
            `INSERT INTO wheel_redeems
               (app_id, app_public_id, tg_id, spin_id, prize_code, prize_title,
                redeem_code, status, issued_at, img, expires_at,
                kind, coins,
                cost_coins,
                cost_cent, cost_currency)
             VALUES (?, ?, ?, ?, ?, ?, ?, 'issued', datetime('now'), ?, NULL, ?, ?, ?, ?, ?)`
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

            String(kind || "item"),
            Number.isFinite(Number(prizeCoins)) ? Math.max(0, Math.floor(Number(prizeCoins))) : 0,

            prizeCostCoins,

            prizeCostCent, // legacy
            prizeCostCurrency // legacy
          )
          .run();

        redeem = {
          id: Number((ins2 as any)?.meta?.last_row_id || (ins2 as any)?.lastInsertRowid || 0),
          redeem_code: redeemCode,
        };
        break;
      } catch (e: any) {
        const msg = String(e?.message || e);

        // schema fallback (если cost_coins/legacy колонок нет)
        if (/no such column:\s*(cost_coins|cost_cent|cost_currency)/i.test(msg)) {
          try {
            const ins2 = await db
              .prepare(
                `INSERT INTO wheel_redeems
                   (app_id, app_public_id, tg_id, spin_id, prize_code, prize_title,
                    redeem_code, status, issued_at, img, expires_at,
                    kind, coins)
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
                String(kind || "item"),
                Number.isFinite(Number(prizeCoins)) ? Math.max(0, Math.floor(Number(prizeCoins))) : 0
              )
              .run();

            redeem = {
              id: Number((ins2 as any)?.meta?.last_row_id || (ins2 as any)?.lastInsertRowid || 0),
              redeem_code: redeemCode,
            };
            break;
          } catch (e2: any) {
            const msg2 = String(e2?.message || e2);
            if (!/unique|constraint/i.test(msg2)) {
              logWheelEvent({
                code: "mini.wheel.spin.fail.db_error",
                msg: "Failed to issue reward (fallback)",
                appPublicId,
                tgUserId,
                route,
                extra: { spinId, error: msg2 },
              });
              return json({ ok: false, error: "REDEEM_CREATE_FAILED" }, 500, request);
            }
            continue;
          }
        }

        if (!/unique|constraint/i.test(msg)) {
          logWheelEvent({
            code: "mini.wheel.spin.fail.db_error",
            msg: "Failed to issue reward",
            appPublicId,
            tgUserId,
            route,
            extra: { spinId, error: msg },
          });
          return json({ ok: false, error: "REDEEM_CREATE_FAILED" }, 500, request);
        }
      }
    }

    if (!redeem?.id) {
      logWheelEvent({
        code: "mini.wheel.spin.fail.db_error",
        msg: "Could not issue reward code",
        appPublicId,
        tgUserId,
        route,
        extra: { spinId },
      });
      return json({ ok: false, error: "REDEEM_CREATE_FAILED" }, 500, request);
    }

    // 6) update spin status -> issued (with snapshots)
    try {
      await db
        .prepare(
          `UPDATE wheel_spins
           SET status='issued',
               prize_code=?,
               prize_title=?,
               prize_coins=?,
               spin_cost=?,
               redeem_id=?,

               prize_kind=?,
               prize_cost_cent=?,
               prize_cost_currency=?,
               coin_value_cents=?,
               prize_cost_coins=?,

               ts_issued=datetime('now')
           WHERE id=?`
        )
        .bind(
          String(prize.code || ""),
          String(prize.title || ""),
          prizeCoins,
          spinCost,
          Number(redeem.id),

          prizeKind,
          prizeCostCent,
          prizeCostCurrency,
          coinValueCents,
          prizeCostCoins,

          spinId
        )
        .run();
    } catch (e: any) {
      const msg = String(e?.message || e);

      // fallback for older schema
      if (/no such column:\s*(prize_cost_coins|coin_value_cents|prize_cost_cent|prize_cost_currency|prize_kind)/i.test(msg)) {
        await db
          .prepare(
            `UPDATE wheel_spins
             SET status='issued',
                 prize_code=?,
                 prize_title=?,
                 prize_coins=?,
                 spin_cost=?,
                 redeem_id=?,
                 ts_issued=datetime('now')
             WHERE id=?`
          )
          .bind(
            String(prize.code || ""),
            String(prize.title || ""),
            prizeCoins,
            spinCost,
            Number(redeem.id),
            spinId
          )
          .run()
          .catch(() => null);
      } else {
        throw e;
      }
    }

    // 7) count issued rewards
    let rewardsCountRow: any;
    try {
      rewardsCountRow = await db
        .prepare(
          `SELECT COUNT(*) AS c
           FROM wheel_redeems
           WHERE app_public_id=? AND tg_id=? AND status='issued'`
        )
        .bind(appPublicId, tgUserId)
        .first();
    } catch (e: any) {
      logWheelEvent({
        code: "mini.wheel.spin.fail.db_error",
        msg: "Failed to count rewards",
        appPublicId,
        tgUserId,
        route,
        extra: { spinId, error: String(e?.message || e) },
      });
      return json({ ok: false, error: "DB_ERROR" }, 500, request);
    }

    const rewards_count = Math.max(0, Number(rewardsCountRow?.c || 0));
    const fresh_state = await buildState(db, (ctx as any).appId, (ctx as any).publicId, tg.id, cfg);

    logWheelEvent({
      code: "mini.wheel.spin.ok",
      msg: "Spin completed",
      appPublicId,
      tgUserId,
      route,
      extra: { spinId, prizeCode: String(prize.code || ""), rewards_count, prize_cost_coins: prizeCostCoins },
    });

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

  // ====== wheel.claim (legacy / noop)
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

  // ====== wheel.rewards (wallet from wheel_redeems)
  if (type === "wheel.rewards" || type === "wheel_rewards") {
    const route = "wheel.rewards";
    const appPublicId = String((ctx as any).publicId || "");
    const tgUserId = String(tg?.id || "");

    try {
      // пробуем с cost_coins, если колонки нет — fallback
      try {
        const rows: any = await db
          .prepare(
            `SELECT id, prize_code, prize_title, redeem_code, status,
                    issued_at, img, expires_at,
                    cost_coins
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
          cost_coins: Math.max(0, Number(r.cost_coins ?? 0)),
        }));

        logWheelEvent({
          code: "mini.wheel.rewards.ok",
          msg: "Rewards fetched",
          appPublicId,
          tgUserId,
          route,
          extra: { rewards_count: rewards.length },
        });

        return json({ ok: true, rewards, rewards_count: rewards.length }, 200, request);
      } catch (e0: any) {
        const msg0 = String(e0?.message || e0);
        if (!/no such column:\s*cost_coins/i.test(msg0)) throw e0;

        const rows: any = await db
          .prepare(
            `SELECT id, prize_code, prize_title, redeem_code, status,
                    issued_at, img, expires_at
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
          cost_coins: 0,
        }));

        logWheelEvent({
          code: "mini.wheel.rewards.ok",
          msg: "Rewards fetched (fallback)",
          appPublicId,
          tgUserId,
          route,
          extra: { rewards_count: rewards.length },
        });

        return json({ ok: true, rewards, rewards_count: rewards.length }, 200, request);
      }
    } catch (e: any) {
      logWheelEvent({
        code: "mini.wheel.rewards.fail.db_error",
        msg: "Failed to fetch rewards",
        appPublicId,
        tgUserId,
        route,
        extra: { error: String(e?.message || e) },
      });
      return json({ ok: false, error: "DB_ERROR" }, 500, request);
    }
  }

  return null;
}


