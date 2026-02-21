// src/services/runtimeConfig.ts
// Derive runtime config (wheel/passport) from Studio blueprint.

export function extractRuntimeConfigFromBlueprint(BP: any) {
  const cfg: any = {
    wheel: { spin_cost: 0, claim_cooldown_h: 24, daily_limit: 0, prizes: [] },
    passport: {
      require_pin: false,
      collect_coins: 0,
      grid_cols: 3,
      styles: [],
      reward_prize_code: "",
      passport_key: "default",
    },
    profile_quiz: { coins_per_correct: 0, max_per_submit: 0 },
    leaderboard: { top_n: 10 },
  };

  const routes = Array.isArray(BP && BP.routes) ? BP.routes : [];
  const blocksDict = (BP && BP.blocks && typeof BP.blocks === "object") ? BP.blocks : {};

  const insts: any[] = [];
  try {
    for (const rt of routes) {
      const blocks = (rt && Array.isArray(rt.blocks)) ? rt.blocks : [];
      for (const b of blocks) insts.push(b);
    }
  } catch (_) {}

  const getProps = (inst: any) => {
    if (!inst) return {};
    if (inst.props && typeof inst.props === "object") return inst.props;
    const id = inst.id != null ? String(inst.id) : "";
    const p = id && (blocksDict as any)[id];
    return (p && typeof p === "object") ? p : {};
  };

// Wheel
const wheel = insts.find((b) => b && (b.key === "bonus_wheel_one" || b.type === "bonus_wheel_one"));
if (wheel) {
  const p: any = getProps(wheel);
  cfg.wheel.spin_cost = Number(p.spin_cost || 0);
  cfg.wheel.claim_cooldown_h = Number(p.claim_cooldown_h || 24);
  cfg.wheel.daily_limit = Number(p.daily_limit || 0);

  const arr = Array.isArray(p.prizes) ? p.prizes : (Array.isArray(p.sectors) ? p.sectors : []);
  cfg.wheel.prizes = arr
    .map((pr: any) => {
      const wRaw = Number(pr && pr.weight);
      const cRaw = Number(pr && pr.coins);

      const kindRaw = String(pr?.kind || "").toLowerCase();
      const kind =
        kindRaw === "coins" ? "coins" :
        kindRaw === "item" ? "item" :
        kindRaw === "physical" ? "item" :
        (Number.isFinite(cRaw) && cRaw > 0) ? "coins" : "item";

      const costCentRaw = Number(pr?.cost_cent ?? pr?.cost ?? 0);
      const qtyLeftRaw = Number(pr?.qty_left ?? pr?.stock_qty ?? 0);

      const trackQty =
        pr?.track_qty === true ||
        Number(pr?.track_qty || 0) === 1 ||
        pr?.stock_qty !== undefined;

      const stopWhenZero =
        pr?.stop_when_zero === undefined ? true : !!pr.stop_when_zero;

      return {
        code: String((pr && pr.code) || "").trim(),
        title: String((pr && (pr.title || pr.name || pr.code)) || "").trim(),

        // IMPORTANT: editor stores basis points already (pct*100)
        weight: Number.isFinite(wRaw) ? Math.max(0, Math.round(wRaw)) : 0,

        // kind + coins
        kind,
        coins: (kind === "coins" && Number.isFinite(cRaw)) ? Math.max(0, Math.round(cRaw)) : 0,

        active: (pr && pr.active === false) ? false : true,

        // visual
        img: pr?.img ? String(pr.img) : "",

        // ✅ NEW: себестоимость приза в МОНЕТАХ (для item)
  cost_coins: (kind === "item" && Number.isFinite(costCoinsRaw))
    ? Math.max(0, Math.round(costCoinsRaw))
    : 0,

  // (пока оставляем как было — ниже ты решишь, выпиливаем ли cost_cent/currency полностью)
  cost_cent: (kind === "item" && Number.isFinite(costCentRaw)) ? Math.max(0, Math.round(costCentRaw)) : 0,
  cost_currency: String(pr?.cost_currency ?? pr?.currency ?? "RUB"),
  cost_currency_custom: String(pr?.cost_currency_custom ?? pr?.currency_custom ?? ""),

  track_qty: (kind === "item") ? !!trackQty : false,
  qty_left: (kind === "item" && Number.isFinite(qtyLeftRaw)) ? Math.max(0, Math.round(qtyLeftRaw)) : 0,
  stop_when_zero: (kind === "item") ? !!stopWhenZero : true,
};

        // economics / inventory (item only)
        cost_cent: (kind === "item" && Number.isFinite(costCentRaw)) ? Math.max(0, Math.round(costCentRaw)) : 0,
        cost_currency: String(pr?.cost_currency ?? pr?.currency ?? "RUB"),
        cost_currency_custom: String(pr?.cost_currency_custom ?? pr?.currency_custom ?? ""),

        track_qty: (kind === "item") ? !!trackQty : false,
        qty_left: (kind === "item" && Number.isFinite(qtyLeftRaw)) ? Math.max(0, Math.round(qtyLeftRaw)) : 0,
        stop_when_zero: (kind === "item") ? !!stopWhenZero : true,
      };
    })
    .filter((x: any) => x.code);
}


  // Styles passport
  const passp = insts.find((b) => b && (b.key === "styles_passport_one" || b.type === "styles_passport_one"));
  if (passp) {
    const p: any = getProps(passp);
    cfg.passport.require_pin = !!p.require_pin;
    cfg.passport.collect_coins = Number(p.collect_coins || 0);
    if (isFinite(p.grid_cols)) cfg.passport.grid_cols = Number(p.grid_cols);

    cfg.passport.reward_prize_code = String(p.reward_prize_code || "").trim();
    if (p.passport_key !== undefined) cfg.passport.passport_key = String(p.passport_key || "default").trim();

    const arr = Array.isArray(p.styles) ? p.styles : [];
    cfg.passport.styles = arr
      .map((s: any) => ({
        code: String((s && s.code) || "").trim(),
        name: String((s && (s.name || s.code)) || "").trim(),
        active: (s && s.active === false) ? false : true,
      }))
      .filter((x: any) => x.code);
  }

  // Profile quiz
  const prof = insts.find((b) => b && (b.key === "profile" || b.type === "profile"));
  if (prof) {
    const p: any = getProps(prof);
    if (isFinite(p.coins_per_correct)) cfg.profile_quiz.coins_per_correct = Number(p.coins_per_correct);
    if (isFinite(p.max_per_submit)) cfg.profile_quiz.max_per_submit = Number(p.max_per_submit);
  }

  return cfg;
}
