// src/services/coinsLedger.ts

// --- COINS LEDGER (аналог _awardCoins / _getLastBalance) ---
export async function getLastBalance(db, appPublicId, tgId){
  const row = await db.prepare(
    `SELECT balance_after FROM coins_ledger
     WHERE app_public_id = ? AND tg_id = ?
     ORDER BY id DESC LIMIT 1`
  ).bind(appPublicId, String(tgId)).first();
  return row ? Number(row.balance_after||0) : 0;
}

export async function setUserCoins(db, appPublicId, tgId, balance){
  await db.prepare(
    `UPDATE app_users SET coins = ? WHERE app_public_id = ? AND tg_user_id = ?`
  ).bind(balance, appPublicId, String(tgId)).run();
}

export async function awardCoins(db, appId, appPublicId, tgId, delta, src, ref_id, note, event_id){
  // идемпотентность по event_id
  if (event_id) {
    const ex = await db.prepare(
      `SELECT balance_after FROM coins_ledger WHERE event_id = ? LIMIT 1`
    ).bind(event_id).first();
    if (ex) return { ok:true, reused:true, balance: Number(ex.balance_after||0) };
  }
  const last = await getLastBalance(db, appPublicId, tgId);
  const bal = Math.max(0, last + Number(delta||0));
  await db.prepare(
    `INSERT INTO coins_ledger (app_id, app_public_id, tg_id, event_id, src, ref_id, delta, balance_after, note)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(appId, appPublicId, String(tgId), event_id||null, String(src||''), String(ref_id||''), Number(delta||0), bal, String(note||'')).run();
  await setUserCoins(db, appPublicId, tgId, bal);
  return { ok:true, balance: bal };
}

export async function spendCoinsIfEnough(db, appId, appPublicId, tgId, cost, src, ref_id, note, event_id){
  cost = Math.max(0, Math.floor(Number(cost || 0)));
  if (cost <= 0) return { ok:true, spent:0, balance: await getLastBalance(db, appPublicId, tgId) };

  const last = await getLastBalance(db, appPublicId, tgId);
  if (last < cost){
    return { ok:false, error:'NOT_ENOUGH_COINS', have:last, need:cost };
  }
  // списываем отрицательным delta, идемпотентно по event_id
  const res = await awardCoins(db, appId, appPublicId, tgId, -cost, src, ref_id, note, event_id);
  return { ok:true, spent:cost, balance: res.balance };
}


