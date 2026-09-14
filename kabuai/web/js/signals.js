// signals.js — EOD買いシグナルの判定ヘルパ・ウォッチ・持ち株コーチ（v4から移植・ロジック不変）
// 反発(反転/強反転)系は v3(2026-07-07)で主軸復帰済み。表示制御は BUY_KEYS の1か所。
const BUY_KEYS = ["strong_reversal", "reversal", "strong_accum", "accum"];
const TRK_WIN_MIN = 52;   // 検証済みエッジのゲート（得意保有の勝率52%以上＆平均プラス）
const BUY_PRICE_MIN = 1000;
const SECTOR_CAP = 3;
const PLAN = { hold: 8, sl: 12 };

function sigTrack(k) { const t = DATA.signal_track; return (t && t.available && t.groups && t.groups[k]) || null; }
function bestHorizon(g, hs) {
  let best = null;
  hs.forEach(h => { const x = g.h && g.h[String(h)]; if (!x || !x.n) return;
    if (best === null || x.win > best.x.win || (x.win === best.x.win && x.avg > best.x.avg)) best = { h, x }; });
  return best;
}
function sigHasEdge(k) {
  if (BUY_KEYS.indexOf(k) < 0) return false;
  const g = sigTrack(k); if (!g || !g.n) return false;
  const b = bestHorizon(g, (DATA.signal_track.horizons) || [5, 10, 20]);
  return !!(b && b.x.win >= TRK_WIN_MIN && b.x.avg > 0);
}
function sigEV(k) {
  if (!sigHasEdge(k)) return null;
  const g = sigTrack(k);
  const b = bestHorizon(g, (DATA.signal_track.horizons) || [5, 10, 20]);
  const ex = (g.exit && g.exit.n) ? g.exit : null;
  const m = (DATA.signals && DATA.signals.groups && DATA.signals.groups[k]) || {};
  return { k, label: m.label || k, emoji: m.emoji || "", exit: ex,
    ev: ex ? ex.avg : b.x.avg, win: ex ? ex.win : b.x.win, hold: ex ? ex.t : b.h, sl: ex ? ex.sl : 12 };
}
function buyStat(r) {
  const a = (r.signals || []).map(sigEV).filter(Boolean);
  if (!a.length) return null;
  a.sort((x, y) => y.ev - x.ev);
  return a[0];
}
const WHY_BUY = {
  strong_reversal: "20日で大きく下げたあとの力強い反発初動",
  reversal: "下げトレンドからの反発の初動",
  strong_accum: "大商いをともなう強い買い集め",
  accum: "出来高をともなう緩やかな買い集め" };
function whyBuy(r) { const st = buyStat(r); return (st && WHY_BUY[st.k]) || ""; }
function exitRep() {
  for (const k of BUY_KEYS) { const g = sigTrack(k); if (g && g.exit && g.exit.n) return g.exit; }
  return { t: 8, sl: 12 };
}
// 先物連動タグ
function ftagRank(r) { const t = r && r.futures_tag; if (t === "自力") return 0; if (t === "中連動") return 1; if (t === "高連動") return 3; return 2; }
function ftagPct(corr) { if (corr == null || isNaN(corr)) return null; const p = Math.round(Math.abs(Number(corr)) * 100); return Number(corr) < 0 ? `日経と逆${p}%` : `日経連動${p}%`; }
function ftagChip(r) {
  const t = r && r.futures_tag; if (!t) return "";
  const c = t === "自力" ? "self" : t === "高連動" ? "high" : "mid";
  const pct = ftagPct(r.futures_corr);
  return `<span class="ftag ${c}" title="日経平均（先物・日経レバ1570）との連動度">${pct ? pct + "・" : ""}${esc(t)}</span>`;
}
function hideLowPrice() { try { return localStorage.getItem("kabuai_lowprice") !== "0"; } catch (e) { return true; } }
function priceOK(r) { return !hideLowPrice() || !(r && r.price > 0 && r.price < BUY_PRICE_MIN); }

// 買い候補（検索/詳細/ウォッチの✅表示・🆕判定に使う）
function candidates() {
  const s = DATA.signals; if (!s) return { list: [], hidden: 0 };
  const lit = (s.order || []).filter(k => BUY_KEYS.indexOf(k) >= 0 && s.groups[k] && s.groups[k].count > 0 && sigHasEdge(k));
  const seen = {}, all = [];
  lit.forEach(k => (s.groups[k].members || []).forEach(r => {
    if (seen[r.code]) return; seen[r.code] = 1;
    all.push({ ...r, signals: (r.signals || []).filter(x => BUY_KEYS.indexOf(x) >= 0 && sigHasEdge(x)) });
  }));
  const pickMin = Number(DATA.pick_min_oku) || 0;
  const thick = all.filter(r => (r.turnover_oku != null ? r.turnover_oku : pickMin) >= pickMin);
  const maxRng = Number(DATA.pick_max_rng) || 0;
  const calm = maxRng ? thick.filter(r => !(r.rng20 != null && r.rng20 >= maxRng)) : thick;
  const sorted = calm.filter(priceOK), hidden = calm.length - sorted.length;
  sorted.sort((a, b) => ((((buyStat(b) || { ev: -1e9 }).ev) - ((buyStat(a) || { ev: -1e9 }).ev)))
    || (ftagRank(a) - ftagRank(b)) || ((a.rsi != null ? a.rsi : 50) - (b.rsi != null ? b.rsi : 50)));
  const secCount = {}, list = []; let dropped = 0;
  for (const r of sorted) {
    const sec = r.sector || ("_" + r.code);
    if ((secCount[sec] || 0) >= SECTOR_CAP) { dropped++; continue; }
    secCount[sec] = (secCount[sec] || 0) + 1; list.push(r);
  }
  return { list, hidden, dropped, wild: thick.length - calm.length };
}
function isCand(r) { return (r.signals || []).some(sigHasEdge); }
let NEW_CODES = new Set();
function computeNewCodes() {
  try {
    const cur = candidates().list.map(r => r.code);
    const prev = JSON.parse(localStorage.getItem("kabuai_seen") || "null");
    NEW_CODES = (prev && prev.date && prev.date !== DATA.data_date && Array.isArray(prev.codes)) ? new Set(cur.filter(c => prev.codes.indexOf(c) < 0)) : new Set();
    localStorage.setItem("kabuai_seen", JSON.stringify({ date: DATA.data_date, codes: cur }));
  } catch (e) { NEW_CODES = new Set(); }
}
function isNew(code) { return NEW_CODES.has(code); }
function newChip(code) { return isNew(code) ? `<span class="chip new">🆕新規</span>` : ""; }
function rngTag(r) { const v = Number(r.rng20); if (r.rng20 == null || !isFinite(v)) return ""; return ` ・ 値動き${(v >= 3 && v < 4) ? `<b class="pos">⚡${v.toFixed(1)}%</b>` : v.toFixed(1) + "%"}/日`; }
function sigChips(r) {
  const edge = (r.signals || []).filter(sigHasEdge);
  if (!edge.length) return "";
  return `<div class="chips">` + newChip(r.code) + edge.map(k => { const e = sigEV(k); return e ? `<span class="chip buy">✅ ${e.emoji}${e.label}<span class="muted" style="font-weight:400"> 勝率${e.win}%</span></span>` : ""; }).join("") + ftagChip(r) + `</div>`;
}
function planLine(r) {
  const st = buyStat(r); if (!st) return "";
  return `<div class="planline"><span class="pl-ev ${st.ev >= 0 ? "pos" : "neg"}">期待値 ${st.ev >= 0 ? "+" : ""}${st.ev}%<small class="muted">/件</small></span>
    <span>勝率${st.win}% ・ 保有${st.hold}日めど ・ 損切り-${st.sl}%</span></div>`;
}
function sigYears(k) { const g = sigTrack(k); return (g && g.exit_years && g.exit_years.length) ? g.exit_years : null; }
function yearLine(ks) {
  const st = buyStat({ signals: ks }); if (!st) return "";
  const ys = sigYears(st.k); if (!ys) return "";
  const pos = ys.filter(y => y.avg > 0).length;
  const cells = ys.map(y => `<span class="yrcell ${y.avg > 0 ? "pos" : "neg"}"><b>${y.y}</b>${y.avg > 0 ? "+" : ""}${y.avg}%<span class="muted" style="display:block;font-size:9px">勝率${y.win}%</span></span>`).join("");
  return `<div class="muted" style="margin-top:9px;font-size:11.5px">📅 年別の実績（この型・保有${st.hold}日/-${st.sl}%）— <b style="color:var(--tx)">${ys.length}年中${pos}年プラス</b>：</div><div class="yrline">${cells}</div>`;
}

// ── ⭐ ウォッチ（localStorage kabuai_watch 継承）──
let WATCH = readWatch();
function readWatch() { try { return JSON.parse(localStorage.getItem("kabuai_watch") || "[]") || []; } catch (e) { return []; } }
function saveWatch() { try { localStorage.setItem("kabuai_watch", JSON.stringify(WATCH)); } catch (e) { /* ignore */ } }
function isWatched(code) { return WATCH.indexOf(code) >= 0; }
function toggleWatch(code) { const i = WATCH.indexOf(code); if (i >= 0) WATCH.splice(i, 1); else WATCH.unshift(code); saveWatch(); return isWatched(code); }
function star(code) { const on = isWatched(code); return `<span class="star${on ? " on" : ""}" data-star="${code}" onclick="onStar(event,'${code}')">${on ? "★" : "☆"}</span>`; }
function onStar(ev, code) {
  ev.preventDefault(); ev.stopPropagation(); const on = toggleWatch(code);
  if (document.querySelectorAll) document.querySelectorAll('[data-star="' + code + '"]').forEach(b => { b.className = "star" + (on ? " on" : ""); b.textContent = on ? "★" : "☆"; });
}
function watchSummaryInner() {
  const found = WATCH.map(c => SEARCH && SEARCH.find(s => s.code === c)).filter(Boolean);
  if (!found.length) return `<span class="muted">⭐ ウォッチ ${WATCH.length}件（本日のデータ対象外）</span>`;
  const chips = found.map(r => `<a class="chip${isCand(r) ? " buy" : ""}" href="#/detail/${r.code}">${esc(r.name)} ${isCand(r) ? "✅" : ""}${pctTag(r.r1) ? " " + pctTag(r.r1) : ""}</a>`).join("");
  const n = found.filter(isCand).length;
  return `<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px"><b>⭐ ウォッチ ${found.length}件</b><a style="margin-left:auto;color:var(--acc);font-weight:600;font-size:12px" href="#/search">一覧 →</a></div>
    <div class="muted" style="font-size:12px;margin-bottom:6px">${n ? `<b class="pos">${n}件に参考シグナル点灯</b>` : "本日、参考シグナルの点灯はありません"}</div><div class="chips">${chips}</div>`;
}

// ── 📒 持ち株コーチ（localStorage kabuai_pos 継承・出口=保有8日/-12%）──
function loadPos() { try { return JSON.parse(localStorage.getItem("kabuai_pos") || "[]"); } catch (e) { return []; } }
function savePos(a) { try { localStorage.setItem("kabuai_pos", JSON.stringify(a)); } catch (e) { /* ignore */ } }
function addPosFromForm(code, name) {
  const pr = parseFloat((document.getElementById("pos-price") || {}).value);
  const dt = (document.getElementById("pos-date") || {}).value;
  const shR = parseFloat((document.getElementById("pos-shares") || {}).value);
  if (!(pr > 0) || !dt) return;
  const rec = { code, name, entry: pr, date: dt }; if (shR > 0) rec.shares = Math.round(shR);
  const a = loadPos(); a.push(rec); savePos(a); render();
}
function delPos(i) { const a = loadPos(); a.splice(i, 1); savePos(a); render(); }
function bizDays(from, to) {
  let d = new Date(from + "T00:00:00"), e = new Date(to + "T00:00:00"), n = 0;
  if (isNaN(d) || isNaN(e) || d > e) return 0;
  while (d <= e && n <= 60) { const w = d.getDay(); if (w !== 0 && w !== 6) n++; d.setDate(d.getDate() + 1); }
  return n;
}
function bizAdd(from, n) {
  let d = new Date(from + "T00:00:00"), c = 0;
  if (isNaN(d)) return "—";
  for (let i = 0; i < 60; i++) { const w = d.getDay(); if (w !== 0 && w !== 6) { c++; if (c >= n) break; } d.setDate(d.getDate() + 1); }
  return `${d.getMonth() + 1}/${d.getDate()}`;
}
function posCoach() {
  const a = loadPos(); if (!a.length) return "";
  const ref = DATA.data_date || "";
  const todayS = todayStr();
  let sum = 0, cnt = 0, winN = 0, loseN = 0, actN = 0, sumYen = 0, hasYen = false;
  const items = a.map((p, i) => {
    const row = (DATA.ranking || []).find(r => r.code === p.code) || (SEARCH && SEARCH.find(r => r.code === p.code)) || null;
    const cur = row ? row.price : null;
    const pnl = (cur != null && p.entry > 0) ? (cur / p.entry - 1) * 100 : null;
    if (pnl != null) { sum += pnl; cnt++; if (pnl > 0) winN++; else if (pnl < 0) loseN++; }
    const yenV = (pnl != null && p.shares > 0) ? Math.round((cur - p.entry) * p.shares) : null;
    if (yenV != null) { sumYen += yenV; hasYen = true; }
    const slP = Math.round(p.entry * (1 - PLAN.sl / 100));
    const day = bizDays(p.date, todayS);
    let act, actCls = "";
    if (cur != null && cur <= slP) { act = `🔴 損切りライン（¥${slP.toLocaleString()}）割れ — <b>今日売って仕切り直し</b>`; actCls = "neg"; actN++; }
    else if (day >= PLAN.hold) { act = `⏰ ${PLAN.hold}営業日目 — <b>今日の大引けで手仕舞い</b>`; actN++; }
    else { act = `🟢 ホールド（${day}日目／${PLAN.hold}日・手仕舞いめど ${bizAdd(p.date, PLAN.hold)}）・損切り ¥${slP.toLocaleString()}`; }
    return `<div class="poschip"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap">
        <a href="#/detail/${p.code}" style="font-weight:800">${esc(p.name || p.code)}</a>
        <span class="muted" style="font-size:11px">${p.code}・${p.date}に¥${(+p.entry).toLocaleString()}${p.shares ? `×${p.shares}株` : ""}</span>
        <span style="margin-left:auto;font-weight:800" class="${pnl != null ? cls(pnl) : ""}">${pnl != null ? fmtPct(pnl) : "—"}${yenV != null ? `<span style="font-size:11px;font-weight:600">（${yenV >= 0 ? "+" : "−"}¥${Math.abs(yenV).toLocaleString()}）</span>` : ""}</span>
        <button class="posdel" onclick="event.preventDefault();delPos(${i})">売った/削除</button></div>
      <div class="${actCls}" style="font-size:12.5px;margin-top:4px">${act}</div></div>`;
  }).join("");
  const avg = cnt ? sum / cnt : null;
  const summary = `<div class="possum"><b>保有 ${a.length}件</b>${avg != null ? `<span>平均損益 <b class="${cls(avg)}">${fmtPct(avg)}</b></span>` : ""}
    ${hasYen ? `<span>合計 <b class="${cls(sumYen)}">${sumYen >= 0 ? "+" : "−"}¥${Math.abs(sumYen).toLocaleString()}</b></span>` : ""}
    ${cnt ? `<span class="muted">🟢利益中${winN} / 🔴損失中${loseN}</span>` : ""}
    ${actN ? `<span class="warn" style="font-weight:700">⚠ 今日うごく ${actN}件</span>` : `<span class="muted">今日はホールドでOK</span>`}</div>`;
  return `<h2>📒 持ち株コーチ</h2><div class="card">${summary}${items}<div class="note" style="margin-top:6px">損益は <b>${ref} の終値</b>ベース。出口は検証済みの1本（保有8日・損切り-12%・利確なし）。</div></div>`;
}
function posForm(code, name, price) {
  const ds = todayStr();
  const owned = loadPos().filter(p => p.code === code).length;
  return `<div class="card"><div style="font-weight:700">📒 買った記録をつける ${owned ? `<span class="tag">記録 ${owned}件</span>` : ""}</div>
    <div class="note" style="margin-top:4px">記録すると🐵EODタブの「持ち株コーチ」が毎日 <b>保有日数・損切りライン・手仕舞い日</b> を教えます。</div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:9px;align-items:center">
      <input id="pos-price" class="posin" type="number" inputmode="decimal" value="${price || ""}" placeholder="買値（円）">
      <input id="pos-shares" class="posin" type="number" inputmode="numeric" placeholder="株数（任意）" style="width:120px">
      <input id="pos-date" class="posin" type="date" value="${ds}">
      <button class="posadd" onclick="addPosFromForm('${code}','${esc(name).replace(/'/g, "")}')">保存</button></div></div>`;
}
