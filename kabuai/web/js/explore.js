// explore.js — 🧭銘柄探検（カテゴリ・スクリーナー・data/explorer.json 遅延fetch・v4から移植）
let EXP = null, EXP_LOADING = false;
async function loadExplorer() {
  if (EXP || EXP_LOADING) return EXP;
  EXP_LOADING = true;
  EXP = await fetchFirst(["../data/explorer.json", "data/explorer.json", "./data/explorer.json"]);
  EXP_LOADING = false;
  return EXP;
}
const CAT_DEFS = {
  break60: ["🚩", "初動ブレイク（10年検証済み）", "60日ぶり高値を初めて更新した陽線×出来高1.5倍以上。負け筋（当日+20%・出来高10倍・抜き幅10%）は除外。10年BT=1件+0.64%/PF1.17、🆕新高値付きは+1.35%/PF1.35。勝率は約半分。"],
  shodo: ["🌱", "初動", "長期低迷から出来高を伴って75日線を上抜けた転換の初日"],
  shodo_wait: ["⏳", "初動待ち", "初動の条件にあと一歩（75日線のすぐ下まで接近）"],
  nagi: ["🤫", "静かな初動（凪ブレイク）", "凪（低ボラ）に静かな出来高増→20日高値を初突破した陽線。爆発前を狙う立花型。"],
  rising: ["🚀", "上昇中", "初動後もトレンド継続中（押しが浅い）"],
  oshime: ["🎯", "押し目", "初動後の上昇から調整中（フィボ押しの範囲内）"],
  rebound: ["⚡", "短期反発候補", "急騰後にフィボ38.2〜50%押しゾーンで押し目形成中"],
  stop_high: ["🔥", "ストップ高", "値幅制限いっぱいまで買われた銘柄（張り付き含む）"],
  precursor: ["🧪", "C/Dに材料が出た", "まだ走っていないランクC/Dで、今日のIR・株探の人気テーマ・資金セクターの熱いグループに当たり、出来高か値が動き始めた銘柄。予測ではなく調べる候補（10年: C/D→A以上は0.8%）。"],
  honmei: ["📝", "本命の台帳（紙）", "初動ブレイクで自分が選んだ本命を記録し、翌寄り建て→20日/−15%で答え合わせ。同じ晩の候補全体と比べる。"] };
function expReload(hash) { loadExplorer().then(() => { if ((location.hash || "#/") === hash) render(); }); }
function viewExplore() {
  if (!EXP) { expReload("#/explore"); return `${dataSubNav("explore")}<h2>🧭 銘柄探検</h2><div class="sk-wrap"><div class="sk"></div><div class="sk"></div></div>`; }
  const c = { ...(EXP.counts || {}), precursor: (DATA.precursor && DATA.precursor.rows || []).length, honmei: honmeiLoad().picks.length };
  const cell = k => { const [e, l, d] = CAT_DEFS[k];
    return `<a class="card" style="display:flex;align-items:center;gap:10px;margin-bottom:8px" href="#/explore/${k}"><span style="font-size:20px">${e}</span>
      <span style="flex:1;min-width:0"><b>${l}</b><span class="note" style="display:block">${d}</span></span>
      <b style="font-size:18px;color:${(c[k] || 0) ? "var(--up)" : "var(--mut)"}">${c[k] || 0}</b><span class="muted" style="font-size:11px">件</span></a>`; };
  return `${dataSubNav("explore")}<h2>🧭 銘柄探検 <span class="sub">${EXP.data_date || ""} 終値時点・優位性は未検証（勝率は出しません）</span></h2>
    <div class="hh">スイング狙い</div>${["break60", "honmei", "precursor", "shodo", "shodo_wait", "nagi", "rising", "oshime"].map(cell).join("")}
    <div class="hh">デイトレ・短期</div>${["rebound", "stop_high"].map(cell).join("")}
    <div class="card note">${esc(EXP.note || "")} 状態は毎日引け後のバッチで更新。</div><p class="disc">${esc(DATA.disclaimer)}</p>`;
}
function catRow(cat, r) {
  const head = `<div class="pk-nm"><b>${esc(r.name)}</b><small>${r.code} ・ ${yen(r.price)} ・ 前日比 ${pctTag(r.r1) || "—"}</small>`;
  let sub = "", right = "";
  if (cat === "stop_high") { sub = `<div class="chips"><span class="chip dn">🔥 ${r.stuck ? "張り付き" : "S高タッチ"}</span>${r.today ? `<span class="chip">当日</span>` : ""}</div>`; right = `<div class="pk-ev">${r.date || ""}</div>`; }
  else if (cat === "shodo") { sub = `<div class="chips"><span class="chip buy">🌱 初動</span><span class="chip">出来高 ${r.volr != null ? r.volr + "倍" : "—"}</span></div>`; right = `<div class="pk-ev">初動 ${yen(r.shodo_price)}<small>${r.shodo_date || ""}</small></div>`; }
  else if (cat === "shodo_wait") { sub = `<div class="chips"><span class="chip">⏳ 75日線まで ${r.ma_gap_pct}%</span><span class="chip">条件 ${r.conds}/3</span></div>`; }
  else if (cat === "break60") { sub = `<div class="chips"><span class="chip buy">🚩 60日ぶり高値ブレイク</span>${r.new250 ? `<span class="chip buy">🆕 新高値(250日)</span>` : ""}<span class="chip">出来高 ${r.volx != null ? r.volx + "倍" : "—"}</span>${r.chg != null ? `<span class="chip">当日 +${r.chg}%</span>` : ""}${r.atr_pct != null ? `<span class="chip ${r.calm ? "" : "dn"}">${r.calm ? "凪" : "荒い"} ATR ${r.atr_pct}%</span>` : ""}${r.today ? `<span class="chip">当日</span>` : ""}</div>`; right = `<div class="pk-ev">${r.date || ""}<br><button class="hbtn ${honmeiHas(r.code) ? "on" : ""}" onclick="event.preventDefault();event.stopPropagation();honmeiToggle('${r.code}')">${honmeiHas(r.code) ? "📝 本命" : "本命"}</button></div>`; }
  else if (cat === "nagi") { sub = `<div class="chips"><span class="chip buy">🤫 凪ブレイク</span><span class="chip">出来高 ${r.volr != null ? r.volr + "倍" : "—"}</span><span class="chip">凪ATR ${r.atr_pct != null ? r.atr_pct + "%" : "—"}</span>${r.today ? `<span class="chip">当日</span>` : ""}</div>`; right = `<div class="pk-ev">${r.date || ""}</div>`; }
  else if (cat === "rising" || cat === "oshime") { sub = `<div class="chips"><span class="chip ${cat === "rising" ? "buy" : ""}">${cat === "rising" ? "🚀" : "🎯"} 押し ${r.pullback_pct}%</span><span class="chip">高値 ${yen(r.hi)}</span></div>`;
    right = `<div class="pk-ev">初動比 <span class="${r.price >= r.shodo_price ? "pos" : "neg"}">${r.shodo_price ? ((r.price / r.shodo_price - 1) * 100).toFixed(1) : "—"}%</span><small>${r.shodo_date || ""}</small></div>`; }
  else if (cat === "rebound") { const tags = (r.tags || []).map(t => `<span class="chip ${t === "下げ止まり" ? "up" : t === "N字" ? "acc" : "dn"}">${esc(t)}</span>`).join("");
    sub = `<div class="chips"><span class="chip ${r.in_zone ? "buy" : ""}">押し ${r.fib_pct}%${r.in_zone ? "（38.2〜50ゾーン）" : ""}</span>${tags}</div>`; right = `<div class="pk-ev">急騰 +${r.surge_gain_pct}%<small>高値 ${r.high_date || ""}</small></div>`; }
  return `<a class="pickrow" href="#/detail/${r.code}">${head}${sub}</div>${right}</a>`;
}
let REB_FILTER = "all", EXP_Q = "", EXP_CAT_CUR = "";
function onExpSearch(v) { EXP_Q = v; const el = document.getElementById("explist"); if (el) el.innerHTML = catListInner(EXP_CAT_CUR); }
function setRebFilter(f) { REB_FILTER = f; render(); }
function catListInner(cat) {
  let items = ((EXP.categories || {})[cat]) || [];
  if (cat === "rebound" && REB_FILTER === "sagedomari") items = items.filter(r => (r.tags || []).includes("下げ止まり"));
  const q = (EXP_Q || "").trim().toLowerCase();
  if (q) items = items.filter(r => r.code.startsWith(q) || String(r.name).toLowerCase().includes(q));
  if (!items.length) return `<div class="card"><div class="empty">該当なし</div></div>`;
  return `<div class="card tight">${items.map(r => catRow(cat, r)).join("")}</div>`;
}
function viewExploreList(cat) {
  if (cat === "honmei") return viewHonmei();
  if (cat === "precursor") return viewPrecursor();
  if (!EXP) { expReload(`#/explore/${cat}`); return `<div class="sk-wrap"><div class="sk"></div></div>`; }
  EXP_CAT_CUR = cat;
  const def = CAT_DEFS[cat]; if (!def) return `<div class="card"><div class="empty">不明なカテゴリです。</div></div>`;
  const [e, l, d] = def, n = (EXP.counts || {})[cat] || 0;
  const hist = (cat === "stop_high" && (EXP.stop_high_history || []).length)
    ? `<div class="card"><div style="font-weight:700;font-size:13px">🗓 日別のストップ高件数（直近）</div><div class="chips" style="margin-top:8px">${EXP.stop_high_history.slice(0, 10).map(h => `<span class="chip">${h.date.slice(5)} <b>${h.count}</b>件${h.stuck ? `（張付${h.stuck}）` : ""}</span>`).join("")}</div></div>` : "";
  const filters = (cat === "rebound") ? `<div class="gchips"><a class="gchip ${REB_FILTER === "all" ? "on" : ""}" onclick="event.preventDefault();setRebFilter('all')">すべて</a><a class="gchip ${REB_FILTER === "sagedomari" ? "on" : ""}" onclick="event.preventDefault();setRebFilter('sagedomari')">下げ止まりのみ</a></div>
       <input class="searchbox" style="margin-bottom:8px" type="search" placeholder="銘柄名・コードで絞り込み" value="${esc(EXP_Q)}" oninput="onExpSearch(this.value)">` : "";
  const b60 = cat === "break60" ? `<div class="banner info" style="margin:0 0 8px"><b>翌朝のルール: 寄りが前日終値+3%超なら買わない</b>（10年で −1.7%/件の筋）。買えたら20日保有・損切り −15%。<br>
    10年（上場廃止込み 12,396件・コスト0.2%）: 土台 +0.54%/件 → 負け筋を切って <b>+0.64%/件・PF1.17</b>、🆕新高値付き <b>+1.35%/件・PF1.35</b>（1日約2本）。勝率は48〜50%＝半分は外れる。凪（ATR4%未満）は外れが小さく、荒いは外れが大きい。並びは新しい日→🆕→出来高順。</div>` : "";
  return `<a class="back" href="#/explore">← 銘柄探検</a><h2>${e} ${l} <span class="sub">${n}件 ・ ${EXP.data_date || ""} 終値時点</span></h2>
    <div class="note" style="margin:0 2px 8px">${d}${cat === "rebound" ? "。38.2〜50%ゾーン優先・押し率順" : ""}</div>${b60}
    ${filters}${hist}<div id="explist">${catListInner(cat)}</div><div class="card note">${esc(EXP.note || "")}</div><p class="disc">${esc(DATA.disclaimer)}</p>`;
}
