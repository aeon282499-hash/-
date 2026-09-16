// live.js — 🔥ライブ（いま動いているセクター／テーマ／銘柄）v5.1
// データ: PC(tachibana_live_flow.py)が立花API（取引所リアルタイム）を約1分で一巡→chimp-liveハブ→
//        WebSocket push（即時）＋30秒ポーリング（保険）。EODデータ(latest.json)とは独立。
// v5.1 (2026-09-17 本人「使いにくい・資金と言われても分からん・33セクターを騰落率で・銘柄をちゃんと入れて」):
//   既定＝🏭セクター（33業種）を騰落率で並べる。並び替えは「騰落率／上昇銘柄の割合／代金／5分の値動き／資金の勢い」。
//   セクターを開くと構成銘柄（当日代金1億以上・代金順で最大80）が出て、銘柄側も「本日／5分／代金／VWAP乖離」で並び替え・昇降切替。
//   「今の資金 ×1.3」は主役から外し、×2以上の時だけ 🔥資金集中 の印として出す。
let LIVE = null, LIVE_AT = 0, LIVE_ERR = "", LIVE_WS = null, LIVE_WS_OK = false, LIVE_BACKOFF = 5000;
let L_SEG = "sectors";                                    // 既定はセクター（本人指示 2026-09-17）
const L_SORT = { sectors: "chg_w", themes: "chg_w" };      // グループの並び（既定＝騰落率）
let L_TAB = "gain";                                       // 個別の既定＝上昇率
let L_Q = "", L_ALL = false;                              // テーマ絞り込み・全件表示（株探≈1,500本のため既定は上位60）
let L_MSORT = "chg", L_MDIR = -1;                         // 構成銘柄の並び（本日騰落率・降順）
const L_TOP = 60;
const L_OPEN = new Set();
let L_THEME_OF = {};
const GROUP_SORTS = [["chg_w", "騰落率"], ["up_ratio", "上昇の割合"], ["tov", "代金"], ["d5_w", "5分の動き"], ["flow5", "資金の勢い"]];
const MEMBER_SORTS = [["chg", "本日"], ["d5", "5分"], ["tov", "代金"], ["vwap_dev", "VWAP乖離"], ["flow5", "資金の勢い"]];

function liveInit() {
  liveFetch();
  liveConnect();
  setInterval(() => { if (!LIVE_WS_OK) liveFetch(); }, CFG.LIVE_POLL_MS);
  setInterval(liveClock, 10000);
}
function liveConnect() {
  if (typeof WebSocket === "undefined") return;
  try {
    const ws = new WebSocket(CFG.LIVE_BASE.replace(/^http/, "ws") + "/ws");
    LIVE_WS = ws;
    ws.onopen = () => { LIVE_WS_OK = true; LIVE_BACKOFF = 5000; liveClock(); };
    ws.onmessage = ev => { if (typeof ev.data === "string" && ev.data.length > 2 && ev.data !== "pong") { try { liveApply(JSON.parse(ev.data)); } catch (e) { /* ignore */ } } };
    const down = () => { LIVE_WS_OK = false; liveClock(); setTimeout(liveConnect, LIVE_BACKOFF); LIVE_BACKOFF = Math.min(60000, LIVE_BACKOFF * 2); };
    ws.onclose = down; ws.onerror = () => { try { ws.close(); } catch (e) { /* ignore */ } };
  } catch (e) { LIVE_WS_OK = false; }
}
async function liveFetch() {
  try {
    const r = await fetch(CFG.LIVE_BASE + "/live.json", { cache: "no-store" });
    if (r.status === 404) { LIVE_ERR = "まだ配信データがありません（平日8:58〜の巡回開始後に届きます）"; liveRefresh(); return; }
    if (!r.ok) { LIVE_ERR = "配信ハブに接続できません (" + r.status + ")"; liveRefresh(); return; }
    liveApply(await r.json());
  } catch (e) { LIVE_ERR = "配信ハブに接続できません"; liveRefresh(); }
}
function liveApply(j) {
  if (!j || !j.ts) return;
  if (LIVE && LIVE.ts === j.ts && LIVE.got === j.got) { LIVE_AT = Date.now(); liveClock(); return; }
  // 圧縮行（並び上位外の株探テーマ: [key,label,n,flow5,flow,chg_w,up_ratio,tov]）を通常のグループに展開
  if (Array.isArray(j.themes_rest)) {
    j.themes = (j.themes || []).concat(j.themes_rest.map(r => ({ key: r[0], label: r[1], n: r[2], flow5: r[3], flow: r[4], chg_w: r[5], up_ratio: r[6], tov: r[7], src: "kabutan", members: [], desc: "" })));
    delete j.themes_rest;
  }
  LIVE = j; LIVE_AT = Date.now(); LIVE_ERR = "";
  L_THEME_OF = {};
  // 銘柄→テーマ（手作りを先に・株探は後ろ）。チップは2つまで出す
  const ordered = (j.themes || []).slice().sort((a, b) => ((a.src === "hand") ? 0 : 1) - ((b.src === "hand") ? 0 : 1));
  ordered.forEach(g => (g.members || []).forEach(c => { (L_THEME_OF[c] = L_THEME_OF[c] || []).push(g.label); }));
  liveRefresh();
}
function liveRefresh() {
  const el = document.getElementById("live-root");
  if (el) el.innerHTML = liveBody();
  const a = document.getElementById("arena-live"); if (a && typeof arenaLiveInner === "function") a.innerHTML = arenaLiveInner();
  const pl = document.getElementById("plan-live"); if (pl && typeof planLiveInner === "function") pl.innerHTML = planLiveInner();   // 📋作戦のいま（v5.2.2）
}
function liveClock() { const el = document.getElementById("live-clock"); if (el) el.innerHTML = liveClockInner(); }

// ── 表示ヘルパ ──
const LIVE_STATE = { pre: "寄り前（気配）", am: "前場", lunch: "昼休み", pm: "後場", closed: "引け後・最終断面" };
function todayStr() { const d = new Date(); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`; }
function liveAgeSec() { if (!LIVE) return null; const t = new Date(String(LIVE.ts).replace(" ", "T")); return isNaN(t) ? null : Math.max(0, Math.round((Date.now() - t.getTime()) / 1000)); }
function flowCls(v) { if (v == null || isNaN(v)) return "f-cold"; if (v >= 2) return "f-hot"; if (v >= 1.3) return "f-warm"; if (v < 0.7) return "f-cold"; return "f-norm"; }
function flowTxt(v) { return (v == null || isNaN(v)) ? "—" : "×" + Number(v).toFixed(v >= 10 ? 0 : 1); }
function hotTag(v) { return (v != null && !isNaN(v) && v >= 2) ? ` <span class="hiflag">🔥資金${flowTxt(v)}</span>` : ""; }
function chgSpan(v, d) { return `<span class="chg ${cls(v)}">${d === 1 ? fmtPct1(v) : fmtPct(v)}</span>`; }
function spark(arr) {
  const a = (arr || []).map(v => (v == null || isNaN(v)) ? null : Number(v));
  const pts = a.map((v, i) => [i, v]).filter(p => p[1] != null);
  if (pts.length < 2) return `<svg class="spark" viewBox="0 0 64 28"></svg>`;
  const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]);
  const x0 = Math.min(...xs), x1 = Math.max(...xs), lo = Math.min(...ys, 1), hi = Math.max(...ys, 1);
  const X = x => 2 + (x1 === x0 ? 30 : (x - x0) / (x1 - x0) * 60), Y = y => 25 - (hi === lo ? 12 : (y - lo) / (hi - lo) * 22);
  const d = pts.map(p => `${X(p[0]).toFixed(1)},${Y(p[1]).toFixed(1)}`).join(" ");
  const last = pts[pts.length - 1][1], col = last >= 1.3 ? "var(--hot)" : last < 0.7 ? "var(--mut)" : "var(--acc)";
  return `<svg class="spark" viewBox="0 0 64 28"><line x1="2" y1="${Y(1).toFixed(1)}" x2="62" y2="${Y(1).toFixed(1)}" stroke="var(--ln2)" stroke-dasharray="2 2"/><polyline points="${d}" fill="none" stroke="${col}" stroke-width="1.6" stroke-linejoin="round"/></svg>`;
}
function liveClockInner() {
  if (!LIVE) return "";
  const age = liveAgeSec(), stale = age != null && age > 180 && LIVE.state !== "closed";
  const dot = LIVE.state === "closed" ? "" : (stale ? "stale" : "on");
  const ageTxt = age == null ? "" : age < 60 ? `${age}秒前` : `${Math.floor(age / 60)}分前`;
  return `<span class="ldot ${dot}"></span><span class="lt">${esc(LIVE.hhmm || "")} <span class="muted" style="font-weight:600;font-size:12px">${esc(LIVE_STATE[LIVE.state] || LIVE.state || "")}</span></span>
    <span class="ls">${LIVE_WS_OK ? "⚡ 即時受信" : "🔄 30秒ごと確認"}${ageTxt ? " ・ 更新 " + ageTxt : ""}</span>
    <span class="lr">${esc(LIVE.source || "")}<br>${esc(LIVE.interval_note || "")}</span>`;
}

function liveIntraday() { return !!(LIVE && (LIVE.state === "am" || LIVE.state === "pm" || LIVE.state === "lunch")); }
function effSort() { return L_SORT[L_SEG] || "chg_w"; }
function liveSetSeg(s) { L_SEG = s; liveRefresh(); }
function liveSetQ(v) { L_Q = v || ""; const el = document.getElementById("live-groups"); if (el) el.innerHTML = groupsInner(); }
function liveToggleAll() { L_ALL = !L_ALL; liveRefresh(); }
function liveSetSort(s) { L_SORT[L_SEG] = s; liveRefresh(); }
function liveSetTab(s) { L_TAB = s; liveRefresh(); }
function liveSetMSort(k) { if (L_MSORT === k) L_MDIR = -L_MDIR; else { L_MSORT = k; L_MDIR = -1; } liveRefresh(); }
function liveToggle(kind, key) { const k = kind + ":" + key; if (L_OPEN.has(k)) L_OPEN.delete(k); else L_OPEN.add(k); liveRefresh(); }

// 銘柄1行: 名前・コード・代金 ／ 本日 ／ 5分 ／ VWAP乖離。資金×2以上だけ 🔥 印
function stockRow(m, opts) {
  if (!m) return "";
  const o = opts || {};
  const themes = (L_THEME_OF[m.code] || []).slice(0, 2).map(t => `<span class="chip acc" style="padding:0 6px;font-size:9.5px">${esc(t)}</span>`).join("");
  return `<a class="srow" href="#/detail/${m.code}">
    <div class="sn"><b>${esc(m.name)}${m.hi ? ' <span class="hiflag">🔺高値</span>' : ""}${hotTag(m.flow5)}</b>
      <small>${m.code}${o.showSector && m.sector ? " ・ " + esc(m.sector) : ""} ・ 代金${oku(m.tov)}${o.showThemes && themes ? " " + themes : ""}</small></div>
    <div class="sv"><span class="${cls(m.chg)}">${fmtPct1(m.chg)}</span><small>本日</small></div>
    <div class="sv"><span class="${cls(m.d5)}">${m.d5 == null ? "—" : fmtPct1(m.d5)}</span><small>5分</small></div>
    <div class="sv"><span class="${cls(m.vwap_dev)}" style="font-size:12px">${m.vwap_dev == null ? "—" : fmtPct1(m.vwap_dev)}</span><small>VWAP</small></div></a>`;
}
function sortMembers(codes) {
  const v = m => (m[L_MSORT] == null || isNaN(m[L_MSORT])) ? (L_MDIR < 0 ? -1e9 : 1e9) : Number(m[L_MSORT]);
  return (codes || []).map(c => LIVE.stocks[c]).filter(Boolean).sort((a, b) => (v(b) - v(a)) * (L_MDIR < 0 ? 1 : -1));
}
function memberSortBar() {
  const arrow = L_MDIR < 0 ? "▼" : "▲";
  return `<div class="lhead" style="margin:8px 0 2px"><span class="muted" style="font-size:11px">並び替え（もう一度押すと逆順）</span>
    <span class="sortsel">${MEMBER_SORTS.map(([k, l]) => `<a class="${L_MSORT === k ? "on" : ""}" onclick="event.preventDefault();liveSetMSort('${k}')">${l}${L_MSORT === k ? " " + arrow : ""}</a>`).join("")}</span></div>`;
}
function groupCard(kind, g) {
  const key = kind + ":" + g.key, open = L_OPEN.has(key);
  const up = g.up_ratio == null ? 0 : Math.round(g.up_ratio * 100);
  const src = g.src === "kabutan" ? ' <span class="chip" style="padding:0 5px;font-size:9px;vertical-align:middle">株探</span>' : "";
  const shown = (g.members || []).length, omitted = Math.max(0, (g.n || 0) - shown);
  let body = "";
  if (open) {
    const rows = sortMembers(g.members).map(m => stockRow(m, { showThemes: kind === "sectors" })).join("");
    const note = omitted ? `<div class="muted" style="font-size:10.5px;padding:6px 0 2px">他${omitted}銘柄は当日代金1億未満のため省略</div>` : "";
    body = `<div class="gbody">${g.desc ? `<div class="gdesc">${esc(g.desc)}</div>` : ""}${rows ? memberSortBar() + rows + note : `<div class="empty">${(g.n && !shown) ? "並び上位外のため構成銘柄は省略中（上位に入ると表示・約1分ごと）" : "構成銘柄のデータなし"}</div>`}</div>`;
  }
  return `<div class="gcard${open ? " open" : ""}">
    <div class="ghead" onclick="liveToggle('${kind}','${esc(g.key)}')">
      <div class="gname"><b>${esc(g.label)}${src}${hotTag(g.flow5)}</b><small>${g.n}銘柄 ・ 上昇${up}% ・ 代金${g.tov != null ? oku(g.tov) : "—"}</small>
        <div class="upbar"><i style="width:${up}%"></i></div></div>
      <div class="gstat">${chgSpan(g.chg_w, 1)}<small>本日</small></div>
      <div class="gstat"><span class="chg ${cls(g.d5_w)}" style="font-size:13px">${g.d5_w == null ? "—" : fmtPct1(g.d5_w)}</span><small>5分</small></div>
      <div class="gstat" style="min-width:36px"><span style="font-size:14px;color:var(--mut)">${open ? "▴" : "▾"}</span></div>
    </div>${body}
  </div>`;
}
function groupsInner() {
  let arr = sortGroups(LIVE[L_SEG]);
  const q = (L_Q || "").trim().toLowerCase();
  if (L_SEG === "themes" && q) arr = arr.filter(g => String(g.label).toLowerCase().includes(q) || String(g.key).toLowerCase().includes(q));
  const total = arr.length;
  const show = (L_SEG === "themes" && !L_ALL && !q) ? arr.slice(0, L_TOP) : arr;
  const more = total > show.length ? `<a class="card" style="display:block;text-align:center;color:var(--acc);font-weight:700;font-size:13px;cursor:pointer" onclick="event.preventDefault();liveToggleAll()">残り ${total - show.length}テーマを表示 ▾</a>`
    : (L_ALL && L_SEG === "themes" && total > L_TOP ? `<a class="card" style="display:block;text-align:center;color:var(--acc);font-weight:700;font-size:13px;cursor:pointer" onclick="event.preventDefault();liveToggleAll()">上位${L_TOP}だけに戻す ▴</a>` : "");
  return (show.map(g => groupCard(L_SEG, g)).join("") || `<div class="empty">該当なし</div>`) + more;
}
function sortGroups(arr) {
  const k = effSort(), v = g => (g[k] == null || isNaN(g[k])) ? -1e9 : Number(g[k]);
  return (arr || []).slice().sort((a, b) => v(b) - v(a));
}
function liveBody() {
  if (!LIVE) {
    return LIVE_ERR ? `<div class="banner warn">📡 ${esc(LIVE_ERR)}</div>${liveExplain()}` : `<div class="sk-wrap"><div class="sk"></div><div class="sk"></div></div>`;
  }
  const mk = LIVE.market || {};
  const tot = (mk.adv || 0) + (mk.dec || 0) + (mk.unch || 0);
  const advPct = tot ? Math.round((mk.adv || 0) / tot * 100) : 0;
  const isToday = String(LIVE.date || "") === todayStr();
  const age = liveAgeSec(), stale = age != null && age > 180 && LIVE.state !== "closed";
  const dayNote = !isToday ? `<div class="banner info">🗓 これは <b>${esc(LIVE.date || "")}</b> の最終断面です。次の配信は平日 8:58〜（PCの巡回タスクが動いている間）。</div>`
    : stale ? `<div class="banner warn">⚠️ 配信が ${Math.floor(age / 60)}分 止まっています。PCの巡回タスク（TachibanaLiveFlow）が動いているか確認。表示は最後に届いた断面。</div>` : "";
  const seg = k => `<a class="${L_SEG === k ? "on" : ""}" onclick="event.preventDefault();liveSetSeg('${k}')">`;
  const sortBtn = (k, l) => `<a class="${effSort() === k ? "on" : ""}" onclick="event.preventDefault();liveSetSort('${k}')">${l}</a>`;
  let body = "";
  if (L_SEG === "themes" || L_SEG === "sectors") {
    const arr = LIVE[L_SEG] || [];
    body = `<div class="lhead"><span class="muted" style="font-size:12px">${L_SEG === "themes" ? `${arr.length}テーマ（手作り${arr.filter(g => g.src === "hand").length}＋株探${LIVE.n_themes_kabutan || 0}）` : `${arr.length}業種`}・タップで構成銘柄</span></div>
      <div class="lhead" style="margin-top:0"><span class="muted" style="font-size:11px">並び替え</span><span class="sortsel">${GROUP_SORTS.map(([k, l]) => sortBtn(k, l)).join("")}</span></div>
      ${L_SEG === "themes" ? `<input class="searchbox" style="padding:9px 12px;font-size:14px;margin-bottom:8px" type="search" placeholder="テーマ名で絞り込み（例: 半導体・防衛・データセンター）" value="${esc(L_Q)}" oninput="liveSetQ(this.value)">` : ""}
      <div id="live-groups">${groupsInner()}</div>`;
  } else {
    const tabs = [["gain", "上昇率"], ["lose", "下落率"], ["tovtop", "代金"], ["hot5", "資金の勢い(5分)"], ["hot", "資金の勢い(当日)"]];
    const list = (LIVE[L_TAB] || []).map(c => LIVE.stocks[c]).filter(Boolean);
    body = `<div class="gchips">${tabs.map(([k, l]) => `<a class="gchip ${L_TAB === k ? "on" : ""}" onclick="event.preventDefault();liveSetTab('${k}')">${l}</a>`).join("")}</div>
      <div class="card tight">${list.map(m => stockRow(m, { showThemes: true, showSector: true })).join("") || `<div class="empty">${L_TAB === "hot5" ? "5分前との比較がまだ取れていません（巡回2周目から出ます）" : "データなし"}</div>`}</div>`;
  }
  const arena = (LIVE.arena || []).map(c => LIVE.stocks[c]).filter(Boolean);
  const arenaStrip = arena.length ? `<div class="hh">🎯 土俵のいま <span class="sub">前夜リストの銘柄・タップで詳細</span></div>
      <div class="card tight">${arena.map(m => stockRow(m, { showSector: true })).join("")}</div>` : "";
  return `
    <div class="livebar" id="live-clock">${liveClockInner()}</div>
    ${dayNote}
    <div class="card">
      <div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>市場全体</b><span class="muted" style="font-size:11.5px">流動性上位${(LIVE.got || 0).toLocaleString()}銘柄（代金1億以上＋テーマ構成）</span></div>
      <div class="kpis">
        <div class="kpi"><div class="l">上昇 / 下落</div><div class="v"><span class="pos">${mk.adv || 0}</span> <span class="muted" style="font-size:12px">/</span> <span class="neg">${mk.dec || 0}</span></div><div class="s">上昇${advPct}%</div></div>
        <div class="kpi"><div class="l">騰落（代金加重）</div><div class="v ${cls(mk.chg_w)}">${fmtPct1(mk.chg_w)}</div><div class="s">大型に引っ張られる値</div></div>
        <div class="kpi"><div class="l">騰落（中央値）</div><div class="v ${cls(mk.chg_med)}">${fmtPct1(mk.chg_med)}</div><div class="s">ふつうの銘柄の値</div></div>
        <div class="kpi"><div class="l">市場の商い</div><div class="v ${flowCls(mk.flow)}">${flowTxt(mk.flow)}</div><div class="s">いつもの何倍か（×1.0＝平常）</div></div>
      </div>
      <div class="adbar"><i style="width:${advPct}%"></i></div>
    </div>
    ${arenaStrip}
    <div class="seg">${seg("sectors")}🏭 セクター</a>${seg("themes")}🔥 テーマ</a>${seg("stocks")}🚀 個別</a></div>
    ${body}
    <div class="legend"><b>本日</b>＝前日終値比の騰落率（セクター/テーマは代金加重）。<b>5分</b>＝直近5分の値動き。<b>上昇の割合</b>＝構成銘柄のうち上がっている割合。<b>資金の勢い</b>＝売買代金がふだんの何倍か（🔥は×2以上＝資金集中）。セクターの構成銘柄は当日代金1億以上を代金順に最大80本。数字は取引所の現在値（立花証券API）で約1分ごと。予測や推奨ではなく観測。</div>
    <p class="disc">${esc(DATA.disclaimer || "")}</p>`;
}
function liveExplain() {
  return `<div class="card note">🔥ライブは、平日の場中にPC側の巡回（立花証券API・約1分で全銘柄一巡）が動いている間だけ流れます。EODの各タブ（🎯土俵・🔻売り・🐵EOD・🧭探検）はいつでも見られます。</div>`;
}
function viewLive() { return `<div id="live-root">${liveBody()}</div>`; }
