// live.js — 🔥ライブ（資金がいま来ているテーマ／セクター／銘柄）v5の主役
// データ: PC(tachibana_live_flow.py)が立花API（取引所リアルタイム）を約1分で一巡→chimp-liveハブ→
//        WebSocket push（即時）＋30秒ポーリング（保険）。EODデータ(latest.json)とは独立。
// 数字の意味: 直近5分の流入倍率 flow5＝5分間の売買代金 ÷ 平常の5分あたり代金（20日平均÷66）。
//            当日倍率 flow＝当日累計代金 ÷ (20日平均代金×時刻別の想定進捗)。1.0＝いつも通り。
let LIVE = null, LIVE_AT = 0, LIVE_ERR = "", LIVE_WS = null, LIVE_WS_OK = false, LIVE_BACKOFF = 5000;
let L_SEG = "themes", L_SORT = "auto", L_TAB = "auto";   // auto=場中は「直近5分」・引け後/寄り前は「当日」
let L_Q = "", L_ALL = false;                              // テーマ絞り込み・全件表示（株探≈1,500本のため既定は上位60）
const L_TOP = 60;
const L_OPEN = new Set();
let L_THEME_OF = {};

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
}
function liveClock() { const el = document.getElementById("live-clock"); if (el) el.innerHTML = liveClockInner(); }

// ── 表示ヘルパ ──
const LIVE_STATE = { pre: "寄り前（気配）", am: "前場", lunch: "昼休み", pm: "後場", closed: "引け後・最終断面" };
function todayStr() { const d = new Date(); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`; }
function liveAgeSec() { if (!LIVE) return null; const t = new Date(String(LIVE.ts).replace(" ", "T")); return isNaN(t) ? null : Math.max(0, Math.round((Date.now() - t.getTime()) / 1000)); }
function flowCls(v) { if (v == null || isNaN(v)) return "f-cold"; if (v >= 2) return "f-hot"; if (v >= 1.3) return "f-warm"; if (v < 0.7) return "f-cold"; return "f-norm"; }
function flowTxt(v) { return (v == null || isNaN(v)) ? "—" : "×" + Number(v).toFixed(v >= 10 ? 0 : 1); }
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
function effSort() { return L_SORT === "auto" ? (liveIntraday() ? "flow5" : "flow") : L_SORT; }
function effTab() { return L_TAB === "auto" ? (liveIntraday() ? "hot5" : "hot") : L_TAB; }
function liveSetSeg(s) { L_SEG = s; liveRefresh(); }
function liveSetQ(v) { L_Q = v || ""; const el = document.getElementById("live-groups"); if (el) el.innerHTML = groupsInner(); }
function liveToggleAll() { L_ALL = !L_ALL; liveRefresh(); }
function liveSetSort(s) { L_SORT = s; liveRefresh(); }
function liveSetTab(s) { L_TAB = s; liveRefresh(); }
function liveToggle(kind, key) { const k = kind + ":" + key; if (L_OPEN.has(k)) L_OPEN.delete(k); else L_OPEN.add(k); liveRefresh(); }

function stockRow(m, opts) {
  if (!m) return "";
  const o = opts || {};
  const themes = (L_THEME_OF[m.code] || []).slice(0, 2).map(t => `<span class="chip acc" style="padding:0 6px;font-size:9.5px">${esc(t)}</span>`).join("");
  return `<a class="srow" href="#/detail/${m.code}">
    <div class="sn"><b>${esc(m.name)}${m.hi ? ' <span class="hiflag">🔺高値</span>' : ""}</b>
      <small>${m.code}${m.sector ? " ・ " + esc(m.sector) : ""} ・ 代金${oku(m.tov)}${m.vwap_dev != null ? ` ・ VWAP${fmtPct1(m.vwap_dev)}` : ""}${o.showThemes && themes ? " " + themes : ""}</small></div>
    <div class="sv"><span class="${cls(m.chg)}">${fmtPct1(m.chg)}</span><small>本日</small></div>
    <div class="sv"><span class="${cls(m.d5)}">${m.d5 == null ? "—" : fmtPct1(m.d5)}</span><small>5分</small></div>
    <div class="sv"><span class="flow ${flowCls(m.flow5)}" style="font-size:13px">${flowTxt(m.flow5)}</span><small>今の資金</small></div></a>`;
}
function groupCard(kind, g) {
  const key = kind + ":" + g.key, open = L_OPEN.has(key);
  const ser = ((LIVE.series || {})[kind] || {})[g.key];
  const up = g.up_ratio == null ? 0 : Math.round(g.up_ratio * 100);
  const members = open ? (g.members || []).map(c => stockRow(LIVE.stocks[c])).join("") : "";
  const src = g.src === "kabutan" ? ' <span class="chip" style="padding:0 5px;font-size:9px;vertical-align:middle">株探</span>' : "";
  return `<div class="gcard${open ? " open" : ""}">
    <div class="ghead" onclick="liveToggle('${kind}','${esc(g.key)}')">
      <div class="gname"><b>${esc(g.label)}${src}</b><small>${g.n}銘柄 ・ 代金${g.tov != null ? oku(g.tov) : "—"} ・ 上昇${up}%${g.flow != null ? ` ・ 当日${flowTxt(g.flow)}` : ""}</small>
        <div class="upbar"><i style="width:${up}%"></i></div></div>
      <div class="gstat"><span class="flow ${flowCls(g.flow5)}">${flowTxt(g.flow5)}</span><small>直近5分の資金</small></div>
      <div class="gstat">${chgSpan(g.chg_w, 1)}<small>本日（代金加重）</small></div>
      ${spark(ser ? ser.flow : null)}
    </div>
    ${open ? `<div class="gbody">${g.desc ? `<div class="gdesc">${esc(g.desc)}</div>` : ""}${members || `<div class="empty">${(g.n && !(g.members || []).length) ? "並び上位外のため構成銘柄は省略中（上位に入ると表示・約1分ごと）" : "構成銘柄のデータなし"}</div>`}</div>` : ""}
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
    body = `<div class="lhead"><span class="muted" style="font-size:12px">${L_SEG === "themes" ? `${arr.length}テーマ（手作り${arr.filter(g => g.src === "hand").length}＋株探${LIVE.n_themes_kabutan || 0}）` : `${arr.length}業種`}・タップで構成銘柄</span>
        <span class="sortsel">${sortBtn("flow5", "今の資金")}${sortBtn("flow", "当日資金")}${sortBtn("chg_w", "騰落率")}</span></div>
      ${L_SEG === "themes" ? `<input class="searchbox" style="padding:9px 12px;font-size:14px;margin-bottom:8px" type="search" placeholder="テーマ名で絞り込み（例: 半導体・防衛・データセンター）" value="${esc(L_Q)}" oninput="liveSetQ(this.value)">` : ""}
      <div id="live-groups">${groupsInner()}</div>`;
  } else {
    const tabs = [["hot5", "今きてる"], ["hot", "当日倍率"], ["gain", "上昇率"], ["tovtop", "代金"], ["lose", "下落率"]];
    const tab = effTab();
    const list = (LIVE[tab] || []).map(c => LIVE.stocks[c]).filter(Boolean);
    body = `<div class="gchips">${tabs.map(([k, l]) => `<a class="gchip ${effTab() === k ? "on" : ""}" onclick="event.preventDefault();liveSetTab('${k}')">${l}</a>`).join("")}</div>
      <div class="card tight">${list.map(m => stockRow(m, { showThemes: true })).join("") || `<div class="empty">${tab === "hot5" ? "5分前との比較がまだ取れていません（巡回2周目から出ます）" : "データなし"}</div>`}</div>`;
  }
  const arena = (LIVE.arena || []).map(c => LIVE.stocks[c]).filter(Boolean);
  const arenaStrip = arena.length ? `<div class="hh">🎯 土俵のいま <span class="sub">前夜リストの銘柄・タップで詳細</span></div>
      <div class="card tight">${arena.map(m => stockRow(m)).join("")}</div>` : "";
  return `
    <div class="livebar" id="live-clock">${liveClockInner()}</div>
    ${dayNote}
    <div class="card">
      <div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>市場全体</b><span class="muted" style="font-size:11.5px">流動性上位${(LIVE.got || 0).toLocaleString()}銘柄（代金1億以上＋テーマ構成）</span></div>
      <div class="kpis">
        <div class="kpi"><div class="l">上昇 / 下落</div><div class="v"><span class="pos">${mk.adv || 0}</span> <span class="muted" style="font-size:12px">/</span> <span class="neg">${mk.dec || 0}</span></div><div class="s">上昇${advPct}%</div></div>
        <div class="kpi"><div class="l">市場の資金（当日）</div><div class="v ${flowCls(mk.flow)}">${flowTxt(mk.flow)}</div><div class="s">平常＝×1.0</div></div>
        <div class="kpi"><div class="l">騰落（代金加重）</div><div class="v ${cls(mk.chg_w)}">${fmtPct1(mk.chg_w)}</div><div class="s">大型に引っ張られる値</div></div>
        <div class="kpi"><div class="l">騰落（中央値）</div><div class="v ${cls(mk.chg_med)}">${fmtPct1(mk.chg_med)}</div><div class="s">ふつうの銘柄の値</div></div>
      </div>
      <div class="adbar"><i style="width:${advPct}%"></i></div>
    </div>
    ${arenaStrip}
    <div class="seg">${seg("themes")}🔥 テーマ</a>${seg("sectors")}🏭 セクター</a>${seg("stocks")}🚀 個別</a></div>
    ${body}
    <div class="legend">テーマ＝手作り34本＋株探テーマ辞書（週次更新・構成銘柄は代金0.5億以上に限定・3銘柄以上のもの）。🔥 <b>今の資金</b>＝直近5分の売買代金がふだんの5分の何倍か（×2以上＝資金が集中）。<b>当日資金</b>＝当日累計代金 ÷ 平常×時刻の進み具合。騰落は前日終値比。数字は取引所の現在値（立花証券API）で約1分ごと。予測や推奨ではなく「いまどこに資金が来ているか」の観測。</div>
    <p class="disc">${esc(DATA.disclaimer || "")}</p>`;
}
function liveExplain() {
  return `<div class="card note">🔥ライブは、平日の場中にPC側の巡回（立花証券API・約1分で全銘柄一巡）が動いている間だけ流れます。EODの各タブ（🎯土俵・🔻売り・🐵EOD・🧭探検）はいつでも見られます。</div>`;
}
function viewLive() { return `<div id="live-root">${liveBody()}</div>`; }
