// detail.js — 銘柄詳細（EOD＋🔥ライブの現在値）・銘柄検索（v4から移植・v5デザイン）
function viewDetail(code) {
  setTimeout(() => loadStockDetail(code), 0);
  return `<a class="back" href="javascript:history.back()">← 戻る</a><div id="dbody"><div class="sk-wrap"><div class="sk"></div><div class="sk"></div></div></div>`;
}
async function loadStockDetail(code) {
  const s = await fetchFirst([`../data/stocks/${code}.json`, `data/stocks/${code}.json`, `./data/stocks/${code}.json`]);
  const el = document.getElementById("dbody"); if (!el) return;
  if (!s) {
    if (!SEARCH) await loadSearch();
    const r = findStock(code);
    el.innerHTML = r ? renderDetail({ code: r.code, name: r.name, indicators: r, signals: r.signals || [],
      futures: (r.futures_tag ? { corr: r.futures_corr, tag: r.futures_tag } : null), chart: null, disclaimer: DATA.disclaimer })
      : `<div class="card">銘柄 ${esc(code)} のEODデータがありません。${liveNow(code) || ""}</div>`;
    return;
  }
  el.innerHTML = renderDetail(s);
  const ch = s.chart;
  const lastC = (ch && ch.c && ch.c.length) ? ch.c[ch.c.length - 1] : ((s.indicators || {}).price || 0);
  drawCandle(ch, (isCand({ signals: s.signals }) && lastC > 0) ? { stop: lastC * (1 - PLAN.sl / 100) } : null);
}
function findStock(code) { return (DATA.ranking || []).find(r => r.code === code) || (SEARCH && SEARCH.find(r => r.code === code)) || undefined; }
// 🔥ライブに載っていれば「いま」を出す（流動性上位・テーマ構成・土俵・上位リストの銘柄だけ）
function liveNow(code) {
  const m = (typeof LIVE !== "undefined" && LIVE && LIVE.stocks) ? LIVE.stocks[code] : null;
  if (!m) return "";
  const themes = (L_THEME_OF[code] || []).map(t => `<span class="chip acc">${esc(t)}</span>`).join("");
  return `<div class="card danger"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>🔥 いま</b><span class="muted" style="font-size:11.5px">${esc(LIVE.hhmm || "")} ${esc(LIVE_STATE[LIVE.state] || "")}・取引所リアルタイム</span></div>
    <div class="kpis">
      <div class="kpi"><div class="l">現在値</div><div class="v">${m.last != null ? yen(m.last) : "—"}</div><div class="s ${cls(m.chg)}">${fmtPct1(m.chg)} 本日</div></div>
      <div class="kpi"><div class="l">5分の値動き</div><div class="v ${cls(m.d5)}">${m.d5 == null ? "—" : fmtPct1(m.d5)}</div><div class="s">${m.hi ? "🔺 高値更新中" : ""}</div></div>
      <div class="kpi"><div class="l">今の資金</div><div class="v ${flowCls(m.flow5)}">${flowTxt(m.flow5)}</div><div class="s">直近5分/平常</div></div>
      <div class="kpi"><div class="l">当日代金</div><div class="v">${oku(m.tov)}</div><div class="s">当日${flowTxt(m.flow)}・VWAP${fmtPct1(m.vwap_dev)}</div></div>
    </div>${themes ? `<div class="chips" style="margin-top:8px">${themes}</div>` : ""}</div>`;
}
function exitGuide(ks, fut) {
  const st = buyStat({ signals: ks }); if (!st) return "";
  const ftNote = fut && fut.tag === "高連動"
    ? `<div class="warn" style="font-size:12px;margin-top:6px">⚠ <b>${ftagPct(fut.corr) || "日経連動 高"}（高連動）</b>: 日経先物と一緒に振られやすく、型が壊れやすい銘柄。</div>`
    : (fut && fut.tag === "自力" ? `<div class="pos" style="font-size:12px;margin-top:6px">🟢 <b>${ftagPct(fut.corr) || "日経連動 低"}（自力）</b>: 日経に振られにくく、自分の需給で動くタイプ。</div>` : "");
  return `<div class="card exitcard"><div style="font-weight:800">🚪 買いの目安（期待値・出口） <span class="muted" style="font-weight:400">（✅${esc(st.emoji)}${esc(st.label)}）</span></div>
    <div style="margin-top:6px"><span class="${st.ev >= 0 ? "pos" : "neg"}" style="font-size:18px;font-weight:800">期待値 ${st.ev >= 0 ? "+" : ""}${st.ev}%</span><span class="muted" style="font-size:11px">/件（出口ルール込みの過去平均）</span></div>
    <div style="margin-top:4px;font-size:13px"><b>保有${st.hold}日めど</b>・<b>損切り-${st.sl}%</b>・利確なし・勝率${st.win}%</div>
    ${yearLine(ks)}${ftNote}
    <div class="note" style="margin-top:6px">※次の寄り付きでのエントリー想定。固定ルールの過去シミュレーション（手数料等未考慮）で将来を保証しません。</div></div>`;
}
function renderDetail(s) {
  const i = s.indicators || {};
  const fut = s.futures || null;
  const futRow = fut ? { futures_tag: fut.tag, futures_corr: fut.corr } : {};
  return `
    <div class="card">
      <div style="display:flex;align-items:center;gap:10px">
        ${star(s.code)}
        <div style="flex:1;min-width:0"><div style="font-size:18px;font-weight:800">${esc(s.name)}</div>
          <div class="pchip muted" style="font-size:12px">${s.code}${i.sector ? " ・ " + esc(i.sector) : ""} ・ ${yen(i.price)} ${pctTag(i.r1)}</div></div>
        <div>${ftagChip(futRow)}</div></div>
      ${sigChips({ signals: s.signals, futures_tag: null }) || `<div class="nosig">本日、参考シグナルの点灯はありません</div>`}
      <div class="ret3">
        <div class="b"><div class="l">前日比</div><div class="v ${cls(i.r1)}">${fmtPct(i.r1)}</div></div>
        <div class="b"><div class="l">5日</div><div class="v ${cls(i.r5)}">${fmtPct(i.r5)}</div></div>
        <div class="b"><div class="l">10日</div><div class="v ${cls(i.r10)}">${fmtPct(i.r10)}</div></div>
        <div class="b"><div class="l">1ヶ月</div><div class="v ${cls(i.r20)}">${fmtPct(i.r20)}</div></div></div>
      <div class="note" style="margin-top:6px">EOD: <b>${DATA.data_date || "最新"} の終値</b>時点（場中は変わりません）。上の🔥いま だけがリアルタイム。</div>
    </div>
    ${liveNow(s.code)}
    ${exitGuide(s.signals, fut)}
    ${posForm(s.code, s.name, i.price)}
    ${s.chart ? `<h2>日足3ヶ月</h2><div class="card"><canvas id="candle" style="width:100%;display:block"></canvas>
      <div class="note" style="margin-top:6px">右軸=価格(¥)・青=5日移動平均。${isCand({ signals: s.signals }) ? `<span class="neg">赤い破線＝直近終値から損切り-${PLAN.sl}%の目安。</span>` : ""}${esc(s.chart_note || "")}</div></div>` : ""}
    <div class="card note">参考値: RSI <b>${i.rsi != null ? Number(i.rsi).toFixed(0) : "—"}</b> ・ 勢いスコア <b>${i.momentum != null ? Number(i.momentum).toFixed(0) : "—"}</b>（${esc(i.grade || "-")}） ・ SR <b>${i.sr != null ? Number(i.sr).toFixed(2) : "—"}</b> ・ 代金 <b>${i.turnover_oku != null ? Math.round(i.turnover_oku) + "億" : "—"}</b> ・ 値動き <b>${i.rng20 != null ? Number(i.rng20).toFixed(1) + "%/日" : "—"}</b>${s.ai && s.ai.note ? `<br>⚠ ${esc(s.ai.note)}<span class="muted">（${esc(s.ai.source === "rule" ? "指標ベースの自動メモ" : "AI要約")}）</span>` : ""}</div>
    <p class="disc">${esc(s.disclaimer || DATA.disclaimer)}</p>`;
}
function drawCandle(ch, opts) {
  const cv = document.getElementById("candle"); if (!cv || !ch || !ch.c || !ch.c.length) return;
  const dpr = window.devicePixelRatio || 1, W = cv.clientWidth || cv.parentElement.clientWidth - 28, H = 240;
  cv.width = W * dpr; cv.height = H * dpr; cv.style.height = H + "px";
  const g = cv.getContext("2d"); g.scale(dpr, dpr); g.clearRect(0, 0, W, H);
  const padL = 8, padR = 50, padT = 10, padB = 22, cw = W - padL - padR, chh = H - padT - padB;
  const o = ch.o, h = ch.h, l = ch.l, c = ch.c, n = c.length;
  const stop = (opts && opts.stop > 0) ? opts.stop : 0;
  let pmin = Math.min(...l), pmax = Math.max(...h); if (pmin === pmax) { pmin *= 0.99; pmax *= 1.01; }
  if (stop) pmin = Math.min(pmin, stop);
  const pad = (pmax - pmin) * 0.08; pmin -= pad; pmax += pad;
  const xOf = i => padL + (n <= 1 ? cw / 2 : i * cw / (n - 1)), band = cw / n;
  const yP = v => padT + (pmax - v) / (pmax - pmin) * chh;
  g.font = "10px sans-serif"; g.strokeStyle = "#2f3d4f"; g.lineWidth = 1; g.fillStyle = "#8e9bad";
  [pmin, (pmin + pmax) / 2, pmax].forEach(v => { const y = yP(v); g.beginPath(); g.moveTo(padL, y); g.lineTo(W - padR, y); g.stroke(); g.textAlign = "left"; g.fillText("¥" + Math.round(v), W - padR + 4, y + 3); });
  const bw = Math.max(2, Math.min(9, band * 0.6));
  for (let i = 0; i < n; i++) { const x = xOf(i), up = c[i] >= o[i];
    g.strokeStyle = up ? "#2bd47e" : "#ff5d6c"; g.fillStyle = up ? "#2bd47e" : "#ff5d6c";
    g.beginPath(); g.moveTo(x, yP(h[i])); g.lineTo(x, yP(l[i])); g.stroke();
    const yo = yP(o[i]), yc = yP(c[i]); g.fillRect(x - bw / 2, Math.min(yo, yc), bw, Math.max(1, Math.abs(yc - yo))); }
  if (n >= 6) {
    g.strokeStyle = "#4f9cff"; g.lineWidth = 1.6; g.beginPath(); let started = false;
    for (let i = 4; i < n; i++) { let s = 0; for (let k = i - 4; k <= i; k++) s += c[k]; const y = yP(s / 5); if (!started) { g.moveTo(xOf(i), y); started = true; } else g.lineTo(xOf(i), y); }
    g.stroke();
    let s5 = 0; for (let k = n - 5; k < n; k++) s5 += c[k];
    g.fillStyle = "#4f9cff"; g.textAlign = "left"; g.fillText("5MA", W - padR + 4, yP(s5 / 5) + 12);
  }
  if (stop) { const y = yP(stop); g.strokeStyle = "#ff5d6c"; g.lineWidth = 1; if (g.setLineDash) g.setLineDash([5, 4]);
    g.beginPath(); g.moveTo(padL, y); g.lineTo(W - padR, y); g.stroke(); if (g.setLineDash) g.setLineDash([]);
    g.fillStyle = "#ff5d6c"; g.textAlign = "left"; g.fillText(`損切り-${PLAN.sl}% ¥${Math.round(stop).toLocaleString()}`, padL + 2, y - 3); }
  g.fillStyle = "#8e9bad"; g.textAlign = "center";
  [0, Math.floor(n / 2), n - 1].forEach(i => { g.fillText((ch.dates[i] || "").slice(5), xOf(i), H - 6); });
}

// ── 銘柄検索 ──
let SEARCH = null, SEARCH_LOADING = false, SEARCH_Q = "";
async function loadSearch() {
  if (SEARCH || SEARCH_LOADING) return SEARCH;
  SEARCH_LOADING = true;
  const j = await fetchFirst(["../data/search_index.json", "data/search_index.json", "./data/search_index.json"]);
  SEARCH = (j && j.stocks) || [];
  SEARCH_LOADING = false;
  return SEARCH;
}
function searchCard(r) {
  const lv = (typeof LIVE !== "undefined" && LIVE && LIVE.stocks) ? LIVE.stocks[r.code] : null;
  return `<a class="rcard" href="#/detail/${r.code}"><div class="top">${star(r.code)}
      <div style="flex:1;min-width:0"><div><b>${esc(r.name)}</b>${r.illiq ? ` <span class="tag warn" style="border-color:var(--wa);font-size:10px">⚠️低流動</span>` : ""}</div>
        <div class="pchip">${r.code}${r.sector ? " ・ " + esc(r.sector) : ""} ・ ${yen(r.price)} ${pctTag(r.r1)}</div></div>
      ${lv ? `<div class="pk-ev"><span class="${cls(lv.chg)}">${fmtPct1(lv.chg)}</span><small>🔥いま ${flowTxt(lv.flow5)}</small></div>` : ""}</div>
      ${planLine(r)}
      <div class="metrics"><span>1日 <b class="${cls(r.r1)}">${fmtPct(r.r1)}</b></span><span>5日 <b class="${cls(r.r5)}">${fmtPct(r.r5)}</b></span><span>1ヶ月 <b class="${cls(r.r20)}">${fmtPct(r.r20)}</b></span><span>値動き/日 <b>${r.rng20 != null && isFinite(Number(r.rng20)) ? Number(r.rng20).toFixed(1) + "%" : "—"}</b></span></div>
      ${r.illiq ? `<div class="nosig">売買代金が少なく買い候補の対象外（指標は参考表示）</div>` : (sigChips(r) || `<div class="nosig">本日、参考シグナルの点灯はありません</div>`)}</a>`;
}
function searchResults(q) {
  if (!SEARCH) return `<div class="card muted">索引を読み込み中…</div>`;
  q = (q || "").trim();
  if (!q) {
    const prompt = `<div class="card note" style="margin-top:10px">コード（例 7203）か銘柄名の一部を入力すると、<b>${SEARCH.length.toLocaleString()}</b> 銘柄から探せます。</div>`;
    const found = [], missing = [];
    WATCH.forEach(c => { const r = SEARCH.find(s => s.code === c); r ? found.push(r) : missing.push(c); });
    if (!found.length && !missing.length) return `<div class="card muted">⭐ カードの☆を押すと気になる銘柄をここに保存できます。</div>` + prompt;
    found.sort((a, b) => (isCand(b) ? 1 : 0) - (isCand(a) ? 1 : 0));
    const miss = missing.length ? `<div class="note" style="margin:8px 2px">${missing.map(esc).join("・")} は本日は対象外</div>` : "";
    return `<div style="font-weight:700;margin:0 2px 7px;color:var(--up)">⭐ ウォッチ中（${found.length + missing.length}件）</div><div class="cardgrid">${found.map(searchCard).join("")}</div>${miss}${prompt}`;
  }
  const ql = q.toLowerCase();
  const hits = SEARCH.filter(r => r.code.startsWith(q) || String(r.name).toLowerCase().includes(ql));
  const score = r => (r.code === q ? 1000 : r.code.startsWith(q) ? 500 : 0) + (isCand(r) ? 100 : 0);
  hits.sort((a, b) => score(b) - score(a));
  const shown = hits.slice(0, 60);
  if (!shown.length) return `<div class="card muted">「${esc(q)}」に一致する銘柄はありません。</div>`;
  const more = hits.length > shown.length ? `<div class="note" style="margin:2px 2px 8px">上位${shown.length}件を表示（${hits.length}件一致）</div>` : `<div class="note" style="margin:2px 2px 8px">${shown.length}件一致</div>`;
  return more + `<div class="cardgrid">${shown.map(searchCard).join("")}</div>`;
}
function onSearchInput(v) { SEARCH_Q = v; const el = document.getElementById("searchres"); if (el) el.innerHTML = searchResults(v); }
function viewSearch() {
  if (!SEARCH) loadSearch().then(() => { const el = document.getElementById("searchres"); if (el) el.innerHTML = searchResults(SEARCH_Q); });
  setTimeout(() => { const i = document.getElementById("searchinput"); if (i && !SEARCH_Q) i.focus(); }, 0);
  return `<h2>🔍 銘柄検索 <span class="sub">EODシグナル・🔥いまの値</span></h2>
    <input id="searchinput" class="searchbox" type="search" inputmode="text" autocomplete="off" placeholder="コード（例 7203）か銘柄名で検索" value="${esc(SEARCH_Q)}" oninput="onSearchInput(this.value)">
    <div class="note" style="margin:8px 2px 12px">✅＝過去検証で勝てた「買いの型」が出ている銘柄。<b style="color:#ffd93d">☆</b>でウォッチ保存。</div>
    <div id="searchres">${searchResults(SEARCH_Q)}</div><p class="disc">${esc(DATA.disclaimer)}</p>`;
}
