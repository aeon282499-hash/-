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
// v8.1 🩳空売りの状況（売り印＋週次の売残）。🐵ランキング上位にある銘柄だけ出せる（latest.json の行に持たせている）
function shortCardFor(code) {
  const r = (DATA.ranking || []).find(x => x.code === code);
  if (!r || (r.shortable == null && r.short_vol == null && !r.jsf_stop)) return "";
  const badges = momShortBadges(r), meter = momShortMeter(r);
  const word = r.jsf_stop ? "制度信用の新規売りが止まっている銘柄。個人は普通には空売りできない（SBI一日信用HYPER/一般信用の在庫なら売れることがある）。"
    : r.shortable === true ? "制度信用で空売りできる銘柄。" : r.shortable === false ? "信用買いだけの銘柄（制度信用の空売りは不可）。" : "";
  const heavy = momHeavy(r) ? " 売残が重い＝上がると売り方の買い戻しが乗る（踏み上げの燃料）。売り方は逆日歩に注意。" : "";
  return `<div class="card ${momHeavy(r) ? "shortsum" : ""}"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>🩳 空売りの状況</b>${r.margin_date ? `<span class="muted" style="font-size:13.5px">残高は ${esc(r.margin_date)} 時点（週次・金曜分）</span>` : ""}</div>
    ${badges ? `<div class="sbadges" style="margin-top:6px">${badges}</div>` : ""}${meter}
    <div class="note" style="margin-top:6px">${esc(word)}${esc(heavy)}${r.short_vol != null && r.long_vol != null ? ` 売残${fmtKabu(r.short_vol)}／買残${fmtKabu(r.long_vol)}。` : ""}</div></div>`;
}
function findStock(code) { return (DATA.ranking || []).find(r => r.code === code) || (SEARCH && SEARCH.find(r => r.code === code)) || undefined; }
// 🔥ライブに載っていれば「いま」を出す（流動性上位・テーマ構成・土俵・上位リストの銘柄だけ）
function liveNow(code) {
  const m = (typeof LIVE !== "undefined" && LIVE && LIVE.stocks) ? LIVE.stocks[code] : null;
  if (!m) return "";
  const themes = (L_THEME_OF[code] || []).map(t => `<span class="chip acc">${esc(t)}</span>`).join("");
  return `<div class="card danger"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>🔥 いま</b><span class="muted" style="font-size:13.5px">${esc(LIVE.hhmm || "")} ${esc(LIVE_STATE[LIVE.state] || "")}・取引所リアルタイム</span></div>
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
    ? `<div class="warn" style="font-size:14px;margin-top:6px">⚠ <b>${ftagPct(fut.corr) || "日経連動 高"}（高連動）</b>: 日経先物と一緒に振られやすく、型が壊れやすい銘柄。</div>`
    : (fut && fut.tag === "自力" ? `<div class="pos" style="font-size:14px;margin-top:6px">🟢 <b>${ftagPct(fut.corr) || "日経連動 低"}（自力）</b>: 日経に振られにくく、自分の需給で動くタイプ。</div>` : "");
  return `<div class="card exitcard"><div style="font-weight:800">🚪 買いの目安（期待値・出口） <span class="muted" style="font-weight:400">（✅${esc(st.emoji)}${esc(st.label)}）</span></div>
    <div style="margin-top:6px"><span class="${st.ev >= 0 ? "pos" : "neg"}" style="font-size:18px;font-weight:800">期待値 ${st.ev >= 0 ? "+" : ""}${st.ev}%</span><span class="muted" style="font-size:13px">/件（出口ルール込みの過去平均）</span></div>
    <div style="margin-top:4px;font-size:15px"><b>保有${st.hold}日めど</b>・<b>損切り-${st.sl}%</b>・利確なし・勝率${st.win}%</div>
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
          <div class="pchip muted" style="font-size:14px">${s.code}${i.sector ? " ・ " + esc(i.sector) : ""} ・ ${yen(i.price)} ${pctTag(i.r1)}</div></div>
        <div>${ftagChip(futRow)}</div></div>
      ${planBannerFor(s.code)}
      <div class="note" style="margin:4px 0 2px"><a href="#/coach/${esc(s.code)}">🧠 AI調教に解剖させる →</a></div>
      ${sigChips({ signals: s.signals, futures_tag: null }) || `<div class="nosig">本日、参考シグナルの点灯はありません</div>`}
      <div class="ret3">
        <div class="b"><div class="l">前日比</div><div class="v ${cls(i.r1)}">${fmtPct(i.r1)}</div></div>
        <div class="b"><div class="l">5日</div><div class="v ${cls(i.r5)}">${fmtPct(i.r5)}</div></div>
        <div class="b"><div class="l">10日</div><div class="v ${cls(i.r10)}">${fmtPct(i.r10)}</div></div>
        <div class="b"><div class="l">1ヶ月</div><div class="v ${cls(i.r20)}">${fmtPct(i.r20)}</div></div></div>
      <div class="note" style="margin-top:6px">EOD: <b>${DATA.data_date || "最新"} の終値</b>時点（場中は変わりません）。</div>
    </div>
    ${shortCardFor(s.code)}
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
      <div style="flex:1;min-width:0"><div><b>${esc(r.name)}</b>${r.illiq ? ` <span class="tag warn" style="border-color:var(--wa);font-size:12px">⚠️低流動</span>` : ""}</div>
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
  const biz = bizNorm(q).length >= 2 ? (bizEnsure(), bizResults(q)) : "";   // v8.5: 2文字以上なら事業内容（有報）も照合
  if (!shown.length) return (biz || `<div class="card muted">「${esc(q)}」に一致する銘柄はありません。</div>`);
  const more = hits.length > shown.length ? `<div class="note" style="margin:2px 2px 8px">上位${shown.length}件を表示（${hits.length}件一致）</div>` : `<div class="note" style="margin:2px 2px 8px">${shown.length}件一致</div>`;
  return more + `<div class="cardgrid">${shown.map(searchCard).join("")}</div>` + biz;
}

// ── 📄 事業内容で探す（v8.5・2026-10-22 本人「PHP銘柄さがせていないのに腹がたってる」）──
// kabuai/biz/<コード先頭1文字>.json（biz_text_pc.py が EDINET の有報から「事業の内容」2,000字＋「研究開発活動」600字を貯めたもの・年1回の文書なので月1更新）。
// 株探の人気テーマにも TDnet の開示にも出ない語（PHP・WordPress・KUSANAGI…）は、ここにしか書いていない。検索した時だけ10分割を順に読む（約22MB・sw.js が2回目以降はキャッシュ）。
let BIZ_IDX = null;          // biz/index.json（{updated, n, n_text, shards:[{k,n,kb}]}）・false＝無い
const BIZ_ROWS = {};         // k → rows（h＝照合用に正規化した本文）
const BIZ_PENDING = {};      // 読み込み中の k
const bizNorm = s => String(s == null ? "" : s).normalize("NFKC").toLowerCase().replace(/\s+/g, "");
function bizTerms(q) {        // 空白区切り＝AND。ir.js の表記ゆれ辞書（irTerms）があれば使う
  if (typeof irTerms === "function") return irTerms(q);
  return String(q || "").split(/[\s　,、]+/).map(bizNorm).filter(Boolean).map(t => [t]);
}
function bizRefresh() { const el = document.getElementById("searchres"); if (el) el.innerHTML = searchResults(SEARCH_Q); }
async function bizLoad(k) {
  if (BIZ_ROWS[k] || BIZ_PENDING[k]) return;
  BIZ_PENDING[k] = 1;
  const j = await fetchFirst([`../biz/${k}.json`, `biz/${k}.json`]);
  delete BIZ_PENDING[k];
  BIZ_ROWS[k] = (j && Array.isArray(j.rows)) ? j.rows.map(r => ({ code: r.code, name: r.name, fy: r.fy, doc: r.doc, h: bizNorm((r.t || "") + " ■ " + (r.r || "")) })) : [];
  bizRefresh();
}
function bizEnsure() {
  if (BIZ_IDX == null) {
    BIZ_IDX = false;
    fetchFirst(["../biz/index.json", "biz/index.json"]).then(j => {
      BIZ_IDX = (j && Array.isArray(j.shards)) ? j : false;
      if (BIZ_IDX) BIZ_IDX.shards.filter(s => s.n > 0).forEach(s => bizLoad(s.k));
      bizRefresh();
    });
  } else if (BIZ_IDX) BIZ_IDX.shards.filter(s => s.n > 0).forEach(s => bizLoad(s.k));
}
function bizSnippet(h, terms) {
  let best = -1, len = 0;
  for (const vs of terms) for (const v of vs) { const i = h.indexOf(v); if (i >= 0 && (best < 0 || i < best)) { best = i; len = v.length; } }
  if (best < 0) return "";
  const a = Math.max(0, best - 45), b = Math.min(h.length, best + len + 75);
  return `${a > 0 ? "…" : ""}${esc(h.slice(a, best))}<mark>${esc(h.slice(best, best + len))}</mark>${esc(h.slice(best + len, b))}${b < h.length ? "…" : ""}`;
}
function bizResults(q) {
  const terms = bizTerms(q); if (!terms.length) return "";
  const byCode = {}; (SEARCH || []).forEach(s => { byCode[s.code] = s; });
  const hits = [];
  for (const k in BIZ_ROWS) for (const r of BIZ_ROWS[k]) if (terms.every(vs => vs.some(v => r.h.includes(v)))) hits.push({ r, s: byCode[r.code] || null });
  hits.sort((a, b) => (b.s ? Number(b.s.r1) || 0 : -999) - (a.s ? Number(a.s.r1) || 0 : -999));   // 今日動いた順・EOD索引に無い（上場廃止など）は最後
  const nSh = BIZ_IDX && BIZ_IDX.shards ? BIZ_IDX.shards.filter(s => s.n > 0).length : 0, loading = Object.keys(BIZ_PENDING).length;
  const sub = BIZ_IDX === false ? "索引なし（PC の biz_text_pc.py が置くと出る）" : loading ? `読み込み中 ${nSh - loading}/${nSh}` : BIZ_IDX ? `有報 ${Number(BIZ_IDX.n_text || 0).toLocaleString()}社・${esc(String(BIZ_IDX.updated || "").slice(0, 10))}` : "";
  const head = `<div class="hh" style="margin-top:14px">📄 事業内容に「${esc(q)}」 ${hits.length}社 <span class="sub">${sub}</span></div>`;
  const rows = hits.slice(0, 80).map(({ r, s }) => `<div class="irrow">
      <div class="irhead"><a href="#/detail/${esc(r.code)}"><b>${esc(s ? s.name : r.name)}</b></a> <small>${esc(r.code)}</small> ${s ? `<span class="num">${yen(s.price)} ${pctTag(s.r1)}</span>` : `<small class="muted">EOD索引に無い</small>`}${r.fy ? ` <small class="muted">有報 ${esc(r.fy)}</small>` : ""}</div>
      <div class="note irsnip">${bizSnippet(r.h, terms)}</div></div>`).join("");
  const more = hits.length > 80 ? `<div class="note">…ほか ${hits.length - 80}社（語を足して絞る）</div>` : "";
  return head + (rows ? `<div class="card tight">${rows}</div>` : `<div class="card"><div class="empty">${loading ? "読み込み中…" : BIZ_IDX === false ? "索引がありません" : "該当なし"}</div></div>`) + more;
}
function onSearchInput(v) { SEARCH_Q = v; const el = document.getElementById("searchres"); if (el) el.innerHTML = searchResults(v); }   // 事業内容の照合（4,230社×2,600字の includes）は数十msで足りるので同期のまま
function viewSearch() {
  if (!SEARCH) loadSearch().then(() => { const el = document.getElementById("searchres"); if (el) el.innerHTML = searchResults(SEARCH_Q); });
  setTimeout(() => { const i = document.getElementById("searchinput"); if (i && !SEARCH_Q) i.focus(); }, 0);
  return `<h2>🔍 銘柄検索 <span class="sub">EODシグナル・終値ベース</span></h2>
    <input id="searchinput" class="searchbox" type="search" inputmode="text" autocomplete="off" placeholder="コード・銘柄名・事業内容の言葉（例 PHP）" value="${esc(SEARCH_Q)}" oninput="onSearchInput(this.value)">
    <div class="note" style="margin:8px 2px 12px">✅＝過去検証で勝てた「買いの型」が出ている銘柄。<b style="color:#ffd93d">☆</b>でウォッチ保存。</div>
    <div id="searchres">${searchResults(SEARCH_Q)}</div><p class="disc">${esc(DATA.disclaimer)}</p>`;
}

// 📋作戦の注文/保有に入っている銘柄は、詳細の先頭に「今日の作戦」を出す（v5.2.3）
function planBannerFor(code) {
  const p = DATA && DATA.plan; if (!p) return "";
  const o = (p.orders || []).find(x => x.code === code), h = (p.holdings || []).find(x => x.code === code), pp = (p.paper || []).find(x => x.code === code);
  if (o) return `<div class="banner dn" style="margin:8px 0"><b>📋 今日の作戦: ${esc(o.system)} ${esc(o.side)} ${esc(o.size)}</b><br><span style="font-size:14.5px">${esc(o.order)}</span>${o.warn ? `<br><span style="font-size:14px">${esc(o.warn)}</span>` : ""}</div>`;
  if (h) return `<div class="banner dn" style="margin:8px 0"><b>📦 保有中: ${esc(h.system)} ${esc(h.side)} ${esc(h.size)}・建値 ${yen(h.entry_open)}（${h.hold_days != null ? h.hold_days + "日目" : ""}）</b><br><span style="font-size:14.5px">${h.exit_today ? "今日が出口日＝大引け成行" : "出口 " + esc(h.exit_date || "") + "（3営業日目の大引け）"}／${esc(h.rule)}</span></div>`;
  if (pp) return `<div class="banner info" style="margin:8px 0"><b>📝 紙の対照: ${esc(pp.system)}</b> … ${esc(pp.size)}</div>`;
  return "";
}
