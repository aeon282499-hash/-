// day.js — チンパン デイトレ v6（2026-10-01 一から作り直し・10月デイトレルール専用）
// 画面: 📐候補（#/）／✅注文前チェック（#/check）／📊今日の紙トレード（#/paper）／✂️上ヒゲ刈り取り候補（#/oshime5・2026-10-04追加・前夜の10本）／🧮電卓（#/calc・買値→利確+1.5%/損切り−3%/株数）
// データ: 立花の巡回（tachibana_live_flow.py）→ Cloudflare chimp-live の payload.fibo（fibo_oct.py が書く）。
//   WebSocket で即時受信・切れている間は30秒ごとに取りに行く。発注はしない。
//   ?demo を付けると demo_fibo.json（9/30 の再生）で動く（場外でも見た目を確認できる）。
const CFG = {
  VERSION: "6.3.0",
  LIVE_BASE: "https://chimp-live.aeon282499.workers.dev",
  POLL_MS: 30000,
  DEMO: typeof location !== "undefined" && /[?&]demo/.test(location.search || ""),
};
const STATE_JP = { pre: "寄り前", am: "前場", lunch: "昼休み", pm: "後場", closed: "引け後" };

// ── 小物 ──
const $ = s => document.querySelector(s);
const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[c]));
const yen = v => (v == null || isNaN(v)) ? "—" : Math.round(Number(v)).toLocaleString();
const sYen = v => (v == null || isNaN(v)) ? "—" : (v > 0 ? "+" : v < 0 ? "−" : "±") + Math.abs(Math.round(v)).toLocaleString() + "円";
const pct1 = v => (v == null || isNaN(v)) ? "—" : (v >= 0 ? "+" : "−") + Math.abs(Number(v)).toFixed(1) + "%";
const cls = v => (v == null || isNaN(v) || v === 0) ? "" : (v > 0 ? "pos" : "neg");

// ── テーマ（kabuai_theme = dark|light・旧v5と共通）──
function applyTheme() {
  let t = "dark";
  try { t = localStorage.getItem("kabuai_theme") || "dark"; } catch (e) { /* ignore */ }
  if (document.documentElement && document.documentElement.setAttribute) document.documentElement.setAttribute("data-theme", t);
}
function toggleTheme() {
  try { localStorage.setItem("kabuai_theme", (localStorage.getItem("kabuai_theme") || "dark") === "dark" ? "light" : "dark"); } catch (e) { /* ignore */ }
  applyTheme();
}

// ── 受信 ──
let LIVE = null, LIVE_ERR = "", WS_OK = false, BACKOFF = 5000;
function liveInit() {
  o5Load(); setInterval(o5Load, 10 * 60 * 1000);   // ✂️前夜の候補（夜に更新）
  if (CFG.DEMO) { fetchJson("demo_fibo.json"); return; }
  liveConnect();
  fetchJson(CFG.LIVE_BASE + "/live.json");
  setInterval(() => { if (!WS_OK) fetchJson(CFG.LIVE_BASE + "/live.json"); }, CFG.POLL_MS);
  setInterval(clock, 10000);
}
function liveConnect() {
  if (typeof WebSocket === "undefined") return;
  try {
    const ws = new WebSocket(CFG.LIVE_BASE.replace(/^http/, "ws") + "/ws");
    ws.onopen = () => { WS_OK = true; BACKOFF = 5000; clock(); };
    ws.onmessage = ev => { if (typeof ev.data === "string" && ev.data.length > 2 && ev.data !== "pong") { try { apply(JSON.parse(ev.data)); } catch (e) { /* ignore */ } } };
    ws.onclose = () => { WS_OK = false; clock(); setTimeout(liveConnect, BACKOFF); BACKOFF = Math.min(BACKOFF * 2, 60000); };
    ws.onerror = () => { try { ws.close(); } catch (e) { /* ignore */ } };
  } catch (e) { /* ポーリングで続ける */ }
}
async function fetchJson(url) {
  try {
    const r = await fetch(url, { cache: "no-store" });
    if (r.status === 404) { LIVE_ERR = "まだデータがありません（平日8:58から届きます）"; render(); return; }
    if (!r.ok) { LIVE_ERR = "配信につながりません（" + r.status + "）"; render(); return; }
    apply(await r.json());
  } catch (e) { LIVE_ERR = "配信につながりません"; render(); }
}
function apply(j) {
  if (!j || !j.ts) return;
  const same = LIVE && LIVE.ts === j.ts;
  LIVE = j; LIVE_ERR = "";
  if (same) clock(); else render();
}
function F() { const f = LIVE && LIVE.fibo; return (f && f.ruleset === "oct") ? f : null; }
function ageSec(ts) { const t = new Date(String(ts || "").replace(" ", "T")); return isNaN(t) ? null : Math.max(0, Math.round((Date.now() - t.getTime()) / 1000)); }
function todayStr() { const d = new Date(); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`; }

function clock() {
  const el = $("#clock"); if (!el) return;
  if (!LIVE) { el.innerHTML = LIVE_ERR ? `<span class="dot"></span>${esc(LIVE_ERR)}` : ""; return; }
  const f = F(), age = ageSec((f && f.ts) || LIVE.ts);
  const live = LIVE.state === "am" || LIVE.state === "pm" || LIVE.state === "lunch";
  const dot = !live ? "" : (age != null && age > 180 ? "stale" : "on");
  const ageTxt = age == null ? "" : age < 60 ? `${age}秒前` : age < 3600 ? `${Math.floor(age / 60)}分前` : `${String((f && f.ts) || LIVE.ts).slice(5, 16)}`;
  el.innerHTML = `<span class="dot ${dot}"></span><b>${esc(String((f && f.ts) || LIVE.ts).slice(11, 16))}</b> ${esc(STATE_JP[LIVE.state] || "")}`
    + `<br>${CFG.DEMO ? "デモ" : (WS_OK ? "⚡即時" : "🔄30秒")}・${esc(ageTxt)}`;
}

// ── 共通の表示 ──
function stopBanner(f) {
  const p = f && f.paper;
  if (!p || !p.stopped) return "";
  return `<div class="banner stop"><b>⛔ 今日は停止</b><br>${esc(p.stop_reason)}。新しい注文は出さない。</div>`;
}
function noData() {
  if (!LIVE) return `<div class="empty">${esc(LIVE_ERR || "読み込み中…")}</div>`;
  const d = String(LIVE.ts || "").slice(0, 10);
  return `<div class="banner warn">10月ルールの候補データがまだありません。<br><span class="muted" style="font-size:14px">PCの監視（FiboDaytrade）が動くと、平日9時台から出ます。最後の配信: ${esc(d)} ${esc(LIVE.hhmm || "")}</span></div>`;
}

// ── 📐 候補 ──
let SHOW_ENDED = false;
const OPEN = new Set();
const ST = {   // status_jp → [カードの色, バッジ, バッジの色, 並び]
  "約定中": ["st-zone", "🟢 約定中", "zone", 0],
  "反発待ち": ["st-zone", "🟢 反発待ち", "zone", 1],
  "接近": ["st-near", "🟡 接近", "near", 1],
  "指値待ち": ["", "⏳ 指値待ち", "wait", 2],
  "波の途中": ["", "📈 波の途中", "wait", 3],
  "決済済み": ["st-end", "✓ 決済", "done", 4],
  "見送り": ["st-skip", "⛔ 見送り", "skip", 5],
  "終了": ["st-end", "− 終了", "wait", 6],
};
const stOf = c => c.status_jp || c.status;
const prioKey = c => [-(c.overlap || 0), c.priority ? 0 : 1];
function byPriority(a, b) {
  const x = prioKey(a), y = prioKey(b);
  return x[0] - y[0] || x[1] - y[1] || String(a.code).localeCompare(String(b.code));
}
function card(c, f) {
  const st = ST[stOf(c)] || ["", stOf(c), "wait", 9];
  const main = String(f.tp_main || 1).replace(/\.0$/, "");
  const tags = [c.overlap >= 2 ? `<span class="tag top">最優先 重なり2</span>` : c.overlap === 1 ? `<span class="tag top">重なり（${esc((c.overlap_items || [])[0])}）</span>` : "",
                c.priority ? `<span class="tag pri">窓2%未満</span>` : ""].join("");
  const dist = (c.last && c.entry) ? (c.last / c.entry - 1) * 100 : null;
  const now = (stOf(c) === "接近" || stOf(c) === "指値待ち") && c.last
    ? `<div class="cd-now">いま ${yen(c.last)}円 ・ 指値まで あと${dist != null ? Math.max(0, dist).toFixed(1) : "—"}%</div>` : "";
  const tr = c.trade;
  const res = tr ? `<div class="cd-res">${["1", "2"].map(k => { const b = (tr.books || {})[k] || {};
      return `<div>+${k}%：${b.exit_type ? `${esc(b.exit_type)} <b class="${cls(b.pnl_yen)}">${sYen(b.pnl_yen)}</b>` : "保有中"}</div>`; }).join("")}</div>` : "";
  const more = OPEN.has(c.code) ? `<div class="cd-more">波: 起点 ${yen(c.origin)} → 高値 ${yen(c.high)}（${esc(c.armed_at || "")} 確定）<br>逆指値の決め方: ${esc(c.stop_kind || "")}（起点割れ と −3% の近い方）`
    + `${tr ? `<br>約定 ${esc(tr.fill_time)}` : ""}</div>` : "";
  return `<div class="cd ${st[0]}" onclick="toggleOpen('${esc(c.code)}')">
    <div class="cd-top"><span class="badge ${st[2]}">${st[1]}</span><span class="nm">${esc(c.name)}</span><span class="code">${esc(c.code)}</span>${tags}</div>
    <div class="px3 num"><div class="ent"><small>${f.entry_mode === "rebound" ? (c.trade ? "買値（反発）" : "38.2%（反発で買い）") : "指値（38.2%）"}</small><b>${yen(c.entry)}</b></div><div class="stp"><small>逆指値</small><b>${yen(c.stop)}</b></div><div><small>株数</small><b>${yen(c.shares)}</b></div></div>
    <div class="cd-line num">利確 <b class="${main === "1" ? "tp-main" : ""}">+1% ${yen(c.tp1)}</b> ／ <b class="${main === "2" ? "tp-main" : ""}">+2% ${yen(c.tp2)}</b></div>
    <div class="cd-line num">最大損失 <b>${yen(c.max_loss)}円</b> ・ 窓 <b>${pct1(c.gap)}</b></div>
    ${stOf(c) === "反発待ち" ? `<div class="cd-now">38.2%にタッチ済み。足が陽線で ${yen(c.entry)} 以上に引けたら買い</div>` : ""}${now}${stOf(c) === "波の途中" ? `<div class="cd-now">高値更新中。止まったら 38.2%＝${yen(c.entry)} に指値（まだ入らない）</div>` : ""}${c.skip_reason ? `<div class="cd-skip">⛔ ${esc(c.skip_reason)}</div>` : ""}${res}${more}</div>`;
}
function toggleOpen(code) { if (OPEN.has(code)) OPEN.delete(code); else OPEN.add(code); render(); }
function toggleEnded() { SHOW_ENDED = !SHOW_ENDED; render(); }
function viewCands() {
  const f = F();
  if (!f) return `<h1>📐 候補</h1>${noData()}`;
  const cs = (f.candidates || []).slice();
  const active = cs.filter(c => ["約定中", "反発待ち", "接近", "指値待ち", "波の途中"].includes(stOf(c)))
    .sort((a, b) => (ST[stOf(a)][3] - ST[stOf(b)][3]) || byPriority(a, b));
  const done = cs.filter(c => stOf(c) === "決済済み").sort(byPriority);
  const ended = cs.filter(c => ["見送り", "終了"].includes(stOf(c))).sort((a, b) => (ST[stOf(a)][3] - ST[stOf(b)][3]) || byPriority(a, b));
  const old = String(f.ts || "").slice(0, 10) !== todayStr() && !CFG.DEMO;
  return `<h1>📐 候補<small>${esc(String(f.ts || "").slice(5, 10))} ・ 並び＝重なり→窓2%未満</small></h1>
    ${old ? `<div class="banner warn">これは ${esc(String(f.ts || "").slice(0, 10))} の結果です（今日の分はまだ）。</div>` : ""}
    ${stopBanner(f)}
    ${active.length ? active.map(c => card(c, f)).join("") : `<div class="empty">いま狙える候補はありません。<br><span style="font-size:14px">朝の波の高値が決まると出ます（11:00まで）。</span></div>`}
    ${done.length ? `<div class="sec">決済した候補（${done.length}）</div>${done.map(c => card(c, f)).join("")}` : ""}
    ${ended.length ? `<button class="toggle" onclick="toggleEnded()">${SHOW_ENDED ? "▲ 見送り・終了を隠す" : `▼ 見送り・終了も見る（${ended.length}）`}</button>${SHOW_ENDED ? ended.map(c => card(c, f)).join("") : ""}` : ""}
    <div class="note">カードを押すと 起点・高値 を表示。<b>発注はしません（候補の提示だけ）</b>。38.2%の指値は1回だけ・ナンピンなし。約定から30分で撤退、11:30で全部決済。</div>`;
}

// ── ✅ 注文前チェック（その日だけ・リロードで消える）──
const CHECKS = [
  ["株価2,000〜10,000円、寄りの窓6%未満", "窓2%未満なら優先"],
  ["寄り付き後に高値を更新している", "寄り天は入らない"],
  ["最初の1分足が「大陰線＋大出来高」ではない", ""],
  ["75MAの上・MAが下向きの並び（短期＜中期＜長期）ではない", "足は9:30までは3分足・以降は15分足"],
  ["38.2%にタッチ→足が陽線で38.2%以上に引けたら買い（1回だけ）", "23.6%では入らない・ナンピンしない・反発前に61.8%割れは見送り"],
  ["逆指値を同時に入れた（起点割れ と −3% の近い方）", "株数＝2万円÷（建値−逆指値）・100株単位・建玉130万円まで"],
  ["利確（+1% / +2%）を入れた・30分で撤退・11:30で全決済", "今日の停止（−4万円・2連敗・8回）に当たっていない"],
];
const CHECKED = new Set();
function toggleCheck(i) { if (CHECKED.has(i)) CHECKED.delete(i); else CHECKED.add(i); render(); }
function clearChecks() { CHECKED.clear(); render(); }
function viewCheck() {
  const f = F(), n = CHECKED.size, all = n === CHECKS.length;
  const head = f && f.paper && f.paper.stopped ? stopBanner(f)
    : all ? `<div class="banner ok"><b>✅ 7つ全部OK</b><br>注文してよい（逆指値と利確も一緒に）。</div>`
      : `<div class="banner">あと <b>${CHECKS.length - n}</b> つ。全部そろうまで注文しない。</div>`;
  return `<h1>✅ 注文前チェック<small>${n} / ${CHECKS.length}</small></h1>${head}
    ${CHECKS.map(([t, s], i) => `<div class="ck ${CHECKED.has(i) ? "on" : ""}" onclick="toggleCheck(${i})"><span class="box">${CHECKED.has(i) ? "✓" : ""}</span><div>${esc(t)}${s ? `<small>${esc(s)}</small>` : ""}</div></div>`).join("")}
    <button class="btn" onclick="clearChecks()">↺ 全部外す（次の銘柄へ）</button>
    <div class="sec">ルール早見</div>
    <div class="rules num">
      <div><span>入る所</span><b>38.2%に指値1回</b></div>
      <div><span>逆指値</span><b>起点割れ と −3% の近い方</b></div>
      <div><span>株数</span><b>2万円 ÷ 損切り幅（100株単位）</b></div>
      <div><span>建玉の上限</span><b>130万円</b></div>
      <div><span>利確</span><b>+1% か +2%（本線 +${esc(String((f && f.tp_main) || 1).replace(/\.0$/, ""))}%）</b></div>
      <div><span>時間</span><b>波は11:00まで・30分で撤退・11:30全決済</b></div>
      <div><span>1日の停止</span><b>−4万円 ／ 2連敗 ／ 8回</b></div>
    </div>
    <div class="note">チェックはこの画面を開いている間だけ覚えています（リロードで消えます）。</div>`;
}

// ── 📊 今日の紙トレード ──
function viewPaper() {
  const f = F();
  if (!f) return `<h1>📊 今日の紙トレード</h1>${noData()}`;
  const p = f.paper || {}, lim = p.limits || {}, main = String(p.tp_pct || f.tp_main || 1).replace(/\.0$/, "");
  const trs = (f.paper_trades || (f.candidates || []).filter(c => c.trade)).slice();
  const used = Math.min(100, Math.max(0, (p.pnl_yen < 0 ? -p.pnl_yen : 0) / Math.abs(lim.pnl_yen || -40000) * 100));
  const byTp = p.pnl_by_tp || {};
  return `<h1>📊 今日の紙トレード<small>本線 +${esc(main)}% ・ ${esc(String(f.ts || "").slice(5, 16))}</small></h1>
    ${p.stopped ? stopBanner(f) : `<div class="banner ok"><b>▶ 続行できる</b><br><span style="font-size:15px">止まる条件: −4万円 ／ 2連敗 ／ 8回</span></div>`}
    <div class="kpi num">
      <div><small>損益（本線）</small><b class="${cls(p.pnl_yen)}">${sYen(p.pnl_yen)}</b><div class="bar"><i style="width:${used.toFixed(0)}%"></i></div><span class="lim">−4万円で停止</span></div>
      <div><small>回数</small><b>${p.trades || 0}<span class="lim"> / ${lim.trades || 8}回</span></b></div>
      <div><small>勝ち ／ 負け</small><b><span class="pos">${p.wins || 0}</span> ／ <span class="neg">${p.losses || 0}</span></b></div>
      <div><small>連敗</small><b>${p.consec_loss || 0}<span class="lim"> / ${lim.consec || 2}</span></b></div>
    </div>
    <div class="sec">+1% と +2% のどちらが良かったか（今日）</div>
    <div class="kpi num"><div><small>+1%で利確</small><b class="${cls(byTp["1"])}">${sYen(byTp["1"])}</b></div><div><small>+2%で利確</small><b class="${cls(byTp["2"])}">${sYen(byTp["2"])}</b></div></div>
    <div class="sec">約定した候補（${trs.length}）</div>
    ${trs.length ? trs.map(c => { const tr = c.trade || {}, bk = tr.books || {};
      return `<div class="tr num"><div class="h"><b>${esc(c.name)}</b><span class="muted">${esc(c.code)} ・ ${esc(tr.fill_time || "")}</span></div>
        <div class="s">${yen(c.entry)}円 × ${yen(c.shares)}株 ・ 逆指値 ${yen(c.stop)}</div>
        <div class="cd-res">${["1", "2"].map(k => { const b = bk[k] || {};
          return `<div>+${k}%：${b.exit_type ? `${esc(b.exit_type)} ${esc(b.exit_time || "")}<br><b class="${cls(b.pnl_yen)}">${sYen(b.pnl_yen)}</b>` : "保有中"}</div>`; }).join("")}</div></div>`; }).join("")
      : `<div class="empty">まだ約定していません。</div>`}
    <div class="note">紙トレード＝アプリの候補を機械的に38.2%で買った場合の記録（実際の注文ではありません）。ボスの実トレードとの比較は月末に。</div>`;
}

// ── ✂️ 上ヒゲ刈り取り候補（前夜の監視リスト・oshime5_candidates.py → ../data/oshime5.json）──
let O5 = null, O5_ERR = "";
async function o5Load() {
  for (const u of ["../data/oshime5.json", "data/oshime5.json"]) {
    try {
      const r = await fetch(u, { cache: "no-store" });
      if (r.ok) { O5 = await r.json(); O5_ERR = ""; render(); return; }
    } catch (e) { /* 次の場所 */ }
  }
  O5_ERR = "まだ候補がありません（平日の夜19時ごろに届きます）"; render();
}
function o5Shares(px) {   // 2万円 ÷（株価×3%）を100株単位で切り捨て
  return px > 0 ? Math.floor(20000 / (px * 0.03) / 100) * 100 : 0;
}
function O5L() {   // 場中のライン（oshime5_live.py → payload.oshime5）。今日の分だけ
  const o = LIVE && LIVE.oshime5;
  return o && (CFG.DEMO || String(o.date || "") === todayStr()) ? o : null;
}
const O5ST = { "約定中": ["st-zone", "🟢 約定中（紙）", "zone"], "ライン点灯": ["st-near", "🎯 ライン点灯", "near"], "決済済み": ["st-end", "✓ 決済", "done"],
  "見送り": ["st-skip", "⛔ 見送り", "skip"], "本日終了": ["st-skip", "⛔ 本日終了", "skip"] };
function o5Live(lr) {
  if (!lr) return "";
  const st = O5ST[lr.status];
  const tr = lr.trade;
  if (lr.status === "ライン点灯" && lr.line) return `<div class="cd-now"><b>${esc(lr.line_bar || "")}の足のあいだ</b> 逆指値の買い <b>${yen(lr.line)}</b>円 → 利確 ${yen(lr.tp)} ／ 損切り ${yen(lr.sl)} ／ ${yen(lr.shares)}株</div>`;
  if (tr) {
    const b1 = (tr.books || {})["1.5"] || {}, b2 = (tr.books || {})["3"] || {};
    return `<div class="cd-res"><div>約定 ${esc(tr.fill_time || "")} ${yen(tr.entry)}円×${yen(tr.shares)}株 ・ 損切り ${yen(tr.sl)}</div>
      <div>+1.5%：${b1.exit_type ? `${esc(b1.exit_type)} ${esc(b1.exit_time || "")} <b class="${cls(b1.pnl_yen)}">${sYen(b1.pnl_yen)}</b>` : `保有中（利確 ${yen(b1.tp)}）`}</div>
      <div class="muted">参考 +3%：${b2.exit_type ? `${esc(b2.exit_type)} ${sYen(b2.pnl_yen)}` : "保有中"}</div></div>`;
  }
  if (lr.skip_reason) return `<div class="cd-skip">⛔ ${esc(lr.skip_reason)}</div>`;
  return st ? "" : `<div class="cd-now muted">${esc(lr.status || "")}${lr.ma ? ` ・ 25MA ${yen(lr.ma)}` : ""}</div>`;
}
function o5Card(r, i) {
  const m = LIVE && LIVE.stocks ? LIVE.stocks[r.code] : null;
  const lr = O5L() ? (O5L().rows || []).find(x => x.code === r.code) : null;
  const st = lr ? O5ST[lr.status] : null;
  const sh = o5Shares(r.close);
  const now = m && m.last ? `<div class="cd-now">いま ${yen(m.last)}円 <span class="${cls(m.chg)}">${pct1(m.chg)}</span>${m.d5 != null ? ` ・ 5分 <span class="${cls(m.d5)}">${pct1(m.d5)}</span>` : ""}</div>` : "";
  return `<div class="cd ${st ? st[0] : ""}">
    <div class="cd-top"><span class="badge ${st ? st[2] : "wait"}">${st ? st[1] : i + 1}</span><span class="nm">${esc(r.name)}</span><span class="code">${esc(r.code)}</span></div>
    ${o5Live(lr)}
    <div class="px3 num"><div><small>前日終値</small><b>${yen(r.close)}</b></div><div><small>5日騰落</small><b class="${cls(r.r5)}">${pct1(r.r5)}</b></div><div><small>値幅ATR</small><b>${r.atr_pct != null ? Number(r.atr_pct).toFixed(1) + "%" : "—"}</b></div></div>
    <div class="cd-line num">時価総額 <b>${r.mcap_oku != null ? yen(r.mcap_oku) : "—"}億</b> ・ 20日平均代金 <b>${r.tov20_oku != null ? Math.round(r.tov20_oku) : "—"}億</b> ・ 呼値 <b>${r.tick != null ? r.tick : "—"}円</b> ・ 株数の目安 <b>${sh > 0 ? yen(sh) + "株" : "100株でも損失2万円超"}</b></div>
    ${now}</div>`;
}
function o5PaperBar() {
  const o = O5L(); if (!o) return "";
  const p = o.paper || {};
  return `<div class="banner ${p.stopped ? "stop" : "ok"}" style="font-size:15px"><b>${p.stopped ? "⛔ " + esc(p.stop_reason) : "▶ 場中ライン稼働中"}</b>
    <br>紙トレード ${p.trades || 0}回（${p.wins || 0}勝${p.losses || 0}敗） <b class="${cls(p.pnl_yen)}">${sYen(p.pnl_yen)}</b> ・ ${esc(String(o.ts || "").slice(11, 16))}更新 ・ ${esc((o.rules || {}).stop || "")}</div>`;
}
let O5SORT = "r5";          // 並べ替え: r5=5日騰落の深い順 / mcap=時価総額の大きい順 / tov=20日代金の多い順（10/8 本人「時価総額の列足して」）
function o5Sorted(rows) {
  const v = r => O5SORT === "mcap" ? -(r.mcap_oku || 0) : O5SORT === "tov" ? -(r.tov20_oku || 0) : (r.r5 != null ? r.r5 : 0);
  return rows.slice().sort((a, b) => v(a) - v(b));
}
function o5Order(rows) {   // 場中は 約定中→ライン点灯→その他 の順。その中は O5SORT の順
  rows = o5Sorted(rows);
  const o = O5L(); if (!o) return rows.map((r, i) => [r, i]);
  const pri = { "約定中": 0, "ライン点灯": 1 };
  const stOf5 = r => { const x = (o.rows || []).find(y => y.code === r.code); return x ? (pri[x.status] != null ? pri[x.status] : 2) : 2; };
  return rows.map((r, i) => [r, i]).sort((a, b) => stOf5(a[0]) - stOf5(b[0]) || a[1] - b[1]);
}
function o5SortBar() {
  const b = (k, lab) => `<button class="tab ${O5SORT === k ? "on" : ""}" onclick="O5SORT='${k}';render()">${lab}</button>`;
  return `<div class="tabs" style="margin:6px 0">${b("r5", "5日騰落の深い順")}${b("mcap", "時価総額の大きい順")}${b("tov", "代金の多い順")}</div>`;
}
function viewOshime5() {
  if (O5 && O5.stopped) return `<h1>✂️ 上ヒゲ刈り取り候補</h1><div class="banner warn">⏹ ${esc(String(O5.stopped))} に停止しました（紙4日7回・60日検証は8/15以降ゼロ）。候補の配信も場中のラインも出ません。</div>`;
  if (!O5 || !(O5.rows || []).length) return `<h1>✂️ 上ヒゲ刈り取り候補</h1><div class="empty">${esc(O5 ? "該当なし（条件に合う銘柄がありませんでした）" : (O5_ERR || "読み込み中…"))}</div>`;
  const old = String(O5.target_date || "") < todayStr() && !CFG.DEMO;
  return `<h1>✂️ 上ヒゲ刈り取り候補<small>${esc(String(O5.target_date || "").slice(5))} 分 ・ ${esc(String(O5.date || "").slice(5))} 引けで選んだ${(O5.rows || []).length}本</small></h1>
    ${old ? `<div class="banner warn">これは ${esc(O5.target_date || "")} 分の候補です（次の分は夜19時ごろ）。</div>` : ""}
    <div class="note" style="margin-bottom:10px"><b>入る</b>: ${esc(O5.entry || "")}<br><b>出る</b>: ${esc(O5.exit || "")}</div>
    <div class="banner warn" style="font-size:15px">⛔ ${(O5.skip_rules || []).map(esc).join("<br>⛔ ")}</div>
    ${o5PaperBar()}
    ${o5SortBar()}
    ${o5Order(O5.rows || []).map(([r, i]) => o5Card(r, i)).join("")}
    <div class="note">選び方: ${esc(O5.rule || "")}（該当${O5.matched != null ? O5.matched : "—"}銘柄）。<br>検証: ${esc(O5.backtest || "")}<br><b>発注はしません（候補の提示だけ）</b>。${esc(O5.note || "")}</div>`;
}

// ── 🧮 電卓（買値 → 利確+1.5%・損切り−3%・株数）──
const CALC = { px: "", sh: "", t500: true, tp: 1.5, sl: 3 };
function tickOf(px, t500) {      // 呼値（TOPIX500 は細かい刻み）
  const T = t500 ? [[1000, 0.1], [3000, 0.5], [10000, 1], [30000, 5], [100000, 10]] : [[3000, 1], [5000, 5], [30000, 10], [50000, 50], [300000, 100]];
  for (const [lim, t] of T) if (px <= lim) return t;
  return t500 ? 50 : 1000;
}
const ceilTick = (v, t) => Math.round(Math.ceil(v / t - 1e-9) * t * 10) / 10;
function calcOut() {
  const px = Number(CALC.px), t500 = CALC.t500;
  if (!(px > 0)) return `<div class="empty" style="font-size:15px">買値を入れると、利確・損切りの値段と株数が出ます。</div>`;
  const t = tickOf(px, t500);
  const tpRaw = px * (1 + CALC.tp / 100), slRaw = px * (1 - CALC.sl / 100);
  const tp = ceilTick(tpRaw, tickOf(tpRaw, t500)), sl = ceilTick(slRaw, tickOf(slRaw, t500));
  const auto = Math.floor(20000 / (px * CALC.sl / 100) / 100) * 100;
  const sh = Number(CALC.sh) > 0 ? Math.floor(Number(CALC.sh)) : auto;
  const f1 = v => Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  return `<div class="px3 num"><div class="ent"><small>利確（指値・+${CALC.tp}%）</small><b>${f1(tp)}</b></div><div class="stp"><small>損切り（逆指値・−${CALC.sl}%）</small><b>${f1(sl)}</b></div><div><small>株数</small><b>${sh > 0 ? sh.toLocaleString() : "—"}</b></div></div>
    <div class="cd-line num">利確したら <b class="pos">${sh > 0 ? sYen((tp - px) * sh) : "—"}</b> ／ 損切りしたら <b class="neg">${sh > 0 ? sYen((sl - px) * sh) : "—"}</b></div>
    <div class="cd-line num">建玉 <b>${sh > 0 ? Math.round(px * sh).toLocaleString() + "円" : "—"}</b> ・ 呼値 <b>${t}円</b>（${t500 ? "TOPIX500" : "一般の銘柄"}）</div>
    ${Number(CALC.sh) > 0 ? "" : `<div class="note" style="margin-top:6px">株数＝損失2万円 ÷（買値×${CALC.sl}%）を100株単位で切り捨て${auto <= 0 ? "。この株価だと100株でも損失が2万円を超えます（株数を入れてください）" : ""}。</div>`}
    <div class="note" style="margin-top:6px">値段は呼値に合わせて切り上げ（計算上の値: 利確 ${tpRaw.toFixed(1)} ／ 損切り ${slRaw.toFixed(1)}）。</div>`;
}
function calcSet(k, v) {
  CALC[k] = v;
  const el = document.getElementById("calc-out");
  if (el) el.innerHTML = calcOut(); else render();
}
function calcPick(code) {
  const r = ((O5 && O5.rows) || []).find(x => x.code === code); if (!r) return;
  const m = LIVE && LIVE.stocks ? LIVE.stocks[code] : null;
  CALC.px = String(m && m.last ? m.last : r.close); CALC.sh = ""; CALC.t500 = true; render();
}
function viewCalc() {
  const picks = ((O5 && O5.rows) || []).map(r => `<a class="tag pri" style="cursor:pointer;margin:0 6px 6px 0;display:inline-block" onclick="calcPick('${esc(r.code)}')">${esc(r.name)}</a>`).join("");
  const inp = "width:100%;font-size:22px;padding:10px 12px;border-radius:12px;border:1px solid var(--ln);background:var(--sf2);color:var(--tx);box-sizing:border-box";
  return `<h1>🧮 電卓<small>利確+${CALC.tp}%・損切り−${CALC.sl}%</small></h1>
    <div class="cd">
      <label style="display:block;font-weight:800;margin-bottom:4px">買値（約定した値段）</label>
      <input type="number" inputmode="decimal" step="0.1" value="${esc(CALC.px)}" oninput="calcSet('px', this.value)" style="${inp}" placeholder="例 3057">
      <label style="display:block;font-weight:800;margin:10px 0 4px">株数（空なら損失2万円で自動）</label>
      <input type="number" inputmode="numeric" step="100" value="${esc(CALC.sh)}" oninput="calcSet('sh', this.value)" style="${inp}" placeholder="自動">
      <label style="display:flex;align-items:center;gap:8px;margin-top:10px;font-size:15px"><input type="checkbox" ${CALC.t500 ? "checked" : ""} onchange="calcSet('t500', this.checked)"> TOPIX500の銘柄（呼値が細かい）</label>
    </div>
    <div class="cd" id="calc-out">${calcOut()}</div>
    ${picks ? `<div class="sec">✂️上ヒゲの候補から入れる</div><div>${picks}</div>` : ""}
    <div class="note">計算だけです。発注はしません。</div>`;
}

// ── ルーター ──
const ROUTES = [["#/check", "check", viewCheck], ["#/paper", "paper", viewPaper], ["#/oshime5", "oshime5", viewOshime5], ["#/calc", "calc", viewCalc], ["#/", "cands", viewCands]];
function render() {
  const h = (typeof location !== "undefined" && location.hash) || "#/";
  const r = ROUTES.find(x => h.startsWith(x[0])) || ROUTES[ROUTES.length - 1];
  ["cands", "check", "paper", "oshime5", "calc"].forEach(k => { const el = document.getElementById("nav-" + k); if (el && el.classList) el.classList.toggle("on", r[1] === k); });
  try { $("#view").innerHTML = r[2](); }
  catch (e) { $("#view").innerHTML = `<div class="banner stop"><b>表示エラー</b><br>${esc(e.message)}</div>`; }
  clock();
}

if (typeof window !== "undefined" && window.addEventListener) {
  window.addEventListener("hashchange", () => { render(); if (window.scrollTo) window.scrollTo(0, 0); });
  try { if (navigator.serviceWorker) navigator.serviceWorker.register("sw.js"); } catch (e) { /* ignore */ }
  applyTheme();
  render();
  liveInit();
}
