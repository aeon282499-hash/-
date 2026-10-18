// calc.js — 🧮電卓（v7・2026-10-13）。v6 day.js の電卓を移植: 買値/売値 → 利確・損切りの値段（呼値に合わせる）・株数（損失2万円）・金額。
// 2026-10-17 細分化: 利確 1.5／3／5% と 損切り 1／2／3% を全部出す。株数は選んだ損切り%（既定3%）で損失2万円。
// 📈買い: 利確=+%（指値・呼値へ切り上げ）／損切り=−%（逆指値・切り上げ）
// 🩳空売り: 利確=−%（買戻し指値・切り下げ）／損切り=+%（買戻し逆指値・切り下げ）
// 🎯前夜の表（arena.js）は calcLevels の既定（+1.5% / −3%）のまま。
//   → どちらも「約束の%より不利にならない側」へ丸める。計算だけ。発注はしない。
const CALC = { px: "", sh: "", t500: false, tp: 1.5, sl: 3, side: "buy", name: "", code: "" };
function tickOf(px, t500) {      // 呼値（TOPIX500 は細かい刻み）
  const T = t500 ? [[1000, 0.1], [3000, 0.5], [10000, 1], [30000, 5], [100000, 10]] : [[3000, 1], [5000, 5], [30000, 10], [50000, 50], [300000, 100]];
  for (const [lim, t] of T) if (px <= lim) return t;
  return t500 ? 50 : 1000;
}
const ceilTick = (v, t) => Math.round(Math.ceil(v / t - 1e-9) * t * 10) / 10;
const floorTick = (v, t) => Math.round(Math.floor(v / t + 1e-9) * t * 10) / 10;
// 建値 px から 利確/損切り/自動株数 を出す（🎯前夜の表と🧮電卓で共用）
function calcLevels(px, side, t500, tp = CALC.tp, sl = CALC.sl, lossYen = 20000) {
  const short = side === "sell";
  const tpRaw = short ? px * (1 - tp / 100) : px * (1 + tp / 100), slRaw = short ? px * (1 + sl / 100) : px * (1 - sl / 100);
  const rnd = short ? floorTick : ceilTick;
  const tpPx = rnd(tpRaw, tickOf(tpRaw, t500)), slPx = rnd(slRaw, tickOf(slRaw, t500));
  const auto = Math.floor(lossYen / (px * sl / 100) / 100) * 100;
  return { tp: tpPx, sl: slPx, auto, tpRaw, slRaw, short };
}
const calcF1 = v => Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const calcYen = v => (v == null || isNaN(v)) ? "—" : (v > 0 ? "+" : v < 0 ? "−" : "±") + Math.abs(Math.round(v)).toLocaleString() + "円";
// 2026-10-17 細分化（本人「利確は1.5・3・5%、損切は1・2・3%で」）: 利確3段×損切り3段を同時に出す。
// 株数は「選んだ損切り%」で損失2万円から決める（損切りを浅くすると株数は増える）。
const CALC_TPS = [1.5, 3, 5], CALC_SLS = [1, 2, 3];
function calcOut() {
  const px = Number(CALC.px), t500 = CALC.t500, short = CALC.side === "sell";
  if (!(px > 0)) return `<div class="empty" style="font-size:16px">${short ? "売値" : "買値"}を入れると、利確・損切りの値段と株数が出ます。</div>`;
  const slSel = CALC_SLS.includes(Number(CALC.sl)) ? Number(CALC.sl) : 3;
  const base = calcLevels(px, CALC.side, t500, CALC.tp, slSel);
  const sh = Number(CALC.sh) > 0 ? Math.floor(Number(CALC.sh)) : base.auto;
  const pnl = (lvl) => short ? (px - lvl) * sh : (lvl - px) * sh;
  const tick = tickOf(px, t500), sgnT = short ? "−" : "+", sgnS = short ? "+" : "−";
  const slSeg = `<div class="seg" style="margin:0 0 8px">${CALC_SLS.map(v => `<a class="${v === slSel ? "on" : ""}" onclick="calcSet('sl',${v})">損切り ${sgnS}${v}%</a>`).join("")}</div>`;
  const tpRows = CALC_TPS.map(tp => { const L = calcLevels(px, CALC.side, t500, tp, slSel); return `<tr><td>利確 ${sgnT}${tp}%</td><td class="num"><b>${calcF1(L.tp)}</b></td><td class="num pos">${sh > 0 ? calcYen(pnl(L.tp)) : "—"}</td><td class="num mut">${L.tpRaw.toFixed(1)}</td></tr>`; }).join("");
  const slRows = CALC_SLS.map(sl => { const L = calcLevels(px, CALC.side, t500, CALC.tp, sl); const on = sl === slSel; return `<tr class="${on ? "sel" : ""}"><td>${on ? "▶ " : ""}損切り ${sgnS}${sl}%</td><td class="num"><b class="neg">${calcF1(L.sl)}</b></td><td class="num neg">${sh > 0 ? calcYen(pnl(L.sl)) : "—"}</td><td class="num mut">${L.auto > 0 ? L.auto.toLocaleString() + "株" : "100株でも2万円超"}</td></tr>`; }).join("");
  return `${slSeg}
    <div class="px3 num"><div class="ent"><small>建値（${short ? "売値" : "買値"}）</small><b>${calcF1(px)}</b></div><div><small>株数（損切り${sgnS}${slSel}%で損失2万円）</small><b>${sh > 0 ? sh.toLocaleString() : "—"}</b></div><div><small>${short ? "売り建玉" : "建玉"}</small><b>${sh > 0 ? Math.round(px * sh).toLocaleString() : "—"}</b></div></div>
    <table class="tbl calc-tbl num"><thead><tr><th>利確（${short ? "買戻し" : ""}指値）</th><th>値段</th><th>利確したら</th><th>計算上</th></tr></thead><tbody>${tpRows}</tbody></table>
    <table class="tbl calc-tbl num" style="margin-top:8px"><thead><tr><th>損切り（${short ? "買戻し" : ""}逆指値）</th><th>値段</th><th>損切りしたら</th><th>自動株数</th></tr></thead><tbody>${slRows}</tbody></table>
    <div class="cd-line num" style="margin-top:6px">呼値 <b>${tick}円</b>（${t500 ? "TOPIX500" : "一般の銘柄"}）・値段は呼値に合わせて${short ? "切り下げ" : "切り上げ"}（約束の%より不利にならない側）。</div>
    ${Number(CALC.sh) > 0 ? `<div class="note" style="margin-top:6px">株数は入力の${sh.toLocaleString()}株で計算。空にすると損切り${sgnS}${slSel}%で損失2万円になる株数（100株単位・切り捨て）に戻ります。「自動株数」の列は損切り幅ごとの株数。</div>` : `<div class="note" style="margin-top:6px">株数＝損失2万円 ÷（${short ? "売値" : "買値"}×${slSel}%）を100株単位で切り捨て${base.auto <= 0 ? "。この株価だと100株でも損失が2万円を超えます（株数を入れてください）" : ""}。損切りを浅くするほど株数は増え、利確の円も増えます（ただし損切りに当たりやすい）。</div>`}
    <div class="note" style="margin-top:6px">${short ? "空売りは信用売り（貸借銘柄だけ・新規売停止や逆日歩は別に確認）。" : ""}手数料・貸株料は含みません。</div>`;
}
function calcSet(k, v) {
  CALC[k] = v;
  const el = document.getElementById("calc-out");
  if (k === "side" || !el) render(); else el.innerHTML = calcOut();   // 売買の切替は見出し・ラベルも変わるので全体を描き直す
}
// 🎯前夜の表から: 銘柄・逆指値の水準・向き・呼値区分を電卓に入れて開く
function calcPick(code, px, side, t500, name) {
  CALC.px = String(px); CALC.sh = ""; CALC.side = side === "sell" ? "sell" : "buy"; CALC.t500 = !!t500; CALC.code = code || ""; CALC.name = name || "";
  if (typeof location !== "undefined") location.hash = "#/calc";
  render();
}
function viewCalc() {
  const short = CALC.side === "sell";
  const seg = `<div class="seg" style="margin-bottom:10px"><a class="${short ? "" : "on"}" onclick="calcSet('side','buy')">📈 買い</a><a class="${short ? "on" : ""}" onclick="calcSet('side','sell')">🩳 空売り</a></div>`;
  const from = CALC.code ? `<div class="note" style="margin-bottom:6px">🎯前夜の表から: <a href="#/detail/${esc(CALC.code)}"><b>${esc(CALC.name || CALC.code)}</b></a>（${esc(CALC.code)}）の${short ? "売り" : "買い"}逆指値の水準を建値に入れています。</div>` : "";
  return `<h2>🧮 電卓 <span class="sub">${short ? "空売り：利確 −1.5／−3／−5%・損切り +1／+2／+3%" : "買い：利確 +1.5／+3／+5%・損切り −1／−2／−3%"}</span></h2>
    ${whenBar("🧮", "注文を出す直前に", "建値を入れる → 利確3段・損切り3段の値段（呼値どおり）と株数（選んだ損切り幅で損失2万円）と金額。計算だけ・発注はしない。")}
    <div class="card">${seg}${from}
      <label style="display:block;font-weight:800;margin-bottom:4px">${short ? "売値（空売りが約定した値段）" : "買値（約定した値段）"}</label>
      <input type="number" inputmode="decimal" step="0.1" value="${esc(CALC.px)}" oninput="calcSet('px', this.value)" class="calc-in" placeholder="例 3057">
      <label style="display:block;font-weight:800;margin:10px 0 4px">株数（空なら損失2万円で自動）</label>
      <input type="number" inputmode="numeric" step="100" value="${esc(CALC.sh)}" oninput="calcSet('sh', this.value)" class="calc-in" placeholder="自動">
      <label style="display:flex;align-items:center;gap:8px;margin-top:10px;font-size:16px"><input type="checkbox" ${CALC.t500 ? "checked" : ""} onchange="calcSet('t500', this.checked)"> TOPIX500の銘柄（呼値が細かい）</label>
    </div>
    <div class="card" id="calc-out">${calcOut()}</div>
    <div class="note">計算だけです。発注はしません。</div>
    <p class="disc">${esc(DATA && DATA.disclaimer || "")}</p>`;
}
