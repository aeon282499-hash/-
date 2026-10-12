// calc.js — 🧮電卓（v7・2026-10-13）。v6 day.js の電卓を移植: 買値/売値 → 利確・損切りの値段（呼値に合わせる）・株数（損失2万円）・金額。
// 📈買い: 利確=+1.5%（指値・呼値へ切り上げ）／損切り=−3%（逆指値・切り上げ）
// 🩳空売り: 利確=−1.5%（買戻し指値・切り下げ）／損切り=+3%（買戻し逆指値・切り下げ）
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
function calcOut() {
  const px = Number(CALC.px), t500 = CALC.t500, short = CALC.side === "sell";
  if (!(px > 0)) return `<div class="empty" style="font-size:14px">${short ? "売値" : "買値"}を入れると、利確・損切りの値段と株数が出ます。</div>`;
  const L = calcLevels(px, CALC.side, t500);
  const sh = Number(CALC.sh) > 0 ? Math.floor(Number(CALC.sh)) : L.auto;
  const pnlTp = short ? (px - L.tp) * sh : (L.tp - px) * sh, pnlSl = short ? (px - L.sl) * sh : (L.sl - px) * sh;
  return `<div class="px3 num"><div class="ent"><small>利確（${short ? "買戻し" : ""}指値・${short ? "−" : "+"}${CALC.tp}%）</small><b>${calcF1(L.tp)}</b></div><div class="stp"><small>損切り（${short ? "買戻し" : ""}逆指値・${short ? "+" : "−"}${CALC.sl}%）</small><b>${calcF1(L.sl)}</b></div><div><small>株数</small><b>${sh > 0 ? sh.toLocaleString() : "—"}</b></div></div>
    <div class="cd-line num">利確したら <b class="pos">${sh > 0 ? calcYen(pnlTp) : "—"}</b> ／ 損切りしたら <b class="neg">${sh > 0 ? calcYen(pnlSl) : "—"}</b></div>
    <div class="cd-line num">${short ? "売り建玉" : "建玉"} <b>${sh > 0 ? Math.round(px * sh).toLocaleString() + "円" : "—"}</b> ・ 呼値 <b>${tickOf(px, t500)}円</b>（${t500 ? "TOPIX500" : "一般の銘柄"}）</div>
    ${Number(CALC.sh) > 0 ? "" : `<div class="note" style="margin-top:6px">株数＝損失2万円 ÷（${short ? "売値" : "買値"}×${CALC.sl}%）を100株単位で切り捨て${L.auto <= 0 ? "。この株価だと100株でも損失が2万円を超えます（株数を入れてください）" : ""}。</div>`}
    <div class="note" style="margin-top:6px">値段は呼値に合わせて${short ? "切り下げ" : "切り上げ"}（計算上の値: 利確 ${L.tpRaw.toFixed(1)} ／ 損切り ${L.slRaw.toFixed(1)}）。${short ? "空売りは信用売り（貸借銘柄だけ・新規売停止や逆日歩は別に確認）。手数料・貸株料は含みません。" : ""}</div>`;
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
  return `<h2>🧮 電卓 <span class="sub">${short ? `空売り：利確−${CALC.tp}%・損切り+${CALC.sl}%` : `買い：利確+${CALC.tp}%・損切り−${CALC.sl}%`}</span></h2>
    ${whenBar("🧮", "注文を出す直前に", "建値を入れる → 利確・損切りの値段（呼値どおり）と株数（損失2万円）と金額。計算だけ・発注はしない。")}
    <div class="card">${seg}${from}
      <label style="display:block;font-weight:800;margin-bottom:4px">${short ? "売値（空売りが約定した値段）" : "買値（約定した値段）"}</label>
      <input type="number" inputmode="decimal" step="0.1" value="${esc(CALC.px)}" oninput="calcSet('px', this.value)" class="calc-in" placeholder="例 3057">
      <label style="display:block;font-weight:800;margin:10px 0 4px">株数（空なら損失2万円で自動）</label>
      <input type="number" inputmode="numeric" step="100" value="${esc(CALC.sh)}" oninput="calcSet('sh', this.value)" class="calc-in" placeholder="自動">
      <label style="display:flex;align-items:center;gap:8px;margin-top:10px;font-size:14px"><input type="checkbox" ${CALC.t500 ? "checked" : ""} onchange="calcSet('t500', this.checked)"> TOPIX500の銘柄（呼値が細かい）</label>
    </div>
    <div class="card" id="calc-out">${calcOut()}</div>
    <div class="note">計算だけです。発注はしません。</div>
    <p class="disc">${esc(DATA && DATA.disclaimer || "")}</p>`;
}
