// v5フロント（2026-09-14 フルリニューアル）の DOMシム検証。
// feedback_kabuai_spa_verify: WebFetchではJSが実行されないので、node vm + DOMシムで
// 全ルートを実データ（data/latest.json・live_flow/latest.json）に対して実行し、
// 表示エラー/NaN/undefined ゼロを確認してから push する。  実行: node _harness_v5.mjs
import fs from "node:fs";
import vm from "node:vm";

const html = fs.readFileSync("web/index.html", "utf8");
const DATA = JSON.parse(fs.readFileSync("data/latest.json", "utf8"));
const SIDX = JSON.parse(fs.readFileSync("data/search_index.json", "utf8"));
const EXPJ = JSON.parse(fs.readFileSync("data/explorer.json", "utf8"));
let LIVEJ = null;
try { LIVEJ = JSON.parse(fs.readFileSync("../live_flow/latest.json", "utf8")); } catch (e) { /* ライブ無しでも通す */ }

// index.html の <script src> 順に読み込む（?v= を除去）
const srcs = [...html.matchAll(/<script src="([^"?]+)(?:\?[^"]*)?"><\/script>/g)].map(m => m[1]);
if (!srcs.length) { console.error("FAIL: script tags not found"); process.exit(1); }
const code = srcs.map(s => fs.readFileSync("web/" + s, "utf8")).join("\n;\n");

const ctxProxy = new Proxy({}, { get: () => () => {}, set: () => true });
const store = {};
function mkEl(id) {
  return { _id: id, _html: "", style: {}, clientWidth: 360, parentElement: { clientWidth: 360 },
    set innerHTML(v) { this._html = v; }, get innerHTML() { return this._html; },
    textContent: "", classList: { toggle() {}, add() {}, remove() {} }, focus() {}, setAttribute() {},
    getContext: () => ctxProxy, querySelector() { return mkEl("c"); }, querySelectorAll() { return []; } };
}
function $get(sel) { return store[sel] || (store[sel] = mkEl(sel)); }
const documentShim = { querySelector: $get, getElementById: id => $get("#" + id), addEventListener() {},
  querySelectorAll: () => [], createElement: () => mkEl("new"), body: mkEl("body"), documentElement: mkEl("html") };
const locationShim = { hash: "#/" };
const windowShim = { addEventListener() {}, scrollTo() {}, location: locationShim, innerWidth: 390, devicePixelRatio: 1 };
const lsStore = {};
const localStorageShim = { getItem: k => (k in lsStore ? lsStore[k] : null), setItem: (k, v) => { lsStore[k] = String(v); }, removeItem: k => { delete lsStore[k]; } };
const stockJson = code => { try { return JSON.parse(fs.readFileSync(`data/stocks/${code}.json`, "utf8")); } catch (e) { return null; } };
const sandbox = { document: documentShim, window: windowShim, location: locationShim, localStorage: localStorageShim,
  console, navigator: {}, history: { back() {} },
  fetch: async (u) => {
    const s = String(u);
    if (s.includes("search_index")) return { ok: true, status: 200, json: async () => SIDX };
    if (s.includes("explorer")) return { ok: true, status: 200, json: async () => EXPJ };
    if (s.includes("live.json")) return LIVEJ ? { ok: true, status: 200, json: async () => LIVEJ } : { ok: false, status: 404, json: async () => ({}) };
    const m = s.match(/stocks\/([^./]+)\.json/);
    if (m) { const j = stockJson(m[1]); return { ok: !!j, status: j ? 200 : 404, json: async () => j }; }
    return { ok: true, status: 200, json: async () => DATA };
  },
  setTimeout: (fn) => 0, clearTimeout() {}, setInterval: () => 0, requestAnimationFrame: fn => fn() };
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
vm.runInContext(code, sandbox);
const ev = expr => vm.runInContext(expr, sandbox);   // let/const はグローバル属性にならないので式で読む
await new Promise(r => setTimeout(r, 50));   // boot.js の load().then(...) を待つ

let fail = 0;
const check = (n, c, x = "") => { if (c) console.log(`  OK ${n}${x ? " — " + x : ""}`); else { fail++; console.log(`  NG ${n}${x ? " — " + x : ""}`); } };
const clean = hv => !hv.includes("表示エラー") && !hv.includes("NaN") && !hv.includes("undefined") && !hv.includes("[object");
const view = () => $get("#view").innerHTML;
const go = h => { locationShim.hash = h; sandbox.render(); return view(); };

console.log("── 0) 起動・データ ──");
check("DATAロード", !!ev("DATA && DATA.data_date"), ev("DATA && DATA.data_date"));
check("ROUTES登録", ev("ROUTES.length") >= 9, `${ev("ROUTES.length")}本`);
check("datepill", $get("#datepill").textContent.includes("EOD"), $get("#datepill").textContent);

console.log("── 1) 🔥ライブ ──");
let hv = go("#/");
check("ライブ初期描画(受信前でも壊れない)", clean(hv) && hv.includes("live-root"));
if (LIVEJ) {
  sandbox.liveApply(LIVEJ);
  hv = $get("#live-root").innerHTML;
  check("ライブ受信後: 既定はセクター(v5.1)", clean(hv) && hv.includes("33業種") && (hv.match(/class="gcard/g) || []).length === 33);
  sandbox.liveSetSeg("themes"); hv = $get("#live-root").innerHTML;
  check("ライブ受信後: テーマ一覧", clean(hv) && hv.includes("テーマ") && hv.includes("gcard"), `${(hv.match(/class="gcard/g) || []).length}カード`);
  check("ライブ: 市場KPI", hv.includes("市場全体") && hv.includes("kpi"));
  const key = LIVEJ.themes[0].key;
  sandbox.liveToggle("themes", key); hv = $get("#live-root").innerHTML;
  check("ライブ: テーマ展開で構成銘柄行", clean(hv) && hv.includes("srow"), `${(hv.match(/class="srow/g) || []).length}行`);
  sandbox.liveSetSort("chg_w"); hv = $get("#live-root").innerHTML; check("ライブ: 騰落率ソート", clean(hv));
  sandbox.liveSetQ("半導体"); hv = $get("#live-groups").innerHTML; check("ライブ: テーマ絞り込み '半導体'", clean(hv) && hv.includes("半導体") && !hv.includes("防衛"));
  sandbox.liveSetQ(""); sandbox.liveToggleAll(); hv = $get("#live-root").innerHTML; check("ライブ: 全件表示トグル", clean(hv)); sandbox.liveToggleAll();
  sandbox.liveSetSeg("sectors"); hv = $get("#live-root").innerHTML; check("ライブ: セクター", clean(hv) && hv.includes("業種"));
  sandbox.liveSetSeg("stocks"); hv = $get("#live-root").innerHTML; check("ライブ: 個別(今きてる)", clean(hv));
  for (const t of ["hot", "gain", "tovtop", "lose"]) { sandbox.liveSetTab(t); hv = $get("#live-root").innerHTML; check(`ライブ: 個別タブ ${t}`, clean(hv) && hv.includes("srow")); }
  check("ライブ: 時計", clean(sandbox.liveClockInner()) && sandbox.liveClockInner().includes(LIVEJ.hhmm));
  { // 📐フィボ候補（合成 payload.fibo）
    const fb = { ts: "2026-09-17 10:25:00", n_watch: 1, candidates: [{ code: "9130", name: "共栄タンカー", label: "ギャップ", rank: "強", origin: 1806, high: 2099, rise: 16.2, mins: 25, vol_ratio: 11.1, fib: { "38.2": 1987, "50": 1952, "61.8": 1918, "78.6": 1869 }, pull_low: 1911, retrace: 64, status: "entered", note: "★エントリー 1948", signal: { entry: 1948, stop: 1890, tp1: 2006, tp2: 2045, rr: 1.0, overlap: 0, overlap_items: [] } }], trades: [{ code: "9130", name: "共栄タンカー", entry: 1948, stop: 1890, half: false, last: 1960, closed: false }] };
    vm.runInContext("LIVE.fibo = " + JSON.stringify(fb), sandbox); sandbox.liveRefresh(); hv = $get("#live-root").innerHTML;
    check("ライブ: 📐フィボ候補(合成)", clean(hv) && hv.includes("フィボ押し目候補") && hv.includes("共栄タンカー") && hv.includes("エントリー"));
    vm.runInContext("delete LIVE.fibo", sandbox); sandbox.liveRefresh(); hv = $get("#live-root").innerHTML; check("ライブ: フィボ無しでも通る", clean(hv) && !hv.includes("フィボ押し目候補"));
  }
  sandbox.liveSetSeg("themes");
} else console.log("  (live_flow/latest.json なし→ライブ受信後の検査はスキップ)");
hv = go("#/about"); check("使い方", clean(hv) && hv.includes("ライブの数字"));

console.log("── 2) 🎯土俵 / 🔻売り / 🐵EOD / 🧭探検 ──");
hv = go("#/arena"); check("土俵", clean(hv) && hv.includes("土俵"));
hv = go("#/plan"); check("作戦(データ無しでも落ちない)", clean(hv) && hv.includes("作戦"));
{ const bak = DATA.plan; DATA.plan = JSON.parse(fs.readFileSync("_plan_sample.json", "utf8"));
  hv = go("#/plan"); check("作戦: サンプル注入", clean(hv) && hv.includes("実弾の注文") && hv.includes("資金ラダー") && hv.includes("寄指"));
  hv = go("#/arena"); check("前夜の準備: 材料＋明日の決算", clean(hv) && hv.includes("材料") && hv.includes("明日の決算発表") && hv.includes("前夜"));
  hv = go("#/plan"); check("作戦: 今やること＋保有＋答え合わせ", clean(hv) && /寄り前|場中|引け|前夜|休場|過去分/.test(hv) && hv.includes("保有中の玉と出口") && hv.includes("直近の答え合わせ") && hv.includes("現金余力"));
  sandbox.planSetCash("20"); hv = view(); check("作戦: 現金余力20万→②ゼロ・崩壊なし", clean(hv) && hv.includes("②ゼロ") && hv.includes("今日は出さない"));
  sandbox.planSetCash("120"); hv = view(); check("作戦: 現金余力120万→①130・崩壊100万", clean(hv) && hv.includes("①130万") && hv.includes("崩壊 100万"));
  sandbox.planSetCash(""); localStorageShim.removeItem("kabuai_cash");
  if (LIVEJ) { vm.runInContext("LIVE = " + JSON.stringify(LIVEJ), sandbox); const live = sandbox.planLiveInner(); check("作戦のいま(ライブ注入)", clean(live)); }
  DATA.plan = { ...DATA.plan, orders: [], paper: [], news: [], earnings_tomorrow: [] }; hv = go("#/plan"); check("作戦: 全部ゼロ件", clean(hv) && hv.includes("撃つ玉なし"));
  DATA.plan = bak; }
hv = go("#/sell"); check("売り", clean(hv) && hv.includes("フェード") && hv.includes("モメンタム終了") && hv.includes("EODランキング"));
check("売り: 💥枠(0件でも出る)", hv.includes("崩壊ショート") && (hv.includes("本日💥なし") || hv.includes("寄指 売り")));
// 💥該当日の描画（合成: 先頭メンバーを💥にして寄指行が出るか）
{ const m0 = DATA.sell_watch && DATA.sell_watch.members && DATA.sell_watch.members[0];
  if (m0) { const bak = {...m0}; Object.assign(m0, {crash: true, shortable: true, entry_min: Math.round(m0.price * 0.97), strong: (m0.turnover_oku || 0) >= 20});
    DATA.sell_watch.crash.count = 1; hv = go("#/sell");
    check("売り: 💥合成1件→寄指行", clean(hv) && hv.includes("寄指 売り") && hv.includes("引け成行で買い戻し") && hv.includes(m0.name));
    Object.assign(m0, bak); delete m0.entry_min; delete m0.strong; DATA.sell_watch.crash.count = 0; } }
hv = go("#/momentum"); check("EODランキング", clean(hv) && hv.includes("モメンタム") && (hv.match(/class="mrow/g) || []).length >= 10, `${(hv.match(/class="mrow/g) || []).length}行`);
check("EOD: 勝ちやすい順張り折りたたみ", hv.includes("勝ちやすい順張り"));
sandbox.setMomGrade("A"); hv = view(); check("EOD: Aフィルタ", clean(hv)); sandbox.setMomGrade("all");
hv = go("#/explore"); await new Promise(r => setTimeout(r, 20)); hv = go("#/explore"); check("探検トップ", clean(hv) && hv.includes("初動"));
for (const c of ev("Object.keys(CAT_DEFS)")) { hv = go(`#/explore/${c}`); check(`探検/${c}`, clean(hv)); }

console.log("── 3) 検索 / 詳細 / ウォッチ / 持ち株 ──");
hv = go("#/search"); await new Promise(r => setTimeout(r, 20));
sandbox.onSearchInput("72"); hv = $get("#searchres").innerHTML; check("検索 '72'", clean(hv) && hv.includes("rcard"));
sandbox.onSearchInput(""); check("検索 空→ウォッチ案内", clean($get("#searchres").innerHTML));
const codes = (DATA.ranking || []).slice(0, 3).map(r => r.code).concat(((DATA.arena || {}).rows || []).slice(0, 2).map(r => r.code));
for (const c of codes) {
  go(`#/detail/${c}`); await sandbox.loadStockDetail(c);
  const d = $get("#dbody").innerHTML; check(`詳細 ${c}`, clean(d) && d.includes("日足") || clean(d) && d.includes("EODデータ"), d.includes("🔥 いま") ? "ライブあり" : "");
}
{ const pc = ((DATA.plan || {}).orders || [])[0]; if (pc) { go(`#/detail/${pc.code}`); await sandbox.loadStockDetail(pc.code); const d = $get("#dbody").innerHTML; check(`詳細 ${pc.code}: 作戦バナー`, clean(d) && d.includes("今日の作戦")); } }
sandbox.toggleWatch(codes[0]); check("ウォッチ追加", sandbox.isWatched(codes[0]));
lsStore["kabuai_pos"] = JSON.stringify([{ code: codes[0], name: "テスト", entry: 1000, date: "2026-09-01", shares: 100 }]);
hv = go("#/momentum"); check("持ち株コーチ描画", clean(hv) && hv.includes("持ち株コーチ"));

console.log("── 4) 不在チェック ──");
check("👑極みタブ導線なし", !html.includes('id="nav-kiwami"') && !html.includes('href="#/kiwami"'));
check("旧ファイル残置", fs.existsSync("web/legacy_v4.html"));
console.log(fail ? `\n${fail} 件 NG` : "\nALL GREEN");
process.exit(fail ? 1 : 0);
