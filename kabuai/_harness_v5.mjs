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
  check("ライブ受信後: テーマ一覧", clean(hv) && hv.includes("テーマ") && hv.includes("gcard"), `${(hv.match(/class="gcard/g) || []).length}カード`);
  check("ライブ: 市場KPI", hv.includes("市場全体") && hv.includes("kpi"));
  const key = LIVEJ.themes[0].key;
  sandbox.liveToggle("themes", key); hv = $get("#live-root").innerHTML;
  check("ライブ: テーマ展開で構成銘柄行", clean(hv) && hv.includes("srow"), `${(hv.match(/class="srow/g) || []).length}行`);
  sandbox.liveSetSort("chg_w"); hv = $get("#live-root").innerHTML; check("ライブ: 騰落率ソート", clean(hv));
  sandbox.liveSetSeg("sectors"); hv = $get("#live-root").innerHTML; check("ライブ: セクター", clean(hv) && hv.includes("業種"));
  sandbox.liveSetSeg("stocks"); hv = $get("#live-root").innerHTML; check("ライブ: 個別(今きてる)", clean(hv));
  for (const t of ["hot", "gain", "tovtop", "lose"]) { sandbox.liveSetTab(t); hv = $get("#live-root").innerHTML; check(`ライブ: 個別タブ ${t}`, clean(hv) && hv.includes("srow")); }
  check("ライブ: 時計", clean(sandbox.liveClockInner()) && sandbox.liveClockInner().includes(LIVEJ.hhmm));
  sandbox.liveSetSeg("themes");
} else console.log("  (live_flow/latest.json なし→ライブ受信後の検査はスキップ)");
hv = go("#/about"); check("使い方", clean(hv) && hv.includes("ライブの数字"));

console.log("── 2) 🎯土俵 / 🔻売り / 🐵EOD / 🧭探検 ──");
hv = go("#/arena"); check("土俵", clean(hv) && hv.includes("土俵"));
hv = go("#/sell"); check("売り", clean(hv) && hv.includes("フェード") && hv.includes("モメンタム終了"));
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
sandbox.toggleWatch(codes[0]); check("ウォッチ追加", sandbox.isWatched(codes[0]));
lsStore["kabuai_pos"] = JSON.stringify([{ code: codes[0], name: "テスト", entry: 1000, date: "2026-09-01", shares: 100 }]);
hv = go("#/momentum"); check("持ち株コーチ描画", clean(hv) && hv.includes("持ち株コーチ"));

console.log("── 4) 不在チェック ──");
check("👑極みタブ導線なし", !html.includes('id="nav-kiwami"') && !html.includes('href="#/kiwami"'));
check("旧ファイル残置", fs.existsSync("web/legacy_v4.html"));
console.log(fail ? `\n${fail} 件 NG` : "\nALL GREEN");
process.exit(fail ? 1 : 0);
