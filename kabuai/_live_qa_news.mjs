// v8.6 📢開示（2026-10-22）の DOMシム検証。feedback_kabuai_spa_verify: node vm + DOMシムで実データ（data/ir_digest.json）に対して
// viewNews を描画し、表示エラー/NaN/undefined ゼロ・数字の行が出る・絞り込みが効くことを確認してから push。  実行: node _live_qa_news.mjs
import fs from "node:fs";
import vm from "node:vm";

const html = fs.readFileSync("web/index.html", "utf8");
const DD = process.env.KABUAI_DATA || "data";
const DATA = JSON.parse(fs.readFileSync(DD + "/latest.json", "utf8"));
const DIGEST = JSON.parse(fs.readFileSync(DD + "/ir_digest.json", "utf8"));

const srcs = [...html.matchAll(/<script src="([^"?]+)(?:\?[^"]*)?"><\/script>/g)].map(m => m[1]);
const code = srcs.map(s => fs.readFileSync("web/" + s, "utf8")).join("\n;\n");
if (!html.includes("v=8.6.0")) { console.log("NG index.html の ?v= が 8.6.0 ではない"); process.exit(1); }
if (!srcs.includes("js/news.js")) { console.log("NG index.html に news.js が無い"); process.exit(1); }

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
let digestOk = true;
const sandbox = { document: documentShim, window: windowShim, location: locationShim, localStorage: localStorageShim,
  console, navigator: {}, history: { back() {} }, Date,
  fetch: async (u) => {
    const s = String(u);
    if (s.includes("ir_digest")) return { ok: digestOk, status: digestOk ? 200 : 404, json: async () => DIGEST };
    if (s.includes("search_index")) return { ok: true, status: 200, json: async () => JSON.parse(fs.readFileSync(DD + "/search_index.json", "utf8")) };
    if (s.includes("explorer")) return { ok: true, status: 200, json: async () => ({}) };   // 404 にすると explore.js の expReload が render→fetch を繰り返す（本番は explorer.json がある）
    if (s.includes("biz/")) return { ok: false, status: 404, json: async () => null };
    return { ok: true, status: 200, json: async () => DATA };
  },
  setTimeout: (fn) => 0, clearTimeout() {}, setInterval: () => 0, requestAnimationFrame: fn => fn() };
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
vm.runInContext(code, sandbox);
const ev = expr => vm.runInContext(expr, sandbox);
const tick = (ms = 60) => new Promise(r => setTimeout(r, ms));
await tick();

let fail = 0;
const check = (n, c, x = "") => { if (c) console.log(`  OK ${n}${x ? " — " + x : ""}`); else { fail++; console.log(`  NG ${n}${x ? " — " + x : ""}`); } };
const clean = hv => !hv.includes("表示エラー") && !hv.includes("NaN") && !hv.includes("undefined") && !hv.includes("[object");
const view = () => $get("#view").innerHTML;
const go = h => { locationShim.hash = h; sandbox.render(); return view(); };
const nav = () => $get("#nav-shodo");

console.log("── 0) 起動 ──");
check("VERSION 8.6.0", ev("CFG.VERSION") === "8.6.0", ev("CFG.VERSION"));
check("ROUTES 16本（#/news を足した）", ev("ROUTES.length") === 16, `${ev("ROUTES.length")}本`);
check("ir_digest.json に日付がある", DIGEST.days && DIGEST.days.length >= 1, `${DIGEST.days.length}日・${DIGEST.days.map(d => d.date + ":" + d.rows.length).join(" ")}`);

console.log("── 1) 📢開示の画面 ──");
let hv = go("#/news");
check("初回: 読み込み中（落ちない）", clean(hv) && hv.includes("📢 開示") && (hv.includes("読み込み中") || hv.includes("irrow")));
await tick(); await tick();
hv = go("#/news");
const nRows = (hv.match(/class="irrow"/g) || []).length;
const total = DIGEST.days.reduce((a, d) => a + d.rows.length, 0);
check("数字の行が全部出る", clean(hv) && nRows === total, `${nRows}行（json ${total}行）`);
check("切替に 📢開示 が付いた・🚩初動の並び", hv.includes("#/news") && hv.includes("#/explore") && hv.includes("#/flow/hot"));
check("上方修正の数字（営業利益 a→b %）が本文に出る", /営業利益 [\d,.]+億→[\d,.]+億 [+-]?[\d.]+%/.test(hv));
check("TOB の価格が出る", hv.includes("TOB価格"));
check("表題は PDF へのリンク", /href="https:\/\/www\.release\.tdnet\.info\/inbs\/\d+\.pdf"/.test(hv));
check("バナー・注釈が無い（vnote 無し）", !hv.includes("vnote") && !hv.includes("検証の数字"));
ev('newsSetF("up")'); hv = view();
const nUp = (hv.match(/class="irrow"/g) || []).length, jUp = DIGEST.days.reduce((a, d) => a + d.rows.filter(r => r.dir > 0).length, 0);
check("↑だけに絞れる", clean(hv) && nUp === jUp, `${nUp}行（json ${jUp}）`);
ev('newsSetF("業績修正")'); hv = view();
const nK = (hv.match(/class="irrow"/g) || []).length, jK = DIGEST.days.reduce((a, d) => a + d.rows.filter(r => r.label === "業績修正").length, 0);
check("種類で絞れる（業績修正）", clean(hv) && nK === jK, `${nK}行（json ${jK}）`);
ev('newsSetF("all")');
hv = go("#/"); check("🐵ホームに 📢 今日の開示 の1行", clean(hv) && hv.includes('href="#/news"'));
hv = go("#/explore"); await tick(); await tick(); hv = go("#/explore");
check("🚩初動の切替にも 📢開示（探検データが無い環境は読み込み表示で可）", clean(hv) && (hv.includes("#/news") || hv.includes("読み込")), hv.includes("#/news") ? "切替あり" : "探検データ未取得");

console.log("── 2) ファイルが無い時 ──");
ev("NEWS = null; NEWS_PENDING = false;"); digestOk = false;
hv = go("#/news"); await tick(); await tick(); hv = go("#/news");
check("404 → 『まだありません』・落ちない", clean(hv) && hv.includes("まだありません"));

console.log(fail ? `\nNG ${fail}件` : "\nALL OK");
process.exit(fail ? 1 : 0);
