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
await new Promise(r => setTimeout(r, 50));
const ev = expr => vm.runInContext(expr, sandbox);
const view = () => $get("#view").innerHTML;
const go = h => { locationShim.hash = h; sandbox.render(); return view(); };
const strip = h => h.replace(/<style[\s\S]*?<\/style>/g, "").replace(/<[^>]+>/g, " ").replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/\s+/g, " ").trim();
const flag = h => { const f = []; for (const k of ["undefined", "NaN", "null", "[object", "表示エラー", "Infinity"]) if (h.includes(k)) f.push(k); return f.length ? "  ⚠️FLAGS: " + f.join(",") : ""; };
const dump = (name, h, n = 2500) => { console.log(`\n████ ${name}${flag(h)}\n${strip(h).slice(0, n)}`); };
if (LIVEJ) vm.runInContext("liveApply(" + JSON.stringify(LIVEJ) + ")", sandbox);
dump("📋 朝の作戦", go("#/plan"), 3500);
dump("🎯 前夜の準備", go("#/arena"), 3000);
dump("🔻 売り", go("#/sell"), 2500);
dump("🐵 EOD", go("#/momentum"), 1500);
let hv = go("#/explore"); await new Promise(r => setTimeout(r, 20)); dump("🧭 探検", go("#/explore"), 1200);
hv = go("#/"); dump("🔥 ライブ(既定)", $get("#live-root").innerHTML, 2500);
sandbox.liveSetSeg("themes"); dump("🔥 テーマ", $get("#live-root").innerHTML, 1500);
sandbox.liveToggle("themes", ev("sortGroups(LIVE.themes)[0].key")); dump("🔥 テーマ1位を開く", $get("#live-root").innerHTML, 1800);
sandbox.liveSetSeg("stocks"); dump("🔥 個別", $get("#live-root").innerHTML, 1200);
go("#/detail/581A"); await sandbox.loadStockDetail("581A"); dump("詳細 GO", $get("#dbody").innerHTML, 1800);
dump("使い方", go("#/about"), 1200);
