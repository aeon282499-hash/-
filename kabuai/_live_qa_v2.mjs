// 公開サイトのライブQA（v2）。実本番データで全viewをDOMシム実行して検証する。
// 実行: $env:NODE_OPTIONS="--use-system-ca"; node _live_qa_v2.mjs
import vm from "node:vm";

const BASE = "https://aeon282499-hash.github.io/-/";
const get = async (p, asJson = true) => {
  const r = await fetch(BASE + p, { cache: "no-store" });
  if (!r.ok) throw new Error(`${p}: HTTP ${r.status}`);
  return asJson ? r.json() : r.text();
};

const [html, DATA, SIDX, EXPJ] = await Promise.all([
  get("web/index.html", false),
  get("data/latest.json"),
  get("data/search_index.json"),
  get("data/explorer.json").catch(() => null),
]);

let fail = 0;
const check = (n, c, x = "") => { if (c) { console.log(`  OK ${n}${x ? " — " + x : ""}`); } else { fail++; console.log(`  NG ${n}${x ? " — " + x : ""}`); } };
const clean = hv => !hv.includes("表示エラー") && !hv.includes("NaN") && !hv.includes("undefined");

// ── データ側 ──
check("schema=kabuai-phase14", DATA.schema === "kabuai-phase14", DATA.schema);
check("futuresメタあり", !!DATA.futures, JSON.stringify(DATA.futures));
const taggedMembers = [];
for (const g of Object.values((DATA.signals || {}).groups || {}))
  for (const m of (g.members || [])) if (m.futures_tag) taggedMembers.push(m);
check("シグナルmembersに連動タグ付与", taggedMembers.length > 0, `${taggedMembers.length}件 例: ${taggedMembers[0] ? taggedMembers[0].name + "=" + taggedMembers[0].futures_tag + " r" + taggedMembers[0].futures_corr : "-"}`);
const sidxTagged = SIDX.stocks.filter(r => r.futures_tag);
check("search_indexにも連動タグ", sidxTagged.length > 0, `${sidxTagged.length}件`);

// ── フロント実行 ──
const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
const appScript = scripts.find(s => s.includes("function render"));
check("v2スクリプト配信", !!appScript && appScript.includes("BUY_KEYS") && appScript.includes("ftagChip"));

const ctxProxy = new Proxy({}, { get: () => () => {}, set: () => true });
const store = {};
function mkEl(id){ return { _id:id,_html:"",style:{},clientWidth:360,parentElement:{clientWidth:360},
  set innerHTML(v){this._html=v;},get innerHTML(){return this._html;},
  textContent:"",classList:{toggle(){},add(){},remove(){}},focus(){},
  getContext:()=>ctxProxy,querySelector(){return mkEl("c");},querySelectorAll(){return [];} }; }
const $get = sel => store[sel] || (store[sel] = mkEl(sel));
const locationShim = { hash: "#/" };
const lsStore = {};
const sandbox = {
  document: { querySelector: $get, getElementById: id => $get("#" + id), addEventListener(){}, querySelectorAll: () => [], createElement: () => mkEl("new"), body: mkEl("body") },
  window: { addEventListener(){}, scrollTo(){}, location: locationShim, innerWidth: 390, devicePixelRatio: 1 },
  location: locationShim, console, navigator: {},
  localStorage: { getItem: k => (k in lsStore ? lsStore[k] : null), setItem: (k, v) => { lsStore[k] = String(v); }, removeItem: k => { delete lsStore[k]; } },
  fetch: async u => {
    const s = String(u);
    if (s.includes("search_index")) return { ok: true, json: async () => SIDX };
    if (s.includes("explorer")) return { ok: !!EXPJ, json: async () => EXPJ };
    const m = s.match(/stocks\/([^./]+)\.json/);
    if (m) { try { const j = await get(`data/stocks/${m[1]}.json`); return { ok: true, json: async () => j }; } catch (e) { return { ok: false }; } }
    return { ok: true, json: async () => DATA };
  },
  setTimeout: () => 0, clearTimeout(){}, requestAnimationFrame: fn => fn(),
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
vm.runInContext(appScript, sandbox);
await sandbox.load();

locationShim.hash = "#/"; sandbox.render();
const hv = $get("#view").innerHTML;
check("home: エラー/NaN/undefinedなし", clean(hv));
check("home: ✅今日の買い候補＋出口規律", hv.includes("今日の買い候補") && hv.includes("損切り-12%"));
// v3(f50b688)で反発(反転/強反転)がBUY_KEYSに復活済み=旧「反転系なし」検査は廃止(2026-07-15同期)
const V3_KEYS = ["strong_reversal", "reversal", "strong_accum", "accum"];
const cands = sandbox.candidates().list;
check("買い候補（v3=反発+買い集め系のみ）", cands.every(r => (r.signals || []).every(k => V3_KEYS.includes(k))), `${cands.length}銘柄`);
if (cands.some(r => r.futures_tag)) {
  const evOf = r => (sandbox.buyStat(r) || { ev: -1e9 }).ev;
  const evs = cands.map(evOf);
  check("sort: 期待値の高い順（勝てる順）が最優先", evs.every((v, i) => i === 0 || v <= evs[i - 1]),
    `ev=${evs.slice(0, 6).join(",")}`);
  let tagOk = true;
  for (let i = 1; i < cands.length; i++)
    if (evOf(cands[i]) === evOf(cands[i - 1]) && sandbox.ftagRank(cands[i]) < sandbox.ftagRank(cands[i - 1])) tagOk = false;
  check("sort: 同率期待値内は自力優先", tagOk,
    cands.map(r => `${r.name}=${r.futures_tag || "?"}`).slice(0, 5).join(" / "));
  check("home: 連動タグchip表示", hv.includes('class="ftag'));
} else check("本日の候補に連動タグ（CIのyfinance成否確認）", false, "タグ付き候補が0件");

await sandbox.loadSearch();
locationShim.hash = "#/search"; sandbox.render();
check("search: 描画OK", clean($get("#view").innerHTML));
locationShim.hash = "#/about"; sandbox.render();
const av = $get("#view").innerHTML;
check("about: 連動タグ説明＋免責", clean(av) && av.includes("日経連動タグ") && av.includes("免責"));

if (cands[0]) {
  try {
    const s = await get(`data/stocks/${cands[0].code}.json`);
    const dv = sandbox.renderDetail(s);
    check("detail: 買い候補の詳細OK", clean(dv) && dv.includes("買いの目安"), `${cands[0].name} futures=${JSON.stringify(s.futures)}`);
  } catch (e) { check("detail: 個別JSON取得", false, e.message); }
}

// ── 🧭 銘柄探検（ライブ） ──
check("explorer.json 配信", !!EXPJ && EXPJ.schema === "kabuai-explorer-1", EXPJ ? JSON.stringify(EXPJ.counts) : "なし");
if (EXPJ) {
  await sandbox.loadExplorer();
  locationShim.hash = "#/explore"; sandbox.render();
  let ev = $get("#view").innerHTML;
  check("explore: カテゴリ画面OK", clean(ev) && ev.includes("銘柄探検") && ev.includes("ストップ高"));
  for (const cat of ["stop_high", "shodo", "rising", "oshime", "rebound"]) {
    locationShim.hash = `#/explore/${cat}`; sandbox.render();
    check(`explore/${cat}: 描画OK(${(EXPJ.counts || {})[cat] || 0}件)`, clean($get("#view").innerHTML));
  }
  // 上昇ランキングはv3(f50b688)で撤去済み=setRankWin検査は廃止(2026-07-15更新)
  check("explorer: S高履歴(日付付き)", (EXPJ.stop_high_history || []).length > 0,
    `${(EXPJ.stop_high_history || []).slice(0, 3).map(h => h.date + ":" + h.count + "件").join(" / ")}`);
}

// ── 🔥テーマ撤去の確認(2026-07-15) ──
check("theme撤去: ライブHTMLにnav-themeなし", !html.includes('id="nav-theme"'));
locationShim.hash = "#/theme"; sandbox.render();
check("theme撤去: #/themeはホームへフォールバック",
  clean($get("#view").innerHTML) && $get("#view").innerHTML.includes("今日の買い候補"));

console.log(fail ? `\nRESULT: ${fail} FAILURE(S)` : "\nRESULT: ALL GREEN (live)");
process.exit(fail ? 1 : 0);
