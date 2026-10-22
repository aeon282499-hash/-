// v8.5 📄事業内容で探す（2026-10-22）の DOMシム検証。feedback_kabuai_spa_verify: node vm + DOMシムで実データ（biz/ の索引）に対して
// searchResults を実行し、表示エラー/NaN/undefined ゼロ・PHP→5250 が出ることを確認してから push。  実行: node _live_qa_biz.mjs
import fs from "node:fs";
import vm from "node:vm";

const html = fs.readFileSync("web/index.html", "utf8");
const DD = process.env.KABUAI_DATA || "data";
const DATA = JSON.parse(fs.readFileSync(DD + "/latest.json", "utf8"));
const SIDX = JSON.parse(fs.readFileSync(DD + "/search_index.json", "utf8"));
const bizJson = k => { try { return JSON.parse(fs.readFileSync(`biz/${k}.json`, "utf8")); } catch (e) { return null; } };

const srcs = [...html.matchAll(/<script src="([^"?]+)(?:\?[^"]*)?"><\/script>/g)].map(m => m[1]);
const code = srcs.map(s => fs.readFileSync("web/" + s, "utf8")).join("\n;\n");
if (!html.includes("v=8.5.0")) { console.log("NG index.html の ?v= が 8.5.0 ではない"); process.exit(1); }

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
const fetched = [];
const sandbox = { document: documentShim, window: windowShim, location: locationShim, localStorage: localStorageShim,
  console, navigator: {}, history: { back() {} }, Date,
  fetch: async (u) => {
    const s = String(u); fetched.push(s);
    if (s.includes("search_index")) return { ok: true, status: 200, json: async () => SIDX };
    let m = s.match(/biz\/index\.json/);
    if (m) { const j = bizJson("index"); return { ok: !!j, status: j ? 200 : 404, json: async () => j }; }
    m = s.match(/biz\/([0-9A-Z])\.json/);
    if (m) { const j = bizJson(m[1]); return { ok: !!j, status: j ? 200 : 404, json: async () => j }; }
    if (s.includes("explorer")) return { ok: true, status: 200, json: async () => ({}) };
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
const codesOf = hv => [...hv.matchAll(/href="#\/detail\/([0-9A-Z]{4})"/g)].map(m => m[1]);

console.log("── 0) 起動 ──");
check("DATAロード", !!ev("DATA && DATA.data_date"), ev("DATA && DATA.data_date"));
check("VERSION 8.5.0", ev("CFG.VERSION") === "8.5.0", ev("CFG.VERSION"));
check("ROUTES 15本（変えていない）", ev("ROUTES.length") === 15, `${ev("ROUTES.length")}本`);

console.log("── 1) 🔍銘柄検索の画面 ──");
let hv = go("#/search");
check("検索箱に『例 PHP』", clean(hv) && hv.includes("事業内容の言葉（例 PHP）"));
await ev("loadSearch()"); await tick();
check("索引ロード", ev("SEARCH && SEARCH.length") > 3000, `${ev("SEARCH && SEARCH.length")}銘柄`);
hv = go("#/"); check("🐵ホームの検索カードの文言", clean(hv) && hv.includes("事業内容（例 PHP）で検索"));

console.log("── 2) 📄事業内容で探す ──");
const idx = bizJson("index");
check("biz/index.json がある", !!idx && idx.n_text > 3500, idx ? `本文あり ${idx.n_text}/${idx.n}社・${idx.updated}` : "無い");
ev('SEARCH_Q = "PHP"'); hv = ev('searchResults("PHP")');
check("初回: 読み込み中の見出し（事業内容に「PHP」）", clean(hv) && hv.includes("事業内容に「PHP」") && (hv.includes("読み込み中") || hv.includes("0社")));
for (let i = 0; i < 20 && Object.keys(ev("BIZ_PENDING")).length; i++) await tick(50);
await tick();
check("10分割を全部読んだ", Object.keys(ev("BIZ_ROWS")).length === idx.shards.filter(s => s.n > 0).length && !Object.keys(ev("BIZ_PENDING")).length, `${Object.keys(ev("BIZ_ROWS")).length}本・fetch ${fetched.filter(u => u.includes("biz/")).length}回`);
hv = ev('searchResults("PHP")');
let cs = codesOf(hv);
check("PHP → 5250 GMOプライム・ストラテジーが出る", clean(hv) && cs.includes("5250"), `${cs.length}社: ${cs.slice(0, 12).join(" ")}`);
check("PHP → 一致箇所に <mark>php</mark>・社数の見出し・有報の日付", hv.includes("<mark>php</mark>") && /事業内容に「PHP」 \d+社/.test(hv) && hv.includes("有報 "));
check("PHP → 値段と前日比の行", /¥[\d,]+ <span style="color:/.test(hv));
hv = ev('searchResults("WAF")'); cs = codesOf(hv);
check("WAF → 4493 サイバーセキュリティクラウド", clean(hv) && cs.includes("4493"), `${cs.length}社: ${cs.slice(0, 10).join(" ")}`);
hv = ev('searchResults("量子暗号")'); cs = codesOf(hv);
check("量子暗号 → 1社以上", clean(hv) && cs.length >= 1, `${cs.length}社: ${cs.slice(0, 10).join(" ")}`);
hv = ev('searchResults("耐量子")'); cs = codesOf(hv);
check("耐量子 → 4498 サイバートラスト・3858 ユビキタスAI", clean(hv) && cs.includes("4498") && cs.includes("3858"), `${cs.length}社: ${cs.join(" ")}`);
hv = ev('searchResults("PHP WordPress")'); cs = codesOf(hv);
check("AND 検索 PHP WordPress → 5250", clean(hv) && cs.includes("5250"), `${cs.length}社`);
hv = ev('searchResults("ｐｈｐ")'); cs = codesOf(hv);
check("全角 ｐｈｐ でも同じ（NFKC）", cs.includes("5250"));
hv = ev('searchResults("zzqxv9")');
check("無い語 → 該当なし（名前にも事業内容にも無い）", clean(hv) && hv.includes("該当なし") && hv.includes(" 0社"));
hv = ev('searchResults("トヨタ")'); cs = codesOf(hv);
check("社名検索は従来どおり＋事業内容の節も付く", clean(hv) && cs.includes("7203") && hv.includes("事業内容に「トヨタ」"));
hv = ev('searchResults("7")');
check("1文字は事業内容を照合しない", clean(hv) && !hv.includes("事業内容に「"));
hv = ev('searchResults("半導体")'); cs = codesOf(hv);
check("半導体 → 80社で打ち切り＋『ほか N社』", clean(hv) && hv.includes("ほか ") && cs.length <= 80 + 60, `${cs.length}社表示`);

console.log("── 3) 索引が無い時 ──");
ev("BIZ_IDX = false; for (const k in BIZ_ROWS) delete BIZ_ROWS[k];");
hv = ev('searchResults("PHP")');
check("索引なし → 『索引なし』の見出しと空メッセージ・落ちない", clean(hv) && hv.includes("索引なし") && hv.includes("索引がありません"));

console.log(fail ? `\nNG ${fail}件` : "\nALL OK");
process.exit(fail ? 1 : 0);
