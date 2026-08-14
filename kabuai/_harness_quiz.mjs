// 🎓デイトレ検定タブの描画検証（feedback_kabuai_spa_verify: WebFetchでは検出不能＝vm+DOMシムで実行してからpush）。
// メニュー→クイック開始→正解/不正解→完走→結果、シャッフル後の正解マッピング整合まで踏む。
import fs from "node:fs";
import vm from "node:vm";

const html = fs.readFileSync("web/index.html", "utf8");
const LATEST = JSON.parse(fs.readFileSync("data/latest.json", "utf8"));
const QUIZRAW = JSON.parse(fs.readFileSync("data/quiz_daytrade.json", "utf8"));
const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
const appScript = scripts.find(s => s.includes("function render"));
if (!appScript) { console.error("FAIL: app script not found"); process.exit(1); }

let PASS = 0, FAIL = 0;
const ok = (label, cond) => { if (cond) { PASS++; console.log("  ok  " + label); } else { FAIL++; console.log("  NG  " + label); } };

const store = {};
const lsData = {};
const mkEl = id => ({ _id: id, _html: "", set innerHTML(v){ this._html = v; }, get innerHTML(){ return this._html; },
  textContent: "", classList: { toggle(){}, add(){}, remove(){} }, querySelector(){ return mkEl("c"); } });
const $get = sel => store[sel] || (store[sel] = mkEl(sel));
const locationShim = { hash: "#/quiz" };
const sandbox = {
  document: { querySelector: $get, getElementById: id => $get("#"+id), addEventListener(){}, createElement: () => mkEl("n"), body: mkEl("body") },
  window: { addEventListener(){}, scrollTo(){}, location: locationShim, innerWidth: 390, matchMedia: () => ({ matches:false, addEventListener(){} }) },
  location: locationShim,
  localStorage: { getItem: k => (k in lsData ? lsData[k] : null), setItem(k,v){ lsData[k]=String(v); }, removeItem(k){ delete lsData[k]; } },
  Chart: function(){ return {}; }, console,
  fetch: async u => ({ ok: true, json: async () => (String(u).includes("quiz_daytrade") ? QUIZRAW : LATEST) }),
  setTimeout, clearTimeout, requestAnimationFrame: fn => fn(),
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
vm.runInContext(appScript, sandbox);

await sandbox.load();
const view = () => $get("#view").innerHTML;
// vm内の let 変数(QZ等)は sandbox のプロパティにならない＝式評価で読む
const QZ = () => vm.runInContext("QZ", sandbox);

console.log("▶ menu");
sandbox.render();                       // QUIZ未ロード=読み込み中→quizReloadが走る
await new Promise(r => setTimeout(r, 20));   // 遅延fetchのマイクロタスクを全部流す
sandbox.render();
ok("メニューが出る", view().includes("デイトレ検定100問"));
ok("クイックボタン", view().includes("10問クイック"));
ok("カテゴリchips", view().includes("機械") && view().includes("検証"));
ok("学習専用の注意書き", view().includes("学習専用"));
ok("表示エラーなし", !view().includes("表示エラー"));

console.log("▶ shuffle整合");
sandbox.quizStart("all");
const byId = Object.fromEntries(QUIZRAW.questions.map(q => [q.id, q]));
const bad = QZ().qs.filter(q => q.c[q.a] !== byId[q.id].c[byId[q.id].a]);
ok("全100問でシャッフル後も正解テキストが一致", QZ().qs.length === 100 && bad.length === 0);
const pos = new Set(QZ().qs.map(q => q.a));
ok("正解位置がばらける(3位置以上)", pos.size >= 3);
sandbox.quizExit();

console.log("▶ quick 1問目 正解→不正解→完走");
sandbox.quizStart("quick");
ok("run画面 1/10", view().includes("1 / 10問"));
let q = QZ().qs[0];
sandbox.quizAnswer(q.a);
ok("正解表示と解説", view().includes("⭕ 正解") && view().includes(q.why.slice(0, 10)));
sandbox.quizNext();
q = QZ().qs[1];
sandbox.quizAnswer((q.a + 1) % 4);
ok("不正解表示", view().includes("❌ 不正解"));
ok("二度押し無効", (sandbox.quizAnswer(q.a), QZ().picked === (q.a + 1) % 4));
for (let i = 2; i <= 9; i++) { sandbox.quizNext(); sandbox.quizAnswer(QZ().qs[i].a); }
sandbox.quizNext();
ok("結果画面", view().includes("🎓 結果") && view().includes("/ 10"));
ok("成績がlocalStorageに載る", JSON.parse(lsData["dtquiz"]).n === 10);
sandbox.quizExit(); sandbox.render();
ok("メニューに通算成績", view().includes("通算成績") && view().includes("回答 10問"));

console.log("▶ 他タブが壊れていないか");
for (const h of ["#/", "#/sell", "#/daytrade", "#/explore", "#/about"]) {
  locationShim.hash = h; sandbox.render();
  ok(h + " 表示エラーなし", !view().includes("表示エラー"));
}

console.log(`\n==== ${PASS} PASS / ${FAIL} FAIL ====`);
process.exit(FAIL ? 1 : 0);
