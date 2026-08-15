// 🎓デイトレ検定タブの描画検証（feedback_kabuai_spa_verify: WebFetchでは検出不能＝vm+DOMシムで実行してからpush）。
// メニュー→クイック開始→正解/不正解→完走→結果、シャッフル後の正解マッピング整合まで踏む。
import fs from "node:fs";
import vm from "node:vm";

const html = fs.readFileSync("web/index.html", "utf8");
const LATEST = JSON.parse(fs.readFileSync("data/latest.json", "utf8"));
const QUIZRAW = JSON.parse(fs.readFileSync("web/quiz_daytrade.json", "utf8"));
const CHARTS = JSON.parse(fs.readFileSync("web/quiz_charts.json", "utf8"));
const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
const appScript = scripts.find(s => s.includes("function render"));
if (!appScript) { console.error("FAIL: app script not found"); process.exit(1); }

let PASS = 0, FAIL = 0;
const ok = (label, cond) => { if (cond) { PASS++; console.log("  ok  " + label); } else { FAIL++; console.log("  NG  " + label); } };

const store = {};
const lsData = {};
const mkEl = id => ({ _id: id, _html: "", set innerHTML(v){ this._html = v; }, get innerHTML(){ return this._html; },
  textContent: "", clientWidth: 340, classList: { toggle(){}, add(){}, remove(){} }, querySelector(){ return mkEl("c"); },
  getContext(){ return { clearRect(){}, beginPath(){}, moveTo(){}, lineTo(){}, stroke(){}, fillRect(){}, fillText(){} }; } });
const $get = sel => store[sel] || (store[sel] = mkEl(sel));
const locationShim = { hash: "#/quiz" };
const sandbox = {
  document: { querySelector: $get, getElementById: id => $get("#"+id), addEventListener(){}, createElement: () => mkEl("n"), body: mkEl("body") },
  window: { addEventListener(){}, scrollTo(){}, location: locationShim, innerWidth: 390, matchMedia: () => ({ matches:false, addEventListener(){} }) },
  location: locationShim,
  localStorage: { getItem: k => (k in lsData ? lsData[k] : null), setItem(k,v){ lsData[k]=String(v); }, removeItem(k){ delete lsData[k]; } },
  Chart: function(){ return {}; }, console,
  fetch: async u => ({ ok: true, json: async () => (String(u).includes("quiz_charts") ? CHARTS
    : String(u).includes("quiz_daytrade") ? QUIZRAW : LATEST) }),
  setTimeout, clearTimeout, requestAnimationFrame: fn => fn(),
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
vm.runInContext(appScript, sandbox);

await sandbox.load();
const view = () => $get("#view").innerHTML;
// vm内の let 変数(QZ等)は sandbox のプロパティにならない＝式評価で読む
const QZ = () => vm.runInContext("QZ", sandbox);

console.log("▶ menu（チャート演習のみ＝知識100問は2026-08-15導線撤去）");
sandbox.render();
ok("メニュー=チャート演習", view().includes("チャート演習") && view().includes("10問やる"));
ok("知識100問の導線が無い", !view().includes("10問クイック") && !view().includes("全100問"));
ok("学習専用の注意書き", view().includes("学習専用"));
ok("表示エラーなし", !view().includes("表示エラー"));

console.log("▶ 残置の知識クイズ関数（復活用に生きているか）");
await sandbox.loadQuiz();               // UIからは呼ばれない＝ハーネスが直接ロード
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

console.log("▶ チャート演習");
locationShim.hash = "#/quiz"; sandbox.render();
ok("メニューに開始ボタン", view().includes("quizChartStart"));
const QZCS = () => vm.runInContext("QZCS", sandbox);
sandbox.quizChartStart();                    // 初回は遅延ロード→自動で再スタート
await new Promise(r => setTimeout(r, 30));
ok("セッション開始 10問", QZCS() && QZCS().qs.length === 10);
sandbox.render();
ok("run画面: 設問と選択肢", view().includes("明日の寄り、どうする？") && view().includes("空売り→引け買戻し"));
await new Promise(r => setTimeout(r, 10));   // setTimeout(qzcDraw)を流して例外が出ないこと
let cc = QZCS().qs[0];
const correct0 = cc.sys === "SELL" ? 1 : 2;
sandbox.quizChartAnswer(correct0);
ok("プロセス一致で正解", view().includes("⭕ 正解") && view().includes("翌日の現実"));
ok("正体の開示", view().includes("正体:") && view().includes(cc.reveal.code));
sandbox.quizChartNext();
sandbox.quizChartAnswer(0);                  // 買いは常に不正解
ok("買いは常に不正解＋PF0.60の解説", view().includes("❌ 不正解") && view().includes("PF0.60"));
for (let i = 2; i <= 9; i++) { sandbox.quizChartNext(); sandbox.quizChartAnswer(QZCS().qs[i].sys === "SELL" ? 1 : 2); }
sandbox.quizChartNext();
ok("チャート演習の結果画面", view().includes("チャート演習 結果"));
ok("チャート成績は別枠(dtquizc)", JSON.parse(lsData["dtquizc"]).n === 10);
sandbox.quizExit(); sandbox.render();
ok("メニューに通算成績", view().includes("通算成績") && view().includes("回答 10問"));

console.log("▶ 生成データの健全性");
ok("ケース数≥90", CHARTS.cases.length >= 90);
ok("全ケース30本の足", CHARTS.cases.every(c => c.bars.length === 30));
ok("GO/NOGO両方いる", CHARTS.cases.some(c => c.sys === "SELL") && CHARTS.cases.some(c => c.sys === "PASS"));
ok("最終足の終値と前日比が整合", CHARTS.cases.every(c => {
  const cl = c.bars[29][3], pv = c.bars[28][3];
  return Math.abs((cl - pv) / pv * 100 - c.meta.gain) < 1.5;
}));
ok("SELLケースは全フィルタ通過", CHARTS.cases.filter(c => c.sys === "SELL").every(c =>
  c.meta.gain >= 7 && c.meta.atr >= 5 && c.meta.dev >= 12 && c.meta.vr < 6 && c.meta.rng > 5));

console.log("▶ 他タブが壊れていないか");
for (const h of ["#/", "#/sell", "#/daytrade", "#/explore", "#/about"]) {
  locationShim.hash = h; sandbox.render();
  ok(h + " 表示エラーなし", !view().includes("表示エラー"));
}

console.log(`\n==== ${PASS} PASS / ${FAIL} FAIL ====`);
process.exit(FAIL ? 1 : 0);
