// v6（デイトレ専用）の DOMシム検証: web/js/day.js を node vm で動かし、4画面（✂️上ヒゲは O5_JSON=パス で実データ）× いくつかのデータ状態で
// 表示エラー/undefined/NaN が出ないことを確かめる。  実行: node _harness_v6.mjs [追加のpayload.json ...]
import fs from "node:fs";
import vm from "node:vm";

const code = fs.readFileSync("web/js/day.js", "utf8");
const demo = JSON.parse(fs.readFileSync("web/demo_fibo.json", "utf8"));
const extra = process.argv.slice(2).map(p => [p, JSON.parse(fs.readFileSync(p, "utf8"))]);

function run(label, payload, hashes, o5) {
  const store = {};
  const mk = id => ({ _id: id, _h: "", set innerHTML(v) { this._h = v; }, get innerHTML() { return this._h; }, classList: { toggle() {} }, setAttribute() {} });
  const get = s => store[s] || (store[s] = mk(s));
  const loc = { hash: "#/", search: "" };
  const ctx = {
    document: { querySelector: get, getElementById: id => get("#" + id), documentElement: mk("html") },
    location: loc, localStorage: { getItem: () => null, setItem() {} }, console, Date, Math, JSON, String, Number, Set, Array, Object, isNaN,
  };
  vm.createContext(ctx);
  vm.runInContext(code + "\n;globalThis.__api={apply,render,toggleCheck,toggleEnded,toggleOpen,setLive:(j)=>{LIVE=j},setErr:(e)=>{LIVE_ERR=e},setO5:(j,e)=>{O5=j;O5_ERR=e||''},calcSet,calcPick};", ctx);
  const api = ctx.__api;
  if (payload === "ERR") api.setErr("配信につながりません"); else if (payload) api.apply(payload);
  if (o5 !== undefined) api.setO5(o5, o5 ? "" : "まだ候補がありません");
  let bad = 0;
  for (const h of hashes) {
    loc.hash = h;
    if (h === "#/") { api.toggleEnded(); const c = (payload && payload.fibo && payload.fibo.candidates || [])[0]; if (c) api.toggleOpen(c.code); }
    if (h === "#/check") { for (let i = 0; i < 7; i++) api.toggleCheck(i); }
    api.render();
    const html = get("#view").innerHTML + get("#clock").innerHTML;
    const probs = ["undefined", "NaN", "表示エラー", "[object Object]"].filter(w => html.includes(w));
    console.log(`${probs.length ? "FAIL" : "ok  "} ${label.padEnd(26)} ${h.padEnd(8)} ${String(html.length).padStart(6)}字 ${probs.join(",")}`);
    if (probs.length) { bad++; const i = html.indexOf(probs[0]); console.log("   …" + html.slice(Math.max(0, i - 160), i + 60).replace(/\s+/g, " ")); }
  }
  return bad;
}
const H = ["#/", "#/check", "#/paper", "#/oshime5", "#/calc"];
let bad = 0;
// ✂️上ヒゲ刈り取り候補（oshime5.json）: 手元の 10/2 引けの出力があれば使う・無ければ最小の見本
const o5p = process.env.O5_JSON;
const o5 = o5p && fs.existsSync(o5p) ? JSON.parse(fs.readFileSync(o5p, "utf8"))
  : { date: "2026-10-02", target_date: "2026-10-05", rule: "見本", matched: 1, entry: "e", exit: "x", skip_rules: ["s"], backtest: "b", note: "n",
      rows: [{ code: "4502", name: "武田薬品工業", close: 5643, r5: -5.5, atr_pct: 1.9, tov20_oku: 254.2, tick: 1 }] };
const o5live = JSON.parse(JSON.stringify(demo)); o5live.stocks = { [o5.rows[0].code]: { last: o5.rows[0].close * 1.01, chg: 1.0, d5: 0.2 } };
bad += run("demo 9/30 9:40", demo, H, o5);
bad += run("上ヒゲ＋ライブの現在値", o5live, ["#/oshime5"], o5);
bad += run("上ヒゲ 該当なし", demo, ["#/oshime5"], Object.assign({}, o5, { rows: [] }));
// 場中ライン（oshime5_live.py の出力・O5L_JSON=パス で実データ・無ければ見本）
const o5lp = process.env.O5L_JSON;
const o5l = o5lp && fs.existsSync(o5lp) ? JSON.parse(fs.readFileSync(o5lp, "utf8"))
  : { ts: "2026-10-02 10:00:00", date: "2026-10-02", paper: { trades: 1, wins: 0, losses: 0, pnl_yen: 0, stopped: false, stop_reason: "" }, rules: { stop: "2連敗" },
      rows: [{ code: o5.rows[0].code, status: "ライン点灯", line: 100, line_bar: "09:50", tp: 101.5, sl: 97, shares: 600 }] };
const withLine = JSON.parse(JSON.stringify(demo)); withLine.oshime5 = o5l; withLine.oshime5.date = new Date().toISOString().slice(0, 10);
bad += run("上ヒゲ＋場中ライン", withLine, ["#/oshime5"], o5);
const o5stop = JSON.parse(JSON.stringify(withLine)); o5stop.oshime5.paper = { trades: 2, wins: 0, losses: 2, pnl_yen: -12000, stopped: true, stop_reason: "2連敗で本日終了" };
bad += run("上ヒゲ 2連敗で終了", o5stop, ["#/oshime5"], o5);
bad += run("上ヒゲ 高い株価（0株）", demo, ["#/oshime5"], Object.assign({}, o5, { rows: [Object.assign({}, o5.rows[0], { close: 9800, atr_pct: null })] }));
for (const [p, j] of extra) bad += run(p.split(/[\/]/).pop(), j, H);
bad += run("データなし（hub 未接続）", "ERR", H, null);
// 🧮電卓: 値段を入れた場合（TOPIX500/一般・株数の手入力・0株になる高い株価・候補タップ）
function runCalc(label, steps, payload, o5x) {
  const store = {}; const mk = id => ({ _h: "", set innerHTML(v) { this._h = v; }, get innerHTML() { return this._h; }, classList: { toggle() {} } });
  const get = s => store[s] || (store[s] = mk(s)); const loc = { hash: "#/calc", search: "" };
  const ctx = { document: { querySelector: get, getElementById: id => get("#" + id), documentElement: mk("html") }, location: loc,
    localStorage: { getItem: () => null, setItem() {} }, console, Date, Math, JSON, String, Number, Set, Array, Object, isNaN };
  vm.createContext(ctx);
  vm.runInContext(code + "\n;globalThis.__c={render,apply,calcSet,calcPick,setO5:(j)=>{O5=j},CALC};", ctx);
  const c = ctx.__c; if (payload) c.apply(payload); if (o5x) c.setO5(o5x); c.render();
  for (const [k, v] of steps) { if (k === "pick") c.calcPick(v); else c.calcSet(k, v); }
  c.render();
  const html = get("#view").innerHTML;
  const probs = ["undefined", "NaN", "表示エラー", "[object Object]"].filter(w => html.includes(w));
  const m = html.match(/利確（指値[^<]*<\/small><b>([^<]+)<\/b>.*?損切り（逆指値[^<]*<\/small><b>([^<]+)<\/b>.*?株数<\/small><b>([^<]+)<\/b>/s);
  console.log(`${probs.length ? "FAIL" : "ok  "} 電卓 ${label.padEnd(22)} ${m ? `利確${m[1]} 損切り${m[2]} 株数${m[3]}` : "（値段なし）"} ${probs.join(",")}`);
  return probs.length ? 1 : 0;
}
bad += runCalc("空", [], null, null);
bad += runCalc("3057円 TOPIX500", [["px", "3057"]], null, null);
bad += runCalc("2245円 TOPIX500", [["px", "2245"]], null, null);
bad += runCalc("3057円 一般", [["px", "3057"], ["t500", false]], null, null);
bad += runCalc("5643円 300株", [["px", "5643"], ["sh", "300"]], null, null);
bad += runCalc("9800円（自動0株）", [["px", "9800"]], null, null);
bad += runCalc("候補タップ（ライブ）", [["pick", o5.rows[0].code]], o5live, o5);
bad += run("旧ルールのfibo", { ts: "2026-09-30 10:00:00", hhmm: "10:00", state: "am", fibo: { candidates: [], trades: [] } }, H);
bad += run("fiboなし", { ts: "2026-09-30 10:00:00", hhmm: "10:00", state: "am" }, H);
const stopped = JSON.parse(JSON.stringify(demo)); stopped.fibo.paper.stopped = true; stopped.fibo.paper.stop_reason = "2連敗で停止";
bad += run("停止中", stopped, H);
console.log(bad ? `\nNG ${bad}件` : "\nすべてOK");
process.exit(bad ? 1 : 0);
