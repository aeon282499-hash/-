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
  vm.runInContext(code + "\n;globalThis.__api={apply,render,toggleCheck,toggleEnded,toggleOpen,setLive:(j)=>{LIVE=j},setErr:(e)=>{LIVE_ERR=e},setO5:(j,e)=>{O5=j;O5_ERR=e||''}};", ctx);
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
const H = ["#/", "#/check", "#/paper", "#/oshime5"];
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
bad += run("上ヒゲ 高い株価（0株）", demo, ["#/oshime5"], Object.assign({}, o5, { rows: [Object.assign({}, o5.rows[0], { close: 9800, atr_pct: null })] }));
for (const [p, j] of extra) bad += run(p.split(/[\/]/).pop(), j, H);
bad += run("データなし（hub 未接続）", "ERR", H, null);
bad += run("旧ルールのfibo", { ts: "2026-09-30 10:00:00", hhmm: "10:00", state: "am", fibo: { candidates: [], trades: [] } }, H);
bad += run("fiboなし", { ts: "2026-09-30 10:00:00", hhmm: "10:00", state: "am" }, H);
const stopped = JSON.parse(JSON.stringify(demo)); stopped.fibo.paper.stopped = true; stopped.fibo.paper.stop_reason = "2連敗で停止";
bad += run("停止中", stopped, H);
console.log(bad ? `\nNG ${bad}件` : "\nすべてOK");
process.exit(bad ? 1 : 0);
