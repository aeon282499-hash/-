// v6（デイトレ専用）の DOMシム検証: web/js/day.js を node vm で動かし、3画面 × いくつかのデータ状態で
// 表示エラー/undefined/NaN が出ないことを確かめる。  実行: node _harness_v6.mjs [追加のpayload.json ...]
import fs from "node:fs";
import vm from "node:vm";

const code = fs.readFileSync("web/js/day.js", "utf8");
const demo = JSON.parse(fs.readFileSync("web/demo_fibo.json", "utf8"));
const extra = process.argv.slice(2).map(p => [p, JSON.parse(fs.readFileSync(p, "utf8"))]);

function run(label, payload, hashes) {
  const store = {};
  const mk = id => ({ _id: id, _h: "", set innerHTML(v) { this._h = v; }, get innerHTML() { return this._h; }, classList: { toggle() {} }, setAttribute() {} });
  const get = s => store[s] || (store[s] = mk(s));
  const loc = { hash: "#/", search: "" };
  const ctx = {
    document: { querySelector: get, getElementById: id => get("#" + id), documentElement: mk("html") },
    location: loc, localStorage: { getItem: () => null, setItem() {} }, console, Date, Math, JSON, String, Number, Set, Array, Object, isNaN,
  };
  vm.createContext(ctx);
  vm.runInContext(code + "\n;globalThis.__api={apply,render,toggleCheck,toggleEnded,toggleOpen,setLive:(j)=>{LIVE=j},setErr:(e)=>{LIVE_ERR=e}};", ctx);
  const api = ctx.__api;
  if (payload === "ERR") api.setErr("配信につながりません"); else if (payload) api.apply(payload);
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
const H = ["#/", "#/check", "#/paper"];
let bad = 0;
bad += run("demo 9/30 9:40", demo, H);
for (const [p, j] of extra) bad += run(p.split(/[\/]/).pop(), j, H);
bad += run("データなし（hub 未接続）", "ERR", H);
bad += run("旧ルールのfibo", { ts: "2026-09-30 10:00:00", hhmm: "10:00", state: "am", fibo: { candidates: [], trades: [] } }, H);
bad += run("fiboなし", { ts: "2026-09-30 10:00:00", hhmm: "10:00", state: "am" }, H);
const stopped = JSON.parse(JSON.stringify(demo)); stopped.fibo.paper.stopped = true; stopped.fibo.paper.stop_reason = "2連敗で停止";
bad += run("停止中", stopped, H);
console.log(bad ? `\nNG ${bad}件` : "\nすべてOK");
process.exit(bad ? 1 : 0);
