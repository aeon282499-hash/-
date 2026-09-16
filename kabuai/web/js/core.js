// core.js — 共通ユーティリティ・データ読込・テーマ・ルーター（v5）
const CFG = {
  VERSION: "5.1.0",
  LIVE_BASE: "https://chimp-live.aeon282499.workers.dev",   // 🔥ライブ配信ハブ（Cloudflare DO）
  LIVE_POLL_MS: 30000,                                        // WebSocketが切れている時のポーリング間隔
};
const fmtPct = v => (v == null || isNaN(v)) ? "—" : (v >= 0 ? "+" : "") + Number(v).toFixed(2) + "%";
const fmtPct1 = v => (v == null || isNaN(v)) ? "—" : (v >= 0 ? "+" : "") + Number(v).toFixed(1) + "%";
const cls = v => (v == null || isNaN(v)) ? "muted" : (v >= 0 ? "pos" : "neg");
const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[c]));
const $ = s => document.querySelector(s);
const yen = v => "¥" + Math.round(Number(v) || 0).toLocaleString();
// 円 → 億（1億未満は小数1桁・それ以上は整数）
const oku = v => { const o = (Number(v) || 0) / 1e8; return o >= 10 ? Math.round(o).toLocaleString() + "億" : o >= 1 ? o.toFixed(1) + "億" : (o * 100).toFixed(0) + "百万"; };
function pctTag(r1) {
  if (r1 == null || isNaN(r1)) return "";
  const c = r1 > 0 ? "var(--up)" : r1 < 0 ? "var(--dn)" : "var(--mut)", s = r1 > 0 ? "+" : "";
  return `<span style="color:${c};font-weight:700">${s}${Number(r1).toFixed(1)}%</span>`;
}
let DATA = null;

async function fetchFirst(cands) {
  for (const u of cands) {
    try { const r = await fetch(u, { cache: "no-store" }); if (r.ok) return await r.json(); } catch (e) { /* next */ }
  }
  return null;
}
async function load() {
  DATA = await fetchFirst(["../data/latest.json", "data/latest.json", "./data/latest.json"]);
  if (!DATA) throw new Error("latest.json が見つかりません（build_data.py を実行してください）");
  if (typeof computeNewCodes === "function") computeNewCodes();
}
function freshnessText() {
  const lag = DATA.data_lag_days;
  if (lag == null) return "終値";
  if (lag <= 0) return "当日終値";
  if (lag === 1) return "前営業日終値";
  return `${lag}日前の終値`;
}

// ── テーマ（dark 既定・localStorage: kabuai_theme = dark|light）──
function applyTheme() {
  let t = "dark";
  try { t = localStorage.getItem("kabuai_theme") || "dark"; } catch (e) { /* ignore */ }
  if (document.documentElement && document.documentElement.setAttribute) document.documentElement.setAttribute("data-theme", t);
  const m = document.querySelector('meta[name="theme-color"]'); if (m && m.setAttribute) m.setAttribute("content", t === "light" ? "#f3f5f8" : "#0a0e14");
}
function toggleTheme() {
  let t = "dark";
  try { t = localStorage.getItem("kabuai_theme") || "dark"; localStorage.setItem("kabuai_theme", t === "dark" ? "light" : "dark"); } catch (e) { /* ignore */ }
  applyTheme();
}

// ── ルーター ──
const ROUTES = [];   // {test:(hash)=>bool, view:(hash)=>html, nav:"live"}
function route(prefix, nav, fn) { ROUTES.push({ prefix, nav, fn }); }
function setNav(h) {
  const r = ROUTES.find(x => h.startsWith(x.prefix)) || ROUTES[ROUTES.length - 1];
  ["plan", "live", "arena", "data"].forEach(k => {
    const el = document.getElementById("nav-" + k); if (el && el.classList) el.classList.toggle("on", r.nav === k);
  });
}
function render() {
  const h = location.hash || "#/";
  setNav(h);
  window.scrollTo(0, 0);
  try {
    const r = ROUTES.find(x => h.startsWith(x.prefix)) || ROUTES[ROUTES.length - 1];
    $("#view").innerHTML = r.fn(h);
  } catch (e) {
    $("#view").innerHTML = `<div class="card"><b>表示エラー</b><div class="muted" style="font-size:12px;margin-top:6px">${esc(e.message)}</div></div>`;
  }
}

// ── v5.2.1 タブ整理（2026-09-17 本人「タブがわかりづらい」）: いつ見るかで4本 ──
//   📋朝の作戦(寄り前) / 🔥場中ライブ / 🎯前夜の準備(土俵+材料+明日の決算) / 📊データ(売り・EOD・探検はこの中でサブタブ)
function whenBar(icon, when, what) {
  return `<div class="banner info" style="margin-bottom:10px"><b>${icon} ${esc(when)}</b> … ${esc(what)}</div>`;
}
function dataSubNav(active) {
  const t = [["sell", "#/sell", "🔻 売り"], ["momentum", "#/momentum", "🐵 EODランキング"], ["explore", "#/explore", "🧭 探検"]];
  return `<div class="seg">${t.map(([k, h, l]) => `<a href="${h}" class="${k === active ? "on" : ""}">${l}</a>`).join("")}</div>`;
}
