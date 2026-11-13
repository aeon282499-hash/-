// uslink.js — 🇺🇸連動（v8.8・2026-11-12 本人「これをモメンタムチンパンアプリ追加するか」）
// 米国連動の寄り逆張り: 前夜の米国株(GLW/MU・時間外込み)から寄りを予想し、行きすぎの線に寄付指値。約定したら引成＋寄り値から5%の逆指値。
// データ: PC(us_link_publish.py)が 平日8:40〜8:58 3分おき＋9:25/11:35/15:45 に chimp-live ハブの live.json の "us_link" キーへ配信。
// 画面は数字の行だけ（feedback_kabuai_numbers_only）。v8.9: 下に順張り（紙）の段（USL.paper）。v8.9.1: 損切り%は配信値（USL.stop・11/13〜2%・1銘柄70万）。
let USL = null, USL_PENDING = false, USL_AT = 0;
function uslEnsure(force) {
  if (USL_PENDING) return;
  if (!force && USL !== null && Date.now() - USL_AT < 60000) return;
  USL_PENDING = true;
  fetch(CFG.LIVE_BASE + "/live.json", { cache: "no-store" })
    .then(r => r.ok ? r.json() : null)
    .then(j => { USL = (j && j.us_link && Array.isArray(j.us_link.rows)) ? j.us_link : false; })
    .catch(() => { USL = false; })
    .finally(() => { USL_PENDING = false; USL_AT = Date.now(); if ((location.hash || "").startsWith("#/uslink")) render(); });
}
function uslPl(v) {
  if (v == null) return "";
  const c = v > 0 ? "var(--up)" : v < 0 ? "var(--dn)" : "var(--mut)";
  return `<b style="color:${c}">${v > 0 ? "+" : ""}${Math.round(v).toLocaleString()}円</b>`;
}
function uslRow(r, pre, mom) {
  const res = pre || !r.status ? "" : r.status === "約定なし" ? `<small class="muted">寄り ${yen(r.open)}・約定なし</small>`
    : r.status === "寄り前" || r.status === "取得失敗" ? `<small class="muted">${esc(r.status)}</small>`
    : `<small>寄り ${yen(r.open)} ${esc(r.status)} ${r.qty}株 → ${yen(r.exit)}</small><br>${uslPl(r.pl)}`;
  return `<tr>
    <td><b>${esc(r.name)}</b><br><small class="muted">${esc(r.us)} ${fmtPct1(r.move)} → ${fmtPct1(r.pred)}</small></td>
    <td style="color:var(${mom ? "--up" : "--dn"})"><b>${yen(r.sell)}</b><br><small>${r.sell_qty}株</small></td>
    <td style="color:var(${mom ? "--dn" : "--up"})"><b>${yen(r.buy)}</b><br><small>${r.buy_qty}株</small></td>
    ${pre ? "" : `<td>${res}</td>`}
  </tr>`;
}
function viewUsLink() {
  uslEnsure(false);
  const head = (sub) => `<h2>🇺🇸 連動 <span class="sub">${sub || ""}</span> <a class="gchip" onclick="event.preventDefault();uslEnsure(true)">🔄</a></h2>`;
  if (!USL) return head("") + `<div class="card empty">${USL === false ? "まだありません（平日8:40から）" : "読み込み中…"}</div>`;
  const pre = USL.phase === "lines";
  const today = todayStr() === USL.date;
  const sub = `${esc(USL.date)}${today ? "" : "（今日の分ではない）"} ・ 線 ${esc(USL.lines_at)} ・ 更新 ${esc((USL.updated_at || "").slice(11))}`;
  const total = !pre && USL.total_pl != null ? `<div class="hh">合計 ${uslPl(USL.total_pl)} <span class="sub">${USL.phase === "close" ? "引け" : "途中・約20分遅れ"}</span></div>` : "";
  return head(sub) + total + `<div class="card tight"><table class="tbl calc-tbl num">
    <thead><tr><th>銘柄</th><th>空売り</th><th>買い</th>${pre ? "" : "<th>結果</th>"}</tr></thead>
    <tbody>${USL.rows.map(r => uslRow(r, pre)).join("")}</tbody></table></div>
    <p class="disc">寄付指値 ・ 約定したら引成＋寄り値から${USL.stop || 5}%の逆指値 ・ 1銘柄${Math.round((USL.budget || 1e6) / 1e4)}万円</p>` + uslPaper(pre);
}
// 順張り(紙・v8.9 2026-11-13 本人「両方やって」): 寄りが上の線以上→買い・下の線以下→空売り。発注しない記録だけ
function uslPaper(pre) {
  const P = Array.isArray(USL.paper) ? USL.paper : [];
  if (!P.length) return "";
  const tot = !pre && USL.paper_total != null ? ` ${uslPl(USL.paper_total)}` : "";
  return `<div class="hh">順張り（紙）${tot}</div><div class="card tight"><table class="tbl calc-tbl num">
    <thead><tr><th>銘柄</th><th>上で買い</th><th>下で空売り</th>${pre ? "" : "<th>結果</th>"}</tr></thead>
    <tbody>${P.map(r => uslRow(r, pre, true)).join("")}</tbody></table></div>`;
}
