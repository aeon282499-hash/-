// news.js — 📢開示（v8.6・2026-10-22 本人「これ毎日自動で読んでチンパンに出して」）
// data/ir_digest.json（ir_digest.py が TDnet の開示 PDF から抜いた数字）を、数字の行だけで出す。バナー・注釈は書かない（feedback_kabuai_numbers_only）。
// 行 = 会社・時刻・値段・前日比 ／ 種類と数字（上方修正なら 営業利益 前回→今回 増減率）／ 表題（PDF へ）／ 反応（既出=その日・引け後=翌日）
let NEWS = null, NEWS_PENDING = false, NEWS_F = "all";
function newsSetF(f) { NEWS_F = f; render(); }
function newsEnsure() {
  if (NEWS !== null || NEWS_PENDING) return;   // false（404）でも二度目を取りに行かない（取りに行くと render→fetch の無限ループ）
  NEWS_PENDING = true;
  fetchFirst(["../data/ir_digest.json", "data/ir_digest.json"]).then(j => { NEWS = (j && Array.isArray(j.days)) ? j : false; NEWS_PENDING = false; render(); });
}
function newsReact(r) {
  const x = r.react || {};
  if (r.intraday) {
    if (x.day_chg == null) return "";
    return `その日の終値 ${pctTag(x.day_chg)}${x.next_gap != null ? ` ／ 翌日 寄り ${pctTag(x.next_gap)}・終値 ${pctTag(x.next_chg)}` : ""}`;
  }
  if (x.next_gap == null) return "";
  return `翌日 ${esc(x.next_date || "")} 寄り ${pctTag(x.next_gap)}・終値 ${pctTag(x.next_chg)}`;
}
function newsRow(r) {
  const col = r.dir > 0 ? "var(--up)" : r.dir < 0 ? "var(--dn)" : "var(--mut)";
  const ar = r.dir > 0 ? "↑" : r.dir < 0 ? "↓" : "→";
  const react = newsReact(r);
  return `<div class="irrow">
    <div class="irhead"><a href="#/detail/${esc(r.code)}"><b>${esc(r.name)}</b></a> <small>${esc(r.code)} ・ ${esc(r.time)}${r.intraday ? "" : " 引け後"}</small>${r.close != null ? ` <span class="num">${yen(r.close)} ${pctTag(r.r1)}</span>` : ""}</div>
    <div style="font-size:15.5px;margin:2px 0;line-height:1.4"><span class="chip" style="color:${col};border-color:${col};padding:0 7px">${ar} ${esc(r.label)}</span> <b style="color:${col}">${esc(r.num || "")}</b>${r.sub ? ` <small class="muted">${esc(r.sub)}</small>` : ""}</div>
    <div class="irtitle"><small>${r.pdf ? `<a href="${esc(r.pdf)}" target="_blank" rel="noopener">${esc(r.title)} 📄</a>` : esc(r.title)}</small></div>
    ${react ? `<div class="num" style="font-size:14.5px">${react}</div>` : ""}
  </div>`;
}
function viewNews() {
  newsEnsure();
  const head = `<h2>📢 開示 <span class="sub">${NEWS && NEWS.generated_at ? esc(NEWS.generated_at) + " 取得" : ""}</span></h2>${flowSubNav("news")}`;
  if (!NEWS) return head + `<div class="card empty">${NEWS === false ? "まだありません（次のビルドで）" : "読み込み中…"}</div>`;
  const all = NEWS.days.flatMap(d => d.rows);
  const kinds = {}; all.forEach(r => { kinds[r.label] = (kinds[r.label] || 0) + 1; });
  const chip = (k, lbl) => `<a class="gchip ${NEWS_F === k ? "on" : ""}" onclick="event.preventDefault();newsSetF('${esc(k)}')">${lbl}</a>`;
  const chips = `<div class="gchips">${chip("all", `すべて ${all.length}`)}${chip("up", `↑ ${all.filter(r => r.dir > 0).length}`)}${chip("dn", `↓ ${all.filter(r => r.dir < 0).length}`)}${Object.keys(kinds).sort((a, b) => kinds[b] - kinds[a]).map(k => chip(k, `${k} ${kinds[k]}`)).join("")}</div>`;
  const keep = r => NEWS_F === "all" || (NEWS_F === "up" && r.dir > 0) || (NEWS_F === "dn" && r.dir < 0) || r.label === NEWS_F;
  const days = NEWS.days.map(d => {
    const rows = d.rows.filter(keep);
    return `<div class="hh">${esc(d.date)} <span class="sub">${rows.length}件（数字で読めた ${d.n_read}／対象 ${d.n_kind}／開示 ${d.n_all}）</span></div>
      <div class="card tight">${rows.length ? rows.map(newsRow).join("") : `<div class="empty">該当なし</div>`}</div>`;
  }).join("");
  return head + chips + days + `<p class="disc">${esc(DATA.disclaimer)}</p>`;
}
