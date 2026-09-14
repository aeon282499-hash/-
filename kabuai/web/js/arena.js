// arena.js — 🎯土俵（デイトレの銘柄選定・方向なし・前夜配信）＋🔥ライブの「いま」を重ねる
function arenaLiveInner() {
  const a = DATA.arena; if (!a || !(a.rows || []).length) return "";
  if (typeof LIVE === "undefined" || !LIVE || !LIVE.stocks) return "";
  const rows = (a.rows || []).map(r => LIVE.stocks[r.code]).filter(Boolean);
  if (!rows.length) return "";
  return `<div class="hh">🔥 土俵のいま <span class="sub">${esc(LIVE.hhmm || "")} ${esc(LIVE_STATE[LIVE.state] || "")}</span></div><div class="card tight">${rows.map(m => stockRow(m)).join("")}</div>`;
}
function viewArena() {
  const a = DATA.arena;
  if (!a || !(a.rows || []).length) return `<h2>🎯 土俵</h2><div class="card"><div class="empty">土俵リストがまだありません（前夜18:50頃の配信後に反映）。</div></div><p class="disc">${esc(DATA.disclaimer)}</p>`;
  const mats = r => (r.materials || []).map(m => `<span class="chip wa">${esc(m.kind)} ${esc(m.time || "")}</span>`).join("");
  const flags = r => (r.flags || []).map(f => `<div class="note" style="margin-top:2px">⚠️ ${esc(f)}</div>`).join("");
  const row = (r, i) => `<a class="pickrow" href="#/detail/${r.code}"><div class="pk-nm"><b>${i + 1}. ${esc(r.name)}</b>
      <small>${r.code}${r.sector ? " ・ " + esc(r.sector) : ""} ・ ${yen(r.close)} <span class="${cls(r.chg)}">${fmtPct(r.chg)}</span> ・ 代金${r.turnover_oku}億${r.tov_ratio ? `(×${r.tov_ratio})` : ""} ・ 値幅${r.range_pct}% ・ ATR${r.atr_pct}%${r.shortable ? " ・ 貸借" + esc(r.shortable) : ""}</small>
      <div class="chips">${mats(r)}</div>${flags(r)}</div>
    <div class="m-px">${r.heat != null ? Math.round(r.heat) : "—"}<br><small class="muted">熱</small></div></a>`;
  const extra = (a.materials_extra || []).map(r => `<a href="#/detail/${r.code}">${esc(r.name)}</a>(${r.code}/${esc((r.materials || [{}])[0].kind || "")})`).join("、");
  return `<h2>🎯 土俵 <span class="sub">${esc(a.target_date || "")} 分（${esc(a.date || "")} 引け・母集団${a.universe || "-"}銘柄）</span></h2>
    ${a.fresh ? "" : `<div class="banner warn">⚠️ これは過去分（${esc(a.target_date || "")}）の断面です。</div>`}
    <div id="arena-live">${arenaLiveInner()}</div>
    <div class="banner info"><b>上がる/下がるは付けません。</b>前夜のデータで「翌日日中に上がる銘柄」は選べない（26年BT＋AIの学習外検証で全滅）。ここは<b>人と注文が集まる順</b>＝勝っているデイトレーダーが前夜にやる「準備」の部分。勝負は場中の板と歩み値＝🔥ライブで「今の資金」を見る。</div>
    <div class="note" style="margin-bottom:10px">📏 ${(a.rules || []).map(esc).join(" ／ ")}</div>
    <div class="card tight">${(a.rows || []).map(row).join("")}</div>
    ${extra ? `<div class="card note"><b>材料あり（土俵外・代金順）</b>: ${extra}</div>` : ""}
    <p class="disc">${esc(DATA.disclaimer)}</p>`;
}
