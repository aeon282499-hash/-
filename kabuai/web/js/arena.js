// arena.js — 🎯前夜の準備（v7・2026-10-13 再設計のホーム・本人決定）
//   ① 上髭逆指値の明日の銘柄（買い／売り・latest.json["uwick"]・uwick_builder.py）＝核。候補と逆指値の水準を並べる道具。勝てるとは書かない
//   ② 土俵15本（arena_watchlist.py・人と注文が集まる順・方向なし）
//   ③ 📰当日の材料（TDnet）・📅明日の決算（plan.js の planNewsSection/planEarnSection）
//   🔥ライブ（立花）は v7 で外した（本人「場中はみない」・取得停止中）。
let UW_SIDE = "buy";
function uwSetSide(s) { UW_SIDE = s === "sell" ? "sell" : "buy"; render(); }
function uwRow(r, i, side) {
  const short = side === "sell";
  const lv = short ? r.sell_level : r.buy_level;
  const L = (typeof calcLevels === "function" && lv > 0) ? calcLevels(lv, side, !!r.t500) : null;
  const reg = [];
  if (short) {
    if (r.jsf_warn) reg.push('<span class="chip wa">⚠️ 注意喚起（逆日歩警戒）</span>');
    if (r.tse_reg) reg.push('<span class="chip wa">⚠️ 取引所規制</span>');
  } else if (r.shortable) reg.push(`<span class="chip">貸借${esc(r.shortable)}${r.jsf_stop ? "・🚫売り禁" : ""}</span>`);
  const tov = `代金 20日${Number(r.tov20_oku).toFixed(0)}億${r.tov_ratio != null ? `（当日×${Number(r.tov_ratio).toFixed(1)}）` : ""}`;
  const lvTxt = Number.isInteger(lv) ? lv.toLocaleString() : Number(lv).toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const tpsl = L ? `<span class="chip">利確 ${esc(String(L.tp))}</span><span class="chip dn">損切り ${esc(String(L.sl))}</span><span class="chip">${L.auto > 0 ? L.auto.toLocaleString() + "株（損失2万円）" : "100株でも損失2万円超"}</span>` : "";
  return `<div class="uwrow"><div class="pk-nm"><b>${i + 1}. <a href="#/detail/${esc(r.code)}">${esc(r.name)}</a></b>
      <small>${esc(r.code)}${r.sector ? " ・ " + esc(r.sector) : ""} ・ ${yen(r.close)} <span class="${cls(r.chg)}">${fmtPct(r.chg)}</span> ・ ${r.body_up ? "陽線" : "陰線"} 上髭${Number(r.uw_pct).toFixed(1)}%（髭/幅${Number(r.uw_ratio).toFixed(2)}） ・ ATR${Number(r.atr_pct).toFixed(1)}%</small>
      <small>${tov} ・ 高値${yen(r.high)} 安値${yen(r.low)} ・ 呼値${esc(String(r.tick))}円${r.t500 ? "（TOPIX500）" : ""}</small>
      <div class="chips">${tpsl}${reg.join("")}<a class="chip" style="cursor:pointer" onclick="event.preventDefault();calcPick('${esc(r.code)}',${Number(lv)},'${short ? "sell" : "buy"}',${r.t500 ? "true" : "false"},'${esc(r.name)}')">🧮 電卓へ</a></div></div>
    <div class="lv"><b class="${short ? "neg" : "pos"}">${lvTxt}</b><small>${short ? "売り逆指値" : "買い逆指値"}<br>${short ? "安値−1呼値" : "高値＋1呼値"}</small></div></div>`;
}
function uwickSection() {
  const u = DATA.uwick;
  const title = `<div class="hh">🕯️ 上髭逆指値の明日の銘柄 <span class="sub">${u && u.target_date ? esc(u.target_date) + " 分（" + esc(u.date || "") + " 引け）" : ""}</span></div>`;
  if (!u) return title + `<div class="card"><div class="empty">まだ作られていません（夕方のビルドで反映）。</div></div>`;
  if (u.error) return title + `<div class="card"><div class="empty">今回は作れませんでした（${esc(u.error)}）。次のビルドで再試行。</div></div>`;
  const short = UW_SIDE === "sell";
  const rows = (short ? u.sell : u.buy) || [];
  const seg = `<div class="seg"><a class="${short ? "" : "on"}" onclick="uwSetSide('buy')">📈 買い（高値抜け） ${(u.buy || []).length}</a><a class="${short ? "on" : ""}" onclick="uwSetSide('sell')">🩳 売り（安値割れ） ${(u.sell || []).length}</a></div>`;
  const stale = u.fresh ? "" : `<div class="banner warn">⚠️ これは過去分（${esc(u.target_date || "")}）の候補です。次の夕方ビルドで更新。</div>`;
  const honest = `<div class="banner info"><b>勝てるとは言いません。</b>日足20年では ${esc((u.stats || {}).buy || "")}。${esc((u.stats || {}).sell || "")}。${(u.stats || {}).intraday ? `<br>${esc(u.stats.intraday)}。` : ""}${esc((u.stats || {}).caveat || "")} ここは<b>候補と逆指値の水準を並べる道具</b>です。</div>`;
  const unknown = short && !u.iss_known ? `<div class="banner warn">⚠️ 貸借区分が取れなかった回は売り側が空になります（J-Quants margin-interest）。</div>` : "";
  const body = rows.length ? `<div class="card tight">${rows.map((r, i) => uwRow(r, i, UW_SIDE)).join("")}</div>`
    : `<div class="card"><div class="empty">${short ? `該当${u.matched || 0}銘柄のうち、貸借○で売り禁でないものがありません。` : `今日は上髭${(u.rules || [])[0] ? "" : ""}の条件に合う銘柄がありません（代金3億以上・300〜10,000円）。`}</div></div>`;
  const foot = `<div class="note" style="margin-top:4px">📏 ${(u.rules || []).map(esc).join(" ／ ")}<br>当日足${u.n_day || 0}銘柄 → 該当${u.matched || 0}${short ? `（貸借×/売り禁で${u.n_sell_blocked || 0}本除外）` : ""}。利確/損切り/株数は🧮電卓と同じ式（+${(u.calc || {}).tp || 1.5}% / −${(u.calc || {}).sl || 3}% / 損失${((u.calc || {}).loss_yen || 20000) / 10000}万円）。</div>`;
  return title + stale + seg + honest + unknown + body + foot;
}
function arenaSection() {
  const a = DATA.arena;
  const title = `<div class="hh" style="margin-top:16px">🎯 土俵 <span class="sub">${a && a.target_date ? esc(a.target_date) + " 分（" + esc(a.date || "") + " 引け・母集団" + (a.universe || "-") + "銘柄）" : ""}</span></div>`;
  if (!a || !(a.rows || []).length) return title + `<div class="card"><div class="empty">土俵リストがまだありません（前夜18:50頃の配信後に反映）。</div></div>`;
  const mats = r => (r.materials || []).map(m => `<span class="chip wa">${esc(m.kind)} ${esc(m.time || "")}</span>`).join("");
  const flags = r => (r.flags || []).map(f => `<div class="note" style="margin-top:2px">⚠️ ${esc(f)}</div>`).join("");
  const row = (r, i) => `<a class="pickrow" href="#/detail/${r.code}"><div class="pk-nm"><b>${i + 1}. ${esc(r.name)}</b>
      <small>${r.code}${r.sector ? " ・ " + esc(r.sector) : ""} ・ ${yen(r.close)} <span class="${cls(r.chg)}">${fmtPct(r.chg)}</span> ・ 代金${r.turnover_oku}億${r.tov_ratio ? `(×${r.tov_ratio})` : ""} ・ 値幅${r.range_pct}% ・ ATR${r.atr_pct}%${r.shortable ? " ・ 貸借" + esc(r.shortable) : ""}</small>
      <div class="chips">${mats(r)}</div>${flags(r)}</div>
    <div class="m-px">${r.heat != null ? Math.round(r.heat) : "—"}<br><small class="muted">熱</small></div></a>`;
  const extra = (a.materials_extra || []).map(r => `<a href="#/detail/${r.code}">${esc(r.name)}</a>(${r.code}/${esc((r.materials || [{}])[0].kind || "")})`).join("、");
  return title + (a.fresh ? "" : `<div class="banner warn">⚠️ これは過去分（${esc(a.target_date || "")}）の断面です。</div>`)
    + `<div class="banner info"><b>上がる/下がるは付けません。</b>前夜のデータで「翌日日中に上がる銘柄」は選べない（26年BT＋AIの学習外検証で全滅）。ここは<b>人と注文が集まる順</b>。</div>
    <div class="note" style="margin-bottom:10px">📏 ${(a.rules || []).map(esc).join(" ／ ")}</div>
    <div class="card tight">${(a.rows || []).map(row).join("")}</div>
    ${extra ? `<div class="card note"><b>材料あり（土俵外・代金順）</b>: ${extra}</div>` : ""}`;
}
function viewArena() {
  const prep = (typeof planNewsSection === "function" ? planNewsSection() : "") + (typeof planEarnSection === "function" ? planEarnSection() : "");
  return `<h2>🌙 前夜の準備 <span class="sub">EOD ${esc(DATA.data_date || "")}・${esc(freshnessText())}</span></h2>
    ${whenBar("🌙", "前夜（夕方のビルド後）に見る", "明日の上髭逆指値の候補と水準 → 土俵 → 材料と決算。注文の文面は📋朝の作戦、値段と株数は🧮電卓。")}
    ${uwickSection()}
    ${arenaSection()}
    ${prep}
    <p class="disc">${esc(DATA.disclaimer)}</p>`;
}
