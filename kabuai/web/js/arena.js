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
// ── 💴資金セクター → 🔗連動株 → 🌙PTS（2026-10-14 本人依頼「毎日PTSや資金の入ってるセクター・連動株をチンパンに配信・連動を中心に」・flow_builder.py）
let FLOW_TAB = "link";   // 既定は連動（本人「連動を中心にして」）
function flowSetTab(t) { FLOW_TAB = t === "sector" ? "sector" : "link"; render(); }
function flowPts(m) {
  if (m == null || m.pts_dev == null) return `<span class="muted">PTS —</span>`;
  return `PTS ${yen(m.pts)} <span class="${cls(m.pts_dev)}">${fmtPct(m.pts_dev)}</span>`;
}
function flowReg(m) {
  const c = [];
  if (m.shortable) c.push(`<span class="chip">貸借${esc(m.shortable)}${m.jsf_stop ? "・🚫売り禁" : ""}</span>`);
  if (m.daily_pub) c.push(`<span class="chip wa">日々公表</span>`);
  if (m.tse_reg) c.push(`<span class="chip wa">規制</span>`);
  return c.join("");
}
function flowLinkedRow(x) {
  const same = x.same_day != null ? `同日${Number(x.same_day).toFixed(0)}%（${x.n_big}回）` : "同日 —";
  return `<div class="uwrow"><div class="pk-nm"><b><a href="#/detail/${esc(x.code)}">${esc(x.name)}</a></b>
      <small>${esc(x.code)}${x.sector ? " ・ " + esc(x.sector) : ""} ・ ${yen(x.close)} <span class="${cls(x.r1)}">${fmtPct(x.r1)}</span> ・ 代金${Number(x.tov_oku).toFixed(0)}億（×${Number(x.tov_ratio).toFixed(1)}） ・ ${flowPts(x)}</small>
      <div class="chips">${flowReg(x)}</div></div>
    <div class="lv"><b>${Number(x.corr).toFixed(2)}</b><small>連動<br>${esc(same)}</small></div></div>`;
}
function flowMoverCard(m) {
  const L = m.lead;
  const head = `<div class="pk-nm" style="margin-bottom:4px"><b>🎯 <a href="#/detail/${esc(L.code)}">${esc(L.name)}</a></b>
      <small>${esc(L.code)}${L.sector ? " ・ " + esc(L.sector) : ""} ・ ${yen(L.close)} <span class="${cls(L.r1)}">${fmtPct(L.r1)}</span> ・ 代金${Number(L.tov_oku).toFixed(0)}億（×${Number(L.tov_ratio).toFixed(1)}） ・ 25MA乖離${fmtPct(L.dev25)} ・ ${flowPts(L)}</small>
      <div class="chips">${flowReg(L)}</div></div>`;
  const body = (m.linked || []).length ? m.linked.map(flowLinkedRow).join("") : `<div class="empty">しきい値以上の連動株なし（単独で動いた）</div>`;
  return `<div class="card tight">${head}${body}</div>`;
}
function flowGroupCard(g) {
  const mark = (g.strong ? "🔥 " : "") + (g.passed ? "" : "⚠️弱 ");
  const head = `<div class="pk-nm" style="margin-bottom:4px"><b>${mark}${esc(g.label)}</b> <span class="chip">${esc(g.kind)}</span>
      <small>代金 ×${Number(g.ratio_med).toFixed(1)}（中央値・合計×${Number(g.ratio_sum).toFixed(1)}） ・ 上昇${Number(g.breadth).toFixed(0)}% ・ 平均<span class="${cls(g.avg_r1)}">${fmtPct(g.avg_r1)}</span> ・ 5日${fmtPct(g.avg_r5)} ・ 熱い本${g.n_hot}/${g.n} ・ 連動${g.coh != null ? Number(g.coh).toFixed(2) : "—"} ・ 主役 ${esc((g.lead || {}).name || "")}</small></div>`;
  const row = m => `<div class="uwrow"><div class="pk-nm"><b><a href="#/detail/${esc(m.code)}">${esc(m.name)}</a></b>${m.code === (g.lead || {}).code ? ' <span class="chip">主役</span>' : ""}
      <small>${esc(m.code)} ・ ${yen(m.close)} <span class="${cls(m.r1)}">${fmtPct(m.r1)}</span> ・ 代金${Number(m.tov_oku).toFixed(0)}億（×${Number(m.tov_ratio).toFixed(1)}） ・ 5日${fmtPct(m.r5)} ・ ${flowPts(m)}</small>
      <div class="chips">${flowReg(m)}</div></div>
    <div class="lv"><b>${m.corr != null ? Number(m.corr).toFixed(2) : "—"}</b><small>主役と<br>の連動</small></div></div>`;
  const outside = (g.linked_out || []).length ? `<div class="note" style="margin-top:4px">🔗 グループ外で主役に連動: ${g.linked_out.map(x => `<a href="#/detail/${esc(x.code)}">${esc(x.name)}</a>（${Number(x.corr).toFixed(2)}・<span class="${cls(x.r1)}">${fmtPct(x.r1)}</span>${x.pts_dev != null ? `・PTS${fmtPct(x.pts_dev)}` : ""}）`).join("、")}</div>` : "";
  return `<div class="card tight">${head}${(g.members || []).map(row).join("")}${outside}</div>`;
}
function flowSection() {
  const f = DATA.flow;
  const title = `<div class="hh" style="margin-top:16px">💴 資金の流れ → 🔗連動株 <span class="sub">${f && f.target_date ? esc(f.target_date) + " 分（" + esc(f.date || "") + " 引け）" : ""}</span></div>`;
  if (!f) return title + `<div class="card"><div class="empty">まだ作られていません（夕方のビルドで反映）。</div></div>`;
  if (f.error) return title + `<div class="card"><div class="empty">今回は作れませんでした（${esc(f.error)}）。次のビルドで再試行。</div></div>`;
  const link = FLOW_TAB === "link";
  const seg = `<div class="seg"><a class="${link ? "on" : ""}" onclick="flowSetTab('link')">🔗 主役→連動 ${(f.movers || []).length}</a><a class="${link ? "" : "on"}" onclick="flowSetTab('sector')">💴 資金セクター ${(f.groups || []).length}</a></div>`;
  const st = f.stats || {};
  const honest = `<div class="banner info"><b>後追い買いは効きません。</b>${esc(st.chase || "")}。<br>${esc(st.day3 || "")}。<br>${esc(st.use || "")}</div>`;
  const p = f.pts || {};
  const ptsNote = p.skipped ? "PTSは取っていません" : (p.known ? `PTS ${esc(p.asof || "")} 時点（株探/ジャパンネクスト）${p.ok}/${p.tried}本` : `PTSは取れませんでした（${p.tried || 0}本試行${p.err && Object.keys(p.err).length ? "・" + esc(Object.keys(p.err).join(",")) : ""}）`);
  const weak = f.weak_day ? `<div class="banner warn">⚠️ 今日は資金が集まったグループが少ない日（基準を満たしたのは${f.n_groups_cand || 0}）。弱い順でも3つまで埋めています。</div>` : "";
  let body;
  if (link) {
    body = (f.movers || []).length ? f.movers.map(flowMoverCard).join("") : `<div class="card"><div class="empty">今日は +7%以上・代金5億以上の主役がいません。</div></div>`;
  } else {
    body = weak + ((f.groups || []).length ? f.groups.map(flowGroupCard).join("") : `<div class="card"><div class="empty">今日は資金が集まったグループがありません。</div></div>`);
  }
  const foot = `<div class="note" style="margin-top:4px">📏 ${(f.rules || []).map(esc).join(" ／ ")}<br>母集団${f.n_universe || 0}銘柄・連動のしきい値${f.floor != null ? Number(f.floor).toFixed(2) : "—"}（偶然の99.9%点${f.null_max != null ? Number(f.null_max).toFixed(2) : "—"}）・${ptsNote}。</div>`;
  return title + seg + honest + body + foot;
}
function viewArena() {
  const prep = (typeof planNewsSection === "function" ? planNewsSection() : "") + (typeof planEarnSection === "function" ? planEarnSection() : "");
  return `<h2>🌙 前夜の準備 <span class="sub">EOD ${esc(DATA.data_date || "")}・${esc(freshnessText())}</span></h2>
    ${whenBar("🌙", "前夜（夕方のビルド後）に見る", "明日の上髭逆指値の候補と水準 → 資金の流れと連動株（PTS付き） → 土俵 → 材料と決算。注文の文面は📋朝の作戦、値段と株数は🧮電卓。")}
    ${uwickSection()}
    ${flowSection()}
    ${arenaSection()}
    ${prep}
    <p class="disc">${esc(DATA.disclaimer)}</p>`;
}
