// plan.js — 📋作戦（明日の作戦を1画面に・2026-09-17 本人依頼「デイトレ用に特化・ニュースを拾って明日狙う銘柄」）
// 出す物は全部「検証済みルールの機械的な出力」＝①実弾の注文 ②紙の対照 ③📰材料(TDnet) ④明日の決算予定 ⑤資金ラダー
function planOrderRow(o) {
  const warn = o.warn ? `<div class="note" style="margin-top:2px;color:var(--dn)">${esc(o.warn)}</div>` : "";
  const sizeCls = /撃たない|紙/.test(o.size || "") ? "" : "dn";
  return `<a class="pickrow" href="#/detail/${o.code}"><div class="pk-nm"><b><span class="chip ${sizeCls}" style="font-weight:800">${esc(o.system)}</span> ${esc(o.name)}</b>
      <small>${o.code} ・ ${esc(o.side)} ・ <b>${esc(o.size)}</b>${o.iss ? " ・ 貸借" + esc(o.iss) : ""}${o.note ? " ・ " + esc(o.note) : ""}</small>
      <div class="chips"><span class="chip ${sizeCls}">${esc(o.order)}</span></div>${warn}</div></a>`;
}
function planNewsRow(n) {
  const kinds = (n.kinds || []).map(k => `<span class="chip wa">${esc(k)}</span>`).join("");
  const touch = (n.touch || []).map(t => `<span class="chip dn">${esc(t)}</span>`).join("");
  const px = n.price != null ? ` ・ ${yen(n.price)} ${pctTag(n.r1) || ""}` : "";
  const tov = n.turnover_oku != null ? ` ・ 代金${Number(n.turnover_oku).toFixed(0)}億` : "";
  return `<a class="pickrow" href="#/detail/${n.code}"><div class="pk-nm"><b>${esc(n.name)}</b>
      <small>${n.code}${n.sector ? " ・ " + esc(n.sector) : ""}${px}${tov} ・ 貸借${esc(n.iss || "?")} ・ ${esc(n.time || "")}${n.after_close ? "（引け後）" : "（場中）"}</small>
      <div class="chips">${kinds}${touch}</div>
      ${n.title ? `<div class="note" style="margin-top:2px">${esc(n.title)}</div>` : ""}</div></a>`;
}
function planEarnRow(e) {
  return `<a class="pickrow" href="#/detail/${e.code}"><div class="pk-nm"><b>${esc(e.name)}</b>
      <small>${e.code} ・ ${esc(e.type || "")}${e.turnover_oku != null ? ` ・ 代金${Number(e.turnover_oku).toFixed(0)}億` : ""} ・ 貸借${esc(e.iss || "?")}</small></div>
      <div class="m-px">${pctTag(e.r1) || "—"}</div></a>`;
}
function viewPlan() {
  const p = DATA.plan;
  if (!p) return `<h2>📋 朝の作戦</h2><div class="card"><div class="empty">作戦データがまだありません（次のビルドで反映）。</div></div><p class="disc">${esc(DATA.disclaimer)}</p>`;
  const orders = p.orders || [], paper = p.paper || [], news = p.news || [], earn = p.earnings_tomorrow || [];
  const head = `<h2>📋 朝の作戦 <span class="sub">${esc(p.target_date || "")} 分（${esc(p.date || "")} 引け・${esc(p.generated_at || "")} 生成）</span></h2>${whenBar("📋", "寄り前に見る", "今日出す注文だけ。書いてある通りに出す。材料と明日の決算は🎯前夜の準備、場中は🔥場中ライブ。")}`;
  // ① 実弾の注文
  const lead = orders.length
    ? `<div class="banner dn">🔴 <b>明日の実弾 ${orders.length}本</b>：書いてある注文をそのまま出す。日計り（フェード/崩壊）は<b>必ず大引けで手仕舞い</b>。</div>`
    : `<div class="banner"><b>明日は撃つ玉なし</b><br><span class="muted" style="font-size:12px">フェードGO・💥崩壊◎・極上・極み売りの4系統すべて該当なし＝撃たないのが正解の日。</span></div>`;
  const ordersHtml = `<div class="hh">🔴 実弾の注文 <span class="sub">フェード①100/②50・崩壊◎50・極上150・極み売り3×100</span></div>${lead}${orders.length ? `<div class="card tight">${orders.map(planOrderRow).join("")}</div>` : ""}`;
  // ② 紙・撃たない
  const paperHtml = paper.length ? `<details class="card" style="margin-top:10px"><summary style="cursor:pointer;font-weight:700">📝 紙の対照・撃たない玉 ${paper.length}件</summary><div class="list" style="margin-top:6px">${paper.map(planOrderRow).join("")}</div></details>` : "";
  // ⑤ 資金ラダー
  const ladderHtml = `<div class="hh" style="margin-top:14px">💰 資金ラダー</div><div class="card note">${(p.ladder || []).map(l => `<div>・${esc(l)}</div>`).join("")}</div>`;
  return head + ordersHtml + paperHtml + ladderHtml + `<p class="disc">${esc(DATA.disclaimer)}</p>`;
}
// 🎯前夜の準備（arena.js）から呼ぶ: 📰材料 と 📅明日の決算
function planNewsSection() { const p = DATA.plan; if (!p) return ""; return planSections(p).newsHtml; }
function planEarnSection() { const p = DATA.plan; if (!p) return ""; return planSections(p).earnHtml; }

function planSections(p) {
  const news = p.news || [], earn = p.earnings_tomorrow || [];
  // 📰 材料
  const kinds = Object.entries(p.news_kinds || {}).sort((a, b) => b[1] - a[1]).map(([k, v]) => `<span class="chip wa">${esc(k)} ${v}</span>`).join("");
  const touched = news.filter(n => (n.touch || []).length), others = news.filter(n => !(n.touch || []).length);
  const newsHtml = `<div class="hh" style="margin-top:14px">📰 材料 <span class="sub">${esc(p.date || "")} のTDnet開示 ${p.news_total || 0}社</span></div>
    <div class="warnbar">⚠️ <b>材料は「上がる/下がる」の予想ではありません。</b>翌日の寄りで買う材料株は26年で全滅（前日上昇率上位は-0.8%/件）。ここは<b>人と注文が集まる場所</b>の一覧。<b>本物の材料で急騰した玉はフェードで撃たない</b>（ATR5%未満の急騰＝翌日も買われる・勝率50.5%）。決算またぎの玉に⚠️。</div>
    ${kinds ? `<div class="chips" style="margin-bottom:8px">${kinds}</div>` : ""}
    ${news.length ? `${touched.length ? `<div class="note" style="margin-bottom:4px"><b>作戦の銘柄に材料あり</b></div><div class="card tight">${touched.map(planNewsRow).join("")}</div>` : ""}
      <details class="card" style="margin-top:8px" ${touched.length ? "" : "open"}><summary style="cursor:pointer;font-weight:700">その他の材料 ${others.length}社（代金順）</summary><div class="list" style="margin-top:6px">${others.map(planNewsRow).join("")}</div></details>`
      : `<div class="card"><div class="empty">この日の材料はまだ取れていません（17時・19時のビルドで反映）。</div></div>`}`;
  // 📅 明日の決算
  const earnHtml = `<div class="hh" style="margin-top:14px">📅 明日の決算発表 <span class="sub">${p.earnings_tomorrow_total || 0}社（JPX予定表）</span></div>
    ${earn.length ? `<details class="card"><summary style="cursor:pointer;font-weight:700">一覧（代金順・上位${earn.length}）</summary><div class="list" style="margin-top:6px">${earn.map(planEarnRow).join("")}</div></details>`
      : `<div class="card"><div class="empty">明日の決算発表予定はありません。</div></div>`}
    <div class="note" style="margin-top:6px">決算持ち越し（紙・休止中）と決算追撃（紙・9:30判定）は別配信。3日持つ極上/極み売りの玉が決算をまたぐ時は上の注文に⚠️を出しています。</div>`;
  return { newsHtml, earnHtml };
}
