// sell.js — 🔻売り（🩳フェード＝実弾の主力・📉モメンタム終了・💥崩壊ショート・セクターローテSELL）
function fadeSection() {
  const fd = (DATA.sell_watch || {}).fade;
  if (!fd || fd.error) return "";
  const picks = fd.picks || [], goMin = fd.go_min != null ? fd.go_min : 7, st = fd.stats || {};
  const chipReg = p => p.reg_note ? ` <span class="chip ${p.jsf_stop ? "dn" : "wa"}">${esc(p.reg_note)}</span>` : "";
  const row = (p, go) => `<a class="pickrow" href="#/detail/${p.code}">
    <div class="pk-nm"><b>${esc(p.name)}${go ? ' <span class="chip dn" style="font-weight:800">🔴 GO</span>' : ' <span class="chip">見送り</span>'}</b>
      <small>${p.code} ・ 前日<b class="pos">+${p.gain != null ? Number(p.gain).toFixed(1) : "—"}%</b>${p.dev25 != null ? ` ・ 25MA乖離+${Number(p.dev25).toFixed(0)}%` : ""}${p.atr_pct != null ? ` ・ ATR${Number(p.atr_pct).toFixed(1)}%` : ""}${p.vol_ratio != null ? ` ・ 出来高${Number(p.vol_ratio).toFixed(1)}倍` : ""}</small>
      <div class="chips">${go ? `<span class="chip dn">寄指 売り ${yen(p.min_entry)}以上 → 引けで買い戻し</span>` : `<span class="chip">${esc(p.nogo_reason || "GO基準未満")}</span>`}${chipReg(p)}</div></div></a>`;
  let body;
  if (!picks.length) body = `<div class="empty">本日は候補なし（前日+5%以上で売れる銘柄がゼロ）</div>`;
  else {
    const gos = picks.filter(p => p.verdict === "GO"), nos = picks.filter(p => p.verdict !== "GO");
    const maxg = Math.max(...picks.map(p => Number(p.gain) || 0));
    const lead = gos.length ? `<div class="banner dn">🔴 <b>本日GO ${gos.length}件</b>：翌営業日（朝ビルドなら今日）の寄りで空売り→<b>当日の引けで必ず買い戻し</b>（持ち越し禁止）。①100万/②50万。</div>`
      : `<div class="banner"><b>今日は撃つ日じゃない（見送り）</b><br><span class="muted" style="font-size:12px">売れる銘柄の最大が前日+${maxg.toFixed(1)}% ＜ GO基準+${goMin}%。撃たないのが正解の日。</span></div>`;
    body = lead + `<div class="card tight">${gos.map(p => row(p, true)).join("")}${nos.map(p => row(p, false)).join("")}</div>`;
  }
  return `<div class="hh">🩳 デイトレ売り（フェード） <span class="sub">${esc(fd.date || "")} 終値時点・実弾の主力</span></div>
    <div class="warnbar">⚠️ <b>空売りは上級者向け・自己責任。</b><b>🚫売り禁</b>＝制度信用の新規売り停止中。SBI一日信用(ハイカラ)/一般信用は別枠在庫＝売れることがある（プレミアム料は配信の帯で判断）。</div>
    ${body}
    <div class="note" style="margin-bottom:12px">ルール: <b>前日+${goMin}%以上の急騰（貸借○・張り付き除外）× ATR5%↑ × 25MA乖離12%↑</b>を翌営業日に「寄指で空売り→引けで買い戻し」。並び順＝乖離とATRの順位平均・上位2本。検証${esc(st.period || "")}: <b>${st.n || "—"}件・勝率${st.win || "—"}%・平均+${st.avg || "—"}%/件・PF${st.pf || "—"}</b>・${esc(st.yearly || "")}。26年延伸でも全時代PF1.26-1.57＝構造的エッジ。</div>`;
}
function sellHero() {
  const st = DATA.sector_today;
  if (!st || !st.is_today || !(st.sell || []).length) return "";
  const s = (st.stats || {}).sell || {};
  const rows = st.sell.map(r => `<a class="pickrow" href="#/detail/${r.code}"><div class="pk-nm"><b>${esc(r.name)}</b><small>${r.code} ・ 前日終値 ${yen(r.price)}${r.sector ? ` ・ ${esc(r.sector)}` : ""}</small>
      <div class="chips"><span class="chip dn">🔻 最弱セクターの急騰売り${(r.day_change != null && !isNaN(r.day_change)) ? ` 前日+${Number(r.day_change).toFixed(1)}%` : ""}</span></div></div></a>`).join("");
  return `<div class="card danger"><div class="hh" style="margin:0 0 6px">🔻 セクターローテSELL <span class="sub">${esc(st.date || "")} 当日分</span></div>
    <div style="font-size:12.5px;line-height:1.65">最弱セクターで急騰した銘柄の反落を取る、<b>過去5年すべてプラス（PF${s.pf != null ? s.pf : 1.37}）</b>の検証済み売り。1日1件まで。</div>
    <div class="list">${rows}</div><div class="note" style="margin-top:8px">🚪 ${esc(st.plan_sell || "")}</div></div>`;
}
function viewSell() {
  const sw = DATA.sell_watch, members = (sw && sw.members) || [];
  const head = `<h2>🔻 売り <span class="sub">${(sw && sw.date) || DATA.data_date || ""} 終値時点</span></h2>${sellHero()}${fadeSection()}
    <div class="hh">📉 モメンタム終了 <span class="sub">上昇が崩れた銘柄</span></div>
    <div class="warnbar">⚠️ <b>空売りの推奨ではありません。</b>直近1ヶ月で大きく上昇した銘柄の<b>上昇モメンタムが終わったサイン</b>。保有者の出口検討・高値づかみ回避に。</div>`;
  if (!members.length) return head + `<div class="card"><div class="empty">本日、モメンタム終了の条件に合致した銘柄はありません。</div></div><div class="card note">${esc((sw && sw.note) || "")}</div><p class="disc">${esc(DATA.disclaimer)}</p>`;
  const cr = (sw && sw.crash) || null;
  const crBar = (cr && cr.count) ? `<div class="card danger"><div style="font-weight:700;color:var(--dn)">💥 崩壊ショート（BT合格）${cr.count}件</div>
      <div class="note" style="margin-top:4px">急騰+${cr.cond.runup20}%以上 × 出来高${cr.cond.vol_x}倍以上 × 当日${cr.cond.r1}%以下 × 5MA割れ初日 × 貸借○ を全部満たす銘柄に💥。検証${esc(cr.stats.period)}: <b>${cr.stats.n}件・勝率${cr.stats.win}%・平均+${cr.stats.avg}%/件・PF${cr.stats.pf}</b>。撃ち方: <b>${esc(cr.how)}</b>。<span class="warn">⚠️ ${esc(cr.caveat)}</span></div></div>` : "";
  const rows = members.slice().sort((a, b) => (b.crash ? 1 : 0) - (a.crash ? 1 : 0)).map(m => {
    const fresh = (m.below5 == null || m.below5 <= 1);
    const shortChip = m.crash ? ' <span class="chip dn" style="font-weight:800">💥崩壊ショート</span>' : (m.shortable === false ? ' <span class="chip wa">貸借✕</span>' : "");
    return `<a class="pickrow" href="#/detail/${m.code}"><div class="pk-nm"><b>${esc(m.name)}${fresh ? ' <span class="chip dn">本日崩れ</span>' : ` <span class="chip">割れ${m.below5}日目</span>`}${shortChip}</b>
        <small>${m.code} ・ ${yen(m.price)} ・ 1ヶ月で+${m.runup20 != null ? Number(m.runup20).toFixed(0) : "—"}%上昇していた銘柄</small>
        <div class="chips"><span class="chip dn">高値から ${m.off_peak20 != null ? Number(m.off_peak20).toFixed(1) : "—"}%</span><span class="chip">5MA乖離 ${m.ma5_dev != null ? Number(m.ma5_dev).toFixed(1) : "—"}%</span><span class="chip">出来高 ${m.vol_x != null ? Number(m.vol_x).toFixed(1) : "—"}倍</span>${m.margin_alert ? '<span class="chip wa">⚠️日々公表</span>' : ""}${m.margin_up != null ? `<span class="chip wa">🧨買残+${Math.round(m.margin_up)}%</span>` : ""}</div></div>
      <div class="m-px">${pctTag(m.r1) || "—"}</div></a>`;
  }).join("");
  return head + crBar + `<div class="card tight">${rows}</div>
    <div class="card note">検出条件: <b>直近1ヶ月で+${(sw && sw.cond && sw.cond.runup20) || 15}%以上走った銘柄</b>が「<b>終値で5日移動平均割れ × 陰線（または-2%超の下げ） × 出来高が平常の${(sw && sw.cond && sw.cond.vol_x) || 1.3}倍以上</b>」になった日。</div>
    <p class="disc">${esc(DATA.disclaimer)}</p>`;
}
