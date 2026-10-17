// momentum.js — 🐵EOD（モメンタム指数ランキング・🏆勝ちやすい順張り・持ち株コーチ・ウォッチ）
// 指数の高さ＝予測ではない（Sは翌10日平均-2.65%）。眺める/ウォッチ用と明示（v4方針を継承）。
let MOM_GRADE = "all";
let MOM_SHORT = false;      // v8: 🔥空売り多だけ（本人「売り金マークと空売り残高多いやつをわかりやすく」2026-10-17）
function setMomGrade(g) { MOM_GRADE = g; render(); }
function toggleMomShort() { MOM_SHORT = !MOM_SHORT; render(); }
const fmtKabu = v => v == null ? "—" : v >= 1e8 ? (v / 1e8).toFixed(1) + "億株" : v >= 1e4 ? Math.round(v / 1e4).toLocaleString() + "万株" : Number(v).toLocaleString() + "株";
function momHeavy(r) { return (r.short_days != null && r.short_days >= 1) || (r.ratio != null && r.ratio <= 1); }
function momShortChips(r) {
  const c = [];
  if (r.jsf_stop) c.push('<span class="chip dn">🚫 売り禁</span>');
  else if (r.shortable === true) c.push('<span class="chip up">貸借○ 空売り可</span>');
  else if (r.shortable === false) c.push('<span class="chip">制度× 空売り不可</span>');
  if (r.jsf_warn) c.push('<span class="chip wa">⚠️ 注意喚起</span>');
  if (r.tse_reg) c.push('<span class="chip wa">規制</span>');
  if (r.daily_pub) c.push('<span class="chip">日々公表</span>');
  if (r.short_vol != null) {
    const heavy = momHeavy(r);
    c.push(`<span class="chip ${heavy ? "acc" : ""}">${heavy ? "🔥 空売り多 " : ""}売残 ${fmtKabu(r.short_vol)}${r.short_chg != null ? `（前週${r.short_chg >= 0 ? "+" : ""}${Math.round(r.short_chg)}%）` : ""}${r.ratio != null ? ` ・ 取組${Number(r.ratio).toFixed(1)}倍` : ""}${r.short_days != null ? ` ・ 出来高${Number(r.short_days).toFixed(1)}日分` : ""}</span>`);
  }
  return c.length ? `<div class="chips" style="margin-top:3px">${c.join("")}</div>` : "";
}
function viewMomentum() {
  if (WATCH.length || loadPos().length) loadSearch().then(() => {
    const el = document.getElementById("watchsum"); if (el) el.innerHTML = watchSummaryInner();
    const pc = document.getElementById("poscoach"); if (pc) pc.innerHTML = posCoach();
  });
  const rk = DATA.ranking || [], gc = DATA.grade_counts || {};
  const chip = (g, lbl) => `<a class="gchip ${MOM_GRADE === g ? "on" : ""}" onclick="event.preventDefault();setMomGrade('${g}')">${lbl}</a>`;
  let list = MOM_GRADE === "all" ? rk : rk.filter(r => r.grade === MOM_GRADE);
  if (MOM_SHORT) list = list.filter(momHeavy);
  const sw = DATA.sell_watch, swCodes = new Set(((sw && sw.members) || []).map(m => m.code));
  const rows = list.map(r => `<a class="mrow" href="#/detail/${r.code}">
      <div class="grb ${esc(r.grade || "D")}">${esc(r.grade || "-")}</div>
      <div class="m-nm"><b>${esc(r.name)}${swCodes.has(r.code) ? ' <span class="chip dn" style="padding:0 6px">🔻終了サイン</span>' : ""}${r.days_cover != null && r.days_cover < 0.25 ? ' <span class="chip up" style="padding:0 6px">🪶信用軽</span>' : ""}</b>
        <small>${r.code} ・ 勢い${r.momentum != null ? Math.round(r.momentum) : "—"} ・ 5日 ${fmtPct(r.r5)} ・ 代金${r.turnover_oku != null ? Math.round(r.turnover_oku) + "億" : "—"} ・ 値動き${r.rng20 != null && isFinite(Number(r.rng20)) ? Number(r.rng20).toFixed(1) + "%" : "—"}</small>${momShortChips(r)}</div>
      <div class="m-px">${yen(r.price)}<br>${pctTag(r.r1) || "—"}</div></a>`).join("");
  const mk = DATA.market || {};
  const segs = mk.segments ? Object.values(mk.segments).filter(s => s && s.available).map(s => `<span class="chip">${esc(String(s.label || "").split("市場")[0])} ${s.score}<b>[${esc(s.grade)}]</b>${esc(s.regime || "")}</span>`).join("") : "";
  return `
    ${dataSubNav("momentum")}
    <a href="#/search" class="card hsearch">🔍 <span style="flex:1">銘柄コード・名前で検索…</span>${WATCH.length ? `<span class="tag">⭐ ${WATCH.length}</span>` : ""}</a>
    <div id="poscoach">${posCoach()}</div>
    ${WATCH.length ? `<div class="card" id="watchsum"><span class="muted">⭐ ウォッチ ${WATCH.length}件を確認中…</span></div>` : ""}
    ${mk.available ? `<div class="card"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>地合い（EOD）</b><span class="muted" style="font-size:11.5px">${esc(mk.label || "")}</span></div>
      <div class="kpis"><div class="kpi"><div class="l">スコア</div><div class="v">${mk.score} <span style="font-size:12px">[${esc(mk.grade)}]</span></div><div class="s">${esc(mk.regime || "")}</div></div>
        <div class="kpi"><div class="l">上昇銘柄</div><div class="v">${mk.breadth_pct}%</div><div class="s">幅</div></div>
        <div class="kpi"><div class="l">20日</div><div class="v ${cls(mk.trail20_pct)}">${fmtPct1(mk.trail20_pct)}</div><div class="s">等加重</div></div>
        <div class="kpi"><div class="l">25MA乖離</div><div class="v ${cls(mk.ma_dev_pct)}">${fmtPct1(mk.ma_dev_pct)}</div><div class="s">等加重</div></div></div>
      ${segs ? `<div class="chips" style="margin-top:8px">${segs}</div>` : ""}</div>` : ""}
    <h2>🐵 モメンタム <span class="sub">${DATA.data_date || ""} 終値・強さ/過熱ランキング</span></h2>
    ${vnote("買い推奨ではありません。S ほど翌10日の平均はマイナス（過熱の目印）。", "眺める／ウォッチ用。印: 🚫売り禁＝制度信用の新規売り不可／貸借○＝空売り可／🔥空売り多＝売残が買残以上（取組1倍以下）または売残が1日の出来高以上＝踏み上げの燃料・売り方は逆日歩に注意。残高は週次（金曜分・火曜公表）。")}
    <div class="gchips">${chip("all", `すべて ${rk.length}`)}${chip("S", `S ${gc.S || 0}`)}${chip("A", `A ${gc.A || 0}`)}${chip("B", `B ${gc.B || 0}`)}${chip("C", "C")}${chip("D", "D")}<a class="gchip ${MOM_SHORT ? "on" : ""}" onclick="event.preventDefault();toggleMomShort()">🔥 空売り多 ${rk.filter(momHeavy).length}</a></div>
    <div class="card tight">${rows || `<div class="empty">該当なし</div>`}</div>
    ${winnableSection()}
    <p class="disc">${esc(DATA.disclaimer)}</p>`;
}
function winnableSection() {
  const w = DATA.winnable; if (!w) return "";
  const ms = w.members || [], st = w.stats || {};
  const row = m => `<a class="pickrow" href="#/detail/${m.code}"><div class="pk-nm"><b>${m.rank}. ${esc(m.name)}</b>
      <small>${m.code} ・ ${yen(m.price)} ・ 前日比 ${pctTag(m.r1) || "—"} ・ 代金${m.turnover_oku != null ? Math.round(m.turnover_oku) + "億" : "—"}${m.sector ? " ・ " + esc(m.sector) : ""}</small>
      <div class="chips"><span class="chip">値動き ${m.vol60 != null ? Number(m.vol60).toFixed(1) + "%/日" : "—"}</span><span class="chip up">250日 ${fmtPct(m.ret250)}</span><span class="chip up">120日 ${fmtPct(m.ret120)}</span></div></div>
    <div class="m-px">${m.score != null ? Math.round(m.score) : "—"}<br><small class="muted">点</small></div></a>`;
  return `<details class="card"><summary>🏆 勝ちやすい順張り <span class="muted" style="font-weight:400;font-size:12px">持ち越し(20〜40日)用・上位${w.top_n || 20}</span></summary>
    <div class="dbody">${vnote("「勢いが強い順」ではなく「静かに上がり続けている中低位株」の順。20〜40日持つ前提。", "翌日の寄りで買って20〜40日持つ前提。デイトレ向きではありません（5日では+0.19%）。")}
    ${ms.length ? `<div class="card tight">${ms.map(row).join("")}</div>` : `<div class="empty">本日は該当なし</div>`}
    <div class="note">ルール: ${esc(w.rule || "")}。検証${esc(st.period || "")}: 上位20を翌日寄りで買って20日後＝<b>市場平均比${esc(st.vs_market_20d || "")}</b>・勝ち年${esc(st.win_years || "")}・時代別 ${esc(st.eras || "")}。</div></div></details>`;
}
