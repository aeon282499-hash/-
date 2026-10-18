// momentum.js — 🐵EOD（モメンタム指数ランキング・🏆勝ちやすい順張り・持ち株コーチ・ウォッチ）
// 指数の高さ＝予測ではない（Sは翌10日平均-2.65%）。眺める/ウォッチ用と明示（v4方針を継承）。
let MOM_GRADE = "all";
let MOM_SHORT = false;      // v8: 🔥空売り多だけ（本人「売り金マークと空売り残高多いやつをわかりやすく」2026-10-17）
let MOM_SORT = "rank";      // v8.1: "rank"=勢い順 / "short"=売残(出来高の日数)多い順
function setMomGrade(g) { MOM_GRADE = g; render(); }
function toggleMomShort() { MOM_SHORT = !MOM_SHORT; render(); }
function toggleMomSort() { MOM_SORT = MOM_SORT === "short" ? "rank" : "short"; render(); }
const fmtKabu = v => v == null ? "—" : v >= 1e8 ? (v / 1e8).toFixed(1) + "億株" : v >= 1e4 ? Math.round(v / 1e4).toLocaleString() + "万株" : Number(v).toLocaleString() + "株";
// 🔥空売り多 ＝ 売残が1日の出来高以上（買い戻すのに1日以上かかる）か、売残が買残以上（取組1倍以下＝売り長）
function momHeavy(r) { return (r.short_days != null && r.short_days >= 1) || (r.ratio != null && r.ratio <= 1); }
// v8.1 売り印（大きい札）: 🚫売り禁 / 貸借○ / 制度× ＋ ⚠️注意喚起・規制・日々公表 ＋ 🔥空売り多
function momShortBadges(r, big = true) {
  const c = [], k = big ? "sbadge" : "chip";
  if (r.jsf_stop) c.push(`<span class="${k} dn">🚫 売り禁</span>`);
  else if (r.shortable === true) c.push(`<span class="${k} up">貸借○ 空売り可</span>`);
  else if (r.shortable === false) c.push(`<span class="${k} mut">制度× 空売り不可</span>`);
  if (r.jsf_warn) c.push(`<span class="${k} wa">⚠️ 注意喚起</span>`);
  if (r.tse_reg) c.push(`<span class="${k} wa">⚠️ 規制</span>`);
  if (r.daily_pub) c.push(`<span class="${k} mut">日々公表</span>`);
  if (r.short_vol != null && momHeavy(r)) c.push(`<span class="${k} hot">🔥 空売り多</span>`);
  return c.join("");
}
// 売残の中身を日本語で: 売残 45万株 ＝ 出来高の2.3日分 ・ 取組2.1倍 ・ 前週+10%  ＋ ゲージ（3日分で満タン）
function momShortMeter(r) {
  if (r.short_vol == null) return "";
  const d = r.short_days, hv = momHeavy(r);
  const lvl = d == null ? "" : d >= 1 ? "hot" : d >= 0.5 ? "wa" : "";
  const pct = d == null ? 0 : Math.max(3, Math.min(100, d / 3 * 100));
  const chg = r.short_chg != null ? `前週 <b class="${r.short_chg >= 0 ? "pos" : "neg"}">${r.short_chg >= 0 ? "+" : ""}${Math.round(r.short_chg)}%</b>` : "";
  const ratio = r.ratio != null ? `取組 <b>${Number(r.ratio).toFixed(1)}倍</b>${r.ratio <= 1 ? "<span class=\"hot\">（売り長）</span>" : ""}` : "";
  const days = d != null ? `＝ 出来高の <b class="${lvl}">${Number(d).toFixed(1)}日分</b>` : "";
  return `<div class="smeter ${hv ? "heavy" : ""}"><div class="sm-line">売残 <b>${fmtKabu(r.short_vol)}</b> ${days}${ratio ? " ・ " + ratio : ""}${chg ? " ・ " + chg : ""}</div>
    <div class="sm-bar"><i class="${lvl}" style="width:${pct.toFixed(0)}%"></i></div></div>`;
}
function momShortChips(r) {
  const b = momShortBadges(r), m = momShortMeter(r);
  return b || m ? `<div class="sbadges">${b}</div>${m}` : "";
}
// 🐵の最上部: 🔥空売り残高が多い銘柄（売残の日数順）と 🚫売り禁 の一覧＝ランキングで銘柄を選ぶ時に一目で分かるように
function momShortSummary(rk) {
  const have = rk.filter(r => r.short_vol != null || r.shortable != null || r.jsf_stop);
  if (!have.length) return "";
  const heavy = rk.filter(momHeavy).sort((a, b) => (b.short_days || 0) - (a.short_days || 0));
  const stop = rk.filter(r => r.jsf_stop);
  const mdate = (rk.find(r => r.margin_date) || {}).margin_date || "";
  const row = r => `<a class="srow" href="#/detail/${r.code}"><div class="grb ${esc(r.grade || "D")}">${esc(r.grade || "-")}</div>
      <div class="s-nm"><b>${esc(r.name)}</b><small>${r.code} ・ 売残${fmtKabu(r.short_vol)}${r.short_chg != null ? `（前週${r.short_chg >= 0 ? "+" : ""}${Math.round(r.short_chg)}%）` : ""}${r.jsf_stop ? ' ・ <span class="neg" style="font-weight:800">🚫売り禁</span>' : ""}${r.jsf_warn ? ' ・ <span class="warn">⚠️注意喚起</span>' : ""}</small></div>
      <div class="s-kv"><div class="v ${r.short_days != null && r.short_days >= 1 ? "hot" : ""}">${r.short_days != null ? Number(r.short_days).toFixed(1) + "日分" : "—"}</div><div class="l">出来高の</div></div>
      <div class="s-kv"><div class="v ${r.ratio != null && r.ratio <= 1 ? "hot" : ""}">${r.ratio != null ? Number(r.ratio).toFixed(1) + "倍" : "—"}</div><div class="l">取組</div></div></a>`;
  return `<div class="card shortsum"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>🔥 空売り残高が多い ${heavy.length}本</b><span class="muted" style="font-size:13.5px">ランキング${rk.length}本のうち・残高は ${esc(mdate)} 時点（週次）</span></div>
    <div class="note" style="margin:4px 0 6px">「出来高の◯日分」＝売り方が全部買い戻すのにかかる日数。<b>1日分以上</b>か<b>取組1倍以下（売残≧買残＝売り長）</b>で🔥。踏み上げの燃料＝上がると売り方の買い戻しが乗る。売り方は逆日歩に注意。</div>
    ${heavy.length ? `<div class="tight">${heavy.map(row).join("")}</div>` : `<div class="empty">今週は該当なし</div>`}
    <div style="margin-top:10px;display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>🚫 売り禁 ${stop.length}本</b><span class="muted" style="font-size:13.5px">制度信用の新規売り停止＝個人は普通には空売りできない（SBI一日信用HYPER/一般信用は別枠在庫）</span></div>
    ${stop.length ? `<div class="chips" style="margin-top:6px">${stop.map(r => `<a class="sbadge dn" href="#/detail/${r.code}">🚫 ${esc(r.name)} <span style="opacity:.75;font-weight:600">${r.code}</span></a>`).join("")}</div>` : `<div class="empty" style="padding:6px 0">売り禁なし</div>`}
    <div class="note" style="margin-top:8px">印の読み方: <span class="sbadge dn lg">🚫 売り禁</span>＝制度信用で新規売り不可 ／ <span class="sbadge up lg">貸借○ 空売り可</span>＝制度信用で空売りできる ／ <span class="sbadge mut lg">制度×</span>＝信用買いだけの銘柄 ／ <span class="sbadge wa lg">⚠️ 注意喚起</span>＝日証金の注意喚起（逆日歩警戒） ／ <span class="sbadge hot lg">🔥 空売り多</span>＝売残が重い。各行の棒は売残÷1日の出来高（3日分で満タン）。</div></div>`;
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
  if (MOM_SORT === "short") list = [...list].sort((a, b) => ((b.short_days ?? -1) - (a.short_days ?? -1)) || ((a.ratio ?? 99) - (b.ratio ?? 99)));
  const sw = DATA.sell_watch, swCodes = new Set(((sw && sw.members) || []).map(m => m.code));
  const rows = list.map(r => `<a class="mrow" href="#/detail/${r.code}">
      <div class="grb ${esc(r.grade || "D")}">${esc(r.grade || "-")}</div>
      <div class="m-nm"><b>${esc(r.name)}${swCodes.has(r.code) ? ' <span class="chip dn" style="padding:0 6px">🔻終了サイン</span>' : ""}${r.days_cover != null && r.days_cover < 0.25 ? ' <span class="chip up" style="padding:0 6px">🪶信用軽</span>' : ""}</b>
        <small>${r.code} ・ 勢い${r.momentum != null ? Math.round(r.momentum) : "—"} ・ 5日 ${fmtPct(r.r5)} ・ 代金${r.turnover_oku != null ? Math.round(r.turnover_oku) + "億" : "—"} ・ 値動き${r.rng20 != null && isFinite(Number(r.rng20)) ? Number(r.rng20).toFixed(1) + "%" : "—"}</small>${momShortChips(r)}</div>
      <div class="m-px">${yen(r.price)}<br>${pctTag(r.r1) || "—"}</div></a>`).join("");
  const mk = DATA.market || {};
  const segs = mk.segments ? Object.values(mk.segments).filter(s => s && s.available).map(s => `<span class="chip">${esc(String(s.label || "").split("市場")[0])} ${s.score}<b>[${esc(s.grade)}]</b>${esc(s.regime || "")}</span>`).join("") : "";
  return `
    <a href="#/search" class="card hsearch">🔍 <span style="flex:1">銘柄コード・名前で検索…</span>${WATCH.length ? `<span class="tag">⭐ ${WATCH.length}</span>` : ""}</a>
    <div id="poscoach">${posCoach()}</div>
    ${WATCH.length ? `<div class="card" id="watchsum"><span class="muted">⭐ ウォッチ ${WATCH.length}件を確認中…</span></div>` : ""}
    ${mk.available ? `<div class="card"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>地合い（EOD）</b><span class="muted" style="font-size:13.5px">${esc(mk.label || "")}</span></div>
      <div class="kpis"><div class="kpi"><div class="l">スコア</div><div class="v">${mk.score} <span style="font-size:14px">[${esc(mk.grade)}]</span></div><div class="s">${esc(mk.regime || "")}</div></div>
        <div class="kpi"><div class="l">上昇銘柄</div><div class="v">${mk.breadth_pct}%</div><div class="s">幅</div></div>
        <div class="kpi"><div class="l">20日</div><div class="v ${cls(mk.trail20_pct)}">${fmtPct1(mk.trail20_pct)}</div><div class="s">等加重</div></div>
        <div class="kpi"><div class="l">25MA乖離</div><div class="v ${cls(mk.ma_dev_pct)}">${fmtPct1(mk.ma_dev_pct)}</div><div class="s">等加重</div></div></div>
      ${segs ? `<div class="chips" style="margin-top:8px">${segs}</div>` : ""}</div>` : ""}
    <h2>🐵 モメンタム <span class="sub">${DATA.data_date || ""} 終値・強さ/過熱ランキング</span></h2>
    ${vnote("買い推奨ではありません。S ほど翌10日の平均はマイナス（過熱の目印）。", "眺める／ウォッチ用。売り印と空売り残高の読み方は上の🔥カードに。残高は週次（金曜分・火曜公表）＝日々の変化は映らない。")}
    ${momShortSummary(rk)}
    <div class="gchips">${chip("all", `すべて ${rk.length}`)}${chip("S", `S ${gc.S || 0}`)}${chip("A", `A ${gc.A || 0}`)}${chip("B", `B ${gc.B || 0}`)}${chip("C", "C")}${chip("D", "D")}<a class="gchip ${MOM_SHORT ? "on" : ""}" onclick="event.preventDefault();toggleMomShort()">🔥 空売り多 ${rk.filter(momHeavy).length}</a><a class="gchip ${MOM_SORT === "short" ? "on" : ""}" onclick="event.preventDefault();toggleMomSort()">${MOM_SORT === "short" ? "📉 売残多い順 ✓" : "📉 売残多い順"}</a></div>
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
  return `<details class="card"><summary>🏆 勝ちやすい順張り <span class="muted" style="font-weight:400;font-size:14px">持ち越し(20〜40日)用・上位${w.top_n || 20}</span></summary>
    <div class="dbody">${vnote("「勢いが強い順」ではなく「静かに上がり続けている中低位株」の順。20〜40日持つ前提。", "翌日の寄りで買って20〜40日持つ前提。デイトレ向きではありません（5日では+0.19%）。")}
    ${ms.length ? `<div class="card tight">${ms.map(row).join("")}</div>` : `<div class="empty">本日は該当なし</div>`}
    <div class="note">ルール: ${esc(w.rule || "")}。検証${esc(st.period || "")}: 上位20を翌日寄りで買って20日後＝<b>市場平均比${esc(st.vs_market_20d || "")}</b>・勝ち年${esc(st.win_years || "")}・時代別 ${esc(st.eras || "")}。</div></div></details>`;
}
