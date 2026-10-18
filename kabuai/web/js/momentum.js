// momentum.js — 🐵EOD（モメンタム指数ランキング・🏆勝ちやすい順張り・持ち株コーチ・ウォッチ）＋ 🩳空売り（v8.4・2026-10-19 本人「この空売りとモメンタムはタブわけて」）
// 指数の高さ＝予測ではない（Sは翌10日平均-2.65%）。眺める/ウォッチ用と明示（v4方針を継承）。
let MOM_GRADE = "all";
let MOM_SHORT = false;      // v8: 🔥空売り多だけ（本人「売り金マークと空売り残高多いやつをわかりやすく」2026-10-17）
let MOM_SORT = "short";     // v8.1: "rank"=勢い順 / "short"=売残(出来高の日数)多い順（v8.4からは🩳空売りタブ側の状態・既定は売残多い順）
let SH_FILTER = "all";      // v8.4 🩳: all / heavy(🔥空売り多) / stop(🚫売り禁) / ok(貸借○)
function setMomGrade(g) { MOM_GRADE = g; render(); }
function toggleMomShort() { MOM_SHORT = !MOM_SHORT; render(); }
function toggleMomSort() { MOM_SORT = MOM_SORT === "short" ? "rank" : "short"; render(); }
function setShFilter(f) { SH_FILTER = f; render(); }
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
// v8.4.2（2026-10-19 本人「取組0.4とかわかりずらい・細かな数字わかりやすく・線の色よくわからん」）: 取組倍率と色の棒をやめて言葉で
//   売り方：買い方 ＝ 売残 vs 買残。ratio(買残÷売残) ≤1 → 売り長 / ≤2 → 互角 / それ以上 → 買い長
function momSideWord(r) {
  if (r.ratio == null) return null;
  const q = Number(r.ratio);
  if (!(q > 0)) return { w: "売り長", cls: "hot", d: "買い方ほぼゼロ" };
  const d = `売り1：買い${q >= 10 ? Math.round(q) : q.toFixed(1)}`;
  if (q <= 1) return { w: "売り長", cls: "hot", d };
  if (q <= 2) return { w: "互角", cls: "wa", d };
  return { w: "買い長", cls: "", d };
}
//   重さ ＝ 全部買い戻すのにかかる日数（1日の出来高で）。🔥重い＝1日分以上か売り長 / やや重い＝0.5日分以上 / 軽い
function momWeight(r) {
  if (r.short_days == null && r.ratio == null) return null;
  if (momHeavy(r)) return { w: "🔥 重い", cls: "hot" };
  if (r.short_days != null && r.short_days >= 0.5) return { w: "やや重い", cls: "wa" };
  return { w: "軽い", cls: "mut" };
}
function momShortMeter(r) {
  if (r.short_vol == null) return "";
  const d = r.short_days, hv = momHeavy(r), side = momSideWord(r), wt = momWeight(r);
  const chg = r.short_chg != null ? `<i class="${r.short_chg >= 0 ? "pos" : "neg"}">先週より ${r.short_chg >= 0 ? "+" : ""}${Math.round(r.short_chg)}%</i>` : "";
  const rows = [
    `<div class="sm-r"><span class="k">空売り残</span><span class="v"><b>${fmtKabu(r.short_vol)}</b> ${chg}</span></div>`,
    d != null ? `<div class="sm-r"><span class="k">買い戻しに</span><span class="v"><b class="${d >= 1 ? "hot" : d >= 0.5 ? "wa" : ""}">${Number(d).toFixed(1)}日分</b> <i>の出来高</i></span></div>` : "",
    side ? `<div class="sm-r"><span class="k">売り方：買い方</span><span class="v"><b class="${side.cls}">${side.w}</b> <i>${side.d}</i></span></div>` : "",
    wt ? `<div class="sm-r"><span class="k">重さ</span><span class="v"><b class="${wt.cls}">${wt.w}</b></span></div>` : "",
  ].join("");
  return `<div class="smeter ${hv ? "heavy" : ""}">${rows}</div>`;
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
      <div class="s-nm"><b>${esc(r.name)}</b><small>${r.code} ・ 売残${fmtKabu(r.short_vol)}${r.short_chg != null ? `（前週${r.short_chg >= 0 ? "+" : ""}${Math.round(r.short_chg)}%）` : ""}${r.jsf_stop ? ' ・ <span class="neg" style="font-weight:800">🚫売り禁</span>' : ""}${r.jsf_warn ? ' ・ <span class="warn">⚠️注意喚起</span>' : ""}</small>${(() => { const sd = momSideWord(r); return sd ? `<small><b class="${sd.cls}">${sd.w}</b>（${sd.d}）</small>` : ""; })()}</div>
      <div class="s-kv"><div class="v ${r.short_days != null && r.short_days >= 1 ? "hot" : ""}">${r.short_days != null ? Number(r.short_days).toFixed(1) + "日分" : "—"}</div><div class="l">買い戻しに</div></div></a>`;
  return `<div class="card shortsum"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>🔥 空売り残高が多い ${heavy.length}本</b><span class="muted" style="font-size:13.5px">ランキング${rk.length}本のうち・残高は ${esc(mdate)} 時点（週次）</span></div>
    <div class="note" style="margin:4px 0 6px">「買い戻しに◯日分」＝売り方が全部買い戻すのにかかる日数（1日の出来高で）。<b>1日分以上</b>か<b>売り長（売り方≧買い方）</b>で🔥。上がると売り方の買い戻しが乗る＝踏み上げの燃料。売り方は逆日歩に注意。</div>
    ${heavy.length ? `<div class="tight">${heavy.map(row).join("")}</div>` : `<div class="empty">今週は該当なし</div>`}
    <div style="margin-top:10px;display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><b>🚫 売り禁 ${stop.length}本</b><span class="muted" style="font-size:13.5px">制度信用の新規売り停止＝個人は普通には空売りできない（SBI一日信用HYPER/一般信用は別枠在庫）</span></div>
    ${stop.length ? `<div class="chips" style="margin-top:6px">${stop.map(r => `<a class="sbadge dn" href="#/detail/${r.code}">🚫 ${esc(r.name)} <span style="opacity:.75;font-weight:600">${r.code}</span></a>`).join("")}</div>` : `<div class="empty" style="padding:6px 0">売り禁なし</div>`}
    <div class="note" style="margin-top:8px">印の読み方: <span class="sbadge dn lg">🚫 売り禁</span>＝制度信用で新規売り不可 ／ <span class="sbadge up lg">貸借○ 空売り可</span>＝制度信用で空売りできる ／ <span class="sbadge mut lg">制度×</span>＝信用買いだけの銘柄 ／ <span class="sbadge wa lg">⚠️ 注意喚起</span>＝日証金の注意喚起（逆日歩警戒） ／ <span class="sbadge hot lg">🔥 空売り多</span>＝売残が重い。重さ: 🔥重い＝買い戻しに1日分以上か売り長 ／ やや重い＝0.5日分以上 ／ 軽い＝それ未満。</div></div>`;
}
function viewMomentum() {
  if (WATCH.length || loadPos().length) loadSearch().then(() => {
    const el = document.getElementById("watchsum"); if (el) el.innerHTML = watchSummaryInner();
    const pc = document.getElementById("poscoach"); if (pc) pc.innerHTML = posCoach();
  });
  const rk = DATA.ranking || [], gc = DATA.grade_counts || {};
  const chip = (g, lbl) => `<a class="gchip ${MOM_GRADE === g ? "on" : ""}" onclick="event.preventDefault();setMomGrade('${g}')">${lbl}</a>`;
  const list = MOM_GRADE === "all" ? rk : rk.filter(r => r.grade === MOM_GRADE);   // v8.4: 🔥/📉 の絞り込みは🩳空売りタブへ
  const sw = DATA.sell_watch, swCodes = new Set(((sw && sw.members) || []).map(m => m.code));
  const rows = list.map(r => `<a class="mrow" href="#/detail/${r.code}">
      <div class="grb ${esc(r.grade || "D")}">${esc(r.grade || "-")}</div>
      <div class="m-nm"><b>${esc(r.name)}${swCodes.has(r.code) ? ' <span class="chip dn" style="padding:0 6px">🔻終了サイン</span>' : ""}${r.days_cover != null && r.days_cover < 0.25 ? ' <span class="chip up" style="padding:0 6px">🪶信用軽</span>' : ""}${r.jsf_stop ? ' <span class="chip dn" style="padding:0 6px">🚫売り禁</span>' : ""}</b>
        <small>${r.code} ・ 勢い${r.momentum != null ? Math.round(r.momentum) : "—"} ・ 5日 ${fmtPct(r.r5)} ・ 代金${r.turnover_oku != null ? Math.round(r.turnover_oku) + "億" : "—"} ・ 値動き${r.rng20 != null && isFinite(Number(r.rng20)) ? Number(r.rng20).toFixed(1) + "%" : "—"}</small></div>
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
    ${vnote("買い推奨ではありません。S ほど翌10日の平均はマイナス（過熱の目印）。", "眺める／ウォッチ用。売り禁・貸借・空売り残高は🩳空売りタブに分けた（同じ50本）。")}
    <div class="gchips">${chip("all", `すべて ${rk.length}`)}${chip("S", `S ${gc.S || 0}`)}${chip("A", `A ${gc.A || 0}`)}${chip("B", `B ${gc.B || 0}`)}${chip("C", "C")}${chip("D", "D")}</div>
    <div class="card tight">${rows || `<div class="empty">該当なし</div>`}</div>
    ${winnableSection()}
    <p class="disc">${esc(DATA.disclaimer)}</p>`;
}
// 🩳 空売り（v8.4）: ランキング50本の 売り禁/貸借/注意喚起 と 空売り残高（売残・出来高の日数・取組・前週比）だけを集めた画面
function viewShort() {
  const rk = DATA.ranking || [];
  const mdate = (rk.find(r => r.margin_date) || {}).margin_date || "";
  const have = rk.filter(r => r.short_vol != null || r.shortable != null || r.jsf_stop);
  if (!rk.length || !have.length) return `<h2>🩳 空売り <span class="sub">${DATA.data_date || ""} 終値</span></h2><div class="card empty">貸借区分・空売り残高がまだ付いていません（次のビルドで）</div><p class="disc">${esc(DATA.disclaimer)}</p>`;
  const nHeavy = rk.filter(momHeavy).length, nStop = rk.filter(r => r.jsf_stop).length, nOk = rk.filter(r => r.shortable === true && !r.jsf_stop).length;
  let list = SH_FILTER === "heavy" ? rk.filter(momHeavy) : SH_FILTER === "stop" ? rk.filter(r => r.jsf_stop) : SH_FILTER === "ok" ? rk.filter(r => r.shortable === true && !r.jsf_stop) : rk;
  if (MOM_SORT !== "rank") list = [...list].sort((a, b) => ((b.short_days ?? -1) - (a.short_days ?? -1)) || ((a.ratio ?? 99) - (b.ratio ?? 99)));
  const fchip = (k, lbl) => `<a class="gchip ${SH_FILTER === k ? "on" : ""}" onclick="event.preventDefault();setShFilter('${k}')">${lbl}</a>`;
  const rows = list.map(r => `<a class="mrow" href="#/detail/${r.code}">
      <div class="grb ${esc(r.grade || "D")}">${esc(r.grade || "-")}</div>
      <div class="m-nm"><b>${esc(r.name)}</b><small>${r.code} ・ ${yen(r.price)} ・ 前日比 ${fmtPct(r.r1)} ・ 代金${r.turnover_oku != null ? Math.round(r.turnover_oku) + "億" : "—"}</small>${momShortChips(r)}</div></a>`).join("");
  return `<h2>🩳 空売り <span class="sub">${DATA.data_date || ""} 終値・残高は ${esc(mdate)} 時点（週次）</span></h2>
    ${vnote("ランキング50本の売り禁・貸借・空売り残高。買い/売りの推奨ではありません。", "売残は週次（金曜分・火曜公表）＝日々の変化は映らない。🔥＝買い戻しに1日分以上か売り長（売り方≧買い方）。踏み上げの燃料ではあるが、売残が厚いほど翌20日は悪い（10年n193万・小型 超過−0.74%）＝『売残が多い＝買い』ではない。")}
    ${momShortSummary(rk)}
    <div class="gchips">${fchip("all", `すべて ${rk.length}`)}${fchip("heavy", `🔥 空売り多 ${nHeavy}`)}${fchip("stop", `🚫 売り禁 ${nStop}`)}${fchip("ok", `貸借○ ${nOk}`)}<a class="gchip ${MOM_SORT === "rank" ? "" : "on"}" onclick="event.preventDefault();toggleMomSort()">${MOM_SORT === "rank" ? "🐵 ランキング順" : "📉 売残多い順 ✓"}</a></div>
    <div class="card tight">${rows || `<div class="empty">該当なし</div>`}</div>
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
