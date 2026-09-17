// plan.js — 📋朝の作戦（2026-09-17 本人依頼「デイトレ用に特化・最強のツールに・ニュースを拾って明日狙う銘柄」
//           → v5.2.2「さらにわかりやすく勝ちやすく」: 今やること(時刻)・現金余力→今日のサイズ・保有中の出口・作戦のいま(ライブ)・答え合わせ）
// 出す物は全部「検証済みルールの機械的な出力」。新しい予想は足さない。
function planOrderRow(o, sizeOverride) {
  const size = sizeOverride || o.size;
  const warn = o.warn ? `<div class="note" style="margin-top:2px;color:var(--dn)">${esc(o.warn)}</div>` : "";
  const off = /撃たない|紙|なし/.test(size || "");
  const sizeCls = off ? "" : "dn";
  return `<a class="pickrow" href="#/detail/${o.code}"><div class="pk-nm"><b><span class="chip ${sizeCls}" style="font-weight:800">${esc(o.system)}</span> ${esc(o.name)}</b>
      <small>${o.code} ・ ${esc(o.side)} ・ <b>${esc(size)}</b>${o.iss ? " ・ 貸借" + esc(o.iss) : ""}${o.note ? " ・ " + esc(o.note) : ""}</small>
      <div class="chips"><span class="chip ${sizeCls}">${off ? "（今日は出さない）" : esc(o.order)}</span></div>${warn}</div></a>`;
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

// ── 今やること（JSTの時計で決める・端末時計）──
function planNow(target_date) {
  const now = new Date(Date.now() + (new Date().getTimezoneOffset() + 540) * 60000);   // JST
  const hm = now.getHours() * 100 + now.getMinutes();
  const ymd = now.toISOString().slice(0, 10);
  const dow = now.getDay();
  if (dow === 0 || dow === 6) return ["🛌 休場日", "作戦は次の営業日分。🎯前夜の準備で材料と決算を確認。"];
  if (target_date && ymd < target_date) return ["🌙 前夜", "明日の注文を確認して寝る。ハイカラ在庫は19時以降に押さえる（フェードは配信の帯どおり）。"];
  if (target_date && ymd > target_date) return ["📅 過去分", "この作戦は終わった日の分。次のビルドで更新。"];
  if (hm < 859) return ["⏰ 寄り前：注文を出す時間", "下の注文をそのまま出す。フェードは寄り成行（下寄りでも建てる・売り禁だけ料の帯で種別を変える）。崩壊/極上の寄指は線より下で寄ったら約定しない＝それで正解（追いかけない）。"];
  if (hm < 1455) return ["🙅 場中：触らない", "日計り（フェード/崩壊）は引けまで持つ。損切りは置かない（日中ストップは26年で全部劣化）。3日持ちの玉は出口日だけ確認。"];
  if (hm < 1531) return ["🔔 引け：手仕舞いの時間", "日計りは15:25までに『引け成行』で買い戻し。今日が出口日の3日持ちも引け成行。持ち越し禁止。"];
  return ["📝 引け後：答え合わせ", "下の『直近の答え合わせ』と🎯前夜の準備（17時のビルドで材料が載る）へ。"];
}

// ── 現金余力 → 今日のサイズ（ラダーの機械的な適用・端末に保存）──
function planCash() { try { const v = localStorage.getItem("kabuai_cash"); return v == null ? null : Number(v); } catch (e) { return null; } }
function planSetCash(v) { try { const n = Number(String(v).replace(/[^\d.]/g, "")); if (isFinite(n)) localStorage.setItem("kabuai_cash", String(n)); } catch (e) { /* ignore */ } render(); }
function planSizes(cash) {
  // 単位: 万円。ラダー（memory/project_daytrade_signal・project_sellwatch_short_candidate）
  if (cash == null) return null;
  if (cash < 15) return { fade1: "①50万", fade2: "なし", kiwami_sell: "新規停止", crash: "なし", gokujo: "150万", level: "15万割れ＝縮小", cls: "dn" };
  if (cash < 30) return { fade1: "①100万", fade2: "なし", kiwami_sell: "新規停止", crash: "なし", gokujo: "150万", level: "30万割れ＝②ゼロ・売り新規停止", cls: "dn" };
  if (cash < 50) return { fade1: "①100万", fade2: "②50万", kiwami_sell: "100万×3", crash: "なし（余裕がない）", gokujo: "150万", level: "通常（崩壊は見送り）", cls: "" };
  if (cash < 100) return { fade1: "①100万", fade2: "②50万", kiwami_sell: "100万×3", crash: "50万", gokujo: "150万", level: "通常", cls: "" };
  return { fade1: "①130万", fade2: "②50万", kiwami_sell: "100万×3", crash: "100万", gokujo: "150万", level: "100万到達＝①130・崩壊100万", cls: "pos" };
}
function planSizeFor(o, sz) {
  if (!sz) return null;
  if (o.system.startsWith("🩳")) return o.size.startsWith("①") ? sz.fade1 : sz.fade2;
  if (o.system.startsWith("💥")) return /撃たない/.test(o.size) ? o.size : sz.crash;
  if (o.system.startsWith("🔻極み売り")) return sz.kiwami_sell;
  if (o.system.startsWith("👑")) return sz.gokujo;
  return null;
}
function planCashBox(cash, sz) {
  const val = cash == null ? "" : String(cash);
  const table = sz ? `<div class="chips" style="margin-top:6px"><span class="chip ${sz.cls}">${esc(sz.level)}</span><span class="chip">フェード ${esc(sz.fade1)}／${esc(sz.fade2)}</span><span class="chip">崩壊 ${esc(sz.crash)}</span><span class="chip">極み売り ${esc(sz.kiwami_sell)}</span><span class="chip">極上 ${esc(sz.gokujo)}</span></div>`
    : `<div class="note" style="margin-top:4px">現金余力（万円）を入れると、今日のサイズがラダー通りに決まります（端末に保存）。</div>`;
  return `<div class="card" style="margin-bottom:10px"><div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap"><b>💰 現金余力</b>
      <input id="plan-cash" inputmode="decimal" value="${esc(val)}" placeholder="例 50" style="width:80px;padding:6px 8px;border:1px solid var(--ln);border-radius:8px;background:var(--sf2);color:var(--tx)"> <span class="muted">万円</span>
      <button class="hbtn" onclick="planSetCash(document.getElementById('plan-cash').value)">決定</button></div>${table}</div>`;
}

// ── 作戦のいま（🔥ライブがある時だけ・寄指ラインの判定＋含み）──
function planLiveInner() {
  const p = DATA.plan; if (!p) return "";
  if (typeof LIVE === "undefined" || !LIVE || !LIVE.stocks) return "";
  const items = [];
  for (const o of (p.orders || [])) {
    const m = LIVE.stocks[o.code]; if (!m) continue;
    const short = o.side === "空売り";
    const lineOk = o.order.includes("寄指") ? (short ? (m.gap == null || m.gap > -3) : true) : true;
    const openPx = (o.prev != null && m.gap != null) ? o.prev * (1 + m.gap / 100) : null;
    const pnl = (openPx && m.last) ? (short ? (openPx - m.last) / openPx * 100 : (m.last - openPx) / openPx * 100) : null;
    items.push(`<a class="pickrow" href="#/detail/${o.code}"><div class="pk-nm"><b>${esc(o.system)} ${esc(o.name)}</b>
      <small>${o.code} ・ 寄り ${m.gap == null ? "—" : fmtPct1(m.gap)} → <b>${lineOk ? "建った想定" : "線より下で寄り＝見送り"}</b> ・ 現在 ${m.last != null ? yen(m.last) : "—"} <span class="${cls(m.chg)}">${fmtPct1(m.chg)}</span></small></div>
      <div class="m-px"><span class="${cls(pnl)}">${pnl == null ? "—" : fmtPct1(pnl)}</span><br><small class="muted">含み</small></div></a>`);
  }
  for (const h of (p.holdings || [])) {
    const m = LIVE.stocks[h.code]; if (!m || !h.entry_open || !m.last) continue;
    const pnl = h.side === "空売り" ? (h.entry_open - m.last) / h.entry_open * 100 : (m.last - h.entry_open) / h.entry_open * 100;
    items.push(`<a class="pickrow" href="#/detail/${h.code}"><div class="pk-nm"><b>${esc(h.system)} ${esc(h.name)}</b>
      <small>${h.code} ・ 建値 ${yen(h.entry_open)} → 現在 ${yen(m.last)} <span class="${cls(m.chg)}">${fmtPct1(m.chg)}</span>${h.exit_today ? " ・ <b style='color:var(--dn)'>今日が出口日＝引け成行</b>" : ""}</small></div>
      <div class="m-px"><span class="${cls(pnl)}">${fmtPct1(pnl)}</span><br><small class="muted">含み</small></div></a>`);
  }
  if (!items.length) return "";
  return `<div class="hh">🔥 作戦のいま <span class="sub">${esc(LIVE.hhmm || "")} ${esc((typeof LIVE_STATE !== "undefined" && LIVE_STATE[LIVE.state]) || "")}</span></div><div class="card tight">${items.join("")}</div>`;
}

function planHoldRow(h) {
  const today = h.exit_today ? `<span class="chip dn" style="font-weight:800">今日が出口日 → 大引け成行</span>` : `<span class="chip">出口 ${esc(h.exit_date || "")}（3営業日目の大引け）</span>`;
  const un = h.unrealized != null ? ` ・ 含み <span class="${cls(h.unrealized)}">${fmtPct1(h.unrealized)}</span>（前日終値）` : "";
  return `<a class="pickrow" href="#/detail/${h.code}"><div class="pk-nm"><b><span class="chip" style="font-weight:800">${esc(h.system)}</span> ${esc(h.name)}</b>
      <small>${h.code} ・ ${esc(h.side)} ${esc(h.size)} ・ 建値 ${h.entry_open != null ? yen(h.entry_open) : "—"}（${esc(h.entry_date || "")}）・ ${h.hold_days != null ? h.hold_days + "日目" : ""}${un}</small>
      <div class="chips">${today}</div><div class="note" style="margin-top:2px">${esc(h.rule)}</div>${h.warn ? `<div class="note" style="color:var(--dn)">${esc(h.warn)}</div>` : ""}</div></a>`;
}
function planRecent(recent) {
  if (!(recent || []).length) return "";
  const rows = recent.map(r => `<div class="card" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;align-items:baseline;gap:8px;flex-wrap:wrap"><b>${esc(r.system)}</b><small class="muted">${esc(r.kind)}</small></div>
      <div style="margin-top:4px">直近${r.n}本 勝ち${r.win} ・ 平均 <span class="${cls(r.avg)}">${fmtPct(r.avg)}</span>/本</div>
      <div class="chips" style="margin-top:4px">${(r.last || []).map(x => `<span class="chip ${x.pnl > 0 ? "pos" : x.pnl < 0 ? "dn" : ""}">${esc(x.date.slice(5))} ${esc(x.name)} ${fmtPct1(x.pnl)}</span>`).join("")}</div></div>`).join("");
  return `<details class="card" style="margin-top:14px"><summary style="cursor:pointer;font-weight:700">📝 直近の答え合わせ（帳簿ベース・あなたの約定とは別物）</summary><div style="margin-top:8px">${rows}</div>
    <div class="note">連敗は年4〜5回の想定内。短期不調でもルールを変えない（BT健在ならサンプル誤差）。数字は帳簿＝紙/実弾の別は各行の注記。</div></details>`;
}

function viewPlan() {
  const p = DATA.plan;
  if (!p) return `<h2>📋 朝の作戦</h2><div class="card"><div class="empty">作戦データがまだありません（次のビルドで反映）。</div></div><p class="disc">${esc(DATA.disclaimer)}</p>`;
  const orders = p.orders || [], paper = p.paper || [], holdings = p.holdings || [];
  const [nowTitle, nowText] = planNow(p.target_date);
  const cash = planCash(), sz = planSizes(cash);
  const head = `<h2>📋 朝の作戦 <span class="sub">${esc(p.target_date || "")} 分（${esc(p.date || "")} 引け・${esc(p.generated_at || "")} 生成）</span></h2>
    <div class="banner dn" style="margin-bottom:8px"><b>${esc(nowTitle)}</b><br><span style="font-size:12.5px">${esc(nowText)}</span></div>
    ${planCashBox(cash, sz)}
    <div id="plan-live">${planLiveInner()}</div>`;
  // ① 実弾の注文
  const live = orders.filter(o => !(sz && /なし|停止/.test(planSizeFor(o, sz) || "")));
  const lead = orders.length
    ? `<div class="banner dn">🔴 <b>今日の実弾 ${live.length}本</b>：書いてある注文をそのまま出す。日計り（フェード/崩壊）は<b>必ず大引けで手仕舞い</b>。${live.length < orders.length ? `<br><span style="font-size:12px">現金余力のラダーで ${orders.length - live.length}本は今日出さない。</span>` : ""}</div>`
    : `<div class="banner"><b>今日は撃つ玉なし</b><br><span class="muted" style="font-size:12px">フェードGO・💥崩壊◎・極上・極み売りの4系統すべて該当なし＝撃たないのが正解の日。</span></div>`;
  const ordersHtml = `<div class="hh">🔴 実弾の注文 <span class="sub">フェード①100/②50・崩壊◎50・極上150・極み売り3×100</span></div>${lead}${orders.length ? `<div class="card tight">${orders.map(o => planOrderRow(o, planSizeFor(o, sz))).join("")}</div>` : ""}`;
  // ② 保有中の玉と出口
  const holdHtml = holdings.length ? `<div class="hh" style="margin-top:14px">📦 保有中の玉と出口 <span class="sub">${holdings.filter(h => h.exit_today).length}本が今日出口</span></div><div class="card tight">${holdings.map(planHoldRow).join("")}</div>` : "";
  // ③ 紙・撃たない
  const paperHtml = paper.length ? `<details class="card" style="margin-top:10px"><summary style="cursor:pointer;font-weight:700">📝 紙の対照・撃たない玉 ${paper.length}件</summary><div class="list" style="margin-top:6px">${paper.map(o => planOrderRow(o)).join("")}</div></details>` : "";
  // ④ 答え合わせ ⑤ ラダー
  const recentHtml = planRecent(p.recent);
  const ladderHtml = `<details class="card" style="margin-top:14px"><summary style="cursor:pointer;font-weight:700">💰 資金ラダー（ルール）</summary><div class="note" style="margin-top:6px">${(p.ladder || []).map(l => `<div>・${esc(l)}</div>`).join("")}</div></details>`;
  return head + ordersHtml + holdHtml + paperHtml + recentHtml + ladderHtml + `<p class="disc">${esc(DATA.disclaimer)}</p>`;
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
