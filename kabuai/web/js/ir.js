// ir.js — 📰 IR（適時開示）の好材料・悪材料（v7.5・2026-10-17 本人「brisk基準のIRの取得＋内容の精査→上がる/下がるを君が判断・既出も含めて」）
// データ: latest.json["ir"]（ir_builder.py）。元は TDnet 直接・分類の基準は brisk のタグ体系（brisk のデータ自体は載せない＝規約）。
// 判定: 📏ルール（表題の語）→ 🤖Claude（PDF冒頭を読んだ判定・ir_judge.py・キーがある時だけ）。最終判定 final は Claude があればそれ。
// 答え合わせ: 📌既出（15:30までの開示）はその日の終値、🌙引け後は翌営業日の寄りギャップ/終値。stats に好/悪ごとの平均と的中率。
let IR_TAB = "good";    // good | bad | check | all
let IR_DAY = "today";   // today | window
function irSetTab(t) { IR_TAB = t; render(); }
function irSetDay(d) { IR_DAY = d; render(); }
const irLab = f => { const l = (f || {}).label; return l === "好材料" ? ["good", "👍 好材料"] : l === "悪材料" ? ["bad", "👎 悪材料"] : l === "要精査" ? ["chk", "🔍 要精査"] : ["neu", "— 中立"]; };
function irBadge(f) {
  const [c, t] = irLab(f); const s = Number((f || {}).score);
  return `<span class="irv ${c}">${t}${!isNaN(s) && s !== 0 ? ` ${s > 0 ? "+" : ""}${s}` : ""}</span>`;
}
function irReact(r) {
  const x = r.react || {};
  const g = x.next_gap, c = x.next_chg, d = x.day_chg;
  if (r.intraday) {
    const day = d != null ? `その日の終値 <b class="${cls(d)}">${fmtPct(d)}</b>` : `その日の終値 —`;
    const nxt = g != null ? ` ／ 翌日 寄り <b class="${cls(g)}">${fmtPct(g)}</b>・終値 <b class="${cls(c)}">${fmtPct(c)}</b>` : "";
    return `📌 既出（${esc(r.time)}）: ${day}${nxt}`;
  }
  if (g == null) return `🌙 引け後（${esc(r.time)}）: 翌営業日の寄りで答え合わせ`;
  return `🌙 引け後（${esc(r.time)}）→ ${esc(x.next_date || "翌日")} 寄り <b class="${cls(g)}">${fmtPct(g)}</b>・終値 <b class="${cls(c)}">${fmtPct(c)}</b>`;
}
function irRow(r) {
  const f = r.final || r.rule || {};
  const src = r.llm && r.llm.label ? `<span class="chip">🤖 Claude${r.llm.conf != null ? ` 自信${r.llm.conf}` : ""}${r.llm.priced ? "・織込み済?" : ""}</span>` : `<span class="chip">📏 表題ルール</span>`;
  const ruleDiff = (r.llm && r.llm.label && r.rule && r.rule.label !== r.llm.label) ? `<span class="chip wa">ルールは${esc(r.rule.label)}</span>` : "";
  const tags = (r.tags || []).filter(t => t !== "事務").slice(0, 4).map(t => `<span class="chip wa">${esc(t)}</span>`).join("");
  const meta = [r.code, r.sector, r.close ? "¥" + Math.round(r.close).toLocaleString() : "", r.tov20_oku != null ? `代金${r.tov20_oku}億` : ""].filter(Boolean).map(esc).join(" ・ ");
  return `<div class="irrow">
    <div class="irhead"><a href="#/detail/${esc(r.code)}"><b>${esc(r.name)}</b></a> <small>${meta}</small> ${irBadge(f)}</div>
    <div class="irtitle">${r.pdf ? `<a href="${esc(r.pdf)}" target="_blank" rel="noopener">${esc(r.title)} 📄</a>` : esc(r.title)}</div>
    <div class="chips">${src}${ruleDiff}${tags}</div>
    ${f.reason ? `<div class="note">${esc(f.reason)}${r.llm && r.llm.facts ? `<br><span class="muted">${esc(r.llm.facts)}</span>` : ""}</div>` : ""}
    <div class="cd-line num" style="font-size:13px">${irReact(r)}</div>
  </div>`;
}
function irStatLine(lab, s) {
  if (!s || !s.n) return `${lab}: 0件`;
  const sign = lab === "好材料" ? "上" : "下";
  const nx = s.n_scored ? `翌日 寄り平均 <b class="${cls(s.gap)}">${fmtPct(s.gap)}</b>（${sign}に ${s.gap_hit}%）・終値 <b class="${cls(s.chg)}">${fmtPct(s.chg)}</b>（${sign}に ${s.chg_hit}%）・寄→引 <b class="${cls(s.oc)}">${fmtPct(s.oc)}</b> n=${s.n_scored}` : "翌日の答え合わせ待ち";
  const dy = s.n_day ? ` ／ 既出のその日 <b class="${cls(s.day)}">${fmtPct(s.day)}</b>（${sign}に ${s.day_hit}%・n=${s.n_day}）` : "";
  return `${lab} ${s.n}件: ${nx}${dy}`;
}
function irSection() {
  const ir = DATA.ir;
  const title = `<div class="hh">📰 IRの好材料・悪材料 <span class="sub">${ir && ir.window ? `${esc(ir.window[ir.window.length - 1])}〜${esc(ir.window[0])}・TDnet` : ""}</span></div>`;
  if (!ir) return title + `<div class="card"><div class="empty">まだ作られていません（夕方のビルドで反映）。</div></div>`;
  if (ir.error) return title + `<div class="card"><div class="empty">今回は作れませんでした（${esc(ir.error)}）。次のビルドで再試行。</div></div>`;
  const rows = ir.rows || [];
  const today = (ir.window || [])[0] || "";
  const inDay = r => IR_DAY === "today" ? r.date === today : true;
  const base = rows.filter(inDay);
  const lab = r => ((r.final || r.rule || {}).label);
  const good = base.filter(r => lab(r) === "好材料"), bad = base.filter(r => lab(r) === "悪材料"), chk = base.filter(r => lab(r) === "要精査");
  const neu = base.filter(r => lab(r) === "中立" && !r.admin), adm = base.filter(r => lab(r) === "中立" && r.admin);
  const byScore = (a, b) => Math.abs(Number((b.final || {}).score) || 0) - Math.abs(Number((a.final || {}).score) || 0) || (b.date + b.time).localeCompare(a.date + a.time);
  const seg1 = `<div class="seg"><a class="${IR_DAY === "today" ? "on" : ""}" onclick="irSetDay('today')">今日 ${esc(today)}</a><a class="${IR_DAY === "window" ? "on" : ""}" onclick="irSetDay('window')">既出を含む ${(ir.window || []).length}日</a></div>`;
  const seg2 = `<div class="seg"><a class="${IR_TAB === "good" ? "on" : ""}" onclick="irSetTab('good')">👍 好 ${good.length}</a><a class="${IR_TAB === "bad" ? "on" : ""}" onclick="irSetTab('bad')">👎 悪 ${bad.length}</a><a class="${IR_TAB === "check" ? "on" : ""}" onclick="irSetTab('check')">🔍 要精査 ${chk.length}</a><a class="${IR_TAB === "all" ? "on" : ""}" onclick="irSetTab('all')">📋 全部 ${base.length}</a></div>`;
  const st = ir.stats || {}, sr = st.rule || {}, sl = st.llm || {};
  const llm = ir.llm || {};
  const llmLine = llm.available ? `🤖 Claude判定 ${llm.judged || 0}件（${esc(llm.model || "")}）${llm.note ? " ・ " + esc(llm.note) : ""}` : `🤖 Claude判定は未稼働（${esc(llm.note || "ANTHROPIC_API_KEY が未設定")}）→ いまは📏表題ルールだけ`;
  const honest = `<div class="banner info"><b>上がる/下がるの判定であって、翌日買えば儲かるという意味ではありません。</b>答え合わせ（直近${st.days || 0}営業日・${esc(st.from || "")}〜${esc(st.to || "")}）:<br>
    📏ルール ${irStatLine("好材料", sr["好材料"])}<br>📏ルール ${irStatLine("悪材料", sr["悪材料"])}
    ${sl["好材料"] && sl["好材料"].n ? `<br>🤖Claude ${irStatLine("好材料", sl["好材料"])}<br>🤖Claude ${irStatLine("悪材料", sl["悪材料"])}` : ""}
    <br>${llmLine}</div>`;
  let list;
  if (IR_TAB === "good") list = good.sort(byScore);
  else if (IR_TAB === "bad") list = bad.sort(byScore);
  else if (IR_TAB === "check") list = chk.sort((a, b) => (b.tov20_oku || 0) - (a.tov20_oku || 0));
  else list = [...good, ...bad, ...chk].sort(byScore);
  const body = list.length ? `<div class="card tight">${list.map(irRow).join("")}</div>` : `<div class="card"><div class="empty">該当なし（${IR_DAY === "today" ? "今日の開示にはまだ無い・夕方のビルドで増える" : "この窓には無い"}）。</div></div>`;
  const rest = IR_TAB === "all" ? `<details class="card" style="margin-top:8px"><summary style="cursor:pointer;font-weight:700">中立 ${neu.length}件（材料性が薄い）</summary><div style="margin-top:6px">${neu.sort((a, b) => (b.date + b.time).localeCompare(a.date + a.time)).map(irRow).join("")}</div></details>
    <details class="card" style="margin-top:8px"><summary style="cursor:pointer;font-weight:700">事務開示 ${adm.length}件（CG報告書・定款・招集・英文・説明会資料・人事）</summary><div class="note" style="margin-top:6px">${adm.map(r => `${esc(r.time)} <a href="#/detail/${esc(r.code)}">${esc(r.name)}</a> ${esc(r.title)}`).join("<br>")}</div></details>` : "";
  const tagc = Object.entries(ir.tags_count || {}).filter(([k]) => !["その他", "事務"].includes(k)).sort((a, b) => b[1] - a[1]).slice(0, 14).map(([k, v]) => `<span class="chip wa">${esc(k)} ${v}</span>`).join("");
  const foot = `<div class="note" style="margin-top:4px">📏 ${(ir.rules || []).map(esc).join(" ／ ")}<br>${esc(ir.basis || "")}<br>取得: ${Object.entries(ir.fetched || {}).slice(0, 4).map(([d, n]) => `${esc(d.slice(5))} ${n == null ? "×" : n + "件"}`).join("・")} ・ 生成 ${esc(ir.generated_at || "")}</div>`;
  return title + seg1 + seg2 + honest + body + rest + (tagc ? `<div class="chips" style="margin-top:8px">${tagc}</div>` : "") + foot;
}
