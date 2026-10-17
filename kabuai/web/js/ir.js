// ir.js — 📰 IR（適時開示）の好材料・悪材料（v7.5・2026-10-17 本人「brisk基準のIRの取得＋内容の精査→上がる/下がるを君が判断・既出も含めて」）
// データ: latest.json["ir"]（ir_builder.py）。元は TDnet 直接・分類の基準は brisk のタグ体系（brisk のデータ自体は載せない＝規約）。
// 判定: 📏ルール（表題の語）→ 🤖Claude（PDF冒頭を読んだ判定・ir_judge.py・キーがある時だけ）。最終判定 final は Claude があればそれ。
// 答え合わせ: 📌既出（15:30より前の開示・15:30ちょうどは引け後）はその日の終値、🌙引け後は翌営業日の寄りギャップ/終値。stats に好/悪ごとの平均と的中率。
// 🔍開示検索（v7.6・2026-10-17 本人「開示検索…例えば フィジカルAI エヌヴィディア サイバーセキュリティ」）: data/ir_text/<日付>.json（ir_text.py・
//   全開示＋PDF本文の冒頭3ページ2,000字）を検索した時だけ読む。社名・コード・表題・タグ・本文を空白区切りの AND で照合（表記ゆれは IR_SYN）。
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
    const day = d != null ? `その日の終値 <b class="${cls(d)}">${fmtPct(d)}</b><span class="muted">（開示前の動き込み）</span>` : `その日の終値 —`;
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

// ── 🔍 開示検索（v7.6）──
let IR_Q = "";               // 検索語（空白区切り＝AND）
let IR_SCOPE = "window";     // window＝画面の窓（3営業日） | all＝本文のある全日（約15営業日・読み込みが重い）
const IR_TXT = {};           // 日付 → {date, n, n_text, rows}（data/ir_text/<日付>.json）
let IR_IDX = null;           // data/ir_text/index.json（{days:[{date,n,n_text}], n, n_text, pending, note}）・false＝無い
const IR_PENDING = {};       // 読み込み中の日付
let IR_TIMER = null;
const IR_EXAMPLES = ["フィジカルAI", "エヌヴィディア", "サイバーセキュリティ", "蓄電池", "データセンター", "量子", "防衛", "上方修正"];   // 株探の人気テーマが無い時の予備
let IR_HOT_ALL = false;      // 人気テーマのチップを30件全部出すか（既定は上位12）
function irHotToggle() { IR_HOT_ALL = !IR_HOT_ALL; render(); }
// 表記ゆれ（正規化後の語 → 同じ扱いにする語）。正規化＝NFKC・小文字・空白なし
const IR_SYN = {
  "エヌヴィディア": ["エヌビディア", "nvidia"], "エヌビディア": ["エヌヴィディア", "nvidia"], "nvidia": ["エヌビディア", "エヌヴィディア"],
  "フィジカルai": ["physicalai", "フィジカル・ai", "フィジカルエーアイ"], "physicalai": ["フィジカルai"],
  "サイバーセキュリティ": ["サイバーセキュリティー", "cybersecurity", "サイバー攻撃", "サイバー防御"], "セキュリティ": ["セキュリティー", "security"],
  "データセンター": ["datacenter", "データセンタ", "dc事業"], "半導体": ["semiconductor"], "生成ai": ["generativeai", "生成系ai"],
  "ロボット": ["ロボティクス", "robot"], "量子": ["quantum", "量子コンピュータ"], "防衛": ["防衛省", "防衛装備", "defense"],
  "蓄電": ["蓄電池", "蓄電システム", "ess"], "蓄電池": ["蓄電", "蓄電システム", "二次電池", "バッテリー", "ess"], "全固体電池": ["全固体", "固体電池"],
  "人工知能": ["ai", "人工知能"], "情報セキュリティ": ["セキュリティ", "security"], "半導体製造装置": ["製造装置", "半導体装置"],
  "量子コンピューター": ["量子コンピュータ", "量子"], "自動運転車": ["自動運転"], "核融合発電": ["核融合", "フュージョン"], "レアアース": ["希土類"],
  "セキュリティソフト": ["セキュリティ"], "半導体部材・部品": ["半導体"], "光デバイス": ["光通信", "光ファイバ", "光電"], "造船": ["船舶"], "宇宙開発関連": ["宇宙", "衛星"], "ペロブスカイト": ["perovskite"], "核融合": ["フュージョン", "fusion"], "宇宙": ["衛星", "space"],
  "上方修正": ["上方修正", "上振れ", "増額修正"], "下方修正": ["下方修正", "下振れ", "減額修正"], "自社株買い": ["自己株式の取得", "自己株式取得"],
};
const irNorm = s => String(s == null ? "" : s).normalize("NFKC").toLowerCase().replace(/\s+/g, "");
function irTerms(q) {   // 株探のテーマ名の飾り（「〜関連」「〜銘柄」・末尾の長音）も落とした形を同じ語として扱う
  return String(q || "").split(/[\s　,、]+/).map(irNorm).filter(Boolean).map(t => {
    const vs = [t, ...(IR_SYN[t] || []).map(irNorm)];
    const bare = t.replace(/(関連|銘柄)$/, ""); if (bare && bare !== t) vs.push(bare, ...(IR_SYN[bare] || []).map(irNorm));
    const noLong = t.replace(/ー$/, ""); if (noLong && noLong !== t) vs.push(noLong);
    return [...new Set(vs.filter(Boolean))];
  });
}
function irRefresh() {
  const el = document.getElementById("irres"); if (el) el.innerHTML = irSearchResults();
  const m = document.getElementById("irmain"); if (m && m.style) m.style.display = IR_Q.trim() ? "none" : "";
}
function irSearch(q) {            // 入力欄の oninput: 全体を描き直すとカーソルが飛ぶので結果だけ差し替える
  IR_Q = q; clearTimeout(IR_TIMER);
  IR_TIMER = setTimeout(() => { irEnsure(); irRefresh(); }, 220);
}
function irSearchSet(q) { IR_Q = q; irEnsure(); render(); }
function irSetScope(s) { IR_SCOPE = s; irEnsure(); render(); }
function irDays() {
  const ir = DATA && DATA.ir; if (!ir) return [];
  if (IR_SCOPE === "all" && IR_IDX && IR_IDX.days) return IR_IDX.days.map(d => d.date);
  return ir.window || [];
}
async function irLoadDay(d) {
  if (IR_TXT[d] || IR_PENDING[d]) return;
  IR_PENDING[d] = 1;
  const j = await fetchFirst([`../data/ir_text/${d}.json`, `data/ir_text/${d}.json`]);
  delete IR_PENDING[d];
  IR_TXT[d] = (j && Array.isArray(j.rows) && j.date === d) ? j : { date: d, n: 0, n_text: 0, rows: null };   // rows null＝ファイルが無い→latest.json の行で代用
  irRefresh();
}
function irEnsure() {
  if (!IR_Q.trim() || !DATA || !DATA.ir) return;
  if (IR_IDX == null) {
    IR_IDX = false;
    fetchFirst(["../data/ir_text/index.json", "data/ir_text/index.json"]).then(j => { IR_IDX = (j && Array.isArray(j.days)) ? j : false; irDays().forEach(irLoadDay); irRefresh(); });
  }
  irDays().forEach(irLoadDay);
}
function irRowsOf(d) {              // 検索対象の行（本文ファイルがあればそれ・無ければ latest.json の窓の行＝表題だけ）
  const f = IR_TXT[d];
  if (f && f.rows) return f.rows.map(r => ({ ...r, date: d }));
  const ir = DATA.ir; return (ir.rows || []).filter(r => r.date === d).map(r => ({ ...r, label: (r.final || r.rule || {}).label, score: (r.final || r.rule || {}).score, t: "" }));
}
function irSnippet(r, terms) {
  const flat = irNorm(r.t);
  if (!flat) return "";
  let best = -1, len = 0;
  for (const vs of terms) for (const v of vs) { const i = flat.indexOf(v); if (i >= 0 && (best < 0 || i < best)) { best = i; len = v.length; } }
  if (best < 0) return "";
  const a = Math.max(0, best - 60), b = Math.min(flat.length, best + len + 90);
  return `${a > 0 ? "…" : ""}${esc(flat.slice(a, best))}<mark>${esc(flat.slice(best, best + len))}</mark>${esc(flat.slice(best + len, b))}${b < flat.length ? "…" : ""}`;
}
function irSearchResults() {
  const q = IR_Q.trim(); if (!q || !DATA || !DATA.ir) return "";
  const terms = irTerms(q); if (!terms.length) return "";
  const days = irDays();
  const hits = [];
  let nRows = 0, nText = 0;
  for (const d of days) {
    for (const r of irRowsOf(d)) {
      nRows++; if (r.t) nText++;
      const hay = irNorm([r.name, r.code, r.title, (r.tags || []).join(" "), r.t].join(" "));
      if (terms.every(vs => vs.some(v => hay.includes(v)))) hits.push(r);
    }
  }
  hits.sort((a, b) => (b.date + b.time).localeCompare(a.date + a.time));
  const loading = Object.keys(IR_PENDING).length;
  const scope = `<div class="seg" style="margin:6px 0"><a class="${IR_SCOPE === "window" ? "on" : ""}" onclick="irSetScope('window')">窓 ${((DATA.ir.window) || []).length}日</a><a class="${IR_SCOPE === "all" ? "on" : ""}" onclick="irSetScope('all')">本文のある全日${IR_IDX && IR_IDX.days ? ` ${IR_IDX.days.length}日` : ""}（重い）</a></div>`;
  const head = `<div class="note" style="margin-top:6px"><b>🔍「${esc(q)}」 ${hits.length}件</b> ／ ${days.length}日 ${nRows.toLocaleString()}開示（本文あり ${nText.toLocaleString()}）${loading ? ` ・ <span class="muted">読み込み中 ${loading}日…</span>` : ""}${IR_IDX === false ? " ・ <span class=\"muted\">本文ファイルが無い（次のビルドで作られる）→いまは表題だけ</span>" : ""}${IR_IDX && IR_IDX.pending ? ` ・ <span class="muted">未取得の本文 ${IR_IDX.pending}件は次のビルドで</span>` : ""}</div>`;
  const rows = hits.slice(0, 150).map(r => {
    const snip = irSnippet(r, terms);
    const tags = (r.tags || []).filter(t => t !== "事務" && t !== "その他").slice(0, 3).map(t => `<span class="chip wa">${esc(t)}</span>`).join("");
    return `<div class="irrow">
      <div class="irhead"><small>${esc(r.date)} ${esc(r.time)}</small> <a href="#/detail/${esc(r.code)}"><b>${esc(r.name)}</b></a> <small>${esc(r.code)}</small> ${irBadge({ label: r.label, score: r.score })} ${tags}</div>
      <div class="irtitle">${r.pdf ? `<a href="${esc(r.pdf)}" target="_blank" rel="noopener">${esc(r.title)} 📄</a>` : esc(r.title)}</div>
      ${snip ? `<div class="note irsnip">${snip}</div>` : (r.t ? "" : `<div class="note muted">本文は未取得（表題・社名で一致）</div>`)}
      <div class="cd-line num" style="font-size:13px">${irReact(r)}</div>
    </div>`;
  }).join("");
  const more = hits.length > 150 ? `<div class="note">…ほか ${hits.length - 150}件（語を足して絞る）</div>` : "";
  return scope + head + irHotLine(q) + (rows ? `<div class="card tight" style="margin-top:6px">${rows}</div>` : `<div class="empty">該当なし${loading ? "（読み込み中）" : ""}。語を減らすか「本文のある全日」に広げる。</div>`) + more;
}
function irHotThemes() { const h = DATA && DATA.ir && DATA.ir.hot_themes; return (h && Array.isArray(h.themes) && h.themes.length) ? h : null; }
function irChip(k, extra = "") { return `<span class="chip ${IR_Q === k ? "acc" : "wa"}" style="cursor:pointer" onclick="irSearchSet('${esc(k)}')">${extra}${esc(k)}</span>`; }
function irSearchBox() {
  const hot = irHotThemes();
  let chips;
  if (hot) {   // 株探 人気テーマ【ベスト30】（PCから取得・3日間のアクセス順）
    const ts = hot.themes.slice(0, IR_HOT_ALL ? 30 : 12);
    chips = `<span class="muted" style="font-size:11px">株探 人気テーマ（${esc(hot.period || "3日間")}・${esc((hot.page_time || hot.fetched_at || "").slice(5))}）:</span>` + ts.map(t => irChip(t.name, `<small class="muted">${t.rank}</small> `)).join("")
      + (hot.themes.length > 12 ? `<span class="chip" style="cursor:pointer" onclick="irHotToggle()">${IR_HOT_ALL ? "上位12に戻す" : `＋${hot.themes.length - 12}`}</span>` : "");
  } else {
    chips = `<span class="muted" style="font-size:11px">例:</span>` + IR_EXAMPLES.map(k => irChip(k)).join("") + `<span class="muted" style="font-size:11px">（株探の人気テーマは PC の kabutan_hot_pc.py が置くと出る）</span>`;
  }
  return `<div class="card" style="margin-bottom:8px">
    <input id="irq" class="searchbox" type="search" inputmode="text" autocomplete="off" placeholder="🔍 開示を検索：社名・コード・表題・本文（PDF冒頭）" value="${esc(IR_Q)}" oninput="irSearch(this.value)">
    <div class="chips" style="margin-top:6px">${chips}${IR_Q ? `<span class="chip" style="cursor:pointer" onclick="irSearchSet('')">✕ 消す</span>` : ""}</div>
    <div id="irres">${irSearchResults()}</div></div>`;
}
function irHotLine(q) {   // 検索語が株探の人気テーマなら、その代表銘柄（株探の表示順3本）を出す
  const hot = irHotThemes(); if (!hot) return "";
  const t = hot.themes.find(x => irNorm(x.name) === irNorm(q)); if (!t || !(t.stocks || []).length) return "";
  return `<div class="note">株探 人気テーマ ${t.rank}位「${esc(t.name)}」の代表: ${t.stocks.map(s => `<a href="#/detail/${esc(s.code)}">${esc(s.name)}</a>`).join("・")}</div>`;
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
  const honest = vnote("上がる/下がるの判定で、翌日買えば儲かる意味ではありません。", `📏ルール ${irStatLine("好材料", sr["好材料"])}<br>📏ルール ${irStatLine("悪材料", sr["悪材料"])}
    ${sl["好材料"] && sl["好材料"].n ? `<br>🤖Claude ${irStatLine("好材料", sl["好材料"])}<br>🤖Claude ${irStatLine("悪材料", sl["悪材料"])}` : ""}
    <br>${llmLine}`, `答え合わせ（直近${st.days || 0}営業日・${(st.from || "").slice(5)}〜${(st.to || "").slice(5)}）`);
  let list;
  if (IR_TAB === "good") list = good.sort(byScore);
  else if (IR_TAB === "bad") list = bad.sort(byScore);
  else if (IR_TAB === "check") list = chk.sort((a, b) => (b.tov20_oku || 0) - (a.tov20_oku || 0));
  else list = [...good, ...bad, ...chk].sort(byScore);
  const body = list.length ? `<div class="card tight">${list.map(irRow).join("")}</div>` : `<div class="card"><div class="empty">該当なし（${IR_DAY === "today" ? "今日の開示にはまだ無い・夕方のビルドで増える" : "この窓には無い"}）。</div></div>`;
  const rest = IR_TAB === "all" ? `<details class="card" style="margin-top:8px"><summary style="cursor:pointer;font-weight:700">中立 ${neu.length}件（材料性が薄い）</summary><div style="margin-top:6px">${neu.sort((a, b) => (b.date + b.time).localeCompare(a.date + a.time)).map(irRow).join("")}</div></details>
    <details class="card" style="margin-top:8px"><summary style="cursor:pointer;font-weight:700">事務開示 ${adm.length}件（CG報告書・定款・招集・英文・説明会資料・人事）</summary><div class="note" style="margin-top:6px">${adm.map(r => `${esc(r.time)} <a href="#/detail/${esc(r.code)}">${esc(r.name)}</a> ${esc(r.title)}`).join("<br>")}</div></details>` : "";
  const tagc = Object.entries(ir.tags_count || {}).filter(([k]) => !["その他", "事務"].includes(k)).sort((a, b) => b[1] - a[1]).slice(0, 14).map(([k, v]) => `<span class="chip wa">${esc(k)} ${v}</span>`).join("");
  const srch = ir.search && !ir.search.error ? `🔍 本文 ${ir.search.n_text || 0}/${ir.search.n || 0}件（${esc(ir.search.note || "")}）<br>` : "";
  const foot = `<div class="note" style="margin-top:4px">📏 ${(ir.rules || []).map(esc).join(" ／ ")}<br>${esc(ir.basis || "")}<br>${srch}取得: ${Object.entries(ir.fetched || {}).slice(0, 4).map(([d, n]) => `${esc(d.slice(5))} ${n == null ? "×" : n + "件"}`).join("・")} ・ 生成 ${esc(ir.generated_at || "")}</div>`;
  if (IR_Q.trim()) irEnsure();
  return title + irSearchBox() + `<div id="irmain" style="${IR_Q.trim() ? "display:none" : ""}">` + seg1 + seg2 + honest + body + rest + (tagc ? `<div class="chips" style="margin-top:8px">${tagc}</div>` : "") + foot + `</div>`;
}
