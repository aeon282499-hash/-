// honmei.js — 📝 初動ブレイクの「本命」台帳 ＋ 🧪 C/D に材料が出た一覧（v7.7・2026-10-17 本人「本命ボタン作って あと新規にDやCの銘柄にセクターやIRがでたとか」）
// 本命: 🧭探検→初動ブレイクの各行の「本命」ボタンで localStorage(kabuai_honmei) に記録（その晩の候補全部のコードも一緒に保存）。
//       答え合わせは data/stocks/<code>.json（60日足）で、翌日寄り建て→20日目の終値 or 安値が建値−15%で損切り（10年BTと同じ執行）。
//       翌朝の寄りが前日終値+3%超なら「見送り」（BTの執行ルール）。確定した結果は localStorage に固定（60日足の窓から外れても残る）。
//       同じ晩の候補全体の平均と並べる＝「自分で調べて選ぶ」が候補全体(当たり率2.5%)に勝つかの検証。
// 材料: latest.json["precursor"]（precursor_builder.py）を #/explore/precursor で表示。
const HONMEI_KEY = "kabuai_honmei";
const HONMEI_T = 20, HONMEI_SL = 15, HONMEI_GAP = 3;
const HONMEI_CACHE = {};        // code → stocks json（null＝無い）
let HONMEI_LOADING = 0;
function honmeiLoad() { try { const j = JSON.parse(localStorage.getItem(HONMEI_KEY) || "null"); if (j && Array.isArray(j.picks)) return { picks: j.picks, results: j.results || {} }; } catch (e) { /* ignore */ } return { picks: [], results: {} }; }
function honmeiSave(st) { try { localStorage.setItem(HONMEI_KEY, JSON.stringify(st)); } catch (e) { /* ignore */ } }
const honmeiKey = (date, code) => `${date}_${code}`;
function honmeiHas(code) { const d = (EXP && EXP.data_date) || ""; return honmeiLoad().picks.some(p => p.code === code && p.date === d); }
function honmeiToggle(code) {
  const st = honmeiLoad(); const d = (EXP && EXP.data_date) || ""; if (!d) return;
  const i = st.picks.findIndex(p => p.code === code && p.date === d);
  if (i >= 0) { st.picks.splice(i, 1); delete st.results[honmeiKey(d, code)]; }
  else {
    const cands = ((EXP.categories || {}).break60 || []);
    const r = cands.find(x => x.code === code) || {};
    let reason = "";
    try { if (typeof prompt === "function") reason = prompt("本命の理由（任意・あとで比べる材料）", "") || ""; } catch (e) { /* ignore */ }
    st.picks.push({ date: d, code, name: r.name || code, price: r.price || null, reason: String(reason).slice(0, 80), cands: cands.map(x => x.code), new250: !!r.new250, calm: r.calm });
  }
  honmeiSave(st); render();
}
function honmeiRemove(date, code) {
  try { if (typeof confirm === "function" && !confirm(`${code} の本命記録を消しますか？`)) return; } catch (e) { /* ignore */ }
  const st = honmeiLoad(); st.picks = st.picks.filter(p => !(p.code === code && p.date === date)); delete st.results[honmeiKey(date, code)]; honmeiSave(st); render();
}
async function honmeiStock(code) {
  if (code in HONMEI_CACHE) return HONMEI_CACHE[code];
  HONMEI_LOADING++;
  const j = await fetchFirst([`../data/stocks/${code}.json`, `data/stocks/${code}.json`]);
  HONMEI_LOADING--;
  HONMEI_CACHE[code] = (j && j.chart && Array.isArray(j.chart.dates)) ? j : null;
  return HONMEI_CACHE[code];
}
// 1本の答え合わせ。{status: "wait"|"skip"|"open"|"done", entry, pnl, days, exit_date, how}
function honmeiEval(j, date) {
  if (!j) return { status: "nodata" };
  const ch = j.chart, i = ch.dates.indexOf(date);
  if (i < 0) return { status: "nodata" };
  const n = ch.dates.length;
  if (i + 1 >= n) return { status: "wait" };                        // 翌日の足がまだ無い
  const e = ch.o[i + 1], c0 = ch.c[i];
  if (!(e > 0)) return { status: "wait" };
  const gap = (e / c0 - 1) * 100;
  if (gap > HONMEI_GAP) return { status: "skip", entry: e, gap };
  const stop = e * (1 - HONMEI_SL / 100);
  const last = Math.min(i + 1 + HONMEI_T, n - 1);
  for (let k = i + 2; k <= last; k++) {
    if (ch.l[k] <= stop) return { status: "done", entry: e, gap, pnl: -HONMEI_SL, days: k - i - 1, exit_date: ch.dates[k], how: "損切り" };
  }
  const done = i + 1 + HONMEI_T <= n - 1;
  return { status: done ? "done" : "open", entry: e, gap, pnl: (ch.c[last] / e - 1) * 100, days: last - i - 1, exit_date: ch.dates[last], how: done ? "20日目" : "進行中" };
}
async function honmeiCompute() {
  const st = honmeiLoad(); let changed = false;
  const need = new Set();
  for (const p of st.picks) { need.add(honmeiKey(p.date, p.code)); for (const c of p.cands || []) need.add(honmeiKey(p.date, c)); }
  for (const key of need) {
    const cur = st.results[key];
    if (cur && (cur.status === "done" || cur.status === "skip")) continue;      // 確定は固定
    const [date, code] = key.split("_");
    const j = await honmeiStock(code);
    const r = honmeiEval(j, date);
    if (r.status === "nodata" && cur) continue;                                 // 窓から外れた＝前の値を残す
    st.results[key] = { ...r, at: todayStr() }; changed = true;
  }
  if (changed) honmeiSave(st);
  return st;
}
function honmeiStats(list) {
  const fin = list.filter(r => r && (r.status === "done" || r.status === "open") && r.pnl != null);
  const done = fin.filter(r => r.status === "done");
  const mean = xs => xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
  return { n: list.length, n_fin: fin.length, n_done: done.length, mean: mean(fin.map(r => r.pnl)), win: fin.length ? fin.filter(r => r.pnl > 0).length / fin.length * 100 : null,
    big: fin.filter(r => r.pnl >= 20).length, skip: list.filter(r => r && r.status === "skip").length };
}
function viewHonmei() {
  const st = honmeiLoad();
  if (st.picks.length) honmeiCompute().then(() => { if ((location.hash || "") === "#/explore/honmei") render(); });
  const picks = [...st.picks].sort((a, b) => (b.date + b.code).localeCompare(a.date + a.code));
  const mine = picks.map(p => st.results[honmeiKey(p.date, p.code)]);
  const restKeys = new Set(); picks.forEach(p => (p.cands || []).forEach(c => { if (!picks.some(q => q.date === p.date && q.code === c)) restKeys.add(honmeiKey(p.date, c)); }));
  const rest = [...restKeys].map(k => st.results[k]);
  const sm = honmeiStats(mine), sr = honmeiStats(rest);
  const f = v => v == null ? "—" : fmtPct(v);
  const head = `<a class="back" href="#/explore">← 銘柄探検</a><h2>📝 本命の台帳 <span class="sub">${picks.length}本 ・ 紙</span></h2>
    <div class="banner info">初動ブレイクで自分が選んだ「本命」が、同じ晩の候補全体に勝つかを測る紙。執行は10年BTと同じ＝翌朝の寄りで建てる（寄りが前日終値+3%超なら見送り）→20日目の終値、安値が建値−15%なら損切り。候補全体の当たり率は2.5%（40本に1本が+37%）。30〜40本たまったら本命の平均と候補全体の平均を比べる。</div>
    <div class="card"><div class="cd-line"><b>本命</b> ${sm.n}本 ／ 結果あり ${sm.n_fin}（確定 ${sm.n_done}）： 平均 <b class="${cls(sm.mean)}">${f(sm.mean)}</b> ・ 勝率 ${sm.win == null ? "—" : sm.win.toFixed(0) + "%"} ・ +20%以上 ${sm.big}本 ・ 見送り ${sm.skip}</div>
      <div class="cd-line"><b>同じ晩の候補の残り</b> ${sr.n}本 ／ 結果あり ${sr.n_fin}： 平均 <b class="${cls(sr.mean)}">${f(sr.mean)}</b> ・ 勝率 ${sr.win == null ? "—" : sr.win.toFixed(0) + "%"} ・ +20%以上 ${sr.big}本</div>
      ${sm.n_fin && sr.n_fin ? `<div class="cd-line">差（本命 − 残り）: <b class="${cls(sm.mean - sr.mean)}">${fmtPct(sm.mean - sr.mean)}</b>${sm.n_fin < 30 ? `<span class="muted">（まだ ${sm.n_fin}本・30本までは参考）</span>` : ""}</div>` : ""}
      ${HONMEI_LOADING ? `<div class="note">答え合わせを読み込み中…</div>` : ""}</div>`;
  if (!picks.length) return head + `<div class="card"><div class="empty">まだ本命がありません。🧭探検 → 🚩初動ブレイク の各行の「本命」を押すと、その晩の日付・値段・候補全部が記録されます。</div></div>`;
  const rowsHtml = picks.map(p => {
    const r = st.results[honmeiKey(p.date, p.code)] || {};
    const res = r.status === "done" ? `<b class="${cls(r.pnl)}">${fmtPct(r.pnl)}</b> <small>${r.how}・${r.days}日</small>`
      : r.status === "open" ? `<b class="${cls(r.pnl)}">${fmtPct(r.pnl)}</b> <small>進行中 ${r.days}日目</small>`
      : r.status === "skip" ? `<span class="muted">見送り</span> <small>寄り ${fmtPct(r.gap)}</small>`
      : r.status === "wait" ? `<span class="muted">翌日の寄り待ち</span>` : `<span class="muted">データ待ち</span>`;
    return `<div class="pickrow"><div class="pk-nm"><b><a href="#/detail/${esc(p.code)}">${esc(p.name)}</a></b> <small>${esc(p.code)} ・ ${esc(p.date)} ・ 終値 ${p.price ? yen(p.price) : "—"}${r.entry ? " → 建値 " + yen(r.entry) : ""}</small>
      <div class="chips">${p.new250 ? `<span class="chip buy">🆕 新高値</span>` : ""}${p.calm === false ? `<span class="chip dn">荒い</span>` : ""}${p.reason ? `<span class="chip wa">${esc(p.reason)}</span>` : ""}<span class="chip">候補 ${(p.cands || []).length}本</span></div></div>
      <div class="pk-ev">${res}<br><a onclick="event.preventDefault();honmeiRemove('${esc(p.date)}','${esc(p.code)}')" href="#" class="muted" style="font-size:11px">消す</a></div></div>`;
  }).join("");
  const copy = `<div class="note" style="margin-top:6px"><a href="#" onclick="event.preventDefault();honmeiCopy()">📋 台帳をテキストでコピー</a>（バックアップ用・端末の localStorage にだけ保存されています）</div>`;
  return head + `<div class="card tight">${rowsHtml}</div>` + copy + `<p class="disc">${esc(DATA.disclaimer)}</p>`;
}
function honmeiCopy() {
  const st = honmeiLoad();
  const lines = st.picks.map(p => { const r = st.results[honmeiKey(p.date, p.code)] || {}; return `${p.date}\t${p.code}\t${p.name}\t${p.price || ""}\t${r.entry ? r.entry.toFixed(1) : ""}\t${r.pnl != null ? r.pnl.toFixed(2) : r.status || ""}\t${p.reason || ""}`; });
  const text = "日付\tコード\t銘柄\t終値\t建値\t損益%\t理由\n" + lines.join("\n");
  try { navigator.clipboard.writeText(text); } catch (e) { /* ignore */ }
}
// ── 🧪 C/D に材料が出た ──
function precRow(r) {
  const irx = r.ir;
  const irChip = irx ? `<span class="chip ${irx.label === "好材料" ? "buy" : irx.label === "悪材料" ? "dn" : "wa"}">📰 ${esc(irx.label)}${irx.score ? ` ${irx.score > 0 ? "+" : ""}${irx.score}` : ""}</span>` : "";
  const th = (r.themes || []).map(t => `<span class="chip acc">🔥 ${t.rank}位 ${esc(t.name)}</span>`).join("");
  const gr = (r.groups || []).map(g => `<span class="chip wa">💴 ${esc(g)}</span>`).join("");
  const mv = r.moving ? `<span class="chip">${r.vol_x != null ? `出来高 ${r.vol_x}倍` : ""}${r.r1 != null ? ` ${fmtPct1(r.r1)}` : ""}</span>` : `<span class="chip muted">まだ動いていない</span>`;
  const title = irx ? `<div class="irtitle" style="font-size:12px">${irx.pdf ? `<a href="${esc(irx.pdf)}" target="_blank" rel="noopener">${esc(irx.time || "")} ${esc(irx.title)} 📄</a>` : esc(irx.title)}</div>` : "";
  return `<div class="pickrow" style="display:block"><div class="pk-nm"><b><a href="#/detail/${esc(r.code)}">${esc(r.name)}</a></b> <small>${esc(r.code)} ・ ${esc(r.sector || "")} ・ ${yen(r.price)} ・ ランク ${esc(r.grade)}（${r.momentum}）・ 代金${r.turnover_oku}億</small>
    <div class="chips">${irChip}${th}${gr}${mv}</div>${title}</div></div>`;
}
function viewPrecursor() {
  const p = DATA.precursor;
  const head = `<a class="back" href="#/explore">← 銘柄探検</a><h2>🧪 C/Dに材料が出た <span class="sub">${p && p.date ? esc(p.date) + " 終値時点" : ""}</span></h2>`;
  if (!p) return head + `<div class="card"><div class="empty">まだ作られていません（次のビルドで）。</div></div>`;
  if (p.error) return head + `<div class="card"><div class="empty">今回は作れませんでした（${esc(p.error)}）。</div></div>`;
  const c = p.counts || {};
  const banner = `<div class="banner info"><b>予測ではなく、調べる候補の一覧。</b>${esc(p.note || "")}<br>ランクC/D（代金1億以上）${(p.n_cd || 0).toLocaleString()}本のうち ${p.n}本： 📰IR ${c.ir || 0}・🔥人気テーマ ${c.theme || 0}・💴資金セクター ${c.flow || 0}${p.hot_time ? `（株探 ${esc(p.hot_time)}）` : ""}。<br>並びは 好材料 → 要精査 → 当たった数 → 出来高。</div>`;
  const rows = (p.rows || []);
  return head + banner + (rows.length ? `<div class="card tight">${rows.map(precRow).join("")}</div>` : `<div class="card"><div class="empty">該当なし（IR も熱いグループの動きも無い日）。</div></div>`) + `<p class="disc">${esc(DATA.disclaimer)}</p>`;
}
