// coach.js — 🧠 AI調教（v8.3・2026-10-19 本人「つくって モメンタムチンパンアプリに追加」＝AI惣菜『AI調教術』の記事）
// 記事の骨子: 勝てない頭でAIを育てない。「冷徹なモメンタム順張りトレーダー」の人格をAIの個人設定（カスタム指示）に貼り、
//            自分の弱気発言・言い訳を糾弾させる。AIの断言は判断にしない（情報源・数字の時点・反対シナリオを確認）。
// この画面がやること（データは全部このアプリのEOD）:
//   ① 人格プロンプト（コピペ用）… ChatGPT/Gemini/Claude の個人設定に貼る文面。
//   ② 銘柄の解剖 … 銘柄コード → このアプリの数字（終値・騰落・勢い・需給・シグナル・今日のIR・資金セクター）＋自分の言い分を
//                   1本のプロンプトにまとめてコピー。APIキーがあればその場で Claude に聞く（鍵は端末の localStorage だけ）。
//   ③ 言い訳チェッカー … 入力した言い分に「戻る・ナンピン・塩漬け・決算まで・割安・噂・取り返す・デイトレ」の型があれば
//                   このアプリで測った数字で即答（AI不要・ルール）。
// 注意: 「上がる/下がる」は言わない。AIの答えも『順張りの条件を満たすか・崩れる条件・どこで降りるか』の形に縛る。
const COACH_KEY_LS = "kabuai_coach_key", COACH_WORDS_LS = "kabuai_coach_words";
const COACH = { code: "", words: "", out: "", err: "", busy: false, prompt: "", model: "claude-opus-5-5" };
const COACH_PERSONA = `【前提ペルソナ】
あなたは市場で数億の資産を築いた冷徹なモメンタム順張りトレーダーです。相談者（私）は日本株の個人トレーダーで、終値（EOD）ベースのモメンタム指標・需給・シグナルで銘柄を選び、翌日の寄りで建てて数日〜20日で降ります。

【思考・行動原則】
1. 割安・バリュー・「いつか戻る」という概念は一切排除せよ。市場の資金が集まっている強い株・トレンドが出ている株の順張りだけを肯定せよ。
2. 感情を完全に排除し、期待値（勝率×平均利益 − 敗率×平均損失）だけでトレードを評価せよ。期待値を示せない提案は却下せよ。
3. 損切りは建てる前に決めた水準で一瞬の迷いもなく執行させよ。含み損の塩漬け・ナンピン・「決算まで持つ」は最大の機会損失とみなせ。
4. 私が弱気な発言・ルールを破る言い訳・後講釈をした場合は、その矛盾と盲点を容赦なく、客観的かつ論理的に糾弾せよ。
5. 根拠には必ず「情報源・数字の時点・反対シナリオ」を添えよ。確認できない数字は「不明」と言い、捏造するな。
6. 「上がる／下がる」の断言はするな。代わりに「順張りの条件を満たすか・崩れる条件は何か・どこで降りるか」を答えよ。

【回答スタイル】
お世辞・慰め・一般論（分散投資・自己責任）は不要。常にプロのストラテジストとして、結論 → 根拠 → 反対シナリオ → 行動（買う／見送る／損切り）の順で、短く厳しく答えよ。`;
// ③ 言い訳チェッカー（ルール）: 型 → このアプリで測った数字つきの即答
const COACH_EXCUSES = [
  { re: /戻る|戻す|戻り|反発|リバ|底打ち|底値|下げ止ま/, tag: "「戻る」", word: "「戻る」は期待値ではなく願望。下げている株の反発買い（MA反発・落ちるナイフ）はこのアプリの26年検証で全形マイナス。順張りなら高値更新側しか買わない。" },
  { re: /ナンピン|難平|買い増|買い下が|買い足/, tag: "ナンピン", word: "負けている玉に資金を足す＝自分で決めた損切り水準を自分で壊す行為。平均取得単価を下げても期待値は上がらない。足すなら勝っている玉だけ。" },
  { re: /塩漬け|長期|配当|いつか|待てば|待って|持ち続け|ホールド/, tag: "塩漬け", word: "含み損の保有は損ではなく機会損失。勢い（モメンタム）が消えた玉は理由が何であれ降りる。配当・長期は順張りトレードの言い訳にならない。" },
  { re: /決算(まで|に期待|をまた|を跨|持ち越)|持ち越し/, tag: "決算持ち越し", word: "決算持ち越しは賭けであってトレードではない（このアプリの決算持ち越しシグナルは実弾休止中）。決算前に降りるか、決算後の反応に順張りで乗る。" },
  { re: /割安|安い|PER|PBR|出遅れ|まだ上がって(い)?ない/, tag: "割安", word: "割安は順張りの根拠にならない。安い株はもっと安くなる。買う理由は『資金が集まって高値を更新している』だけ。" },
  { re: /噂|掲示板|SNS|ツイ|Xで|みんな|有名人|インフルエンサー|仕手/, tag: "噂", word: "出所と時点を確認できない情報は根拠ゼロ。この銘柄の数字（終値・出来高・需給・IR）だけで判断し、噂は捨てる。" },
  { re: /損切り(し|でき|したく)?(ない|ません|たくない)|もう少し|ギリギリ|ちょっとだけ/, tag: "損切り先送り", word: "損切りは建てる前に決めた水準で機械的に執行する。『もう少し』は毎回同じ言葉で損を大きくする。守れないなら建てない。" },
  { re: /取り返|取り戻|負けを|リベンジ|焦|イライラ|悔し/, tag: "取り返す", word: "感情トレード。負けを取り返す目的の玉は期待値がマイナス。サイズを半分に落とし、ルール通りの玉だけ。" },
  { re: /デイトレ|当日|寄りで買って引け|日計り|スキャ/, tag: "デイトレ", word: "このアプリは終値のスナップショット。S銘柄を寄りで買って引けで売る当日完結は17ヶ月通しで−62.8%＝負け確定。翌日の寄りで建てて数日〜20日の順張りだけ。" },
  { re: /倍|爆益|一発|全力|フルレバ|全部/, tag: "一発狙い", word: "初動ブレイクの+0.6〜1.4%/件は『40本に1本の+37%』が作っている。1本に全力は裾を取れない上に損切り1回で終わる。サイズは損切り幅で決める（🧮電卓）。" },
];
function coachLoadWords() { try { return localStorage.getItem(COACH_WORDS_LS) || ""; } catch (e) { return ""; } }
function coachKey() { try { return localStorage.getItem(COACH_KEY_LS) || ""; } catch (e) { return ""; } }
function coachSetKey(v) { try { if (v) localStorage.setItem(COACH_KEY_LS, v.trim()); else localStorage.removeItem(COACH_KEY_LS); } catch (e) { /* ignore */ } render(); }
function coachSet(k, v) {
  COACH[k] = v;
  if (k === "words") { try { localStorage.setItem(COACH_WORDS_LS, v); } catch (e) { /* ignore */ } const el = document.getElementById("coach-excuse"); if (el) el.innerHTML = coachExcuseHtml(); }
  if (k === "code") { COACH.out = ""; COACH.err = ""; const el = document.getElementById("coach-stock"); if (el) el.innerHTML = coachStockHtml(); }
}
function coachExcuses(words) { const w = String(words || ""); return COACH_EXCUSES.filter(x => x.re.test(w)); }
function coachExcuseHtml() {
  const hits = coachExcuses(COACH.words);
  if (!COACH.words.trim()) return `<div class="note">買いたい理由・迷い・言い訳をそのまま書く。型があればここで即答、全部はAIに貼る。</div>`;
  if (!hits.length) return `<div class="note">言い訳の型は見つからない。解剖はAIに任せる（下のボタン）。</div>`;
  return hits.map(h => `<div class="warn" style="font-size:12.5px;margin-top:6px">🔪 <b>${esc(h.tag)}</b> … ${esc(h.word)}</div>`).join("");
}
// 銘柄の事実を集める（ランキング50本は latest.json の行、それ以外は search_index の行）
function coachFacts(code) {
  const r = (DATA.ranking || []).find(x => x.code === code) || (typeof SEARCH !== "undefined" && SEARCH && SEARCH.find(x => x.code === code)) || null;
  if (!r) return null;
  const sig = (r.signals || []).map(k => { const m = (DATA.signals && DATA.signals.groups && DATA.signals.groups[k]) || {}; return m.label || k; });
  const uw = DATA.uwick || {};
  const uwb = (uw.buy || []).find(x => x.code === code), uws = (uw.sell || []).find(x => x.code === code);
  const irRows = ((DATA.ir && DATA.ir.rows) || []).filter(x => x.code === code).slice(0, 5)
    .map(x => `${x.date || ""} ${(x.final || x.rule || {}).label || ""}「${x.title || ""}」`.trim());
  const groups = ((DATA.flow && DATA.flow.groups) || []).filter(g => (g.members || []).some(m => m.code === code) || (g.members_out || []).some(m => m.code === code) || (g.lead && g.lead.code === code))
    .map(g => `${g.kind || ""}「${g.label}」${g.lead && g.lead.code === code ? "（主役）" : ""}`);
  const pre = ((DATA.precursor && DATA.precursor.rows) || []).find(x => x.code === code);
  const themes = pre && pre.themes ? pre.themes.map(t => `${t.rank}位 ${t.name}`) : [];
  return { r, sig, uwb, uws, irRows, groups, themes };
}
const coachN = (v, d = 1) => (v == null || isNaN(v)) ? "不明" : Number(v).toFixed(d);
const coachP = v => (v == null || isNaN(v)) ? "不明" : (v >= 0 ? "+" : "") + Number(v).toFixed(1) + "%";
function coachStockPrompt(code) {
  const f = coachFacts(code); if (!f) return "";
  const r = f.r, d = DATA.data_date || "最新営業日";
  const need = [];
  const shortLine = r.jsf_stop ? "🚫売り禁（制度信用の新規売り停止）" : r.shortable === true ? "貸借○（空売り可）" : r.shortable === false ? "制度× （信用買いのみ）" : "区分 不明";
  const sv = r.short_vol != null ? `売残 ${Math.round(r.short_vol / 1e4)}万株${r.short_chg != null ? `（前週比${coachP(r.short_chg)}）` : ""}・取組倍率 ${coachN(r.ratio, 2)}・売残÷1日の出来高 ${coachN(r.short_days)}日分` : "売残 不明（ランキング上位50本以外は載せていない）";
  const lines = [
    `【銘柄】${r.name}（${code}）${r.sector ? " ・ " + r.sector : ""}`,
    `【時点】${d} の終値（場中は変わらない・翌営業日の寄りで建てる前提）`,
    `【値動き】終値 ${yen(r.price)} ・ 前日比 ${coachP(r.r1)} ・ 5日 ${coachP(r.r5)} ・ 10日 ${coachP(r.r10)} ・ 1ヶ月 ${coachP(r.r20)}${r.ret120 != null ? ` ・ 120日 ${coachP(r.ret120)}` : ""}${r.ret250 != null ? ` ・ 250日 ${coachP(r.ret250)}` : ""}`,
    `【勢い】モメンタム指数 ${coachN(r.momentum, 0)}（${r.grade || "-"}）・ RSI ${coachN(r.rsi, 0)} ・ 出来高÷20日平均 ${coachN(r.vol_x, 2)}倍 ・ 代金 ${r.turnover_oku != null ? Math.round(r.turnover_oku) + "億円" : "不明"} ・ 20日値動き ${coachN(r.rng20)}%/日${r.ma5_dev != null ? ` ・ 5日線乖離 ${coachP(r.ma5_dev)}` : ""}${r.off_peak20 != null ? ` ・ 20日高値から −${coachN(Math.abs(r.off_peak20))}%` : ""}${r.futures_tag ? ` ・ 日経連動 ${r.futures_tag}` : ""}`,
    `【需給】${shortLine}${r.jsf_warn ? " ・ ⚠️注意喚起" : ""}${r.tse_reg ? " ・ 規制" : ""}${r.daily_pub ? " ・ 日々公表" : ""} ・ ${sv}`,
    `【シグナル（終値ルール）】${f.sig.length ? f.sig.join(" / ") : "点灯なし"}`,
    `【上髭逆指値（明日の銘柄）】${f.uwb ? `買い側: 高値 ${yen(f.uwb.high)} 抜けで買い（上髭${coachN(f.uwb.uw_pct)}%）` : f.uws ? `売り側: 安値 ${yen(f.uws.low)} 割れで売り` : "載っていない"}`,
    `【今日のIR】${f.irRows.length ? f.irRows.join(" ／ ") : "なし（または未取得）"}`,
    `【資金セクター（今日 資金が集まった業種/テーマ）】${f.groups.length ? f.groups.join(" ／ ") : "該当なし"}`,
    `【株探の人気テーマ】${f.themes.length ? f.themes.join(" ／ ") : "該当なし／未確認"}`,
  ];
  if (r.price > 0 && r.price < BUY_PRICE_MIN) need.push(`株価${BUY_PRICE_MIN}円未満の低位株（このアプリの買い候補では既定で除外）`);
  if (r.rng20 != null && r.rng20 >= 6) need.push("20日値動き6%以上の荒い株（値動き帯検証で除外側）");
  if (r.r1 != null && r.r1 >= 20) need.push("当日+20%以上（初動ブレイク10年検証で負け筋: 平均−1.5%）");
  if (r.vol_x != null && r.vol_x >= 10) need.push("出来高10倍以上（負け筋: 平均−0.7%）");
  if (r.rsi != null && r.rsi >= 85) need.push("RSI 85以上の過熱");
  const words = COACH.words.trim();
  return `${COACH_PERSONA}

＝＝＝＝＝ 相談 ＝＝＝＝＝
以下は私のアプリ（終値ベース・J-Quants日足）の数字です。数字はこの時点のもので、場中の値動きは含みません。

${lines.join("\n")}
${need.length ? `【アプリの検証で負け筋に当たる点】${need.join(" ／ ")}\n` : ""}
【私の言い分】
${words || "（なし）"}

【あなたへの問い】
1. この銘柄は「順張りの条件」を満たすか（資金が集まり、高値側を更新しているか）。満たさないなら何が欠けているか。
2. 崩れる条件（この数字がこうなったら順張りは終わり）を具体的に。
3. 翌営業日の寄りで建てるなら、損切り水準（%と理由）と、降りる目安（日数・条件）。寄りが前日終値+3%超で始まった場合の扱いも。
4. 私の言い分に矛盾・盲点・言い訳があれば容赦なく指摘せよ。
5. 上の数字で確認できないことは「不明」と言い、追加で見るべき情報源（時点つき）を挙げよ。「上がる／下がる」の断言は不要。`;
}
function coachCopy(btn, text, label) {
  const done = () => { try { btn.textContent = "✅ コピーした"; setTimeout(() => { btn.textContent = label; }, 1500); } catch (e) { /* ignore */ } };
  try {
    if (typeof navigator !== "undefined" && navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text).then(done, () => bookCopyFallback(text, done));
    else bookCopyFallback(text, done);
  } catch (e) { /* ignore */ }
}
function coachCopyPersona(btn) { coachCopy(btn, COACH_PERSONA, "📋 人格をコピー"); }
function coachCopyStock(btn) { const p = coachStockPrompt(COACH.code); if (p) coachCopy(btn, p, "📋 解剖プロンプトをコピー"); }
// APIキーがある時だけ: ブラウザから直接 Anthropic Messages API（SDKの無い素のHTMLなので fetch）。鍵は端末の localStorage のみ・サーバには置かない。
async function coachAsk(kind) {
  kind = kind || "stock";
  const key = coachKey(), prompt = kind === "screen" ? coachScreenPrompt() : coachStockPrompt(COACH.code);
  if (!key || !prompt || COACH.busy) return;
  COACH.busy = true; COACH.err = ""; COACH.out = ""; COACH.outKind = kind; coachPaintOut();
  const t0 = Date.now();
  try {
    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": key, "anthropic-version": "2023-06-01", "anthropic-dangerous-direct-browser-access": "true", "anthropic-beta": "server-side-fallback-2026-07-01" },
      body: JSON.stringify({ model: COACH.model, max_tokens: 4000, output_config: { effort: "medium" }, fallbacks: "default",
        system: COACH_PERSONA, messages: [{ role: "user", content: prompt.slice(COACH_PERSONA.length).trim() }] }),
    });
    const j = await res.json().catch(() => null);
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${(j && j.error && j.error.message) || "エラー"}`);
    if (j.stop_reason === "refusal") throw new Error(`回答を断られた（${(j.stop_details && j.stop_details.category) || "理由不明"}）`);
    COACH.out = (j.content || []).filter(b => b.type === "text").map(b => b.text).join("\n").trim() || "（空の回答）";
    const u = j.usage || {}; COACH.usage = `${j.model || COACH.model} ・ 入力${u.input_tokens || 0}／出力${u.output_tokens || 0}トークン ・ ${((Date.now() - t0) / 1000).toFixed(0)}秒`;
  } catch (e) { COACH.err = String(e && e.message || e); }
  COACH.busy = false; coachPaintOut();
}
function coachPaintOut() {
  for (const id of ["coach-out", "coach-screen-out"]) { const el = document.getElementById(id); if (!el) continue; el.innerHTML = ((id === "coach-screen-out") === (COACH.outKind === "screen")) ? coachOutHtml() : ""; }
}
function coachOutHtml() {
  if (COACH.busy) return `<div class="note">🤖 Claude が解剖中…（${esc(COACH.model)}）</div>`;
  if (COACH.err) return `<div class="note" style="color:var(--dn)">⚠️ ${esc(COACH.err)}</div>`;
  if (!COACH.out) return "";
  return `<div class="card" style="margin:0;border-color:rgba(79,156,255,.45)"><div style="font-weight:800">🤖 Claude の解剖 <span class="muted" style="font-weight:400;font-size:12px">${esc(COACH.usage || "")}</span></div>
    <pre style="white-space:pre-wrap;font-size:14px;line-height:1.7;margin:6px 0 0">${esc(COACH.out)}</pre></div>`;
}
// ── 🏆 最強スクリーニング（人格の6か条をアプリの数字で機械的に当てる・2026-10-19 本人「きみが銘柄最強を選定して提案するんだよ スクリーニングして」）──
// 母集団 = 🐵ランキング50本 ＋ 順張り系シグナル（強買い集め/買い集め/加速/昇格）の点灯銘柄。反転系（押し目/反転）は母集団に入れない（順張りだけ）。
// 除外 = アプリの検証で負け筋に当たるもの。点数 = トレンドの長さ・高値への近さ・資金の集まり・静かさ。勝率は約半分のまま＝選定であって保証ではない。
const COACH_UP_GROUPS = ["strong_accum", "accum", "accel", "promote"];
function coachScreenRows() {
  const seen = new Map();
  for (const r of (DATA.ranking || [])) seen.set(r.code, Object.assign({}, r));
  const groups = (DATA.signals && DATA.signals.groups) || {};
  for (const k of COACH_UP_GROUPS) for (const m of ((groups[k] || {}).members || [])) if (!seen.has(m.code)) seen.set(m.code, Object.assign({}, m));
  const minOku = DATA.pick_min_oku || 10, maxRng = DATA.pick_max_rng || 6;
  const out = [];
  for (const r of seen.values()) {
    const no = [];
    if (r.price > 0 && r.price < BUY_PRICE_MIN) no.push(`低位 ${yen(r.price)}`);
    if (r.rng20 != null && r.rng20 >= maxRng) no.push(`荒い ${coachN(r.rng20)}%/日`);
    if (r.r1 != null && r.r1 >= 20) no.push(`当日 +${coachN(r.r1, 0)}%`);
    if (r.vol_x != null && r.vol_x >= 10) no.push(`出来高 ふだんの${coachN(r.vol_x, 0)}倍`);
    if (r.rsi != null && r.rsi >= 85) no.push("過熱（RSI " + coachN(r.rsi, 0) + "）");
    if (r.turnover_oku != null && r.turnover_oku < minOku) no.push(`代金 ${Math.round(r.turnover_oku)}億`);
    if (r.r20 != null && r.r20 <= 0) no.push(`1ヶ月 ${coachP(r.r20)}`);
    const sig = r.signals || [];
    if (sig.length && !sig.some(k => COACH_UP_GROUPS.includes(k)) && sig.some(k => /dip|reversal/.test(k))) no.push("反転系だけ");
    let sc = 0; const why = [];
    if (r.r20 != null && r.r20 > 0) { const p = Math.min(25, Math.round(r.r20 * 25 / 30)); sc += p; why.push(`1ヶ月 ${coachP(r.r20)}`); }
    if (r.ret120 != null && r.ret120 > 0) { sc += 10; why.push(`120日 ${coachP(r.ret120)}`); }
    if (r.ret250 != null && r.ret250 > 0) { sc += 10; why.push(`250日 ${coachP(r.ret250)}`); }
    if (r.off_peak20 != null) { const o = Math.abs(r.off_peak20); if (o <= 2) { sc += 15; why.push("20日高値圏"); } else if (o <= 5) { sc += 8; why.push(`高値から −${coachN(o)}%`); } }
    if (r.grade === "S" || r.grade === "A") { sc += 10; why.push(`${r.grade}評価`); } else if (r.grade === "B") sc += 5;
    let gs = 0; for (const k of sig) gs += k === "strong_accum" ? 15 : k === "accum" ? 8 : k === "accel" ? 8 : k === "promote" ? 5 : 0;
    if (gs) { sc += Math.min(20, gs); why.push(sig.filter(k => COACH_UP_GROUPS.includes(k)).map(k => (groups[k] || {}).label || k).join("/")); }
    if (r.rng20 != null) { if (r.rng20 < 3) { sc += 10; why.push("静か"); } else if (r.rng20 < 4.5) sc += 5; }
    if (r.turnover_oku != null && r.turnover_oku >= 30) sc += 5;
    if (r.rsi != null && r.rsi >= 75) { sc -= 5; why.push("過熱ぎみ"); }
    out.push({ r, no, sc, why });
  }
  const ok = out.filter(x => !x.no.length).sort((a, b) => b.sc - a.sc), ng = out.filter(x => x.no.length);
  return { ok, ng, n: out.length };
}
function coachScreenPrompt() {
  const { ok } = coachScreenRows(); if (!ok.length) return "";
  const d = DATA.data_date || "最新営業日";
  const rows = ok.slice(0, 8).map((x, i) => { const r = x.r; return `${i + 1}. ${r.name}（${r.code}）終値 ${yen(r.price)} ・ 前日比 ${coachP(r.r1)} ・ 5日 ${coachP(r.r5)} ・ 1ヶ月 ${coachP(r.r20)}${r.ret250 != null ? ` ・ 250日 ${coachP(r.ret250)}` : ""} ・ 指数 ${coachN(r.momentum, 0)}（${r.grade || "-"}）・ RSI ${coachN(r.rsi, 0)} ・ 代金 ${r.turnover_oku != null ? Math.round(r.turnover_oku) + "億" : "不明"} ・ 20日値動き ${coachN(r.rng20)}%/日${r.off_peak20 != null ? ` ・ 20日高値から −${coachN(Math.abs(r.off_peak20))}%` : ""} ・ 篩い ${x.sc}点（${x.why.join("・")}）`; });
  const words = COACH.words.trim();
  return `${COACH_PERSONA}

＝＝＝＝＝ 相談 ＝＝＝＝＝
以下は私のアプリ（終値ベース・J-Quants日足）が機械的に篩った順張り候補です。${d} の終値で、場中の値動きは含みません。負け筋（低位・荒い・当日+20%・出来高10倍・RSI85・代金10億未満・1ヶ月マイナス・反転系）は除外済み。

【候補】
${rows.join("\n")}
${words ? `\n【私の言い分】\n${words}\n` : ""}
【あなたへの問い】
1. この中から翌営業日の寄りで建てる1本を選べ（該当なしなら「見送り」と言え）。選ぶ理由は上の数字だけで示せ。
2. 選ばなかった候補は、1行ずつ落とした理由を書け。
3. 選んだ1本の損切り水準（%と理由）・降りる目安（日数・条件）・寄りが前日終値+3%超で始まった場合の扱い。
4. 期待値の見立て（勝率×平均利益−敗率×平均損失）。根拠が無ければ「不明」と言え。参考: このアプリの初動ブレイクは10年で1件+0.64%・勝率約半分。
5. 上の数字で確認できないことは「不明」と言い、見るべき情報源（時点つき）を挙げよ。「上がる／下がる」の断言は不要。`;
}
function coachCopyScreen(btn) { const p = coachScreenPrompt(); if (p) coachCopy(btn, p, "📋 候補ごとコピー"); }
function coachScreenHtml() {
  const { ok, ng, n } = coachScreenRows(), hasKey = !!coachKey();
  if (!n) return `<div class="empty">データ読み込み中</div>`;
  const top = ok.slice(0, 5);
  const row = (x, i) => { const r = x.r; return `<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:6px 0;border-top:1px solid var(--line)">
      <b style="font-size:15px">${i + 1}</b><a href="#/detail/${esc(r.code)}"><b>${esc(r.name)}</b></a><span class="muted" style="font-size:12px">${esc(r.code)}</span>
      <span>${yen(r.price)}</span>${pctTag(r.r1)}
      <span class="muted" style="font-size:12px">${esc(x.why.join("・"))}</span>
      <span style="font-size:12px;flex-basis:100%">損切 <b>${yen(Math.round(r.price * 0.97))}</b>（−3%）${r.ma5_dev != null ? ` ・ 5日線 <b>${yen(Math.round(r.price / (1 + r.ma5_dev / 100)))}</b>` : ""} ・ 利確 <b>${yen(Math.round(r.price * 1.05))}</b>（+5%）・ 寄り ${yen(Math.round(r.price * 1.03))} 超は見送り</span>
      <button class="btn" style="margin-left:auto" onclick="event.preventDefault();coachPick('${esc(r.code)}');var e=document.getElementById('coach-stock');if(e&&e.scrollIntoView)e.scrollIntoView({behavior:'smooth'})">解剖</button></div>`; };
  return `<div class="note">${n}本 → 除外 ${ng.length}本 → 残 ${ok.length}本 ・ 勝率≈50%・+0.64%/件（初動ブレイク10年）</div>
    ${top.length ? top.map(row).join("") : `<div class="empty">今日は全部が負け筋。見送り。</div>`}
    <div class="chips" style="margin-top:8px"><button class="btn" onclick="event.preventDefault();coachCopyScreen(this)">📋 候補ごとコピー</button>
      ${hasKey ? `<button class="btn" onclick="event.preventDefault();coachAsk('screen')" ${COACH.busy ? "disabled" : ""}>🤖 Claude に1本選ばせる</button>` : ""}</div>
    ${ng.length ? `<details class="note" style="margin-top:6px"><summary style="cursor:pointer">除外 ${ng.length}本の理由</summary>${ng.slice(0, 40).map(x => `<div>${esc(x.r.name)} ${esc(x.r.code)}: ${esc(x.no.join("・"))}</div>`).join("")}</details>` : ""}
    <div id="coach-screen-out" style="margin-top:8px">${COACH.outKind === "screen" ? coachOutHtml() : ""}</div>`;
}
function coachStockHtml() {
  const code = COACH.code.trim();
  if (!code) return `<div class="note">🏆の「解剖」か、コード入力。</div>`;
  const f = coachFacts(code);
  if (!f) return `<div class="note">${esc(code)} はこのアプリのデータに無い（ランキング・検索の対象外か、まだ読み込み中）。</div>`;
  const r = f.r, hasKey = !!coachKey();
  const badges = (typeof momShortBadges === "function" && r.short_vol != null) ? momShortBadges(r, false) : "";
  return `<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap"><a href="#/detail/${esc(code)}"><b style="font-size:16px">${esc(r.name)}</b></a><span class="muted" style="font-size:12px">${esc(code)}${r.sector ? " ・ " + esc(r.sector) : ""}</span>
      <span class="chip ${r.grade === "S" || r.grade === "A" ? "up" : ""}">${esc(r.grade || "-")} 指数${coachN(r.momentum, 0)}</span>${pctTag(r.r1)}</div>
    ${badges ? `<div class="sbadges" style="margin-top:6px">${badges}</div>` : ""}
    <div class="note" style="margin-top:6px">終値 <b>${yen(r.price)}</b> ・ 5日 ${fmtPct1(r.r5)} ・ 1ヶ月 ${fmtPct1(r.r20)} ・ RSI ${coachN(r.rsi, 0)} ・ 出来高 ${r.vol_x != null ? "ふだんの" + coachN(r.vol_x, 1) + "倍" : "—"} ・ 代金 ${r.turnover_oku != null ? Math.round(r.turnover_oku) + "億" : "—"} ・ シグナル ${f.sig.length ? esc(f.sig.join("/")) : "なし"} ・ IR ${f.irRows.length}件 ・ 資金 ${f.groups.length ? esc(f.groups.join("/")) : "なし"}</div>
    <div class="chips" style="margin-top:8px"><button class="btn" onclick="event.preventDefault();coachCopyStock(this)">📋 解剖プロンプトをコピー</button>
      ${hasKey ? `<button class="btn" onclick="event.preventDefault();coachAsk()" ${COACH.busy ? "disabled" : ""}>🤖 Claude に聞く</button>` : ""}</div>
    <details class="note" style="margin-top:6px"><summary style="cursor:pointer">貼る文面を見る</summary><pre style="white-space:pre-wrap;font-size:12px;line-height:1.6;margin:6px 0 0">${esc(coachStockPrompt(code))}</pre></details>
    <div id="coach-out" style="margin-top:8px">${COACH.outKind !== "screen" ? coachOutHtml() : ""}</div>`;
}
function coachPickChips() {
  const uw = ((DATA.uwick || {}).buy || []).slice(0, 6);
  const chip = (x, k) => `<span class="chip ${k}" style="cursor:pointer" onclick="coachPick('${esc(x.code)}')">${esc(x.name)}</span>`;
  return uw.length ? `<div class="note" style="margin-top:4px">🌙 上髭 買い側: ${uw.map(x => chip(x, "acc")).join(" ")}</div>` : "";
}
function coachPick(code) { COACH.code = code; const el = document.getElementById("coach-code"); if (el) el.value = code; coachSet("code", code); }
function viewCoach(h) {
  const m = String(h || "").match(/^#\/coach\/([0-9A-Za-z]{4,5})/); if (m) COACH.code = m[1];
  if (!COACH.words) COACH.words = coachLoadWords();
  if (typeof loadSearch === "function" && typeof SEARCH !== "undefined" && !SEARCH) setTimeout(() => { loadSearch().then(() => { const el = document.getElementById("coach-stock"); if (el) el.innerHTML = coachStockHtml(); }); }, 0);
  const key = coachKey();
  return `<h2>🧠 AI調教</h2>
    <h2 style="font-size:15px">🏆 最強候補（${esc(DATA.data_date || "")} 終値・機械の篩い）</h2>
    <div class="card" id="coach-screen">${coachScreenHtml()}</div>
    <h2 style="font-size:15px">🔪 解剖</h2>
    <div class="card">
      <input id="coach-code" type="text" inputmode="numeric" maxlength="5" value="${esc(COACH.code)}" oninput="coachSet('code', this.value.trim())" class="calc-in" style="font-size:18px" placeholder="銘柄コード">
      ${coachPickChips()}
      <textarea rows="3" oninput="coachSet('words', this.value)" class="calc-in" style="font-size:14px;line-height:1.5;resize:vertical;margin-top:8px" placeholder="私の言い分（買いたい理由・迷い・言い訳をそのまま）">${esc(COACH.words)}</textarea>
      <div id="coach-excuse" style="margin-top:6px">${coachExcuseHtml()}</div>
    </div>
    <div class="card" id="coach-stock">${coachStockHtml()}</div>
    <h2 style="font-size:15px">🎭 人格（AIの個人設定に貼る）</h2>
    <div class="card"><div class="chips"><button class="btn" onclick="event.preventDefault();coachCopyPersona(this)">📋 人格をコピー</button><span class="note">ChatGPT カスタム指示／Gemini 保存した情報／Claude プロジェクト指示</span></div>
      <details class="note" style="margin-top:6px"><summary style="cursor:pointer">全文を見る</summary><pre style="white-space:pre-wrap;font-size:12px;line-height:1.7;margin:6px 0 0">${esc(COACH_PERSONA)}</pre></details>
      ${vnote("", "元ネタ: AI惣菜『AI調教術』（2026-07-18）。数字は終値スナップショット。負け筋の数字: 反発買い＝26年全形マイナス・デイトレ＝17ヶ月−62.8%・初動ブレイク当日+20%＝−1.5%/件・出来高10倍＝−0.7%/件・初動ブレイク10年 +0.64%/件 PF1.17・勝率約半分。篩いの点数は機械的な並べ替えで、勝率を上げる証明はない。", "出典と数字")}</div>
    <h2 style="font-size:15px">🔑 Claude に直接聞く（任意）</h2>
    <div class="card">
      <div style="display:flex;gap:6px;align-items:center">
        <input id="coach-key" type="password" value="${esc(key)}" class="calc-in" style="font-size:13px;flex:1" placeholder="sk-ant-…" autocomplete="off">
        <button class="btn" onclick="event.preventDefault();coachSetKey(document.getElementById('coach-key').value)">${key ? "更新" : "保存"}</button>
        ${key ? `<button class="btn" onclick="event.preventDefault();coachSetKey('')">消す</button>` : ""}</div>
      <div class="note" style="margin-top:6px">この端末だけに保存・Anthropic へ直接・1回 数円〜十数円${key ? ` ・ 🔑 ${esc(key.slice(0, 10))}…` : ""}</div>
    </div>
    <p class="disc">${esc(DATA.disclaimer || "")}</p>`;
}
