// v7フロント（2026-10-13 再設計）の DOMシム検証。feedback_kabuai_spa_verify: node vm + DOMシムで全ルートを実データに対して実行し、
// 表示エラー/NaN/undefined ゼロを確認してから push。 実行: node _harness_v7.mjs   データ置き場は KABUAI_DATA で差し替え可（本番の latest.json を curl したもの推奨）
import fs from "node:fs";
import vm from "node:vm";

const html = fs.readFileSync("web/index.html", "utf8");
const DD = process.env.KABUAI_DATA || "data";
const DATA = JSON.parse(fs.readFileSync(DD + "/latest.json", "utf8"));
const SIDX = JSON.parse(fs.readFileSync(DD + "/search_index.json", "utf8"));
const EXPJ = JSON.parse(fs.readFileSync(DD + "/explorer.json", "utf8"));

const srcs = [...html.matchAll(/<script src="([^"?]+)(?:\?[^"]*)?"><\/script>/g)].map(m => m[1]);
if (!srcs.length) { console.error("FAIL: script tags not found"); process.exit(1); }
const code = srcs.map(s => fs.readFileSync("web/" + s, "utf8")).join("\n;\n");

const ctxProxy = new Proxy({}, { get: () => () => {}, set: () => true });
const store = {};
function mkEl(id) {
  return { _id: id, _html: "", style: {}, clientWidth: 360, parentElement: { clientWidth: 360 },
    set innerHTML(v) { this._html = v; }, get innerHTML() { return this._html; },
    textContent: "", classList: { toggle() {}, add() {}, remove() {} }, focus() {}, setAttribute() {},
    getContext: () => ctxProxy, querySelector() { return mkEl("c"); }, querySelectorAll() { return []; } };
}
function $get(sel) { return store[sel] || (store[sel] = mkEl(sel)); }
const documentShim = { querySelector: $get, getElementById: id => $get("#" + id), addEventListener() {},
  querySelectorAll: () => [], createElement: () => mkEl("new"), body: mkEl("body"), documentElement: mkEl("html") };
const locationShim = { hash: "#/" };
const windowShim = { addEventListener() {}, scrollTo() {}, location: locationShim, innerWidth: 390, devicePixelRatio: 1 };
const lsStore = {};
const localStorageShim = { getItem: k => (k in lsStore ? lsStore[k] : null), setItem: (k, v) => { lsStore[k] = String(v); }, removeItem: k => { delete lsStore[k]; } };
const stockJson = code => { try { return JSON.parse(fs.readFileSync(`${DD}/stocks/${code}.json`, "utf8")); } catch (e) { return null; } };
const sandbox = { document: documentShim, window: windowShim, location: locationShim, localStorage: localStorageShim,
  console, navigator: {}, history: { back() {} },
  fetch: async (u) => {
    const s = String(u);
    if (s.includes("search_index")) return { ok: true, status: 200, json: async () => SIDX };
    if (s.includes("explorer")) return { ok: true, status: 200, json: async () => EXPJ };
    const m = s.match(/stocks\/([^./]+)\.json/);
    if (m) { const j = stockJson(m[1]); return { ok: !!j, status: j ? 200 : 404, json: async () => j }; }
    return { ok: true, status: 200, json: async () => DATA };
  },
  setTimeout: (fn) => 0, clearTimeout() {}, setInterval: () => 0, requestAnimationFrame: fn => fn() };
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
vm.runInContext(code, sandbox);
const ev = expr => vm.runInContext(expr, sandbox);
await new Promise(r => setTimeout(r, 50));

let fail = 0;
const check = (n, c, x = "") => { if (c) console.log(`  OK ${n}${x ? " — " + x : ""}`); else { fail++; console.log(`  NG ${n}${x ? " — " + x : ""}`); if (process.env.HDEBUG) console.log("     >> " + String(x || view()).replace(/\s+/g, " ").slice(0, 700)); } };
const clean = hv => !hv.includes("表示エラー") && !hv.includes("NaN") && !hv.includes("undefined") && !hv.includes("[object");
const view = () => $get("#view").innerHTML;
const go = h => { locationShim.hash = h; sandbox.render(); return view(); };

console.log("── 0) 起動・データ ──");
check("DATAロード", !!ev("DATA && DATA.data_date"), ev("DATA && DATA.data_date"));
check("ROUTES登録", ev("ROUTES.length") === 11, `${ev("ROUTES.length")}本`);
check("datepill", $get("#datepill").textContent.includes("EOD"), $get("#datepill").textContent);
check("v7: LIVE未定義でも起動", ev("typeof LIVE") === "undefined");

console.log("── 1) 🌙前夜の準備（ホーム）＋🕯️上髭 ──");
let hv = go("#/");
check("ホーム＝前夜の準備", clean(hv) && hv.includes("前夜の準備") && hv.includes("上髭逆指値") && hv.includes("土俵"));
// 📰 IR（v7.5・2026-10-17）: 無い時は空メッセージ・ある時は 好/悪/要精査 のタブと答え合わせ
{ const bak = DATA.ir; DATA.ir = null; hv = go("#/"); check("IR 無し→空メッセージ", clean(hv) && hv.includes("IRの好材料・悪材料") && hv.includes("まだ作られていません"));
  const mk = (id, date, time, code, name, title, tags, label, score, llm, react) => ({ id, date, time, intraday: time <= "15:30", code, name, sector: "情報･通信業", title, pdf: "https://www.release.tdnet.info/inbs/" + id + ".pdf", tags, material: true, admin: false,
    rule: { label, score, reason: "r" }, llm, final: llm || { label, score, reason: "r", model: "rule" }, react: react || { day_chg: null, next_gap: null, next_chg: null, next_oc: null, next_date: null }, close: 1234, tov20_oku: 12.3 });
  const D = DATA.data_date;
  DATA.ir = { date: D, generated_at: "x", window: [D, "2026-01-01"], fetched: { [D]: 3, "2026-01-01": 2 },
    rows: [mk("a1", D, "15:30", "7203", "トヨタ", "自己株式の取得に関するお知らせ", ["自社株買い"], "好材料", 2, null),
           mk("a2", D, "16:00", "6758", "ソニー", "業績予想の修正に関するお知らせ", ["予想修正"], "要精査", 0, { label: "悪材料", score: -2, reason: "通期を12%下方修正", conf: 80, priced: false, model: "claude-opus-5-5", facts: "営業益 1,000→880億円" }),
           mk("a3", D, "17:00", "9984", "ＳＢＧ", "第三者割当による新株式発行", ["エクイティ"], "悪材料", -2, null),
           mk("b1", "2026-01-01", "14:00", "8306", "三菱ＵＦＪ", "増配のお知らせ", ["配当"], "好材料", 2, null, { day_chg: 1.5, next_gap: 0.8, next_chg: 0.2, next_oc: -0.6, next_date: "2026-01-02" })],
    tags_count: { "自社株買い": 1, "予想修正": 1, "エクイティ": 1, "配当": 1 },
    stats: { days: 2, from: "2026-01-01", to: D, rule: { "好材料": { n: 2, n_scored: 1, gap: 0.8, gap_hit: 100, chg: 0.2, chg_hit: 100, oc: -0.6, oc_hit: 0, day: 1.5, day_hit: 100, n_day: 1 }, "悪材料": { n: 1, n_scored: 0 } }, llm: { "好材料": { n: 0 }, "悪材料": { n: 1, n_scored: 0 } } },
    llm: { available: true, judged: 1, model: "claude-opus-5-5", note: "n" }, basis: "b", rules: ["r1"] };
  hv = go("#/");
  check("IR 今日: 👍1/👎2/🔍0 と答え合わせ", clean(hv) && hv.includes("👍 好 1") && hv.includes("👎 悪 2") && hv.includes("🔍 要精査 0") && hv.includes("翌日 寄り平均") && hv.includes("Claude判定 1件"));
  check("IR 好タブ: トヨタ行・既出・表題ルール", (hv.match(/class="irrow"/g) || []).length === 1 && hv.includes("トヨタ") && hv.includes("📌 既出") && hv.includes("📏 表題ルール"));
  sandbox.irSetTab("bad"); hv = view();
  check("IR 悪タブ: Claude判定の行にルール差とfacts", (hv.match(/class="irrow"/g) || []).length === 2 && hv.includes("🤖 Claude") && hv.includes("ルールは要精査") && hv.includes("営業益 1,000→880億円") && hv.includes("翌営業日の寄りで答え合わせ"));
  sandbox.irSetDay("window"); sandbox.irSetTab("good"); hv = view();
  check("IR 既出を含む: 三菱ＵＦＪの反応つき", (hv.match(/class="irrow"/g) || []).length === 2 && hv.includes("三菱ＵＦＪ") && hv.includes("+1.50%") && hv.includes("+0.80%"));
  sandbox.irSetTab("all"); hv = view(); check("IR 全部タブ", clean(hv) && hv.includes("中立 0件") && hv.includes("事務開示 0件"));
  // 🔍開示検索（v7.6）: 検索箱＋例チップ・本文ファイル（IR_TXT）を注入して AND 検索・表記ゆれ・本文の一致箇所・無い日は表題で代用
  check("IR 検索箱と例チップ", hv.includes('id="irq"') && hv.includes("フィジカルAI") && hv.includes("エヌヴィディア") && hv.includes('id="irmain" style=""'));
  ev(`IR_IDX = { days: [{ date: "${D}", n: 2, n_text: 2 }, { date: "2026-01-01", n: 1, n_text: 0 }], n: 3, n_text: 2, pending: 1 };
      IR_TXT["${D}"] = { date: "${D}", n: 2, n_text: 2, rows: [
        { id: "a1", time: "15:30", code: "7203", name: "トヨタ", title: "自己株式の取得に関するお知らせ", pdf: "https://x/a1.pdf", tags: ["自社株買い"], intraday: false, label: "好材料", score: 2, react: { day_chg: null, next_gap: null, next_chg: null, next_date: null }, t: "当社は、フィジカル ＡＩ 戦略の一環として NVIDIA と協業し、自己株式を取得します。", p: 2 },
        { id: "a9", time: "12:00", code: "4689", name: "ＬＩＮＥヤフー", title: "サイバーセキュリティ対策の強化について", pdf: "", tags: ["その他"], intraday: true, label: "中立", score: 0, react: { day_chg: -0.5, next_gap: null, next_chg: null, next_date: null }, t: "セキュリティ 体制を強化", p: 1 }] };`);
  sandbox.irSearchSet("フィジカルAI"); hv = view();
  check("IR 検索: フィジカルAI→本文で1件・一致箇所・一覧は隠す", clean(hv) && hv.includes("🔍「フィジカルAI」 1件") && hv.includes("<mark>フィジカルai</mark>") && hv.includes("トヨタ") && hv.includes('id="irmain" style="display:none"'));
  sandbox.irSearchSet("エヌヴィディア"); hv = view(); check("IR 検索: 表記ゆれ エヌヴィディア→nvidia", hv.includes("🔍「エヌヴィディア」 1件") && hv.includes("<mark>nvidia</mark>"));
  sandbox.irSearchSet("サイバーセキュリティ トヨタ"); hv = view(); check("IR 検索: AND で0件", hv.includes("「サイバーセキュリティ トヨタ」 0件") && hv.includes("該当なし"));
  sandbox.irSearchSet("セキュリティ"); hv = view(); check("IR 検索: 表題・本文の両方で1件（既出の反応つき）", hv.includes("「セキュリティ」 1件") && hv.includes("ＬＩＮＥヤフー") && hv.includes("📌 既出") && hv.includes("-0.50%"));
  sandbox.irSetScope("all"); sandbox.irSearchSet("増配"); hv = view(); check("IR 検索: 本文ファイルの無い日は表題で代用（三菱ＵＦＪ）", hv.includes("「増配」 1件") && hv.includes("三菱ＵＦＪ") && hv.includes("本文は未取得"));
  DATA.ir.hot_themes = { fetched_at: "2026-10-17 20:40", page_time: "2026-10-17 20:30", period: "3日間のアクセス", themes: Array.from({ length: 30 }, (_, i) => ({ rank: i + 1, name: i === 0 ? "サイバーセキュリティ" : i === 4 ? "蓄電池" : `テーマ${i + 1}`, stocks: i === 0 ? [{ code: "3692", name: "ＦＦＲＩセキュリティ" }, { code: "4498", name: "サイバートラスト" }] : [] })) };
  sandbox.irSearchSet("サイバーセキュリティ"); hv = view();
  check("IR 検索: 株探 人気テーマのチップ(上位12)＋代表銘柄", clean(hv) && hv.includes("株探 人気テーマ（3日間のアクセス・10-17 20:30）") && hv.includes("蓄電池") && !hv.includes("テーマ13") && hv.includes("＋18") && hv.includes("1位「サイバーセキュリティ」の代表") && hv.includes("ＦＦＲＩセキュリティ"));
  sandbox.irHotToggle(); hv = view(); check("IR 検索: 人気テーマ30件全部", hv.includes("テーマ30") && hv.includes("上位12に戻す")); sandbox.irHotToggle();
  sandbox.irSearchSet("宇宙開発関連"); hv = view(); check("IR 検索: 「〜関連」を落として本文と照合（衛星→0件でも宇宙の語で）", hv.includes("「宇宙開発関連」 0件"));
  ev(`IR_TXT["${D}"].rows[1].t = "量子コンピュータの開発";`); sandbox.irSearchSet("量子コンピューター"); hv = view(); check("IR 検索: 末尾の長音を落として照合", hv.includes("「量子コンピューター」 1件") && hv.includes("<mark>量子コンピュータ</mark>"));
  DATA.ir.hot_themes = null; hv = go("#/"); check("IR 検索: 人気テーマ無し→予備の例（蓄電池入り）", hv.includes("例:") && hv.includes("蓄電池") && hv.includes("kabutan_hot_pc.py"));
  sandbox.irSetScope("window"); sandbox.irSearchSet(""); hv = view(); check("IR 検索: 消すと一覧に戻る", hv.includes('id="irmain" style=""') && hv.includes("👍 好 2") && !hv.includes("🔍「"));
  sandbox.irSetDay("today"); sandbox.irSetTab("good"); DATA.ir = bak; }
const mkRow = (code, name, close, high, low, t500, extra = {}) => Object.assign({ code, name, sector: "情報･通信業", close, open: close * 0.99, high, low, chg: 1.23, uw_pct: 4.1, rng_pct: 6.2, uw_ratio: 0.66, atr_pct: 5.5,
  turnover_oku: 42.1, tov20_oku: 30.5, tov_ratio: 1.4, vol_ratio: 1.3, body_up: true, t500, tick: t500 ? 0.5 : 1, buy_level: high + (t500 ? 0.5 : 1), sell_level: low - (t500 ? 0.5 : 1),
  shortable: "○", jsf_stop: false, jsf_warn: false, tse_reg: false }, extra);
const uw = { date: DATA.data_date, target_date: "2099-01-04", generated_at: "x", fresh: true, n_day: 4000, matched: 3, iss_known: true, alert_known: true, scale_known: true,
  calc: { tp: 1.5, sl: 3, loss_yen: 20000 }, rules: ["r1", "r2"], stats: { period: "p", buy: "b", sell: "s", control: "c", caveat: "v" },
  buy: [mkRow("3103", "ユニチカ", 1520, 1602, 1499, false), mkRow("7003", "三井Ｅ＆Ｓ", 4204, 4447, 4190, true, { jsf_warn: true }), mkRow("9999", "テスト売り禁", 800, 850, 790, false, { jsf_stop: true })],
  sell: [mkRow("3103", "ユニチカ", 1520, 1602, 1499, false), mkRow("7003", "三井Ｅ＆Ｓ", 4204, 4447, 4190, true, { jsf_warn: true })], n_sell_blocked: 1 };
{ const bak = DATA.uwick, fbak = DATA.flow; DATA.flow = null;   // flow も uwrow を使うので行数チェックの間は外す
  DATA.uwick = uw; hv = go("#/");
  check("上髭 買い3本", clean(hv) && (hv.match(/class="uwrow/g) || []).length === 3 && hv.includes("1,603") && hv.includes("4,447.5") && hv.includes("買い逆指値") && hv.includes("勝てるとは言いません"));
  check("上髭 買い: 利確/損切り/株数チップ", hv.includes("利確 1") && hv.includes("損切り 1") && hv.includes("株（損失2万円）") && hv.includes("🧮 電卓へ"));
  sandbox.uwSetSide("sell"); hv = view();
  check("上髭 売り2本（売り禁は除外済み・注意喚起チップ）", clean(hv) && (hv.match(/class="uwrow/g) || []).length === 2 && hv.includes("売り逆指値") && hv.includes("1,498") && hv.includes("注意喚起") && hv.includes("1本除外"));
  sandbox.uwSetSide("buy");
  DATA.uwick = { ...uw, fresh: false }; hv = go("#/"); check("上髭 過去分バナー", clean(hv) && hv.includes("過去分"));
  DATA.uwick = { ...uw, buy: [], sell: [], matched: 0 }; hv = go("#/"); check("上髭 0件", clean(hv) && hv.includes("条件に合う銘柄がありません"));
  DATA.uwick = { ...uw, sell: [], iss_known: false }; sandbox.uwSetSide("sell"); hv = go("#/"); check("上髭 売り: 貸借不明バナー", clean(hv) && hv.includes("貸借区分が取れなかった")); sandbox.uwSetSide("buy");
  DATA.uwick = { error: "boom" }; hv = go("#/"); check("上髭 error", clean(hv) && hv.includes("今回は作れませんでした"));
  DATA.uwick = null; hv = go("#/"); check("上髭 無し", clean(hv) && hv.includes("まだ作られていません"));
  DATA.uwick = bak; DATA.flow = fbak; }
{ const bak = DATA.arena; DATA.arena = null; hv = go("#/"); check("土俵なしでも落ちない", clean(hv) && hv.includes("土俵リストがまだありません")); DATA.arena = bak; }

console.log("── 1b) 💴資金の流れ→🔗連動株（flow_builder・2026-10-14）──");
{ const bak = DATA.flow;
  let fl = null;
  try { fl = JSON.parse(fs.readFileSync("_flow_test.json", "utf8")); } catch (e) { fl = null; }
  const mk = (code, name, r1, extra = {}) => Object.assign({ code, name, sector: "非鉄金属", close: 1234, r1, r5: 3.2, tov_oku: 56.7, tov_ratio: 2.9, dev25: 8.1, rng: 5.0, shortable: "○", jsf_stop: false, daily_pub: false, tse_reg: false }, extra);
  const sample = { date: DATA.data_date, target_date: "2099-01-04", fresh: true, generated_at: "x", n_universe: 1067, window: 60, floor: 0.5, null_max: 0.42, n_groups_cand: 1, weak_day: true,
    groups: [{ kind: "テーマ", label: "インジウム", n: 6, n_hot: 2, passed: true, strong: true, score: 2.5, ratio_med: 2.1, ratio_sum: 1.8, breadth: 100, avg_r1: 6.8, avg_r5: 4.3, coh: 0.41,
      lead: mk("5711", "三菱マテリアル", 5.84, { n_links: 2, n_big: 7 }), n_chain: 2, n_out: 3, n_big: 7,
      members: [mk("5711", "三菱マテリアル", 5.84, { corr: null, n_links: 2, n_big: 7 }), mk("5713", "住友金属鉱山", 10.76, { corr: 0.71, n_links: 2, same_day: 86, n_big: 7, pts: 1300, pts_dev: -0.03, pts_time: "23:58 10/13" }), mk("5714", "ＤＯＷＡ", 5.64, { corr: 0.59, n_links: 1, same_day: 57, n_big: 7, daily_pub: true })],
      members_out: [mk("5706", "三井金属", 1.2, { corr: 0.31, n_links: 0, same_day: 43, n_big: 7, shortable: "×" })],
      linked_out: [mk("4401", "ＡＤＥＫＡ", 2.1, { corr: 0.65, same_day: 100, n_big: 5, pts: 3000, pts_dev: 1.2 })],
      hidden: [mk("4382", "ＨＥＲＯＺ", 27.1, { corr_grp: 0.59, tov_ratio: 35.5, pts: 1428, pts_dev: 2.1 })] }],
    movers: [{ lead: mk("5713", "住友金属鉱山", 10.76, { pts: 1300, pts_dev: -0.03 }), linked: [mk("5714", "ＤＯＷＡ", 5.64, { corr: 0.78, same_day: 100, n_big: 9 }), mk("5707", "東邦亜鉛", 9.38, { corr: 0.66, same_day: null, n_big: 1, shortable: "×" })] },
             { lead: mk("3350", "メタプラネット", 16.06), linked: [] }],
    pts: { known: true, ok: 38, tried: 65, asof: "23:58 10/13", err: {}, skipped: false }, iss_known: true, alert_known: true,
    rules: ["r1", "r2"], stats: { chase: "後追い検証", day3: "祭り3日目", use: "使い道" }, elapsed_s: 1 };
  DATA.flow = sample; sandbox.flowSetTab("link"); hv = go("#/");
  check("連動 既定タブ＝主役→連動（2主役・連動2本＋単独）", clean(hv) && hv.includes("資金の流れ") && hv.includes("🎯") && hv.includes("住友金属鉱山") && hv.includes("0.78") && hv.includes("同日100%（9回）") && hv.includes("同日 —") && hv.includes("単独で動いた") && hv.includes("後追い買いは効きません"));
  check("連動: PTS表示（取れた/取れない）", hv.includes("PTS ¥1,300") && hv.includes("PTS —") && hv.includes("23:58 10/13 時点") && hv.includes("38/65本"));
  check("連動: 貸借/売り禁/日々公表チップ", hv.includes("貸借○") && hv.includes("貸借×"));
  sandbox.flowSetTab("sector"); hv = go("#/");
  check("資金セクター タブ（🔥・主役チップ・グループ外連動・弱い日バナー）", clean(hv) && hv.includes("🔥 インジウム") && hv.includes("×2.1") && hv.includes("上昇100%") && hv.includes("主役</span>") && hv.includes("グループ外で主役に連動") && hv.includes("ＡＤＥＫＡ") && hv.includes("日々公表") && hv.includes("資金が集まったグループが少ない日"));
  check("資金セクター v7.2: 鎖の本数・同日連動・+5%日・鎖の外(折りたたみ)・🧲隠れ連動", hv.includes("⛓️鎖2本／6") && hv.includes("鎖2本") && hv.includes("同日86%") && hv.includes("+5%日 7回") && hv.includes("鎖の外 3本") && hv.includes("三井金属") && hv.includes("0.31") && hv.includes("偶然の範囲") && hv.includes("群全体に連動") && hv.includes("ＨＥＲＯＺ") && hv.includes("0.59") && hv.includes("×36"));
  { const g0 = { ...sample.groups[0], members_out: [], hidden: [], n_chain: 0, n_out: 0 }; DATA.flow = { ...sample, groups: [g0] }; hv = go("#/"); check("資金セクター v7.2: 鎖の外/隠れ連動が空でも崩れない", clean(hv) && !hv.includes("鎖の外") && !hv.includes("群全体に連動")); DATA.flow = sample; hv = go("#/"); }
  DATA.flow = { ...sample, groups: [], movers: [], weak_day: false }; hv = go("#/"); check("資金セクター 0件", clean(hv) && hv.includes("資金が集まったグループがありません"));
  sandbox.flowSetTab("link"); hv = go("#/"); check("連動 0件", clean(hv) && hv.includes("主役がいません"));
  DATA.flow = { ...sample, pts: { known: false, ok: 0, tried: 40, err: { http403: 40 }, skipped: false } }; hv = go("#/"); check("PTS取れず（403）の注記", clean(hv) && hv.includes("PTSは取れませんでした") && hv.includes("http403"));
  DATA.flow = { ...sample, pts: { skipped: true } }; hv = go("#/"); check("PTSスキップの注記", clean(hv) && hv.includes("PTSは取っていません"));
  if (fl && !fl.error) { DATA.flow = fl; hv = go("#/"); check(`実データ(_flow_test.json ${fl.date}) 連動タブ`, clean(hv) && hv.includes("資金の流れ"), `主役${(fl.movers || []).length}`); sandbox.flowSetTab("sector"); hv = go("#/"); check("実データ 資金セクタータブ", clean(hv), `グループ${(fl.groups || []).length}`); sandbox.flowSetTab("link"); }
  else console.log("  (skip) _flow_test.json なし＝実データ検証は省略");
  // 📚連動集（v7.3・2026-10-15）
  { const bb = DATA.linkbook;
    const mkb = (code, name, hub, extra = {}) => Object.assign({ code, name, sector: "銀行業", close: 1234, r1: 1.2, r5: 3.4, tov_med_oku: 9.8, rng20: 2.3, hub, shortable: "○", jsf_stop: false, daily_pub: false, tse_reg: false }, extra);
    const sampleB = { date: DATA.data_date, target_date: "2099-01-04", n_universe: 804, n_days: 81, half_days: 40, period: "06/16〜10/14", floor: 0.5, floor_half: 0.54, n_edges: 4195, n_groups: 72,
      groups: [{ label: "地方銀行", kind: "テーマ", alts: ["金利上昇メリット"], sectors: "銀行業12", n: 2, n_core: 5, n_theme: 9, codes: "8336 7380", cross: false, corr: 0.8, stab: 0.76, h1: 0.8, h2: 0.82, score: 2.6, avg_r1: 0.5, avg_r5: 1.1, tov_med_oku: 9.8, members: [mkb("8336", "武蔵野銀行", 0.83), mkb("7380", "十六フィナンシャルグループ", 0.83, { daily_pub: true, shortable: "×" })] },
               { label: "地方銀行", kind: "テーマ", alts: [], sectors: "銀行業2", n: 2, cross: true, corr: 0.7, stab: 0.6, h1: 0.6, h2: 0.7, score: 2.0, avg_r1: -0.3, avg_r5: 0.2, tov_med_oku: 30, members: [mkb("7337", "ひろぎんホールディングス", 0.7), mkb("7327", "第四北越フィナンシャルグループ", 0.7)] }],
      rules: ["b1"], stats: { chase: "翌日買い検証", use: "使い道B" } };
    DATA.flow = sample; DATA.linkbook = sampleB; sandbox.flowSetTab("book"); hv = go("#/");
    check("📚連動集 タブ（番号・重複ラベルの（2）・安定/前半後半・仲間との平均・代金中央・🧩・alts・買い推奨ではない・📋コピー・核/テーマ内）", clean(hv) && hv.includes("📚 連動集 2") && hv.includes("コードをコピー") && hv.includes("8336 7380") && hv.includes("核5本（上位2本を表示）／テーマ内9本") && hv.includes("SBIに登録") && hv.includes("1. 地方銀行") && hv.includes("2. 🧩 地方銀行（2）") && hv.includes("安定 <b>0.76") && hv.includes("前半0.80・後半0.82") && hv.includes("武蔵野銀行") && hv.includes("0.83") && hv.includes("金利上昇メリット") && hv.includes("買い推奨ではありません") && hv.includes("翌日買い検証") && hv.includes("81営業日") && hv.includes("日々公表"));
    DATA.linkbook = { ...sampleB, groups: [] }; hv = go("#/"); check("📚連動集 0件", clean(hv) && hv.includes("両期間で成立する連動グループがありません"));
    DATA.linkbook = { error: "boomB" }; hv = go("#/"); check("📚連動集 error", clean(hv) && hv.includes("boomB"));
    DATA.linkbook = null; hv = go("#/"); check("📚連動集 無し", clean(hv) && hv.includes("まだ作られていません"));
    DATA.flow = null; DATA.linkbook = sampleB; hv = go("#/"); check("flow 無しでも📚連動集は見られる", clean(hv) && hv.includes("1. 地方銀行"));
    let rb = null; try { rb = JSON.parse(fs.readFileSync("_linkbook_test.json", "utf8")); } catch (e) { rb = null; }
    if (rb && !rb.error) { DATA.flow = sample; DATA.linkbook = rb; hv = go("#/"); check(`実データ(_linkbook_test.json ${rb.date}) 📚連動集`, clean(hv) && hv.includes("📚 連動集"), `グループ${(rb.groups || []).length}/${rb.n_groups}`); }
    else console.log("  (skip) _linkbook_test.json なし");
    DATA.linkbook = bb; sandbox.flowSetTab("link"); }
  DATA.flow = { error: "boom" }; hv = go("#/"); check("flow error", clean(hv) && hv.includes("今回は作れませんでした"));
  DATA.flow = null; hv = go("#/"); check("flow 無し", clean(hv) && hv.includes("まだ作られていません"));
  DATA.flow = bak; }

console.log("── 2) 🧮電卓 ──");
hv = go("#/calc"); check("電卓 空", clean(hv) && hv.includes("買値を入れると"));
sandbox.calcSet("px", "3057"); hv = $get("#calc-out").innerHTML; check("電卓 買い 3057 一般（呼値5円→3,105）", clean(hv) && hv.includes("3,105") && hv.includes("2,966") && hv.includes("200"));
sandbox.calcSet("t500", true); hv = $get("#calc-out").innerHTML; check("電卓 買い 3057 TOPIX500", clean(hv) && hv.includes("3,103") && hv.includes("2,965.5"));
sandbox.calcSet("side", "sell"); hv = view(); check("電卓 空売り切替（見出し・ラベル）", clean(hv) && hv.includes("空売り：利確") && hv.includes("売値（空売りが約定した値段）") && hv.includes("3,011") && hv.includes("3,148") && hv.includes("買戻し"));
sandbox.calcSet("sh", "300"); hv = $get("#calc-out").innerHTML; check("電卓 空売り 300株", clean(hv) && hv.includes("300") && hv.includes("売り建玉"));
sandbox.calcPick("3103", 1603, "buy", false, "ユニチカ"); hv = view(); check("電卓 前夜から受け取り", clean(hv) && locationShim.hash === "#/calc" && hv.includes("ユニチカ") && hv.includes("1603") && hv.includes("1,628") && hv.includes("買い：利確"));
sandbox.calcSet("px", "9800"); sandbox.calcSet("sh", ""); hv = $get("#calc-out").innerHTML; check("電卓 9800円（自動0株）", clean(hv) && hv.includes("100株でも損失が2万円を超えます"));

console.log("── 3) 📋作戦（✅チェック吸収） ──");
hv = go("#/plan"); check("作戦(データ無しでも落ちない)", clean(hv) && hv.includes("作戦"));
{ const bak = DATA.plan; DATA.plan = JSON.parse(fs.readFileSync("_plan_sample.json", "utf8"));
  hv = go("#/plan"); check("作戦: サンプル注入", clean(hv) && hv.includes("実弾の注文") && hv.includes("資金ラダー") && hv.includes("寄指"));
  check("作戦: ✅注文前チェック", hv.includes("注文前チェック") && hv.includes("0 / 7") && hv.includes("あと <b>7</b>"));
  for (let i = 0; i < 7; i++) sandbox.planToggleCheck(i); hv = view(); check("作戦: チェック7/7→OKバナー", clean(hv) && hv.includes("7つ全部OK"));
  sandbox.planClearChecks(); hv = view(); check("作戦: 全部外す", hv.includes("0 / 7"));
  hv = go("#/"); check("前夜: 材料＋明日の決算", clean(hv) && hv.includes("材料") && hv.includes("明日の決算発表"));
  sandbox.planSetCash("20"); hv = go("#/plan"); check("作戦: 現金余力20万→②ゼロ", clean(hv) && hv.includes("今日は出さない"));
  sandbox.planSetCash(""); localStorageShim.removeItem("kabuai_cash");
  DATA.plan = { ...DATA.plan, orders: [], paper: [], news: [], earnings_tomorrow: [] }; hv = go("#/plan"); check("作戦: 全部ゼロ件", clean(hv) && hv.includes("撃つ玉なし"));
  DATA.plan = bak; }

console.log("── 4) 📊データ: 🔻売り / 🐵EOD / 🧭探検 / 検索 / 詳細 ──");
hv = go("#/sell"); check("売り", clean(hv) && hv.includes("フェード") && hv.includes("モメンタム終了") && hv.includes("EODランキング"));
hv = go("#/momentum"); check("EODランキング", clean(hv) && hv.includes("モメンタム") && (hv.match(/class="mrow/g) || []).length >= 10, `${(hv.match(/class="mrow/g) || []).length}行`);
sandbox.setMomGrade("A"); hv = view(); check("EOD: Aフィルタ", clean(hv)); sandbox.setMomGrade("all");
hv = go("#/explore"); await new Promise(r => setTimeout(r, 20)); hv = go("#/explore"); check("探検トップ", clean(hv) && hv.includes("初動"));
for (const c of ev("Object.keys(CAT_DEFS)")) { hv = go(`#/explore/${c}`); check(`探検/${c}`, clean(hv)); }
hv = go("#/explore/break60"); check("探検/break60 翌朝ルールと新高値の数字", clean(hv) && hv.includes("寄りが前日終値+3%超なら買わない") && hv.includes("+1.35%/件"));
{ const bak = EXPJ.categories.break60; EXPJ.categories.break60 = [{ code: "3042", name: "セキュアヴェイル", price: 325, r1: 9.1, date: "2026-10-01", volx: 4.2, hi60: 314, chg: 9.1, over60: 3.5, new250: true, atr_pct: 2.1, calm: true, today: true },
    { code: "9999", name: "荒い株", price: 1000, r1: 3, date: "2026-10-01", volx: 2.0, hi60: 980, chg: 3.0, over60: 2.0, new250: false, atr_pct: 6.5, calm: false, today: false }];
  hv = go("#/explore/break60"); check("探検/break60 行: 🆕新高値・凪/荒いのチップ", clean(hv) && hv.includes("🆕 新高値(250日)") && hv.includes("凪 ATR 2.1%") && hv.includes("荒い ATR 6.5%") && hv.includes("当日 +9.1%"));
  EXPJ.categories.break60 = bak; }
hv = go("#/search"); await new Promise(r => setTimeout(r, 20));
sandbox.onSearchInput("72"); hv = $get("#searchres").innerHTML; check("検索 '72'", clean(hv) && hv.includes("rcard"));
const codes = (DATA.ranking || []).slice(0, 2).map(r => r.code).concat(((DATA.arena || {}).rows || []).slice(0, 1).map(r => r.code));
for (const c of codes) {
  go(`#/detail/${c}`); await sandbox.loadStockDetail(c);
  const d = $get("#dbody").innerHTML; check(`詳細 ${c}`, clean(d) && (d.includes("日足") || d.includes("EODデータ") || d.includes("の終値")), process.env.HDEBUG ? d.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").slice(0, 400) : "");
}
hv = go("#/about"); check("使い方 v7", clean(hv) && hv.includes("v7") && hv.includes("上髭逆指値") && !hv.includes("ライブの数字の読み方"));

console.log("── 5) 不在チェック・残置 ──");
check("ナビは4本（前夜/作戦/データ/電卓）", (html.match(/<nav>[\s\S]*?<\/nav>/) || [""])[0].split("<a ").length - 1 === 4 && html.includes('id="nav-arena"') && html.includes('id="nav-calc"') && !html.includes('id="nav-live"'));
check("live.js を読み込まない", !srcs.includes("js/live.js") && srcs.includes("js/calc.js"));
check("旧画面残置", fs.existsSync("web/legacy_v5.html") && fs.existsSync("web/legacy_v6.html") && fs.existsSync("web/js/day.js") && fs.existsSync("web/js/live.js"));
console.log(fail ? `\n${fail} 件 NG` : "\nALL GREEN");
process.exit(fail ? 1 : 0);
