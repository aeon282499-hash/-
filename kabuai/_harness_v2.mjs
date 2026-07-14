// v2フロント（2026-07-02 作り直し）の DOMシム検証。
// feedback_kabuai_spa_verify: WebFetchではJSが実行されないので、node vm + DOMシムで
// 全viewを実データに対して実行し、表示エラー/NaN/undefined ゼロを確認してからpushする。
import fs from "node:fs";
import vm from "node:vm";

const html = fs.readFileSync("web/index.html", "utf8");
const DATA = JSON.parse(fs.readFileSync("data/latest.json", "utf8"));
const SIDX = JSON.parse(fs.readFileSync("data/search_index.json", "utf8"));
const EXPJ = JSON.parse(fs.readFileSync("data/explorer.json", "utf8"));

const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
const appScript = scripts.find(s => s.includes("function render"));
if (!appScript) { console.error("FAIL: app script not found"); process.exit(1); }

const ctxProxy = new Proxy({}, { get: () => () => {}, set: () => true });
const store = {};
function mkEl(id){ return { _id:id,_html:"",style:{},clientWidth:360,parentElement:{clientWidth:360},
  set innerHTML(v){this._html=v;},get innerHTML(){return this._html;},
  textContent:"",classList:{toggle(){},add(){},remove(){}},focus(){},
  getContext:()=>ctxProxy,
  querySelector(){return mkEl("c");},querySelectorAll(){return [];} }; }
function $get(sel){ return store[sel]||(store[sel]=mkEl(sel)); }
const documentShim={querySelector:$get,getElementById:id=>$get("#"+id),addEventListener(){},
  querySelectorAll:()=>[],createElement:()=>mkEl("new"),body:mkEl("body")};
const locationShim={hash:"#/"};
const windowShim={addEventListener(){},scrollTo(){},location:locationShim,innerWidth:390,devicePixelRatio:1};
const lsStore={};
const localStorageShim={getItem:k=>(k in lsStore?lsStore[k]:null),setItem:(k,v)=>{lsStore[k]=String(v);},removeItem:k=>{delete lsStore[k];}};
const stockJson=code=>{
  try{return JSON.parse(fs.readFileSync(`data/stocks/${code}.json`,"utf8"));}catch(e){return null;}
};
const sandbox={document:documentShim,window:windowShim,location:locationShim,localStorage:localStorageShim,
  console,navigator:{},
  fetch: async (u)=>{
    const s=String(u);
    if(s.includes("search_index")) return {ok:true,json:async()=>SIDX};
    if(s.includes("explorer")) return {ok:true,json:async()=>EXPJ};
    const m=s.match(/stocks\/([^./]+)\.json/);
    if(m){const j=stockJson(m[1]);return {ok:!!j,json:async()=>j};}
    return {ok:true,json:async()=>DATA};
  },
  setTimeout:(fn)=>0, clearTimeout(){}, requestAnimationFrame:fn=>fn()};
sandbox.globalThis=sandbox;
vm.createContext(sandbox);
vm.runInContext(appScript, sandbox);
await sandbox.load();

let fail=0;
const check=(n,c,x="")=>{ if(c){console.log(`  OK ${n}${x?" — "+x:""}`);} else {fail++;console.log(`  NG ${n}${x?" — "+x:""}`);} };
const clean=hv=>!hv.includes("表示エラー")&&!hv.includes("NaN")&&!hv.includes("undefined");

// ── 0) 先物連動タグを実データに注入（ローカルはSSL遮断でタグ0のため・ソート検証も兼ねる） ──
const BUYSET=new Set(["strong_reversal","reversal","strong_accum","accum"]);
const before=sandbox.candidates().list;
const pickMin=Number(DATA.pick_min_oku)||0;
check("買い候補が存在（v3=反発＋買い集め）", before.length>0, `${before.length}銘柄`);
check("候補のシグナルはBUY_KEYS(4種)のみ", before.every(r=>(r.signals||[]).every(k=>BUYSET.has(k))));
check("v3: 反発（反転/強反転）が候補に含まれる", before.some(r=>(r.signals||[]).some(k=>k==="reversal"||k==="strong_reversal")));
check("v3: ピックは板の厚い(≥"+pickMin+"億)のみ", pickMin>0&&before.every(r=>r.turnover_oku==null||r.turnover_oku>=pickMin));
let tagged={};
if(before.length>=3){
  tagged={[before[0].code]:{futures_corr:0.82,futures_tag:"高連動"},
          [before[1].code]:{futures_corr:0.55,futures_tag:"中連動"},
          [before[2].code]:{futures_corr:0.31,futures_tag:"自力"}};
  for(const g of Object.values(DATA.signals.groups))
    for(const m of (g.members||[])) if(tagged[m.code]) Object.assign(m,tagged[m.code]);
  for(const r of SIDX.stocks) if(tagged[r.code]) Object.assign(r,tagged[r.code]);
}

// ── 1) ホーム ──
locationShim.hash="#/"; sandbox.render();
let hv=$get("#view").innerHTML;
check("home: 表示エラー/NaN/undefinedなし", clean(hv));
check("home: ✅今日の買い候補ヒーロー", hv.includes("今日の買い候補")&&hv.includes("期待"));
check("v3 home: 反発（反転初動）が候補に表示される", hv.includes("反転")||hv.includes("⚡")||hv.includes("🔄"));
check("home: 出口の規律（8日/-12%/分散）", hv.includes("損切り-12%")&&hv.includes("分散")&&hv.includes("保有約8日"));
// 旧UIの痕跡なし。「地合い」は④の不調バナー文言（不調な地合い）で正当に使うため除外。
check("home: ホームに旧UI(ランキング/爆益/セクター)を出さない",
  !hv.includes("ランキング")&&!hv.includes("爆益")&&!hv.includes("セクター"));
if(before.length>=3){
  const after=sandbox.candidates().list;
  const evOf=r=>(sandbox.buyStat(r)||{ev:-1e9}).ev;
  const evs=after.map(evOf);
  check("sort: 期待値の高い順（勝てる順）が最優先", evs.every((v,i)=>i===0||v<=evs[i-1]), `ev=${evs.slice(0,6).join(",")}`);
  let tagOk=true;
  for(let i=1;i<after.length;i++)
    if(evOf(after[i])===evOf(after[i-1])&&sandbox.ftagRank(after[i])<sandbox.ftagRank(after[i-1])) tagOk=false;
  check("sort: 同率期待値内は自力→…→高連動", tagOk,
    after.slice(0,4).map(r=>`${r.name}=${r.futures_tag||"?"}`).join(" / "));
  // 候補が多いとhero上位8のみのため、全件展開してタグ描画を確実に検証
  sandbox.toggleHeroExpand(); locationShim.hash="#/"; sandbox.render();
  const hvx=$get("#view").innerHTML;
  check("home: 連動タグchip表示（日経連動%表記）", hvx.includes("日経連動31%・自力")&&hvx.includes("日経連動82%・高連動")&&hvx.includes('class="ftag'));
  sandbox.toggleHeroExpand();
}
// 広がり日バナー（本日broadのデータ）。v3では反発（強反転）が候補に出るので
// 「強反転の語を出さない」旧ルール（反発非表示時代）は撤廃＝バナー表示だけ確認。
if(DATA.rebound&&DATA.rebound.mode==="broad")
  check("home: 全面リバウンド日バナー表示", hv.includes("全面リバウンド日"));

// ── 1.5) 新機能: 分散サイジング / 年別実績 / 広がり日スタッツ / ヒーロー上位N ──
{
  // 分散サイジング（資金→1銘柄いくら）
  lsStore["kabuai_capital"]="300000";  // 30万円
  locationShim.hash="#/"; sandbox.render(); let sv=$get("#view").innerHTML;
  check("sizer: 投資予定額の入力＋1銘柄の目安", clean(sv)&&sv.includes("投資予定額")&&sv.includes("1銘柄の目安"));
  check("sizer: 各候補に株数/予算の目安を表示", sv.includes("1銘柄めやす")||sv.includes("予算を上げる"));
  delete lsStore["kabuai_capital"];
  sandbox.render(); sv=$get("#view").innerHTML;
  check("sizer: 未入力時は促し文のみ（株数は出さない）", sv.includes("投資予定額")&&!sv.includes("1銘柄めやす"));
  // 年別のロバストネス（表示中の型が全年プラス）
  check("proof: 年別プラスの裏づけを表示", sv.includes("単年のまぐれでない裏づけ"));
  // 広がり日スタッツ（本日broad）
  if(DATA.rebound&&DATA.rebound.mode==="broad"&&DATA.rebound.history)
    check("broad: 過去の全面リバウンド日の勝率/平均", sv.includes("日で検証")&&sv.includes("勝率"));
  // ヒーロー上位N＋展開
  const nCand=sandbox.candidates().list.length;
  if(nCand>8){
    check("hero: 上位8件のみ＋残りを展開できる", sv.includes("すべて見る"));
    const rows1=(sv.match(/class="pickrow"/g)||[]).length;
    sandbox.toggleHeroExpand(); const ev2=$get("#view").innerHTML;
    check("hero: 展開で全件表示＋戻すリンク",
      ev2.includes("上位だけ表示に戻す")&&(ev2.match(/class="pickrow"/g)||[]).length>rows1,
      `折畳${rows1}→展開${(ev2.match(/class="pickrow"/g)||[]).length}`);
    sandbox.toggleHeroExpand();
  }
}

// ── 1.6) 🆕 新規点灯ハイライト（前回見た日から新しく出た候補・当日だけ光る） ──
{
  const cur=sandbox.candidates().list.map(r=>r.code);
  if(cur.length>=2){
    lsStore["kabuai_seen"]=JSON.stringify({date:"2000-01-01",codes:cur.slice(1)}); // 先頭だけ未知＝新規
    sandbox.computeNewCodes();
    check("new: 前回いなかった候補が🆕、いた候補は非🆕", sandbox.isNew(cur[0])&&!sandbox.isNew(cur[1]));
    sandbox.computeNewCodes(); // 同一data_dateで再計算→毎日光り続けない
    check("new: 同じ日付では新規が消える", sandbox.candidates().list.every(r=>!sandbox.isNew(r.code)));
    lsStore["kabuai_seen"]=JSON.stringify({date:"2000-01-01",codes:cur.slice(1)});
    sandbox.computeNewCodes();
    locationShim.hash="#/"; sandbox.render();
    check("new: ホームに🆕チップ描画", $get("#view").innerHTML.includes("🆕新規"));
    lsStore["kabuai_seen"]=JSON.stringify({date:DATA.data_date,codes:cur}); sandbox.computeNewCodes(); // 後続へ影響させない
  }
}

// ── 1.7) 今日の日タイプ（🌊追い風 / ✅ふつう / 🚫見送り の3段階） ──
{
  const modeSave=DATA.rebound?DATA.rebound.mode:undefined;
  // 追い風（本日broad）
  locationShim.hash="#/"; sandbox.render(); let dv=$get("#view").innerHTML;
  check("dayband: 見出し「今日は」を常時表示", dv.includes("dayband")&&dv.includes("今日は「"));
  if(DATA.rebound&&DATA.rebound.mode==="broad")
    check("dayband: 追い風の日＝全面リバウンド日＋実績", dv.includes("追い風の日")&&dv.includes("日で検証"));
  // ふつうの日（modeをbroad以外に・候補は残す）
  if(DATA.rebound){
    DATA.rebound.mode="none"; sandbox.render(); dv=$get("#view").innerHTML;
    // 追い風でない候補ありの日＝ふつう / 不調地合い(cold)なら「待ち寄りの日」に変わる(②)
    check("dayband: 候補あり追い風なし＝ふつう/待ち寄り",
      (dv.includes("ふつうの日")||dv.includes("待ち寄りの日"))&&!dv.includes("追い風の日"));
    DATA.rebound.mode=modeSave;
  }
  // 見送りの日（候補0件）
  {
    const save={};
    for(const k of ["strong_reversal","reversal","strong_accum","accum"]){save[k]=DATA.signals.groups[k];
      DATA.signals.groups[k]={...save[k],count:0,members:[]};}
    sandbox.render(); dv=$get("#view").innerHTML;
    check("dayband: 見送りの日（候補0件）＋『注意して買う日』ラベルを作らない",
      dv.includes("見送りの日")&&!dv.includes("注意して買う日")&&!dv.includes("注意の日"));
    for(const k of ["strong_reversal","reversal","strong_accum","accum"]) DATA.signals.groups[k]=save[k];
    sandbox.render();
  }
}

// ── 1.8) v3.1: 業種分散キャップ / なぜ買い一言 ──
{
  const c=sandbox.candidates();
  const bySec={}; c.list.forEach(r=>{if(r.sector)bySec[r.sector]=(bySec[r.sector]||0)+1;});
  const maxSec=Math.max(0,...Object.values(bySec));
  check("① 業種分散: 同業種は最大3件まで", maxSec<=3, `最大同業種=${maxSec} / ${c.sectors}業種 / 除外${c.dropped}`);
  locationShim.hash="#/"; sandbox.render(); const hv18=$get("#view").innerHTML;
  if(c.sectors>1) check("① ホームに『◯業種に分散』表示", hv18.includes("🧩")&&hv18.includes("業種</b>に分散"));
  check("③ 買い候補に💡なぜ買いの一言", hv18.includes("💡")&&(hv18.includes("反発")||hv18.includes("買い集め")));
  check("④ 最近の調子メーター表示", hv18.includes("買い候補の最近の調子"));
  if(DATA.picks_scoreboard&&DATA.picks_scoreboard.regime==="cold"){
    check("④ cold地合いで不調バナー(メーター)", hv18.includes("いまは不調な地合い"));
    check("② 冷え相場: 候補直前に不調注意", hv18.includes("いまは不調地合い"));
    // 待ち寄りバナーは「追い風でない日」に出る→modeを非broadにして確認
    const svm=DATA.rebound?DATA.rebound.mode:undefined;
    if(DATA.rebound)DATA.rebound.mode="sparse";
    sandbox.render(); const hc=$get("#view").innerHTML;
    check("② 冷え相場: dayBanner『待ち寄りの日』", hc.includes("待ち寄りの日"));
    if(DATA.rebound)DATA.rebound.mode=svm;
    sandbox.render();
  }
}

// ── 2) 低位株カット ──
{
  const withCut=sandbox.candidates();
  lsStore["kabuai_lowprice"]="0";
  const noCut=sandbox.candidates();
  check("低位株toggle: 表示に切替で件数が増えるか同じ", noCut.list.length>=withCut.list.length,
    `cut=${withCut.list.length} all=${noCut.list.length} hidden=${withCut.hidden}`);
  check("低位株カット既定: ¥1000未満が出ない", withCut.list.every(r=>!(r.price>0&&r.price<1000)));
  lsStore["kabuai_lowprice"]="1";
}

// ── 2.5) 値動き天井（≥6%/日の宝くじ株は買い候補に出さない・2026-07-09 _edge_by_volatility.py） ──
{
  const maxRng=Number(DATA.pick_max_rng)||0;
  check("値動き: pick_max_rng(6%/日)がデータに存在", maxRng===6, `pick_max_rng=${DATA.pick_max_rng}`);
  const c0=sandbox.candidates();
  check("値動き: 候補に≥6%/日の宝くじ株が出ない", c0.list.every(r=>r.rng20==null||r.rng20<maxRng));
  const mems=Object.values(DATA.signals.groups).flatMap(g=>g.members||[]);
  check("値動き: 全メンバーにrng20フィールド（ビルド経路の欠落検知）", mems.length>0&&mems.every(m=>"rng20" in m));
  const victim=c0.list[0];
  if(victim){
    const saved=[];
    for(const g of Object.values(DATA.signals.groups))
      for(const m of (g.members||[])) if(m.code===victim.code){saved.push([m,m.rng20]); m.rng20=9.9;}
    const c1=sandbox.candidates();
    check("値動き: 9.9%/日に書き換えた銘柄が候補から消える（wildカウント）",
      !c1.list.some(r=>r.code===victim.code)&&c1.wild>=1, `wild=${c1.wild}`);
    for(const [m,v] of saved) m.rng20=v;
  }
  locationShim.hash="#/"; sandbox.render();
  const hv25=$get("#view").innerHTML;
  check("値動き: 買い候補カードに値動き%/日を表示", hv25.includes("値動き"));
}

// ── 3) 0件日ブランチ ──
{
  const save={};
  for(const k of ["strong_reversal","reversal","strong_accum","accum"]){save[k]=DATA.signals.groups[k];
    DATA.signals.groups[k]={...save[k],count:0,members:[]};}
  sandbox.render(); const zv=$get("#view").innerHTML;
  check("0件日: 「買わない日」を明示・エラーなし", clean(zv)&&zv.includes("買わない日"));
  for(const k of ["strong_accum","accum"]) DATA.signals.groups[k]=save[k];
}

// ── 4) 📒持ち株コーチ ──
{
  await sandbox.loadSearch();
  const r=SIDX.stocks.find(s=>s.price>1000);
  const today=new Date(); const ds=`${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,"0")}-${String(today.getDate()).padStart(2,"0")}`;
  lsStore["kabuai_pos"]=JSON.stringify([{code:r.code,name:r.name,entry:r.price,date:ds}]);
  sandbox.render(); hv=$get("#view").innerHTML;
  check("coach: ホールド指示", clean(hv)&&hv.includes("持ち株コーチ")&&hv.includes("損切り ¥"));
  check("coach: 合計サマリー（保有数・平均損益）", hv.includes("保有 1件")&&hv.includes("平均損益"));
  lsStore["kabuai_pos"]=JSON.stringify([{code:r.code,name:r.name,entry:r.price*2,date:ds}]);
  sandbox.render(); hv=$get("#view").innerHTML;
  check("coach: 損切り割れ検知", hv.includes("損切りライン")&&hv.includes("今日売って"));
  const past=new Date(); past.setDate(past.getDate()-14);
  const pds=`${past.getFullYear()}-${String(past.getMonth()+1).padStart(2,"0")}-${String(past.getDate()).padStart(2,"0")}`;
  lsStore["kabuai_pos"]=JSON.stringify([{code:r.code,name:r.name,entry:r.price,date:pds,plan:"swing"}]);
  sandbox.render(); hv=$get("#view").innerHTML;
  check("coach: 期限超過で手仕舞い指示（旧plan記録も互換）", hv.includes("今日の大引けで手仕舞い"));
  lsStore["kabuai_pos"]=JSON.stringify([{code:r.code,name:r.name,entry:Math.round(r.price*0.9),date:ds,shares:100}]);
  sandbox.render(); hv=$get("#view").innerHTML;
  check("② コーチ: 株数記録で実損益(円)＋合計を表示", clean(hv)&&hv.includes("×100株")&&hv.includes("合計"));
  sandbox.delPos(0); sandbox.render(); hv=$get("#view").innerHTML;
  check("coach: 削除で消える", !hv.includes("持ち株コーチ"));
}

// ── 5) 検索 ──
{
  locationShim.hash="#/search"; sandbox.render();
  await sandbox.loadSearch();
  let sv=sandbox.searchResults("7203");
  check("search: コード検索OK", clean(sv)&&sv.includes("7203"));
  const cand=sandbox.candidates().list[0];
  if(cand){
    sv=sandbox.searchResults(cand.code);
    check("search: 買い候補カードに期待値＋✅", sv.includes("期待値")&&sv.includes("✅"), cand.name);
  }
  const il=SIDX.stocks.find(s=>s.illiq);
  if(il){sv=sandbox.searchResults(il.code);
    check("search: 低流動の正直表示", sv.includes("低流動"));}
  sv=sandbox.searchResults("ZZZZZZ");
  check("search: 0件メッセージ", sv.includes("一致する銘柄はありません"));
  // ウォッチ
  sandbox.toggleWatch("7203");
  sv=sandbox.searchResults("");
  check("search: 空クエリでウォッチ一覧", sv.includes("ウォッチ中"));
  check("watchsum: ホーム用サマリー描画", clean(sandbox.watchSummaryInner()));
  sandbox.toggleWatch("7203");
}

// ── 6) 銘柄詳細（per-stock JSON・チャート・高連動注意） ──
{
  const cand=sandbox.candidates().list.find(r=>stockJson(r.code));
  if(cand){
    const s=stockJson(cand.code);
    s.futures={corr:0.82,tag:"高連動"};
    let dv=sandbox.renderDetail(s);
    check("detail: 買い候補の詳細OK", clean(dv)&&dv.includes("買いの目安")&&dv.includes("期待値"));
    check("detail: 年別の実績(exit_years)を表示", dv.includes("年別の実績")&&dv.includes("年プラス"));
    check("detail: 買い候補チャートに損切り-12%破線の説明", dv.includes("破線")&&dv.includes("損切り-12%"));
    check("detail: 高連動の注意書き", dv.includes("高連動")&&dv.includes("振られやすく"));
    s.futures={corr:0.31,tag:"自力"};
    dv=sandbox.renderDetail(s);
    check("detail: 自力の狙い目注記", dv.includes("自力")&&dv.includes("型が出やすい"));
    const lc=(s.chart&&s.chart.c&&s.chart.c.length)?s.chart.c[s.chart.c.length-1]:0;
    sandbox.drawCandle(s.chart, lc>0?{stop:lc*0.88}:null);   // 損切りライン付きで例外なく完走するか
    check("detail: チャート描画（損切りライン付き）が例外なし", true);
    // 非候補銘柄（シグナルなし）の詳細
    const plain=SIDX.stocks.find(r=>!(r.signals||[]).length&&stockJson(r.code));
    if(plain){
      const s2=stockJson(plain.code);
      const dv2=sandbox.renderDetail(s2);
      check("detail: 非候補は「買い候補ではありません」", clean(dv2)&&dv2.includes("買い候補ではありません"));
    }
    // 詳細フォールバック（per-stock JSONなし→検索行から描画）
    await sandbox.loadStockDetail("9999XX");
    check("detail: 存在しないコードでもエラーにならない", clean($get("#dbody").innerHTML));
  } else check("detail: 対象候補が見つからない", false);
}

// ── 6.5) 🧭 銘柄探検（Phase1〜3） ──
{
  await sandbox.loadExplorer();
  locationShim.hash="#/explore"; sandbox.render();
  let ev=$get("#view").innerHTML;
  check("explore: カテゴリ画面OK", clean(ev)&&ev.includes("銘柄探検"));
  check("explore: 7カテゴリ＋件数表示", ["初動","初動待ち","静かな初動","上昇中","押し目","短期反発候補","ストップ高"].every(l=>ev.includes(l)));
  check("explore: 静かな初動に直近不調の正直表示", ev.includes("2026年は不調"));
  check("explore: スイング/デイトレの区分", ev.includes("スイング狙い")&&ev.includes("デイトレ・短期"));
  check("explore: 未検証の正直表示（上昇ランキングは撤去済み）", ev.includes("未検証")&&!ev.includes("上昇ランキング"));
  // 各カテゴリ一覧
  for(const cat of ["stop_high","shodo","shodo_wait","nagi","rising","oshime","rebound"]){
    locationShim.hash=`#/explore/${cat}`; sandbox.render();
    ev=$get("#view").innerHTML;
    check(`explore/${cat}: 一覧OK(${(EXPJ.counts||{})[cat]||0}件)`, clean(ev));
  }
  // ストップ高: 日別履歴(日付付きDB相当)と張り付き表示
  locationShim.hash="#/explore/stop_high"; sandbox.render(); ev=$get("#view").innerHTML;
  check("stop_high: 日別件数の履歴表示", ev.includes("日別のストップ高件数"));
  check("stop_high: 張り付き/タッチ区別", ev.includes("張り付き")||ev.includes("S高タッチ"));
  // rebound: フィルタ・検索
  locationShim.hash="#/explore/rebound"; sandbox.render(); ev=$get("#view").innerHTML;
  check("rebound: ゾーン/タグ表示", ev.includes("押し ")&&ev.includes("急騰 +"));
  check("rebound: フィルタUI", ev.includes("すべて")&&ev.includes("下げ止まりのみ"));
  sandbox.setRebFilter("sagedomari"); ev=$get("#view").innerHTML;
  const sagedomariN=(EXPJ.categories.rebound||[]).filter(r=>(r.tags||[]).includes("下げ止まり")).length;
  check("rebound: 下げ止まりフィルタ動作", clean(ev), `対象${sagedomariN}件`);
  sandbox.setRebFilter("all");
  sandbox.onExpSearch("ZZZZ99"); // 検索0件でも落ちない
  check("rebound: 検索0件セーフ", clean($get("#explist").innerHTML||""));
  sandbox.onExpSearch("");
  // 上昇ランキング(#/explore/ranking)はv3で撤去（ユーザー指示「上昇ランキングいらない」）→ルート無しを確認
  locationShim.hash="#/explore/ranking"; sandbox.render();
  check("explore: 撤去した#/explore/rankingでも落ちない（カテゴリ扱いにフォールバック）", clean($get("#view").innerHTML));
  // 不明カテゴリセーフ
  locationShim.hash="#/explore/nazo"; sandbox.render();
  check("explore: 不明カテゴリでも落ちない", clean($get("#view").innerHTML));
}

// ── 6.8) 🔻 今日の売り（セクターローテSELL・発火日だけ表示） ──
{
  const save=DATA.sector_today;
  DATA.sector_today={date:DATA.data_date,is_today:true,
    sell:[{code:"9999",name:"テスト売り",price:1234,sector:"テスト業",day_change:25.3,rsi:88}],
    plan_sell:"当日9:00 寄り成行で空売り → 損切り+3%/利確-5%のOCO・RSI50か3日目大引けで買い戻し",
    stats:{sell:{pf:1.37,cum:45.9,pos_years:"5/5"}}};
  locationShim.hash="#/"; sandbox.render(); let sv=$get("#view").innerHTML;
  check("sellHero: 発火日に表示（PF/OCO/信用口座の明示）",
    clean(sv)&&sv.includes("今日の売り")&&sv.includes("PF1.37")&&sv.includes("信用口座")&&sv.includes("OCO"));
  DATA.sector_today={...DATA.sector_today,is_today:false};
  sandbox.render(); sv=$get("#view").innerHTML;
  check("sellHero: 過去分（is_today=false）は非表示", !sv.includes("今日の売り"));
  DATA.sector_today={...DATA.sector_today,is_today:true,sell:[]};
  sandbox.render(); sv=$get("#view").innerHTML;
  check("sellHero: 売り0件は非表示", !sv.includes("今日の売り"));
  DATA.sector_today=save;
  sandbox.render();
}

// ── 6.9) 🐵 モメンタム / 🔥 テーマ タブ（v3新規） ──
{
  locationShim.hash="#/momentum"; sandbox.render();
  let mv=$get("#view").innerHTML;
  check("momentum: 描画OK・エラーなし", clean(mv)&&mv.includes("強さ・過熱ランキング"));
  check("momentum: 買い推奨でない正直フレーム(-2.65%)", mv.includes("買い推奨ではありません")&&mv.includes("-2.65%"));
  check("momentum: グレードバッジ＋フィルタchip", mv.includes('class="grb')&&mv.includes('class="gchip'));
  sandbox.setMomGrade("S"); mv=$get("#view").innerHTML;
  check("momentum: グレードSフィルタでも落ちない", clean(mv));
  sandbox.setMomGrade("all");

  // 🔥テーマは2026-07-15本人指示で撤去: タブが無い＋#/themeはホームへフォールバック
  locationShim.hash="#/theme"; sandbox.render();
  const tv=$get("#view").innerHTML;
  check("theme撤去: タブなし", !html.includes('id="nav-theme"'));
  check("theme撤去: #/themeはホームにフォールバックし例外なし",
    clean(tv)&&tv.includes("今日の買い候補"));

  check("nav: 4タブ（今日の買い/モメンタム/探検/使い方・検索は非nav）",
    html.includes('id="nav-momentum"')&&
    html.includes('id="nav-explore"')&&html.includes('id="nav-about"')&&!html.includes('id="nav-search"'));
}

// ── 7) 使い方（about） ──
{
  locationShim.hash="#/about"; sandbox.render();
  const av=$get("#view").innerHTML;
  check("about: 描画OK", clean(av));
  check("about: 日経連動タグの説明", av.includes("日経連動タグ")&&av.includes("1570")&&av.includes("日経と逆"));
  check("about: 今日の売りの説明", av.includes("今日の売り")&&av.includes("信用口座")&&av.includes("稀"));
  check("about: 出口・免責・JPX", av.includes("損切り")&&av.includes("免責")&&av.includes("J-Quants"));
  check("about: EOD/場中不変の明示", av.includes("終値")&&av.includes("場中"));
  check("about: v3の反発＋モメンタム/テーマ説明", av.includes("反発")&&av.includes("モメンタム")&&av.includes("買い推奨ではありません"));
}

console.log(fail?`\nRESULT: ${fail} FAILURE(S)`:"\nRESULT: ALL GREEN");
process.exit(fail?1:0);
