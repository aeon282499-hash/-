// v4フロント（2026-07-18 本人指示: ✅今日の買い撤去・🐵モメンタム=ホーム・🔻売りタブ新設）の DOMシム検証。
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

// ── 0) 内部パイプライン最低限（買い候補はUI撤去だが検索/詳細/ウォッチで使うため生存確認） ──
{
  const c=sandbox.candidates();
  check("internal: candidates()が例外なく動く", Array.isArray(c.list), `${c.list.length}銘柄`);
}

// ── 1) ホーム（v4=🐵モメンタムがホーム・買い候補ヒーロー撤去） ──
locationShim.hash="#/"; sandbox.render();
let hv=$get("#view").innerHTML;
check("home: 表示エラー/NaN/undefinedなし", clean(hv));
check("home: 🐵モメンタムランキングがホーム", hv.includes("強さ・過熱ランキング")&&hv.includes('class="grb'));
check("home: ✅今日の買い候補ヒーローが出ない", !hv.includes("今日の買い候補"));
check("home: 検索カードあり", hv.includes("銘柄コード・名前で検索"));
check("home: 買い推奨でない正直フレーム(-2.65%)", hv.includes("買い推奨ではありません")&&hv.includes("-2.65%"));
check("home: 🔻売りタブへの導線", hv.includes("#/sell"));

// ── 1.5) 🔻終了サインchip（sell_watch銘柄がランキングにいれば表示） ──
{
  const save=DATA.sell_watch;
  const rk=(DATA.ranking||[])[0];
  if(rk){
    DATA.sell_watch={date:DATA.data_date,members:[{code:rk.code,name:rk.name,price:rk.price,
      r1:-5.0,off_peak20:-8.0,ma5_dev:-3.2,vol_x:2.5,below5:1,runup20:25.0,turnover_oku:12}],count:1,
      cond:{runup20:15,vol_x:1.3},note:"test"};
    locationShim.hash="#/"; sandbox.render();
    check("home: ランキング銘柄に🔻終了サインchip", $get("#view").innerHTML.includes("🔻終了サイン"));
    DATA.sell_watch=save; sandbox.render();
  }
}

// ── 1.6) 🪶信用軽chip（days_cover<0.25で表示・以上/欠損で非表示） ──
{
  const rk=(DATA.ranking||[])[0];
  if(rk){
    const save=rk.days_cover;
    rk.days_cover=0.10;
    locationShim.hash="#/"; sandbox.render();
    check("home: 買残回転0.10日→🪶信用軽chip表示", $get("#view").innerHTML.includes("🪶信用軽"));
    rk.days_cover=1.50; sandbox.render();
    const withHeavy=$get("#view").innerHTML.split("🪶信用軽").length-1;
    check("home: 1.50日→chip非表示(凡例1箇所のみ)", withHeavy===1);
    if(save===undefined) delete rk.days_cover; else rk.days_cover=save;
    sandbox.render();
  }
}

// ── 2) 🔻売りタブ（v4新設） ──
{
  locationShim.hash="#/sell"; sandbox.render();
  let sv=$get("#view").innerHTML;
  check("sell: 描画OK・エラーなし", clean(sv));
  check("sell: 空売り推奨でないの明示", sv.includes("空売りの推奨ではありません"));
  const n=(DATA.sell_watch&&DATA.sell_watch.members||[]).length;
  if(n){
    check(`sell: モメンタム終了リスト表示(${n}件)`, sv.includes("高値から")&&sv.includes("5MA乖離")&&sv.includes("出来高"));
    check("sell: 本日崩れ/割れN日目の区別", sv.includes("本日崩れ")||sv.includes("割れ"));
    check("sell: 検出条件の明示（5MA割れ×陰線×出来高）", sv.includes("5日移動平均割れ")&&sv.includes("陰線"));
    // 売り銘柄は詳細チャート(日足+5MA)が必ず出る=stocks/<code>.jsonがエクスポートされている
    const m0=DATA.sell_watch.members[0];
    const sj=stockJson(m0.code);
    check("sell: 掲載銘柄の日足チャートJSONが存在", !!(sj&&sj.chart&&sj.chart.c&&sj.chart.c.length), m0.code);
    if(sj&&sj.chart){ sandbox.drawCandle(sj.chart,null); check("sell: 日足+5MA描画が例外なし", true); }
  }
  check("sell: 日足+5MAの案内文", sv.includes("5MA線"));
  // 信用バッジ（買残急増・日々公表）
  {
    const save=DATA.sell_watch;
    DATA.sell_watch={date:DATA.data_date,count:1,cond:{runup20:15,vol_x:1.3},note:"t",
      members:[{code:"7504",name:"テスト高速",price:3575,r1:-4.8,off_peak20:-9.6,ma5_dev:-6.3,
                vol_x:1.8,below5:2,runup20:20.1,turnover_oku:5.2,margin_chg:85.3,margin_alert:true}]};
    sandbox.render(); const bv=$get("#view").innerHTML;
    check("sell: 🧨買残急増バッジ表示", bv.includes("🧨買残+85%"));
    check("sell: ⚠️日々公表バッジ表示", bv.includes("⚠️日々公表"));
    check("sell: バッジの説明文", bv.includes("信用買残")&&bv.includes("日々公表銘柄"));
    DATA.sell_watch={...DATA.sell_watch,
      members:[{...DATA.sell_watch.members[0],margin_chg:5.0,margin_alert:false}]};
    sandbox.render();
    check("sell: 買残+30%未満/非指定はバッジ(chip)なし",
      !$get("#view").innerHTML.includes("🧨買残+5%")&&!$get("#view").innerHTML.includes(">⚠️日々公表</span>"));
    DATA.sell_watch=save; sandbox.render();
  }
  // 空状態
  const save=DATA.sell_watch;
  DATA.sell_watch={date:DATA.data_date,members:[],count:0,cond:{runup20:15,vol_x:1.3},note:"none"};
  sandbox.render(); sv=$get("#view").innerHTML;
  check("sell: 0件日の空状態メッセージ", clean(sv)&&sv.includes("合致した銘柄はありません"));
  DATA.sell_watch=save; sandbox.render();
}

// ── 2.5) 🔻セクターローテSELLヒーロー（発火日だけ・売りタブ上部に統合） ──
{
  const save=DATA.sector_today;
  DATA.sector_today={date:DATA.data_date,is_today:true,
    sell:[{code:"9999",name:"テスト売り",price:1234,sector:"テスト業",day_change:25.3,rsi:88}],
    plan_sell:"当日9:00 寄り成行で空売り → 損切り+3%/利確-5%のOCO・RSI50か3日目大引けで買い戻し",
    stats:{sell:{pf:1.37,cum:45.9,pos_years:"5/5"}}};
  locationShim.hash="#/sell"; sandbox.render(); let sv=$get("#view").innerHTML;
  check("sellHero: 発火日に売りタブ上部へ表示（PF/OCO/信用口座）",
    clean(sv)&&sv.includes("今日の売り")&&sv.includes("PF1.37")&&sv.includes("信用口座")&&sv.includes("OCO"));
  DATA.sector_today={...DATA.sector_today,is_today:false};
  sandbox.render(); sv=$get("#view").innerHTML;
  check("sellHero: 過去分（is_today=false）は非表示", !sv.includes("今日の売り（空売り）"));
  DATA.sector_today={...DATA.sector_today,is_today:true,sell:[]};
  sandbox.render(); sv=$get("#view").innerHTML;
  check("sellHero: 売り0件は非表示", !sv.includes("今日の売り（空売り）"));
  DATA.sector_today=save;
}

// ── 3) 📒持ち株コーチ（ホーム=モメンタムに残置） ──
{
  await sandbox.loadSearch();
  const r=SIDX.stocks.find(s=>s.price>1000);
  const today=new Date(); const ds=`${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,"0")}-${String(today.getDate()).padStart(2,"0")}`;
  lsStore["kabuai_pos"]=JSON.stringify([{code:r.code,name:r.name,entry:r.price,date:ds}]);
  locationShim.hash="#/"; sandbox.render(); hv=$get("#view").innerHTML;
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
  check("coach: 株数記録で実損益(円)＋合計を表示", clean(hv)&&hv.includes("×100株")&&hv.includes("合計"));
  sandbox.delPos(0); sandbox.render(); hv=$get("#view").innerHTML;
  check("coach: 削除で消える", !hv.includes("持ち株コーチ"));
}

// ── 4) 検索 ──
{
  locationShim.hash="#/search"; sandbox.render();
  await sandbox.loadSearch();
  let sv=sandbox.searchResults("7203");
  check("search: コード検索OK", clean(sv)&&sv.includes("7203"));
  const cand=sandbox.candidates().list[0];
  if(cand){
    sv=sandbox.searchResults(cand.code);
    check("search: シグナル点灯銘柄に期待値チップ", sv.includes("期待値"), cand.name);
  }
  const il=SIDX.stocks.find(s=>s.illiq);
  if(il){sv=sandbox.searchResults(il.code);
    check("search: 低流動の正直表示", sv.includes("低流動"));}
  sv=sandbox.searchResults("ZZZZZZ");
  check("search: 0件メッセージ", sv.includes("一致する銘柄はありません"));
  sandbox.toggleWatch("7203");
  sv=sandbox.searchResults("");
  check("search: 空クエリでウォッチ一覧", sv.includes("ウォッチ中"));
  check("watchsum: ホーム用サマリー描画", clean(sandbox.watchSummaryInner()));
  sandbox.toggleWatch("7203");
}

// ── 5) 銘柄詳細（per-stock JSON・チャート・高連動注意） ──
{
  const cand=sandbox.candidates().list.find(r=>stockJson(r.code));
  if(cand){
    const s=stockJson(cand.code);
    s.futures={corr:0.82,tag:"高連動"};
    let dv=sandbox.renderDetail(s);
    check("detail: シグナル銘柄の詳細OK", clean(dv)&&dv.includes("買いの目安")&&dv.includes("期待値"));
    check("detail: 年別の実績(exit_years)を表示", dv.includes("年別の実績")&&dv.includes("年プラス"));
    check("detail: 高連動の注意書き", dv.includes("高連動")&&dv.includes("振られやすく"));
    s.futures={corr:0.31,tag:"自力"};
    dv=sandbox.renderDetail(s);
    check("detail: 自力の狙い目注記", dv.includes("自力")&&dv.includes("型が出やすい"));
    const lc=(s.chart&&s.chart.c&&s.chart.c.length)?s.chart.c[s.chart.c.length-1]:0;
    sandbox.drawCandle(s.chart, lc>0?{stop:lc*0.88}:null);
    check("detail: チャート描画（損切りライン付き）が例外なし", true);
    const plain=SIDX.stocks.find(r=>!(r.signals||[]).length&&stockJson(r.code));
    if(plain){
      const s2=stockJson(plain.code);
      const dv2=sandbox.renderDetail(s2);
      check("detail: 非点灯は「参考シグナルの点灯はありません」", clean(dv2)&&dv2.includes("参考シグナルの点灯はありません"));
    }
    await sandbox.loadStockDetail("9999XX");
    check("detail: 存在しないコードでもエラーにならない", clean($get("#dbody").innerHTML));
  } else check("detail: 対象候補が見つからない", false);
}

// ── 6) 🧭 銘柄探検 ──
{
  await sandbox.loadExplorer();
  locationShim.hash="#/explore"; sandbox.render();
  let ev=$get("#view").innerHTML;
  check("explore: カテゴリ画面OK", clean(ev)&&ev.includes("銘柄探検"));
  check("explore: 7カテゴリ＋件数表示", ["初動","初動待ち","静かな初動","上昇中","押し目","短期反発候補","ストップ高"].every(l=>ev.includes(l)));
  check("explore: 未検証の正直表示", ev.includes("未検証"));
  for(const cat of ["stop_high","shodo","shodo_wait","nagi","rising","oshime","rebound"]){
    locationShim.hash=`#/explore/${cat}`; sandbox.render();
    ev=$get("#view").innerHTML;
    check(`explore/${cat}: 一覧OK(${(EXPJ.counts||{})[cat]||0}件)`, clean(ev));
  }
  locationShim.hash="#/explore/nazo"; sandbox.render();
  check("explore: 不明カテゴリでも落ちない", clean($get("#view").innerHTML));
}

// ── 7) ルーティング/ナビ（v4） ──
{
  locationShim.hash="#/momentum"; sandbox.render();
  check("route: #/momentumも描画OK", clean($get("#view").innerHTML));
  locationShim.hash="#/theme"; sandbox.render();
  const tv=$get("#view").innerHTML;
  check("theme撤去: #/themeはホーム(モメンタム)へフォールバック", clean(tv)&&tv.includes("強さ・過熱ランキング"));
  check("nav: v4の4タブ（モメンタム=home/売り/探検/使い方）",
    html.includes('id="nav-home"')&&html.includes('id="nav-sell"')&&
    html.includes('id="nav-explore"')&&html.includes('id="nav-about"')&&
    !html.includes('id="nav-momentum"')&&!html.includes('id="nav-search"'));
  check("nav: 今日の買いタブが無い", !html.match(/<a[^>]*>[^<]*<span class="i">✅<\/span>今日の買い<\/a>/));
}

// ── 8) 使い方（about） ──
{
  locationShim.hash="#/about"; sandbox.render();
  const av=$get("#view").innerHTML;
  check("about: 描画OK", clean(av));
  check("about: v4のタブ説明（モメンタム/売り/探検）", av.includes("モメンタム")&&av.includes("売り")&&av.includes("探検"));
  check("about: モメンタム終了の説明（5MA割れ）", av.includes("5MA割れ")&&av.includes("空売りの推奨ではなく"));
  check("about: セクターローテSELLの説明", av.includes("信用口座")&&av.includes("稀"));
  check("about: 日経連動タグの説明", av.includes("日経連動タグ")&&av.includes("1570")&&av.includes("日経と逆"));
  check("about: 出口・免責・JPX", av.includes("損切り")&&av.includes("免責")&&av.includes("J-Quants"));
  check("about: EOD/場中不変の明示", av.includes("終値")&&av.includes("場中"));
  check("about: 旧ホーム(今日はどんな日か)の説明を撤去", !av.includes("今日はどんな日か"));
}

console.log(fail?`\nRESULT: ${fail} FAILURE(S)`:"\nRESULT: ALL GREEN");
process.exit(fail?1:0);
