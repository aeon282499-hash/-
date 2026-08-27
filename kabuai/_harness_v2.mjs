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
  // 💥 崩壊ショート（2026-08-01追加）
  {
    const save=DATA.sell_watch;
    const base={code:"6976",name:"テスト誘電",price:5200,r1:-12.9,off_peak20:-14.0,ma5_dev:-8.1,
                vol_x:2.4,below5:1,runup20:175.0,turnover_oku:88.0};
    const crashMeta={count:1,cond:{runup20:30,vol_x:2.0,r1:-5,first_day:true,shortable:true},
      stats:{n:425,per_year:43,avg:0.78,win:57.6,pf:1.47,era1:0.95,era2:0.61,period:"2016-2026/07"},
      how:"翌日の寄りで空売り → その日の大引けで買い戻す（当日完結）",
      caveat:"逆日歩は未測定。年+0.78%は逆日歩0.8%で消える水準なので実弾前に要実測。"};
    DATA.sell_watch={date:DATA.data_date,count:2,cond:{runup20:15,vol_x:1.3},note:"t",
      crash:crashMeta,
      members:[{...base,code:"1111",name:"貸借ダメ社",crash:false,shortable:false},
               {...base,crash:true,shortable:true}]};
    sandbox.render(); const cv=$get("#view").innerHTML;
    check("sell: 💥崩壊ショートの見出しと件数", cv.includes("💥 崩壊ショート（BT合格）1件"));
    check("sell: 💥のBT数字（勝率/PF/両期間）", cv.includes("57.6%")&&cv.includes("PF1.47")&&cv.includes("両期間プラス"));
    check("sell: 💥の撃ち方の明示", cv.includes("寄りで空売り")&&cv.includes("大引けで買い戻す"));
    check("sell: 💥の逆日歩の警告", cv.includes("逆日歩は未測定"));
    check("sell: 💥は推奨でないと明示", cv.includes("推奨ではありません"));
    check("sell: 💥バッジが該当行に付く", cv.includes("💥崩壊ショート"));
    check("sell: 貸借✕は別バッジで区別", cv.includes("貸借✕"));
    check("sell: 💥該当が先頭に並ぶ",
      cv.indexOf("テスト誘電")>=0 && cv.indexOf("テスト誘電")<cv.indexOf("貸借ダメ社"));
    // crashキーが無い旧データでも落ちない（後方互換）
    DATA.sell_watch={date:DATA.data_date,count:1,cond:{runup20:15,vol_x:1.3},note:"t",
      members:[{...base,crash:undefined,shortable:undefined}]};
    sandbox.render(); const ov=$get("#view").innerHTML;
    check("sell: crash未提供でも描画OK", clean(ov)&&!ov.includes("崩壊ショート（BT合格）"));
    check("sell: crash未提供なら💥バッジも出ない", !ov.includes("💥崩壊ショート"));
    DATA.sell_watch=save; sandbox.render();
  }
  // 🩳 デイトレ売り（フェード・2026-07-23追加）
  {
    const save=DATA.sell_watch;
    const base={date:"2026-07-23",count:0,cond:{runup20:15,vol_x:1.3},note:"t",members:[]};
    // GO日: 売り禁GO1件+見送り1件（2026-07-23: 売り禁は除外せず🚫バッジ表示に変更）
    DATA.sell_watch={...base,fade:{date:"2026-07-23",go:1,banned:0,go_min:12,
      stats:{n:632,win:54.3,avg:0.71,pf:1.35,period:"2022-2026/07",y2026_avg:1.59},
      picks:[{code:"7014",name:"名村造船所",gain:18.2,vol_ratio:6.1,range_pct:9.0,min_entry:4160,
              short_mark:"○",borrow:"◎売残少(空売り楽・よく落ちる)",jsf_stop:true,
              reg_note:"🚫売り禁(制度✕・ハイカラ/一般信用の在庫があれば可)・⚠️日証金注意喚起(逆日歩警戒)",verdict:"GO",nogo_reason:""},
             {code:"3104",name:"富士紡HD",gain:9.2,vol_ratio:1.7,range_pct:8.2,min_entry:4085,
              short_mark:"○",borrow:"○普通",jsf_stop:false,reg_note:"",verdict:"NOGO",nogo_reason:"前日+9%<12%＝薄い(コスト後トントン帯)"}]}};
    sandbox.render(); const fv=$get("#view").innerHTML;
    check("fade: セクション表示", fv.includes("🩳 デイトレ売り"));
    check("fade: GO行（寄指ライン+引け買い戻し）", fv.includes("🔴 GO")&&fv.includes("¥4,160以上")&&fv.includes("引けで買い戻し"));
    check("fade: 見送り行（理由つき）", fv.includes("見送り")&&fv.includes("薄い"));
    check("fade: 売り禁は除外せず🚫バッジ表示", fv.includes("🚫売り禁")&&fv.includes("ハイカラ"));
    check("fade: 売り禁chipは赤系(jsf_stop)", fv.includes("rgba(234,57,67,.6)")&&fv.includes("🚫売り禁"));
    check("fade: 規制注記chip", fv.includes("注意喚起"));
    check("fade: BT成績の明示", fv.includes("PF1.35")&&fv.includes("勝率54.3%"));
    check("fade: 上級者向け警告（在庫は発注画面で最終確認）", fv.includes("自己責任")&&fv.includes("一日信用")&&fv.includes("最終確認"));
    // NOGOのみの日=「撃つ日じゃない」リード
    DATA.sell_watch={...base,fade:{...DATA.sell_watch.fade,go:0,banned:0,
      picks:DATA.sell_watch.fade.picks.filter(p=>p.verdict!=="GO")}};
    sandbox.render();
    check("fade: 見送り日のリード表示", $get("#view").innerHTML.includes("今日は撃つ日じゃない"));
    // 候補ゼロ日
    DATA.sell_watch={...base,fade:{date:"2026-07-23",go:0,banned:0,go_min:12,picks:[],stats:{}}};
    sandbox.render();
    check("fade: 候補ゼロ日の表示", $get("#view").innerHTML.includes("本日は候補なし"));
    // fade欠落/error時はセクション非表示（後方互換）
    DATA.sell_watch={...base};
    sandbox.render();
    check("fade: データ無しなら非表示", !$get("#view").innerHTML.includes("🩳"));
    DATA.sell_watch={...base,fade:{error:true}};
    sandbox.render();
    check("fade: error時も非表示", !$get("#view").innerHTML.includes("🩳"));
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
  check("explore: 8カテゴリ＋件数表示", ["初動ブレイク","初動","初動待ち","静かな初動","上昇中","押し目","短期反発候補","ストップ高"].every(l=>ev.includes(l)));
  check("explore: 未検証の正直表示", ev.includes("未検証"));
  check("explore: break60はBT済み表記", ev.includes("10年検証済み")&&ev.includes("PF1.22"));
  for(const cat of ["stop_high","shodo","shodo_wait","nagi","rising","oshime","rebound","break60"]){
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
  check("nav: 5タブ（モメンタム/売り/極み/探検/使い方・2026-08-28デイトレ→極み）",
    html.includes('id="nav-home"')&&html.includes('id="nav-sell"')&&html.includes('id="nav-kiwami"')&&
    html.includes('id="nav-explore"')&&html.includes('id="nav-about"')&&
    !html.includes('id="nav-daytrade"')&&!html.includes('id="nav-quiz"')&&
    !html.includes('id="nav-momentum"')&&!html.includes('id="nav-search"'));
  check("nav: 今日の買いタブが無い", !html.match(/<a[^>]*>[^<]*<span class="i">✅<\/span>今日の買い<\/a>/));
}

// ── 7.4) 👑 極み（2026-08-28 デイトレタブと入替） ──
{
  const save=DATA.kiwami;
  // データなし
  DATA.kiwami=null;
  locationShim.hash="#/kiwami"; sandbox.render();
  let kv=$get("#view").innerHTML;
  check("kiwami: データなしでも安全描画", clean(kv)&&kv.includes("シグナルデータがまだありません"));
  // fresh=これから執行する分（買い1・売り1）
  DATA.kiwami={date:"2099-01-04",fresh:true,
    buy:[{code:"7203",name:"トヨタ自動車",price:3000,limit:3120,rsi:41.2,dev:-5.3,turnover_oku:500}],
    sell:[{code:"8309",name:"三井住友トラスト",price:6734,status:"open"}],
    plan_buy:"寄指で買い",plan_sell:"寄り成行で空売り"};
  sandbox.render(); kv=$get("#view").innerHTML;
  check("kiwami: fresh日=買い/売り行が描画・過去分警告なし",
    clean(kv)&&kv.includes("トヨタ自動車")&&kv.includes("寄指上限 ¥3,120")&&kv.includes("三井住友トラスト")&&!kv.includes("過去分"));
  // 過去分の断面
  DATA.kiwami={date:"2020-01-06",fresh:false,buy:[],sell:[],plan_buy:"p",plan_sell:"p"};
  sandbox.render(); kv=$get("#view").innerHTML;
  check("kiwami: 過去分は⚠️明示＋該当なしカード", clean(kv)&&kv.includes("過去分")&&kv.includes("この日の該当なし"));
  DATA.kiwami=save;
  // デイトレは導線撤去でも直リンク生存（コード残置の確認）
  locationShim.hash="#/daytrade"; sandbox.render();
  check("daytrade: タブ撤去後も#/daytrade直リンクは描画OK", clean($get("#view").innerHTML));
}

// ── 7.5) ⚡ 裁量デイトレ・ウォッチ（2026-07-24） ──
{
  const save=DATA.daytrade_watch;
  const _mkbuy=n=>Array.from({length:n},(_,i)=>({code:"B"+i,name:"買い"+i,r1:9-i*0.5,price:1000,turnover_oku:30,sector:"電気機器"}));
  const _mksell=n=>Array.from({length:n},(_,i)=>({code:"S"+i,name:"売り"+i,r1:-9+i*0.5,price:1000,turnover_oku:30,sector:"銀行業",short_mark:i===0?"○":"×",jsf_stop:i===1}));
  // 2026-07-26: 15分足MA5ルールは look-ahead バグで失効。タブは「検証済みルール」でなく
  // 「前日に大きく動いた銘柄の一覧＋失効の明示」に変更。stats も訂正後の実測値に差替え。
  DATA.daytrade_watch={date:"2026-07-24",move_min:4,top:5,
    stats:{buy_pf:0.50,sell_pf:1.14,buy_avg:-0.95,sell_h1:0.87,sell_h2:1.37,verdict:"invalidated",tf:"15分足",ma:5},
    buy:_mkbuy(8), sell:_mksell(7), buy_total:8, sell_total:7};
  locationShim.hash="#/daytrade"; sandbox.render();
  const dv=$get("#view").innerHTML;
  check("daytrade: 描画OK・エラーなし", clean(dv));
  check("daytrade: 買い候補表示", dv.includes("買い候補")&&dv.includes("買い0")&&dv.includes("+9.0%"));
  check("daytrade: 売り候補表示", dv.includes("売り候補")&&dv.includes("-9.0%"));
  check("daytrade: 貸借バッジ", dv.includes("貸借○"));
  check("daytrade: 🚫売り禁バッジ", dv.includes("🚫売り禁"));
  check("daytrade: フェードと別物の明示", dv.includes("別物"));
  check("daytrade: 失効の明示（未来情報の混入）", dv.includes("未来情報の混入")&&dv.includes("失効"));
  check("daytrade: 訂正後PFを表示", dv.includes("PF0.5")&&dv.includes("PF1.14"));
  check("daytrade: 買いは負けと明示", dv.includes("買い方向は検証で負け")&&dv.includes("-0.95"));
  check("daytrade: 売りはWF不合格と明示", dv.includes("前半0.87")&&dv.includes("合格せず"));
  check("daytrade: 唯一息のある型(VWAPバウンス)を提示", dv.includes("VWAPから+3%")&&dv.includes("後半PF1.19"));
  check("daytrade: 3回消えた経緯の明示", dv.includes("3回とも消えた"));
  check("daytrade: 本気の資金はフェードへ誘導", dv.includes("本気の資金はそちら"));
  check("daytrade: 出口カード(5MA=ブレーキ/VWAP=ターゲット)", dv.includes("5MAとVWAPの役割")&&dv.includes("ブレーキ")&&dv.includes("ターゲット")&&dv.includes("1/3"));
  check("daytrade: 旧PF主張が残っていない", !dv.includes("PF2.40")&&!dv.includes("PF1.95")&&!dv.includes("前後半どちらもプラス"));
  check("daytrade: 損切利確の図解SVG", dv.includes("<svg")&&dv.includes("MA5（線路）")&&dv.includes("①押し目で買い")&&dv.includes("③MA5割れで手仕舞い"));
  check("daytrade: 上位5のみ本命表示", dv.includes("買い0")&&dv.includes("買い4")&&dv.split("📂 全部見る")[0].indexOf("買い5")===-1);
  check("daytrade: 📂全部見るdetailsで残り表示", dv.includes("📂 全部見る（残り3銘柄")&&dv.includes("買い7")&&dv.includes("売り6"));
  // 空・error後方互換
  DATA.daytrade_watch={date:"2026-07-24",move_min:4,buy:[],sell:[],stats:{}};
  sandbox.render();
  check("daytrade: 該当なし日の表示", $get("#view").innerHTML.includes("様子見日"));
  DATA.daytrade_watch={error:true}; sandbox.render();
  check("daytrade: error時も落ちない", clean($get("#view").innerHTML));
  DATA.daytrade_watch=save;
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
