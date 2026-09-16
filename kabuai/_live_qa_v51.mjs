// v5.1 ライブ: セクター既定・騰落率並び・構成銘柄の並び替え/昇降・「今の資金」主役外し を DOMシムで検査
import fs from "node:fs"; import vm from "node:vm";
const html=fs.readFileSync("web/index.html","utf8"); const DATA=JSON.parse(fs.readFileSync("data/latest.json","utf8"));
const LIVEJ=JSON.parse(fs.readFileSync("../live_flow/latest.json","utf8"));
const srcs=[...html.matchAll(/<script src="([^"?]+)(?:\?[^"]*)?"><\/script>/g)].map(m=>m[1]);
const code=srcs.map(s=>fs.readFileSync("web/"+s,"utf8")).join("\n;\n");
const store={}; const mkEl=id=>({_html:"",style:{},set innerHTML(v){this._html=v},get innerHTML(){return this._html},textContent:"",classList:{toggle(){},add(){},remove(){}},setAttribute(){},querySelector(){return mkEl("c")},querySelectorAll(){return[]}});
const $get=s=>store[s]||(store[s]=mkEl(s));
const sandbox={document:{querySelector:$get,getElementById:id=>$get("#"+id),addEventListener(){},querySelectorAll:()=>[],createElement:()=>mkEl("n"),body:mkEl("b"),documentElement:mkEl("h")},
 window:{addEventListener(){},scrollTo(){},location:{hash:"#/"},innerWidth:390},location:{hash:"#/"},localStorage:{getItem:()=>null,setItem(){},removeItem(){}},console,navigator:{},history:{back(){}},
 fetch:async()=>({ok:true,status:200,json:async()=>DATA}),setTimeout:()=>0,clearTimeout(){},setInterval:()=>0,requestAnimationFrame:fn=>fn()};
sandbox.globalThis=sandbox; vm.createContext(sandbox); vm.runInContext(code,sandbox); await new Promise(r=>setTimeout(r,50));
let fail=0; const check=(n,c,x="")=>{ if(c) console.log(`  OK ${n}${x?" — "+x:""}`); else {fail++; console.log(`  NG ${n}${x?" — "+x:""}`);} };
const clean=h=>!h.includes("表示エラー")&&!h.includes("NaN")&&!h.includes("undefined")&&!h.includes("[object");
sandbox.render(); sandbox.liveApply(LIVEJ); let hv=$get("#live-root").innerHTML;
check("既定=セクター", clean(hv)&&hv.includes("33業種")&&(hv.match(/class="gcard/g)||[]).length===33);
check("既定の並び=騰落率がon", /class="on"[^>]*>騰落率/.test(hv));
const labels=[...hv.matchAll(/<div class="gname"><b>([^<]+)/g)].map(m=>m[1].trim());
const bySort=LIVEJ.sectors.slice().sort((a,b)=>(b.chg_w??-999)-(a.chg_w??-999)).map(g=>g.label);
check("騰落率の降順に並ぶ", JSON.stringify(labels)===JSON.stringify(bySort), labels.slice(0,3).join(" > "));
check("『今の資金』の文言が主役から消えた", !hv.includes("今の資金")&&!hv.includes("直近5分の資金"));
for (const k of ["up_ratio","tov","d5_w","flow5"]) { sandbox.liveSetSort(k); hv=$get("#live-root").innerHTML; check(`並び替え ${k}`, clean(hv)&&(hv.match(/class="gcard/g)||[]).length===33); }
sandbox.liveSetSort("chg_w");
const top=LIVEJ.sectors.slice().sort((a,b)=>(b.chg_w??-999)-(a.chg_w??-999))[0];
const body=h=>{const i=h.indexOf('<div class="gbody">'); if(i<0) return ""; const j=h.indexOf('<div class="gcard', i); return h.slice(i, j<0?h.length:j);};
sandbox.liveToggle("sectors",top.key); hv=body($get("#live-root").innerHTML);
const rows=(hv.match(/class="srow/g)||[]).length;
check("セクター展開で構成銘柄行", clean(hv)&&rows===top.members.length, `${top.label} ${rows}行 (payload ${top.members.length}本/n=${top.n})`);
check("銘柄の並び替えバー", hv.includes("並び替え（もう一度押すと逆順）")&&hv.includes("VWAP乖離"));
const names=h=>[...h.matchAll(/<div class="sn"><b>([^<]+)/g)].map(m=>m[1].trim());
const first=names(hv)[0]; sandbox.liveSetMSort("chg"); hv=body($get("#live-root").innerHTML); const firstAsc=names(hv)[0];
check("同じキーをもう一度で昇順(先頭が変わる)", first!==firstAsc&&hv.includes("本日 ▲"), `${first} → ${firstAsc}`);
sandbox.liveSetMSort("tov"); hv=body($get("#live-root").innerHTML);
const tovOrder=top.members.map(c=>LIVEJ.stocks[c]).filter(Boolean).sort((a,b)=>b.tov-a.tov).map(m=>m.name);
check("代金順(降順)", JSON.stringify(names(hv))===JSON.stringify(tovOrder));
for (const k of ["d5","vwap_dev","flow5"]) { sandbox.liveSetMSort(k); hv=body($get("#live-root").innerHTML); check(`銘柄並び ${k}`, clean(hv)&&(hv.match(/class="srow/g)||[]).length===rows); }
sandbox.liveSetSeg("stocks"); hv=$get("#live-root").innerHTML; check("個別 既定=上昇率", clean(hv)&&/gchip on[^>]*>上昇率/.test(hv)&&hv.includes("srow"));
sandbox.liveSetSeg("themes"); hv=$get("#live-root").innerHTML; check("テーマ", clean(hv)&&hv.includes("テーマ"));
console.log(fail?`${fail} 件 NG`:"ALL OK"); process.exit(fail?1:0);
