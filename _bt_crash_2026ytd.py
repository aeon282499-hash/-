# 2026年YTD: crash_short_watch.py と同じ定義で「ルール通り撃っていたら」を J-Quants 日足で再現
import io,sys,pickle,os; sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8')
import numpy as np, pandas as pd
from screener import _jquants_id_token, batch_download_jquants, fetch_tse_universe, is_etf_ticker
from crash_short_watch import _iss_map
CACHE="_bt_crash_2026ytd_cache.pkl"
tok=_jquants_id_token()
if os.path.exists(CACHE): data,uni,iss=pickle.load(open(CACHE,"rb"))
else:
    data=batch_download_jquants(tok,start="2025-11-10",end="2026-09-16"); uni=fetch_tse_universe(tok); iss=_iss_map(tok)
    pickle.dump((data,uni,iss),open(CACHE,"wb"))
nm=dict(uni); rows=[]
for tk,df in data.items():
    n=nm.get(tk)
    if n is None or is_etf_ticker(tk,n) or len(df)<30: continue
    df=df.sort_index(); o=df.Open.astype(float); c=df.Close.astype(float); v=df.Volume.astype(float)
    ma5=c.rolling(5).mean(); below=(c<ma5)
    cv=c*v
    for i in range(25,len(df)-1):
        if not (bool(below.iloc[i]) and not bool(below.iloc[i-1])): continue
        if c.iloc[i-1]<=0 or c.iloc[i]<=0: continue
        r1=(c.iloc[i]/c.iloc[i-1]-1)*100
        if r1>-5: continue
        v20p=v.iloc[i-20:i].mean(); volx=v.iloc[i]/v20p if v20p>0 else 0
        if volx<2: continue
        tov=cv.iloc[i-19:i+1].mean()/1e8
        if tov<5 or c.iloc[i]<100: continue
        c21=c.iloc[i-20]; runup=(c.iloc[i-19:i+1].max()/c21-1)*100 if c21>0 else 0
        if runup<30: continue
        o1,c1=o.iloc[i+1],c.iloc[i+1]
        if not(o1>0 and c1>0): continue
        rows.append(dict(entry=df.index[i+1].strftime("%Y-%m-%d"),d0=df.index[i].strftime("%Y-%m-%d"),code=tk[:4],name=n,iss=iss.get(tk[:4],"?"),
                         O1=o1,gap=(o1/c.iloc[i]-1)*100,pnl=(o1-c1)/o1*100,tov=tov,ma5_up=bool(ma5.iloc[i]>=ma5.iloc[i-1])))
D=pd.DataFrame(rows); D=D[D.entry>="2026-01-01"].sort_values("entry")
D.to_csv("_bt_crash_2026ytd_events.csv",index=False,encoding="utf-8-sig")
D=D[D.iss=="2"].copy(); D["skip"]=D.gap<=-3; D["strong"]=D.tov>=20
D["sh"]=(300000//D.O1//100*100).astype(int); D["yen"]=D.pnl/100*D.O1*D.sh
def pf(x): x=np.asarray(x,float); n=-x[x<=0].sum(); return x[x>0].sum()/n if n else 99
def summ(l,x):
    if len(x)==0: print(l,"0件"); return
    print(f"{l}: n={len(x)} 勝率{(x.pnl>0).mean()*100:.0f}% 平均{x.pnl.mean():+.2f}% PF{pf(x.pnl):.2f} 30万/本{x.yen.sum():+,.0f}円(高額で買えない{(x.sh==0).sum()}本=0) 最悪{x.pnl.min():+.1f}%")
E=D[~D.skip]
print("=== 2026/1/1〜9/15 アプリと同じ定義・💥×貸借○×寄り-3%以下見送り×引け買戻し ===")
summ("全件",E); summ("  ◎代金20億+",E[E.strong]); summ("  20億未満",E[~E.strong])
one=E.sort_values(["entry","strong","tov"],ascending=[True,False,False]).groupby("entry").head(1); summ("1日1本(◎優先)",one)
summ("見送り玉(撃っていたら)",D[D.skip])
summ("参考: 貸借✕(撃てない)",pd.read_csv("_bt_crash_2026ytd_events.csv").query("iss!=2 and gap>-3").assign(sh=0,yen=0))
for l,x in (("全件",E),("1日1本",one)):
    cum=x.yen.cumsum(); print(f"{l}の累積: 最大DD {(cum-cum.cummax()).min():+,.0f}円 最終 {cum.iloc[-1]:+,.0f}円")
E2=E.assign(m=E.entry.str[:7]); print(E2.groupby("m").agg(n=("pnl","size"),勝率=("pnl",lambda x:round((x>0).mean()*100)),平均=("pnl",lambda x:round(x.mean(),2)),円=("yen",lambda x:round(x.sum()))).to_string())
print("\n負け上位5:"); print(E.sort_values("pnl").head(5)[["entry","code","name","gap","pnl","yen"]].round(1).to_string(index=False))
print("勝ち上位5:"); print(E.sort_values("pnl",ascending=False).head(5)[["entry","code","name","gap","pnl","yen"]].round(1).to_string(index=False))
