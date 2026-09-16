# ユニチカ型(直近5日が乱高下)を事前に弾けたか: 26年プールに「d0を除く直近5日の最大|日次変化|」「±10%日の数」を付けて層別
import io,sys,pickle; sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8')
import numpy as np, pandas as pd
C=pickle.load(open("_bt_crashshort_26y_pool.pkl","rb")); ISS=pickle.load(open("_iss_type_by_year.pkl","rb")); yrs=sorted(ISS)
C["iss"]=C.apply(lambda r:ISS[min(yrs,key=lambda k:abs(k-r.year))].get(r.ticker[:4],"?"),axis=1)
D=C[(C.year>=2017)&(C.iss=="2")&(C.run20>=30)&(C.vr>=2)&(C.chg<=-5)&(C.gap>-3)].copy()
ALL=pickle.load(open("tachibana_history.pkl","rb"))["all_data"]
feat={}
for tk,g in D.groupby("ticker"):
    df=ALL.get(tk)
    if df is None: continue
    df=df.dropna(subset=["Close"]); df=df[df["Close"]>0]; c=df["Close"].astype(float); ch=(c/c.shift(1)-1)*100
    idx=pd.DatetimeIndex(df.index); 
    for r in g.itertuples():
        d0=pd.Timestamp(r.entry)  # entry列は約定日(d0の翌営業日)
        i=idx.get_indexer([d0])[0]
        if i<6: continue
        prev5=ch.iloc[i-6:i-1]; prev1=ch.iloc[i-2]
        feat[r.Index]=(prev5.abs().max(), int((prev5.abs()>=10).sum()), prev1)
F=pd.DataFrame.from_dict(feat,orient="index",columns=["max5","n10","prev1"]); D=D.join(F,how="inner")
def pf(x): x=np.asarray(x,float); n=-x[x<=0].sum(); return x[x>0].sum()/n if n else 99
def s(l,x):
    if len(x)==0: print(f"  {l:<34} 0件"); return
    print(f"  {l:<34} n{len(x):>4} {x.pnl.mean():+.2f}% 勝率{(x.pnl>0).mean()*100:.0f}% PF{pf(x.pnl):.2f} 最悪{x.pnl.min():+.1f}% 5%点{np.percentile(x.pnl,5):+.1f}%")
print(f"2017-26 💥貸借○(見送り後) 特徴付与 n={len(D)}")
s("全体",D)
print("直近5日(d0除く)の最大|日次変化|:")
for lo,hi in ((0,5),(5,10),(10,15),(15,99)): s(f"  {lo}〜{hi}%",D[(D.max5>=lo)&(D.max5<hi)])
print("直近5日の±10%日の数:")
for k in (0,1,2): s(f"  {k}日"+("以上" if k==2 else ""),D[D.n10==k] if k<2 else D[D.n10>=2])
print("前日(d0-1)の変化:")
for lo,hi in ((-99,-5),(-5,0),(0,5),(5,10),(10,99)): s(f"  {lo}〜{hi}%",D[(D.prev1>=lo)&(D.prev1<hi)])
print("除外案: 直近5日に±15%日あり→見送り"); s("残る玉",D[D.max5<15]); s("除外される玉",D[D.max5>=15])
print("除外案: 前日+10%以上(前日が急反発)→見送り"); s("残る玉",D[D.prev1<10]); s("除外される玉",D[D.prev1>=10])
# ユニチカ自身
u=D[(D.ticker=="3103.T")&(D.year==2026)]; print("ユニチカ8/12:",u[["entry","max5","n10","prev1","pnl"]].round(1).to_string(index=False) if len(u) else "プール定義(前日終値ベース急騰27%)では非該当")
