"""追加指数の夜レッグ探索: 15→9JST(月〜木)・欧州型01→16JST(火〜金)・HK 17→10JST(月〜木)・US 06→23JST。直前レッグ≤0フィルタ。2016-05〜。"""
import pandas as pd, numpy as np, pickle
D=pickle.load(open('_xm_more_idx_h1.pkl','rb'))
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
def leg(df,h_in,h_out,dows,spread_pct,swap_yr):
    o=df.open; a=o[o.index.hour==h_in]; b=o[o.index.hour==h_out]; res=[]
    for t,pa in a.items():
        nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=26))]
        if len(nb)==0: continue
        res.append((t.normalize(),pa,nb.iloc[0],(nb.index[0]-t).total_seconds()/3600,t.dayofweek))
    L=pd.DataFrame(res,columns=['d','a','b','hrs','dow']).set_index('d'); L['pct']=(L.b/L.a-1)*100
    L['prev']=L.pct.shift(1); L=L[L.dow.isin(dows)]
    L['net']=L.pct-spread_pct-swap_yr/365*(L.hrs/24); return L
def stats(n):
    if len(n)<200: return f'n={len(n)} 不足'
    y=n.groupby(n.index.year).sum(); yrs=len(y); h=n.index.year<=2020
    return f'n={len(n):4d} net/回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{yrs} 前{n[h].mean() if h.sum()>50 else float("nan"):+.3f} 後{n[~h].mean():+.3f} 年率{n.sum()/max(1,(n.index[-1]-n.index[0]).days/365):+.1f}% 最悪{n.min():+.1f}%'
W={'15→9':(15,9,[0,1,2,3]),'01→16':(1,16,[1,2,3,4]),'17→10':(17,10,[0,1,2,3]),'06→23':(6,23,[1,2,3,4]),'05→22':(5,22,[1,2,3,4])}
legs={}
for s,v in D.items():
    sp=v['spec']; spread_pct=sp['spread']/sp['bid']*100; df=jst(v['h1'])
    if len(df)<3000: print(f'\n== {s}: データ短い({len(df)}本)'); continue
    print(f'\n== {s} spread{spread_pct:.3f}% swapL{sp["swap_long"]}%/年 {sp["cur"]} ==')
    for k,(hi,ho,dows) in W.items():
        L=leg(df,hi,ho,dows,spread_pct,sp['swap_long'])
        if len(L)<200: continue
        F=L[L.prev<=0]; print(f'  {k} 全 {stats(L.net)}'); print(f'  {k} 前≤0 {stats(F.net)}')
        legs[(s,k)]=L
pickle.dump(legs,open('_xm_idx2_legs.pkl','wb'))
