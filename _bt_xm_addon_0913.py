"""③夜の途中の追加/半分手仕舞い(H1始値で判定) ④GER40の金曜夜(木JST01:00→金16:00)と週末(土01:00→月16:00)"""
import pandas as pd, numpy as np
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']; S=D['spec']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
def stats(n):
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[n.index.year<=2020].mean():+.3f} 後{n[n.index.year>2020].mean():+.3f} 合計{n.sum():+.0f}%'
def legs(df,h_in,h_out,dows,sym,mid_hours):
    sp=S[sym]; spread_pct=sp['spread']/sp['bid']*100; o=df.open; a=o[o.index.hour==h_in]; b=o[o.index.hour==h_out]; res=[]
    for t,pa in a.items():
        nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=30))]
        if len(nb)==0: continue
        te=nb.index[0]; mids={}
        for mh in mid_hours:
            m=o[(o.index>t)&(o.index<te)&(o.index.hour==mh)]
            mids[mh]=m.iloc[0] if len(m) else np.nan
        res.append((t.normalize(),pa,nb.iloc[0],(te-t).total_seconds()/3600,t.dayofweek,*[mids[h] for h in mid_hours]))
    L=pd.DataFrame(res,columns=['d','a','b','hrs','dow']+[f'm{h}' for h in mid_hours]).set_index('d'); L['pct']=(L.b/L.a-1)*100
    L['prev']=L.pct.shift(1); L['cost']=spread_pct+sp['swap_long']/365*(L.hrs/24); return L
for nm,sym,hi,ho,dows,mids in [('JP','JP225Cash',15,9,[0,1,2,3],[21,1,5]),('US','US500Cash',15,9,[0,1,2,3],[21,1,5]),('DE','GER40Cash',1,16,[1,2,3,4],[5,9,13])]:
    df=jst(H[sym]); L=legs(df,hi,ho,dows,sym,mids); F=L[L.dow.isin(dows)&(L.prev<=0)]
    base=F.pct-F.cost; print(f'\n== {nm} 基本: {stats(base)}')
    for mh in mids:
        mid=(F[f'm{mh}']/F.a-1)*100; rest=(F.b/F[f'm{mh}']-1)*100      # 途中時点の含み / 残り区間のリターン
        for thr in [-1.0,-0.5,0.5,1.0]:
            m=(mid<=thr) if thr<0 else (mid>=thr)
            if m.sum()<100: continue
            add=base+ (rest-F.cost*0.5).where(m,0)      # 同量を追加(残り区間を2倍)
            half=base-(rest*0.5).where(m,0)             # 半分手仕舞い
            print(f'  {mh:02d}時点で{"≤" if thr<0 else "≥"}{abs(thr):.1f}%の夜(n={int(m.sum())}): 残り区間{stats(rest[m]-F.cost[m]*0.5)} | 追加なら合計{add.sum():+.0f}%(基本{base.sum():+.0f}) 半分手仕舞い{half.sum():+.0f}%')
# GER40 金曜夜/週末
df=jst(H['GER40Cash']); sp=S['GER40Cash']; spread_pct=sp['spread']/sp['bid']*100; o=df.open; a=o[o.index.hour==1]; b=o[o.index.hour==16]; res=[]
for t,pa in a.items():
    nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=80))]
    if len(nb)==0: continue
    res.append((t.normalize(),pa,nb.iloc[0],(nb.index[0]-t).total_seconds()/3600,t.dayofweek))
L=pd.DataFrame(res,columns=['d','a','b','hrs','dow']).set_index('d'); L['pct']=(L.b/L.a-1)*100; L['prev']=L.pct.shift(1); L['net']=L.pct-spread_pct-sp['swap_long']/365*(L.hrs/24)
print('\n== GER40 曜日別(直前≤0) ==')
for d in [1,2,3,4,5]:
    x=L[(L.dow==d)&(L.prev<=0)]; print(f'  JST曜{d}(建て) 平均保有{x.hrs.mean():.0f}h: {stats(x.net)}')
