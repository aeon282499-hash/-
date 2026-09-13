"""全銘柄×建て時刻24×保有9 を「直前レッグ≤0」フィルタ付きで総当たり(2016-05〜)。コスト=スプレッド+スワップ(保有h/24泊)。"""
import pandas as pd, numpy as np, pickle
A=pd.read_pickle('_xm_multi_hist.pkl'); syms={}
for k,v in A['h1'].items(): syms[k]=(v,A['spec'][k])
for f in ['_xm_more_idx_h1.pkl','_xm_metals_fx2_h1.pkl']:
    for k,v in pickle.load(open(f,'rb')).items(): syms[k]=(v['h1'],v['spec'])
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']
    idx=df.index; y=idx.year
    # 欧州DST: 3月最終日曜01:00〜10月最終日曜01:00 (UTC) → サーバーUTC+3 else +2
    def dst(ts):
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        return pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1)
    # 年ごとに境界を作って高速化
    off=pd.Series(2,index=idx)
    for yy in sorted(set(y)):
        mar=max(d for d in range(25,32) if pd.Timestamp(yy,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(yy,10,d).dayofweek==6)
        m=(idx>=pd.Timestamp(yy,3,mar,1))&(idx<pd.Timestamp(yy,10,octb,1)); off[m]=3
    df.index=idx-pd.to_timedelta(off.values,unit='h')+pd.Timedelta(hours=9); return df[~df.index.duplicated()]
HOLDS=[1,2,3,4,6,9,12,15,18]; rows=[]
for s,(df,sp) in syms.items():
    d=jst(df)
    if len(d)<10000: continue
    o=d.open; bid=sp['bid'] if sp['bid']>0 else float(d.close.iloc[-1]); spread_pct=sp['spread']/bid*100
    if sp['swap_mode']==3: swap_night=sp['swap_long']/365
    else: swap_night=sp['swap_long']*sp['point']/bid*100   # points→価格→%
    for h in range(24):
        a=o[o.index.hour==h]
        if len(a)<500: continue
        for k in HOLDS:
            b=o.reindex(a.index+pd.Timedelta(hours=k)); pct=(b.values/a.values-1)*100
            L=pd.Series(pct,index=a.index).dropna()
            if len(L)<800: continue
            prev=L.shift(1); net=L-spread_pct+swap_night*(k/24)*(1 if swap_night<=0 else 1)   # swap_long は負なら負担
            for side in (1,-1):
                n=(net if side==1 else (-L-spread_pct+( (sp['swap_short']/365 if sp['swap_mode']==3 else sp['swap_short']*sp['point']/bid*100)*(k/24))))
                for fl,mask in [('prev≤0',prev<=0),('prev>0',prev>0),('all',prev.notna())]:
                    x=n[mask]
                    if len(x)<300: continue
                    h1=x[x.index.year<=2020]; h2=x[x.index.year>2020]
                    if len(h1)<100 or len(h2)<100: continue
                    y=x.groupby(x.index.year).sum()
                    rows.append(dict(sym=s,h=h,k=k,side='買' if side==1 else '売',flt=fl,n=len(x),mean=x.mean(),t=x.mean()/x.std()*np.sqrt(len(x)),t1=h1.mean()/h1.std()*np.sqrt(len(h1)),t2=h2.mean()/h2.std()*np.sqrt(len(h2)),yrs=f'{int((y>0).sum())}/{len(y)}',tot=x.sum()))
R=pd.DataFrame(rows); R.to_csv('_xm_sweep_filtered_0913.csv',index=False)
pd.set_option('display.width',220)
print('セル数',len(R),' 銘柄数',R.sym.nunique())
F=R[(R.flt=='prev≤0')]
ok=F[(F.t>3)&(F.t1>1.5)&(F.t2>1.5)].sort_values('t',ascending=False)
print(f'\n== prev≤0 で t>3 かつ前後半ともt>1.5: {len(ok)}セル ==')
print(ok.head(40).round(2).to_string(index=False))
ok2=R[(R.flt=='prev>0')&(R.t>3)&(R.t1>1.5)&(R.t2>1.5)].sort_values('t',ascending=False)
print(f'\n== prev>0(順張り側) で同条件: {len(ok2)}セル ==');print(ok2.head(15).round(2).to_string(index=False))
