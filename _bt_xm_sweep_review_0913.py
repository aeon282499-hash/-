import pandas as pd, numpy as np, pickle
R=pd.read_csv('_xm_sweep_filtered_0913.csv'); R['hx']=(R.h+R.k)%24
bad=lambda h: h in (4,5,6,7,8)          # サーバー22〜02時=メンテ/ロールオーバー帯
F=R[(R.flt=='prev≤0')&(R.t>3)&(R.t1>1.5)&(R.t2>1.5)]
clean=F[~F.h.map(bad)&~F.hx.map(bad)].sort_values('t',ascending=False)
print('== 汚染時間帯(建て/決済がJST4〜8時)を除いた生存 ==');print(clean.round(3).to_string(index=False))
# 金 04→22 の深掘り(建て04時は22:00サーバー=メンテ前の実足)
A=pd.read_pickle('_xm_multi_hist.pkl'); df=A['h1']['GOLD.']; sp=A['spec']['GOLD.']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; idx=df.index; off=pd.Series(2,index=idx)
    for yy in sorted(set(idx.year)):
        mar=max(d for d in range(25,32) if pd.Timestamp(yy,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(yy,10,d).dayofweek==6)
        m=(idx>=pd.Timestamp(yy,3,mar,1))&(idx<pd.Timestamp(yy,10,octb,1)); off[m]=3
    df.index=idx-pd.to_timedelta(off.values,unit='h')+pd.Timedelta(hours=9); return df[~df.index.duplicated()]
d=jst(df); o=d.open; bid=sp['bid']; spread_pct=sp['spread']/bid*100; swap_pct=sp['swap_long']*sp['point']/bid*100
def leg(h,k,thr=0.0,spread_mult=1.0):
    a=o[o.index.hour==h]; b=o.reindex(a.index+pd.Timedelta(hours=k)); L=pd.Series((b.values/a.values-1)*100,index=a.index).dropna(); prev=L.shift(1)
    net=L-spread_pct*spread_mult+swap_pct*(k/24); x=net[prev<=thr]; return x
def stats(n):
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[n.index.year<=2020].mean():+.3f} 後{n[n.index.year>2020].mean():+.3f} sd{n.std():.2f} 最悪{n.min():+.1f} 合計{n.sum():+.0f}%'
print('\n== GOLD. 04:00JST買→+k時間・直前レッグ≤0 ==')
for h,k in [(4,18),(3,18),(4,17),(4,19),(2,18),(3,19),(4,12),(4,6),(4,20),(9,13),(10,12)]:
    print(f'  {h:02d}→{(h+k)%24:02d}: {stats(leg(h,k))}')
x=leg(4,18); print('\n年別合計%:',x.groupby(x.index.year).sum().round(1).to_dict())
print('曜日別:',x.groupby(x.index.dayofweek).mean().round(3).to_dict())
for thr in [-0.5,-0.25,0,0.25,0.5]: print(f'  閾値≤{thr:+.2f}: {stats(leg(4,18,thr))}')
print('  スプレッド2倍:',stats(leg(4,18,0,2.0)))
a=o[o.index.hour==4]; b=o.reindex(a.index+pd.Timedelta(hours=18)); L=pd.Series((b.values/a.values-1)*100,index=a.index).dropna(); prev=L.shift(1); net=L-spread_pct+swap_pct*0.75
print('  フィルタ無し:',stats(net)); print('  prev>0:',stats(net[prev>0]))
rng=np.random.default_rng(3); v=x.values; nb=len(v)//20+1; ts=[]
for _ in range(1000):
    sg=np.repeat(rng.choice([-1,1],nb),20)[:len(v)]; yv=v*sg; ts.append(yv.mean()/yv.std()*np.sqrt(len(yv)))
print(f'  噪音床: 実t={v.mean()/v.std()*np.sqrt(len(v)):.2f} vs |t|99%点{np.percentile(np.abs(ts),99):.2f}')
x.to_pickle('_gold_0422_legs.pkl')
