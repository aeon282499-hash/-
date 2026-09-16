"""USDTRY/EURTRY の日中ロング(スワップを払わずリラの減価を取る)の精査: 年別・最悪日・上位3日除去・時刻プロファイル・スプレッド履歴"""
import pandas as pd, numpy as np, pickle
P=pickle.load(open('_xm_rest_h1.pkl','rb'))
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; idx=df.index
    off=pd.Series(2,index=idx)
    for yy in sorted(set(idx.year)):
        mar=max(d for d in range(25,32) if pd.Timestamp(yy,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(yy,10,d).dayofweek==6)
        m=(idx>=pd.Timestamp(yy,3,mar,1))&(idx<pd.Timestamp(yy,10,octb,1)); off[m]=3
    df.index=idx-pd.to_timedelta(off.values,unit='h')+pd.Timedelta(hours=9); return df[~df.index.duplicated()]
def stats(n):
    y=n.groupby(n.index.year).sum(); h=n.index.year<=2020
    return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[h].mean():+.3f} 後{n[~h].mean():+.3f} 合計{n.sum():+.0f}% 最悪{n.min():+.2f}%({n.idxmin().date()}) 最良{n.max():+.2f}%'
for sym in ['USDTRY.','EURTRY.','USDTRY']:
    d=jst(P[sym]['h1']); sp=P[sym]['spec']; pt=sp['point']; o=d.open
    spy=(d['spread'].replace(0,np.nan)*pt/d.close*100).groupby(d.index.year).median()
    print(f'\n===== {sym} spec spread{sp["spread"]/sp["bid"]*100:.3f}% swapL{sp["swap_long"]}pt(={sp["swap_long"]*pt/sp["bid"]*100:+.3f}%/夜) 年別バーspread中央値%: '+' '.join(f'{k}:{v:.3f}' for k,v in spy.items()))
    spread_pct=(d['spread'].replace(0,np.nan)*pt/d.close*100)   # 時点ごとの実スプレッド
    for h,k in [(9,9),(14,4),(10,8),(15,3),(9,5)]:
        a=o[o.index.hour==h]; b=o.reindex(a.index+pd.Timedelta(hours=k)); L=pd.Series((b.values/a.values-1)*100,index=a.index).dropna()
        cost=spread_pct.reindex(L.index).fillna(spread_pct.median()); n=L-cost; prev=L.shift(1)
        print(f'  {h:02d}→{h+k:02d}時 全: {stats(n)}')
        print(f'         prev>0: {stats(n[prev>0])}')
        top=n.nlargest(3); print(f'         上位3日除去: 合計{(n.sum()-top.sum()):+.0f}% t={n.drop(top.index).mean()/n.drop(top.index).std()*np.sqrt(len(n)-3):+.2f}  年別: '+' '.join(f'{y}:{v:+.0f}' for y,v in n.groupby(n.index.year).sum().items()))
    # 時刻プロファイル(1時間保有・全日)の前半/後半
    prof=[]
    for h in range(24):
        a=o[o.index.hour==h]; b=o.reindex(a.index+pd.Timedelta(hours=1)); L=pd.Series((b.values/a.values-1)*100,index=a.index).dropna()
        prof.append((h,L[L.index.year<=2020].mean(),L[L.index.year>=2023].mean()))
    print('  1時間グロス%×100 (時刻: 16-20/23-26): '+' '.join(f'{h}:{a*100:+.1f}/{b*100:+.1f}' for h,a,b in prof))
    # 建玉分布: 9→18の日次リターンの分位
    a=o[o.index.hour==9]; b=o.reindex(a.index+pd.Timedelta(hours=9)); L=pd.Series((b.values/a.values-1)*100,index=a.index).dropna()
    print('  9→18 グロス分位 1%:{:+.2f} 5%:{:+.2f} 50%:{:+.2f} 95%:{:+.2f} 99%:{:+.2f}  -3%以下の日数{}  -5%以下{}'.format(*L.quantile([.01,.05,.5,.95,.99]).values,int((L<=-3).sum()),int((L<=-5).sum())))
