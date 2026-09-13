"""M5(直近約1年)で建て/決済の分オフセットを比較。フィルタは直前レッグ≤0(月曜無条件はJP/US)。"""
import pandas as pd, numpy as np, pickle
M=pickle.load(open('_xm_m5_recent.pkl','rb'))
def jst(df):
    idx=pd.DatetimeIndex(df.t); off=pd.Series(2,index=idx)
    for yy in sorted(set(idx.year)):
        mar=max(d for d in range(25,32) if pd.Timestamp(yy,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(yy,10,d).dayofweek==6)
        m=(idx>=pd.Timestamp(yy,3,mar,1))&(idx<pd.Timestamp(yy,10,octb,1)); off[m]=3
    s=pd.Series(df.open.values,index=idx-pd.to_timedelta(off.values,unit='h')+pd.Timedelta(hours=9)); return s[~s.index.duplicated()]
def legs(o,eh,em,xh,xm,dows,cross_day=True):
    a=o[(o.index.hour==eh)&(o.index.minute==em)]; tgt=a.index.normalize()+pd.Timedelta(days=1 if cross_day else 0)+pd.Timedelta(hours=xh,minutes=xm)
    b=o.reindex(tgt); L=pd.Series((b.values/a.values-1)*100,index=a.index).dropna(); return L[L.index.dayofweek.isin(dows)]
def stats(n): return f'n={len(n):3d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 合計{n.sum():+.1f}%'
for sym,eh,xh,dows,cross in [('JP225Cash',15,9,[0,1,2,3],True),('US500Cash',15,9,[0,1,2,3],True),('GER40Cash',1,16,[1,2,3,4],False)]:
    o=jst(M[sym]); base=legs(o,eh,0,xh,0,dows,cross); prev=base.shift(1); mask=(prev<=0)|((base.index.dayofweek==0)&(sym!='GER40Cash'))
    print(f'\n== {sym} 期間{o.index[0].date()}〜{o.index[-1].date()} 基準{eh:02d}:00→{xh:02d}:00 {stats(base[mask])} ==')
    for em in [-15,-5,0,5,15,30]:
        eh2,em2=(eh+(em//60 if em>=0 else -1)),(em%60); L=legs(o,eh2,em2,xh,0,dows,cross); m=mask.reindex(L.index.normalize()+pd.Timedelta(hours=eh)).fillna(False).values if False else mask.values[:len(L)] if len(L)==len(mask) else None
        Lm=L[L.index.normalize().isin(base[mask].index.normalize())]; print(f'  建て{eh2:02d}:{em2:02d}: {stats(Lm)}')
    for xm in [-15,-5,0,5,10,15,30]:
        xh2,xm2=(xh+(xm//60 if xm>=0 else -1)),(xm%60); L=legs(o,eh,0,xh2,xm2,dows,cross); Lm=L[L.index.normalize().isin(base[mask].index.normalize())]; print(f'  決済{xh2:02d}:{xm2:02d}: {stats(Lm)}')
