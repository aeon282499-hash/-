"""個別株CFDの夜(その日の最終H1足の終値→翌日の最初のH1足の始値)。コスト: swap mode5=年率%(ロング負担)を1泊分、スプレッドは未知なので gross と 損益分岐スプレッドを出す。
直前レッグ≤0 フィルタ、銘柄横断の等ウェイト集計、US/EU別。"""
import pandas as pd, numpy as np, pickle
D=pickle.load(open('_xm_stocks_h1.pkl','rb')); rows=[]; port={}
for s,v in D.items():
    df=v['h1'].copy(); df.index=pd.to_datetime(df.time,unit='s'); df=df[df.index>='2016-05-01']
    if len(df)<3000: continue
    day=df.index.normalize(); first=df.groupby(day).open.first(); last=df.groupby(day).close.last(); cnt=df.groupby(day).size()
    ok=cnt>=4
    first=first[ok]; last=last[ok]
    night=(first.shift(-1)/last-1)*100          # 当日引け→翌営業日寄り
    night=night.dropna(); dow=night.index.dayofweek
    gap_days=(pd.Series(night.index,index=night.index).shift(-1)-pd.Series(night.index,index=night.index)).dt.days
    sw=v['spec']['swap_long']/365*np.where(gap_days.reindex(night.index)>=3,3,1)     # 週末3泊
    net=night+sw
    prev=night.shift(1)
    eu='US' if v['spec']['cur']=='USD' else 'EU'
    port[s]=pd.DataFrame({'net':net,'prev':prev,'gross':night})
    f=net[prev<=0]
    rows.append(dict(sym=s[:14],mkt=eu,cur=v['spec']['cur'],n=len(net),gross_night=night.mean(),net_night=net.mean(),t=net.mean()/net.std()*np.sqrt(len(net)),
                     f_n=len(f),f_net=f.mean(),f_t=f.mean()/f.std()*np.sqrt(len(f)),be_spread=f.mean(),yrs_pos=int((f.groupby(f.index.year).sum()>0).sum()),yrs=f.index.year.nunique(),swap=v['spec']['swap_long'],margin=v['spec']['margin']))
R=pd.DataFrame(rows); pd.set_option('display.width',220)
print(R.sort_values('f_net',ascending=False).round(3).to_string(index=False))
print('\n== 市場別 平均 (夜/回・%)  ==')
print(R.groupby('mkt')[['gross_night','net_night','f_net','f_t']].mean().round(3))
# 等ウェイト・ポートフォリオ(全銘柄の夜を日付で平均) 全夜 vs 直前≤0
for mk in ['US','EU']:
    syms=[s for s in port if (D[s]['spec']['cur']=='USD')==(mk=='US')]
    A=pd.concat([port[s].net.rename(s) for s in syms],axis=1); P=pd.concat([port[s].prev.rename(s) for s in syms],axis=1)
    allm=A.mean(axis=1); fm=A.where(P<=0).mean(axis=1).dropna()
    def st(x): y=x.groupby(x.index.year).sum(); return f'n={len(x)} 夜{x.mean():+.3f}% t={x.mean()/x.std()*np.sqrt(len(x)):+.2f} 勝{int((y>0).sum())}/{len(y)} 年率{x.sum()/((x.index[-1]-x.index[0]).days/365):+.1f}%'
    print(f'\n== {mk} 等ウェイト {len(syms)}銘柄 (スプレッド前) ==\n  全夜: {st(allm.dropna())}\n  直前≤0: {st(fm)}')
    print('  XM株CFDのスプレッドが0.1%なら:',f'全夜{allm.mean()-0.1:+.3f}%  直前≤0 {fm.mean()-0.1:+.3f}%')
