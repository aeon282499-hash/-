"""FOMC声明の夜(米D日14:00ET=JST D+1 03/04時)を跨ぐレッグの成績。JP/US: 建て日=D(JST)。GER40: 建て日=D+1(01:00JST)。"""
import pandas as pd, numpy as np
src=open('_bt_xm_monday_0913.py',encoding='utf-8').read().split("print('GER40 火曜JST")[0]; exec(src)
F=set(pd.to_datetime(open('_fomc_dates.txt').read().split()))
def stats(n):
    if len(n)==0: return 'n=0'
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[n.index.year<=2020].mean():+.3f} 後{n[n.index.year>2020].mean():+.3f} sd{n.std():.2f} 最悪{n.min():+.1f}'
for nm,L,shift in [('日経',LJ,0),('US500',LU,0),('GER40',LG,1)]:
    sel=L[(L.prev<=0)|((L.dow==0)&(nm!='GER40'))] if nm!='GER40' else L[L.prev<=0]
    isf=sel.index.map(lambda d: (d-pd.Timedelta(days=shift)) in F)
    print(f'{nm}: FOMC夜 {stats(sel.net[isf])}\n{" "*len(nm)}  他の夜  {stats(sel.net[~isf])}')
    # 追加ルール(日経のみ): FOMC夜の01時追加
