"""金 夜ドリフト(15:00JST買→翌9:00売・月〜木)に前夜≤0フィルタ。Zero実コスト: spread+手数料$0.21・swap_long -$0.97/泊(水3倍)。"""
import pandas as pd, numpy as np
m1=pd.read_pickle('_fx_xauusd_m1.pkl'); j=m1.copy(); j.index=j.index+pd.Timedelta(hours=9)
o=j.open; a=o[(o.index.hour==15)&(o.index.minute==0)]; b=o[(o.index.hour==9)&(o.index.minute==0)]
res=[]
for t,pa in a.items():
    nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=20))]
    if len(nb)==0: continue
    res.append((t.normalize(),pa,nb.iloc[0],t.dayofweek))
L=pd.DataFrame(res,columns=['d','a','b','dow']).set_index('d'); L['gross']=L.b-L.a; L['pct']=(L.b/L.a-1)*100
L['prev']=L.pct.shift(1); L=L[L.dow<=3]
sw=np.where(L.dow==2,3,1)*-0.97
def net(gr,swap): return gr-0.21+swap
def stats(n):
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} 晩{n.mean():+.2f}$ t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[n.index.year<=2020].mean():+.2f} 後{n[n.index.year>2020].mean():+.2f} 年{n.sum()/10.7:+.0f}$/oz'
for lab,swap in [('Zero swap-0.97',sw),('swap0(KIWAMI極)',0)]:
    n=net(L.gross,swap); print(f'[{lab}] 全夜:   {stats(n)}')
    for thr in [0,-0.25,-0.5]:
        m=L.prev<=thr; print(f'[{lab}] 前夜≤{thr:+.2f}: {stats(n[m])}   / 前夜>{thr:+.2f}: {stats(n[~m])}')
