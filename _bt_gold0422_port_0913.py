"""金 04:00JST買→22:00JST売(直前レッグ≤0)を現行4本(×1.25サイズ・月曜無条件・追加ルール)に足す。スワップ=1泊(水曜3泊)・Zero $0.21往復。"""
import pandas as pd, numpy as np
src=open('_bt_xm_addon_recheck_0913.py',encoding='utf-8').read().split("print('追加玉")[0]; exec(src)
add=base+(j.m1*rest/100).where(mid<=-0.5,0); X=X_of(add)
A=pd.read_pickle('_xm_multi_hist.pkl'); gd=jst(A['h1']['GOLD.']); o=gd.open; o=o[~o.index.duplicated()]
a=o[o.index.hour==4]; b=o.reindex(a.index+pd.Timedelta(hours=18)); L=pd.DataFrame({'a':a.values,'b':b.values},index=a.index).dropna()
L['pct']=(L.b/L.a-1)*100; L['prev']=L.pct.shift(1); L=L[L.prev<=0]
sw=np.where(L.index.dayofweek==2,3,1)*-0.97
L['usd']=(L.b-L.a)-0.21+sw                     # $/oz
Lq=L.usd*148.0; Lq.index=Lq.index.normalize(); X['q']=Lq.reindex(X.index).fillna(0)  # 0.01lot=1oz
print('金04→22 1oz: 1日sd=%.0f円 平均=%+.0f円 相関 j %.2f u %.2f d %.2f'%(X.q[X.q!=0].std(),X.q[X.q!=0].mean(),X[(X.q!=0)&(X.j!=0)][['q','j']].corr().iloc[0,1],X[(X.q!=0)&(X.u!=0)][['q','u']].corr().iloc[0,1],X[(X.q!=0)&(X.d!=0)][['q','d']].corr().iloc[0,1]))
def run(P,xq,B0=58966,stop=25000):
    B=B0;st=False;peak=B;dd=0; Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values;Q=P.q.values
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st: B=max(0,B+np.floor(B/50000)/100*100*Gg[i]*148+np.floor(B/8750*10)/10*Jj[i]+np.floor(B/31250)*Uu[i]+np.floor(B/50000)*Dd[i]+(np.floor(B/xq)*Q[i] if xq else 0))
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,st,dd
rng=np.random.default_rng(5); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; SS=[rng.choice(yrs,5) for _ in range(300)]
for xq in [0,300000,200000,120000,80000,58000]:
    e=[];st=0;dds=[]
    for s in SS:
        P=pd.concat([bl[y] for y in s]); B,x,dd=run(P,xq); e.append(B); st+=x; dds.append(dd)
    e=np.array(e); print(f'  金04→22 {xq:>7}円/0.01: 中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% DD中央{np.median(dds)*100:.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f}')
print('年別(5.9万起点・無し→58000円/0.01):')
for y in yrs:
    B0,_,_=run(bl[y],0); B1,_,_=run(bl[y],58000); print(f'  {y}: {B0/1e4:.1f}万 → {B1/1e4:.1f}万')
