import pandas as pd, numpy as np
src=open('_bt_xm_addon_recheck_0913.py',encoding='utf-8').read().split("print('追加玉")[0]; exec(src)
add=base+(j.m1*rest/100).where(mid<=-0.5,0)
def build(us_all=False,de_all=False):
    U=LU[(LU.prev<=0)|(LU.dow==0)] if not us_all else LU; G=LG[LG.prev<=0] if not de_all else LG
    X=pd.DataFrame({'g':gold,'j':add,'u':U.a*U.net/100*0.1*uj.reindex(U.index).ffill(),'d':G.a*G.net/100*0.1*ej.reindex(G.index).ffill()}).fillna(0); return X[X.index>='2016-05-26']
def run(P,xg=50000,B0=58966,stop=25000):
    B=B0;st=False;peak=B;dd=0; Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st: B=max(0,B+(np.floor(B/xg)/100*100*Gg[i]*148 if xg else 0)+np.floor(B/8750*10)/10*Jj[i]+np.floor(B/31250)*Uu[i]+np.floor(B/50000)*Dd[i])
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,st,dd
def evalp(X,xg=50000,seed=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[];st=0;dds=[]
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x,dd=run(P,xg); e.append(B); st+=x; dds.append(dd)
    e=np.array(e); return f'中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% DD中央{np.median(dds)*100:.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f}'
X=build()
print('== 金15分ショート ==')
for xg,lab in [(50000,'今(5万円/0.01)'),(0,'オフ'),(100000,'10万円/0.01'),(25000,'2.5万円/0.01')]: print(f'  {lab:14s}:',evalp(X,xg))
g16=gold[gold.index.year<=2024]; print(f'  参考: 金15分の2016-24平均{g16.mean():+.3f}$/回・2025-26 {gold[gold.index.year>=2025].mean():+.3f}$/回')
print('== S&P/GER40 のフィルタ有無 ==')
print('  現行(S&P月曜のみ無条件・GER40フィルタ) :',evalp(build()))
print('  S&P全夜無条件                        :',evalp(build(True,False)))
print('  GER40全夜無条件                      :',evalp(build(False,True)))
print('  両方無条件                           :',evalp(build(True,True)))
