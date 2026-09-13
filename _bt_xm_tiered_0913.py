"""直前レッグの大きさで枚数を段階化: 通常1x・直前≤-0.5%で1.5x/2x(全レッグ)。金昼は無し(0枚)。"""
import pandas as pd, numpy as np
src=open('_bt_xm_addon_recheck_0913.py',encoding='utf-8').read().split("print('追加玉")[0]; exec(src)
add=base+(j.m1*rest/100).where(mid<=-0.5,0)
def build(mult,thr=-0.5):
    kj=np.where(j.prev<=thr,mult,1.0); ku=np.where(u.prev<=thr,mult,1.0); kd=np.where(g.prev<=thr,mult,1.0)
    X=pd.DataFrame({'g':gold,'j':add*kj,'u':u.a*u.net/100*0.1*uj.reindex(u.index).ffill()*ku,'d':g.a*g.net/100*0.1*ej.reindex(g.index).ffill()*kd}).fillna(0); return X[X.index>='2016-05-26']
def run(P,B0=58966,stop=25000):
    B=B0;st=False;peak=B;dd=0; Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st: B=max(0,B+np.floor(B/50000)/100*100*Gg[i]*148+np.floor(B/8750*10)/10*Jj[i]+np.floor(B/31250)*Uu[i]+np.floor(B/50000)*Dd[i])
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,st,dd
def evalp(X,seed=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[];st=0;dds=[]
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x,dd=run(P); e.append(B); st+=x; dds.append(dd)
    e=np.array(e); return f'中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% DD中央{np.median(dds)*100:.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f}'
print('現行(1x)          :',evalp(build(1.0)))
for mult,thr in [(1.5,-0.5),(2.0,-0.5),(1.5,-0.25),(2.0,-1.0),(0.5,-0.5)]:
    print(f'直前≤{thr}%で{mult}x :',evalp(build(mult,thr)))
