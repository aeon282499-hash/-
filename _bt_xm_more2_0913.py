import pandas as pd, numpy as np
src=open('_bt_xm_addon_recheck_0913.py',encoding='utf-8').read().split("print('追加玉")[0]; exec(src)
add=base+(j.m1*rest/100).where(mid<=-0.5,0); X=X_of(add)
# (1) GER40 追加: 建て01:00JST → 判定 05/09/13時 → 16時決済
dG=jst(H['GER40Cash']).open; dG=dG[~dG.index.duplicated()]; spG=S['GER40Cash']; spr=spG['spread']/spG['bid']*100
g=LG[LG.prev<=0].copy()
def stats(n):
    if len(n)<50: return f'n={len(n)} 不足'
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[n.index.year<=2020].mean():+.3f} 後{n[n.index.year>2020].mean():+.3f}'
print('== GER40 途中追加(残り区間の成績・コスト込み) ==')
for mh in [5,9,13]:
    m=dG.reindex(g.index+pd.Timedelta(hours=mh)); m.index=g.index; midp=(m/g.a-1)*100; restp=(g.b/m-1)*100-spr
    for thr in [-0.25,-0.5,-1.0]:
        mm=midp<=thr; print(f'  {mh:02d}時 ≤{thr:+.2f}%: {stats(restp[mm])}')
    print(f'  {mh:02d}時 >0     : {stats(restp[midp>0])}')
# (2) 全停止ラインの置き方
def run(P,stop,B0=58966):
    B=B0;st=False;peak=B;dd=0; Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values
    for i in range(len(Gg)):
        if stop>0 and B<stop: st=True
        if not st and B>1000: B=max(0,B+np.floor(B/50000)/100*100*Gg[i]*148+np.floor(B/8750*10)/10*Jj[i]+np.floor(B/31250)*Uu[i]+np.floor(B/50000)*Dd[i])
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,st,dd
rng=np.random.default_rng(5); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; SS=[rng.choice(yrs,5) for _ in range(300)]
print('\n== 全停止ライン(残高がこれを割ったら新規停止・人が判断) ==')
for stop in [0,10000,15000,25000,35000,45000]:
    e=[];st=0;dds=[]
    for s in SS:
        P=pd.concat([bl[y] for y in s]); B,x,dd=run(P,stop); e.append(B); st+=x; dds.append(dd)
    e=np.array(e); print(f'  ライン{stop:>6}円: 中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 下位5%{np.percentile(e,5)/1e4:5.1f} 停止{st/3:3.0f}% DD中央{np.median(dds)*100:.0f}% E[log]{np.mean(np.log(np.maximum(e,1000)/58966)):+.2f}')
