"""2026-09-17 未検証の追試: A 実測スワップ(9/14-16の約定)でレッグ/ポートフォリオ再計算 B US500の担がれ追加・日経追加の倍率/2段目 C ゲート込み金15分ショートのサイズ D 週末レッグ(US500/GER40)"""
import pandas as pd, numpy as np, copy
src=open('_bt_xm_addon_recheck_0913.py',encoding='utf-8').read().split("print('追加玉")[0]; exec(src)
B0=58966
def run(P,units=(8750,31250,50000),xg=50000,stop=25000):
    B=B0;st=False;peak=B;dd=0; Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st: B=max(0,B+(np.floor(B/xg)/100*100*Gg[i]*JPY if xg else 0)+np.floor(B/units[0]*10)/10*Jj[i]+np.floor(B/units[1])*Uu[i]+np.floor(B/units[2])*Dd[i])
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,st,dd
def evalp(X,units=(8750,31250,50000),xg=50000,seed=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[];st=0;dds=[]
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x,dd=run(P,units,xg); e.append(B); st+=x; dds.append(dd)
    e=np.array(e); return f'中央{np.median(e)/1e4:6.1f}万 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% DD中央{np.median(dds)*100:.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/B0)):+.2f}'
def Xof(jser,useru=None,userd=None,gser=None):
    uu=u if useru is None else useru; gg=g if userd is None else userd; gs=gold if gser is None else gser
    X=pd.DataFrame({'g':gs,'j':jser,'u':uu.a*uu.net/100*0.1*uj.reindex(uu.index).ffill(),'d':gg.a*gg.net/100*0.1*ej.reindex(gg.index).ffill()}).fillna(0); return X[X.index>='2016-05-26']
add=base+(j.m1*rest/100).where(mid<=-0.5,0)
print('現行(×1.25・01時≤-0.5%同量追加):',evalp(Xof(add)))

print('\n== A 実測スワップ(9/14-16の約定から): JP -30円/6.7lot=-0.0071%/18h  US500 -19円/0.1lot=-0.0169%/18h  GER40 -60円/0.1lot=-0.0136%/15h ==')
print('   BTの前提: JP 4.45%/年→-0.0091%/18h  US 1.21%→-0.0025%  GER 3.33%→-0.0057%  (US/GERは実測がBTの7倍/2.4倍)')
S0=copy.deepcopy(S)
def rebuild(sw):
    for k,v in sw.items(): S[k]['swap_long']=v
    lj=legs('JP225Cash',15,9,[0,1,2,3],80); lu=legs('US500Cash',15,9,[0,1,2,3],80); lg=legs('GER40Cash',1,16,[1,2,3,4],26)
    for k,v in S0.items(): S[k]['swap_long']=v['swap_long']
    return lj,lu,lg
for lab,sw in [('BT前提',{'JP225Cash':4.45,'US500Cash':1.21,'GER40Cash':3.33}),('実測',{'JP225Cash':3.5,'US500Cash':8.2,'GER40Cash':7.9}),('実測+配当(JP1.8%/US1.3%/年を日割り加算)',{'JP225Cash':3.5-1.8,'US500Cash':8.2-1.3,'GER40Cash':7.9})]:
    lj,lu,lg=rebuild(sw); jj=lj[(lj.prev<=0)|(lj.dow==0)]; uu2=lu[(lu.prev<=0)|(lu.dow==0)]; gg2=lg[lg.prev<=0]
    print(f'  [{lab}]'); print('    JP :',stats(jj.net)); print('    US :',stats(uu2.net)); print('    GER:',stats(gg2.net))
    m1=[]; o=jst(H['JP225Cash']).open
    for t in jj.index:
        x=o[(o.index>t+pd.Timedelta(hours=15))&(o.index<t+pd.Timedelta(hours=33))&(o.index.hour==1)]; m1.append(x.iloc[0] if len(x) else np.nan)
    jj=jj.copy(); jj['m1']=m1; md=(jj.m1/jj.a-1)*100; rs=(jj.b/jj.m1-1)*100-S['JP225Cash']['spread']/S['JP225Cash']['bid']*100
    ad=jj.a*jj.net/100+(jj.m1*rs/100).where(md<=-0.5,0)
    print('    ポートフォリオ:',evalp(Xof(ad,uu2,gg2)))

print('\n== B US500の担がれ追加(建てた夜のh時JSTに建値比≤thrなら同量追加) ==')
ou=jst(H['US500Cash']).open; spu=S['US500Cash']['spread']/S['US500Cash']['bid']*100
def mid_at(o,L,h):
    m=[]
    for t in L.index:
        x=o[(o.index>t+pd.Timedelta(hours=15))&(o.index<t+pd.Timedelta(hours=33))&(o.index.hour==h)]; m.append(x.iloc[0] if len(x) else np.nan)
    return pd.Series(m,index=L.index)
for h in [23,1,3]:
    mu=mid_at(ou,u,h); midu=(mu/u.a-1)*100; restu=(u.b/mu-1)*100-spu
    for thr in [-0.25,-0.5,-1.0]:
        m=midu<=thr; print(f'  {h:02d}時≤{thr:+.2f}% 追加玉の残り区間: {stats(restu[m])}  発動{m.mean()*100:.0f}%')
    for thr in [-0.5]:
        u2=u.copy(); addu=u.a*u.net/100+(mu*restu/100).where(midu<=thr,0)
        X=Xof(add); X['u']=(addu*0.1*uj.reindex(u.index).ffill()).reindex(X.index).fillna(0)
        print(f'    → ポートフォリオ({h:02d}時≤{thr}%同量追加):',evalp(X))

print('\n== B2 日経追加の倍率(01時≤-0.5%)と2段目(03時≤-1.0%でさらに同量) ==')
for mult in [0.5,1.0,1.5,2.0]:
    ad=base+(j.m1*rest/100*mult).where(mid<=-0.5,0); print(f'  倍率{mult:.1f}:',evalp(Xof(ad)))
oj=jst(H['JP225Cash']).open; spj=S['JP225Cash']['spread']/S['JP225Cash']['bid']*100
m3=mid_at(oj,j,3); mid3=(m3/j.a-1)*100; rest3=(j.b/m3-1)*100-spj
for thr in [-0.75,-1.0,-1.5]:
    mm=(mid3<=thr); print(f'  03時≤{thr}% 2段目玉の残り区間: {stats(rest3[mm])} 発動{mm.mean()*100:.0f}%')
    ad=add+(m3*rest3/100).where(mm,0); print(f'    → ポートフォリオ(01時追加+03時2段目):',evalp(Xof(ad)))
m3b=mid_at(oj,j,3); 
for thr in [-0.5,-1.0]:
    mm=(mid3<=thr); ad=base+(m3*rest3/100).where(mm,0); print(f'  参考: 追加を03時≤{thr}%だけにする:',evalp(Xof(ad)))

print('\n== C 金15分ショートのサイズ(自己判断ゲートN40/閾値0.10込み) ==')
gate=(gold.rolling(40).mean().shift(1)>0.10).fillna(False); gg=gold.where(gate,0.0)
print(f'  ゲート稼働率{gate.mean()*100:.0f}%  ゲート後の平均{gold[gate].mean():+.3f}$/回(n={int(gate.sum())})  ゲート無し{gold.mean():+.3f}')
for xg,lab in [(0,'オフ'),(100000,'10万円/0.01'),(50000,'今 5万円/0.01'),(25000,'2.5万円/0.01'),(12500,'1.25万円/0.01'),(6250,'6,250円/0.01')]:
    print(f'  {lab:14s}: ゲート込み {evalp(Xof(add,gser=gg),xg=xg)}   | ゲート無し {evalp(Xof(add),xg=xg)}')
# 年別: ゲート込み金の$/oz合計
y=gg.groupby(gg.index.year).sum(); print('  ゲート込み年別$/oz:',' '.join(f'{k}:{v:+.0f}' for k,v in y.items()))

print('\n== D 週末レッグ(金曜JST建て→月曜決済) ==')
for sym,hi,ho,lab in [('JP225Cash',15,9,'JP 金15→月9'),('US500Cash',15,9,'US 金15→月9'),('GER40Cash',16,16,'GER 金16→月16'),('GER40Cash',1,16,'GER 金01→月16(現行の金曜玉を月曜まで延長)')]:
    L=legs(sym,hi,ho,[4],80); L=L[L.hrs>48]
    print(f'  {lab:36s} 全: {stats(L.net)} | 直前≤0: {stats(L.net[L.prev<=0])}')
