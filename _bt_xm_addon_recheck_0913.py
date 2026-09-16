# ⚠️2026-09-17判明: S[sym]["swap_long"]は負値(コスト)で格納されているのに legs() は「- swap_long」で引く＝スワップを収入として計上するバグ。正しい数字は _bt_xm_grid_0917.py(正値に置換+9/14-16実測)を見ること。
"""月曜(JST)の夜はフィルタ無し案: JP/US(月曜=週末レッグ後)・GER40(火曜JST=欧州月曜夜)。EA定義(直前レッグ・週末レッグ含む)でレッグを作り直し、ポートフォリオE[log]で比較。"""
import pandas as pd, numpy as np
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']; S=D['spec']; JPY=148.0
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timedelta(0)+pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
uj=jst(H['USDJPY']).close.resample('1D').last().ffill(); ej=jst(H['EURUSD']).close.resample('1D').last().ffill()*uj
def legs(sym,h_in,h_out,dows,maxh):
    d=jst(H[sym]); sp=S[sym]; spread_pct=sp['spread']/sp['bid']*100; o=d.open; a=o[o.index.hour==h_in]; b=o[o.index.hour==h_out]; res=[]
    for t,pa in a.items():
        nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=maxh))]
        if len(nb)==0: continue
        res.append((t.normalize(),pa,nb.iloc[0],(nb.index[0]-t).total_seconds()/3600,t.dayofweek))
    L=pd.DataFrame(res,columns=['d','a','b','hrs','dow']).set_index('d'); L['pct']=(L.b/L.a-1)*100
    L['prev']=L.pct.shift(1)          # EA定義: 直前レッグ(週末レッグ含む)
    L['net']=L.pct-spread_pct-sp['swap_long']/365*(L.hrs/24); return L[L.dow.isin(dows)]
LJ=legs('JP225Cash',15,9,[0,1,2,3],80); LU=legs('US500Cash',15,9,[0,1,2,3],80); LG=legs('GER40Cash',1,16,[1,2,3,4],26)
def stats(n):
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[n.index.year<=2020].mean():+.3f} 後{n[n.index.year>2020].mean():+.3f} 合計{n.sum():+.0f}%'
gold=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']
def sel(L,monday_free,mon_dow):
    m=(L.prev<=0)|(monday_free&(L.dow==mon_dow)); return L[m]
def build(jm,um,gm):
    j=sel(LJ,jm,0); u=sel(LU,um,0); g=sel(LG,gm,1)
    X=pd.DataFrame({'g':gold,'j':j.a*j.net/100,'u':u.a*u.net/100*0.1*uj.reindex(u.index).ffill(),'d':g.a*g.net/100*0.1*ej.reindex(g.index).ffill()}).fillna(0); return X[X.index>='2016-05-26']
def run(P,B0=58966,stop=25000):
    B=B0;st=False; Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st: B=max(0,B+np.floor(B/50000)/100*100*Gg[i]*JPY+np.floor(B/7000*10)/10*Jj[i]+np.floor(B/25000)*Uu[i]+np.floor(B/40000)*Dd[i])
    return B,st
def evalp(X,seed=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[];st=0
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x=run(P); e.append(B); st+=x
    e=np.array(e); return f'中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f}'
print('\n== ポートフォリオ(EA定義・追加ルール無し) ==')
for jm,um,gm,lab in [(0,0,0,'現行(全日フィルタ)'),(1,0,0,'日経の月曜だけ無条件'),(0,1,0,'US500の月曜だけ無条件'),(1,1,0,'日経+US500の月曜無条件'),(1,1,1,'+GER40の火曜JSTも無条件'),(0,0,1,'GER40の火曜JSTだけ無条件')]:
    print(f'  {lab:22s}:',evalp(build(bool(jm),bool(um),bool(gm))))
Xa=build(False,False,False); Xb=build(True,True,False)
print('年別(現行 vs 日経+US500月曜無条件・8.4枚/0.2/0.1換算の合計円):')
for yv in sorted(set(Xa.index.year)):
    ca=(Xa.j*8.4+Xa.u*2+Xa.d*1)[Xa.index.year==yv].sum()/1e4; cb=(Xb.j*8.4+Xb.u*2+Xb.d*1)[Xb.index.year==yv].sum()/1e4; print(f'  {yv}: {ca:+.1f}万 → {cb:+.1f}万')

d=jst(H['JP225Cash']); o=d.open; m1=[]
for t in LJ.index:
    x=o[(o.index>t+pd.Timedelta(hours=15))&(o.index<t+pd.Timedelta(hours=33))&(o.index.hour==1)]; m1.append(x.iloc[0] if len(x) else np.nan)
LJ['m1']=m1
sp=S['JP225Cash']; spread_pct=sp['spread']/sp['bid']*100
j=LJ[(LJ.prev<=0)|(LJ.dow==0)]; u=LU[(LU.prev<=0)|(LU.dow==0)]; g=LG[LG.prev<=0]
base=j.a*j.net/100; mid=(j.m1/j.a-1)*100; rest=(j.b/j.m1-1)*100-spread_pct
def X_of(jser):
    X=pd.DataFrame({'g':gold,'j':jser,'u':u.a*u.net/100*0.1*uj.reindex(u.index).ffill(),'d':g.a*g.net/100*0.1*ej.reindex(g.index).ffill()}).fillna(0); return X[X.index>='2016-05-26']
print('追加玉(01時≤-0.5%)の残り区間:',stats(rest[mid<=-0.5]),' 月曜のみ:',stats(rest[(mid<=-0.5)&(j.dow==0)]))
print('現行(追加なし)     :',evalp(X_of(base)))
for thr in [-0.25,-0.5,-0.75]:
    add=base+(j.m1*rest/100).where(mid<=thr,0); print(f'01時≤{thr}%同量追加 :',evalp(X_of(add)))
