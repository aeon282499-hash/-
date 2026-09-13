"""日経の夜: 01:00JST時点で含み≤thrなら同量追加 → 閾値高原とポートフォリオE[log]"""
import pandas as pd, numpy as np
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']; S=D['spec']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
J=jst(H['JP225Cash']); sp=S['JP225Cash']; spread_pct=sp['spread']/sp['bid']*100
o=J.open; a=o[o.index.hour==15]; b=o[o.index.hour==9]; res=[]
for t,pa in a.items():
    nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=30))]
    if len(nb)==0: continue
    te=nb.index[0]; mids={}
    for mh in [21,23,1,3]:
        m=o[(o.index>t)&(o.index<te)&(o.index.hour==mh)]; mids[mh]=m.iloc[0] if len(m) else np.nan
    res.append((t.normalize(),pa,nb.iloc[0],(te-t).total_seconds()/3600,t.dayofweek,mids[21],mids[23],mids[1],mids[3]))
L=pd.DataFrame(res,columns=['d','a','b','hrs','dow','m21','m23','m1','m3']).set_index('d'); L['pct']=(L.b/L.a-1)*100; L['prev']=L.pct.shift(1)
F=L[(L.dow<=3)&(L.prev<=0)].copy(); F['cost']=spread_pct+sp['swap_long']/365*(F.hrs/24)
def stats(n):
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[n.index.year<=2020].mean():+.3f} 後{n[n.index.year>2020].mean():+.3f}'
print('== 残り区間(追加玉の成績・コスト込み) 時刻×閾値 ==')
for mh in [21,23,1,3]:
    mid=(F[f'm{mh}']/F.a-1)*100; rest=(F.b/F[f'm{mh}']-1)*100-spread_pct
    for thr in [-0.25,-0.5,-0.75,-1.0,-1.5]:
        m=mid<=thr
        if m.sum()<60: continue
        print(f'  {mh:02d}時 ≤{thr:+.2f}%: {stats(rest[m])}')
    print(f'  {mh:02d}時 >0     : {stats(rest[mid>0])}')
# ポートフォリオ: 01時≤-0.5%で同量追加
gold=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']; N=pd.read_pickle('_xm_night_legs.pkl'); JPY=148.0
U=jst(H['US500Cash']); G=jst(H['GER40Cash']); uj=jst(H['USDJPY']).close.resample('1D').last().ffill(); ej=jst(H['EURUSD']).close.resample('1D').last().ffill()*uj
def filt(X): X=X[X.pct.shift(1)<=0]; return X[X.index.dayofweek<4]
us=filt(N['US500Cash']); de=pd.read_pickle('_ger40_night_legs.pkl')
base=F.a*(F.pct-F.cost)/100
def addon(mh,thr,mult=1.0):
    mid=(F[f'm{mh}']/F.a-1)*100; rest=(F.b/F[f'm{mh}']-1)*100-spread_pct
    return base+(F[f'm{mh}']*rest/100*mult).where(mid<=thr,0)
def run(P,B0=58966,stop=25000):
    B=B0;st=False; Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st: B=max(0,B+np.floor(B/50000)/100*100*Gg[i]*JPY+np.floor(B/7000*10)/10*Jj[i]+np.floor(B/25000)*Uu[i]+np.floor(B/40000)*Dd[i])
    return B,st
def evalp(j,seed=5):
    X=pd.DataFrame({'g':gold,'j':j,'u':us.a*us.net/100*0.1*uj.reindex(us.index).ffill(),'d':de.a*de.net/100*0.1*ej.reindex(de.index).ffill()}).fillna(0); X=X[X.index>='2016-05-26']
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[];st=0
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x=run(P); e.append(B); st+=x
    e=np.array(e); return f'中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f}'
print('\n== ポートフォリオ ==')
print('  現行                 :',evalp(base))
for mh,thr in [(1,-0.5),(1,-0.75),(1,-1.0),(21,-0.5),(23,-0.5)]:
    print(f'  {mh:02d}時≤{thr}%で同量追加 :',evalp(addon(mh,thr)))
print('  01時≤-0.5%で半量追加  :',evalp(addon(1,-0.5,0.5)))
