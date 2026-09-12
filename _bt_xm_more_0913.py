"""追加未検証: (A)日経 金曜15:00→月曜9:00の週末持ち越し (B)US500夜を今の日経7000円/lotに足した時のE[log] (C)金 経済指標発表後のドリフト(データ駆動でイベント分検出)"""
import pandas as pd, numpy as np
JPY=148.0
# ---------- (A) 週末持ち越し ----------
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']
    out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
jp=jst(H['JP225Cash'])
o=jp.open; o9=o[o.index.hour==9]; o15=o[o.index.hour==15]
fri=o15[o15.index.dayofweek==4]
res=[]
for t,a in fri.items():
    nxt=o9[(o9.index>t)&(o9.index<=t+pd.Timedelta(days=3,hours=1))]
    if len(nxt) and nxt.index[0].dayofweek==0: res.append((t.normalize(),a,nxt.iloc[0]))
w=pd.DataFrame(res,columns=['d','a','b']).set_index('d'); w['pct']=(w.b/w.a-1)*100
w['net']=w.pct-0.012-4.53/365*3   # スプレッド+3日分スワップ
y=w.net.groupby(w.index.year).sum()
print(f'(A) 日経 金→月 持ち越し n={len(w)} net/回{w.net.mean():+.3f}% t={w.net.mean()/w.net.std()*np.sqrt(len(w)):+.2f} 勝ち年{int((y>0).sum())}/{len(y)} 年率{w.net.sum()/10.3:+.1f}% 最悪{w.net.min():+.1f}%')
prev=o9.reindex(w.index+pd.Timedelta(hours=9)).values/o15.shift(1).reindex(w.index+pd.Timedelta(hours=15)).values  # 前夜(木15→金9)
pn=(o9[o9.index.dayofweek==4]); pn.index=pn.index.normalize()
th=o15[o15.index.dayofweek==3]; th.index=th.index.normalize()+pd.Timedelta(days=1)
w['prev']=(pn.reindex(w.index)/th.reindex(w.index)-1)*100
for lab,m in [('前夜≤0',w.prev<=0),('前夜>0',w.prev>0)]:
    x=w.net[m]; yy=x.groupby(x.index.year).sum(); print(f'    {lab}: n={len(x)} net/回{x.mean():+.3f}% t={x.mean()/x.std()*np.sqrt(len(x)):+.2f} 勝ち年{int((yy>0).sum())}/{len(yy)}')
# ---------- (B) US500を足す ----------
g=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']
N=pd.read_pickle('_xm_night_legs.pkl')
jpn=N['JP225Cash']; jpn=jpn[jpn.pct.shift(1)<=0]; jpn=jpn[jpn.index.dayofweek<4]
usn=N['US500Cash']; usn=usn[usn.pct.shift(1)<=0]; usn=usn[usn.index.dayofweek<4]
X=pd.DataFrame({'g':g,'j':jpn.a*jpn.net/100,'u':usn.a*usn.net/100*JPY*0.1/1}).fillna(0)   # u=0.1lot(名目=index*0.1*148円?) → US500 1lot=名目index$ → 0.1lot=index*0.1*148円
X=X[X.index>='2016-05-26']
def run(G,J,U,xg,xj,xu,B0=58966,stop=25000):
    B=B0;peak=B;dd=0;st=False
    for i in range(len(G)):
        if B<stop: st=True
        if not st:
            lot=np.floor(B/xg)/100; jl=np.floor(B/xj*10)/10; ul=np.floor(B/xu*10)/10 if xu>0 else 0
            B=max(0,B+lot*100*G[i]*JPY+jl*J[i]+ul*U[i])
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,dd,st
rng=np.random.default_rng(3); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; S=[rng.choice(yrs,5) for _ in range(300)]
print('\n(B) 日経7000円/lot固定に US500夜(前夜≤0) を足す: X_us円/0.1lot → 中央値(万)/停止率/E[log]')
for xj,xu in [(7000,0),(7000,60000),(7000,40000),(7000,25000),(7000,15000),(10000,15000),(10000,10000),(12000,8000)]:
    e=[];st=0
    for s in S:
        P=pd.concat([bl[y] for y in s]); B,dd,x=run(P.g.values,P.j.values,P.u.values,50000,xj,xu); e.append(B); st+=x
    e=np.array(e); print(f'  日経{xj:>6} US{xu:>6}: 中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f}')
print('  US500の1晩(0.1lot)のsd=%.0f円 / 日経1lotのsd=%.0f円 / 相関=%.2f'%(X.u[X.u!=0].std(),X.j[X.j!=0].std(),X[(X.u!=0)&(X.j!=0)][['j','u']].corr().iloc[0,1]))
# ---------- (C) 金 指標発表後ドリフト ----------
m1=pd.read_pickle('_fx_xauusd_m1.pkl'); m1=m1[m1.index.dayofweek<5]
rngm=(m1.high-m1.low); ret=m1.close-m1.open
# 各(時:分)ごとにレンジのz(同時刻の過去100日中央値比)。z>6 のUS時間(12:30/13:30/14:00/18:00/19:00 UTC)の分を「発表分」とする
key=m1.index.strftime('%H:%M'); med=rngm.groupby(key).transform(lambda s:s.rolling(100,min_periods=30).median())
z=rngm/med
ev=m1[(z>6)&(key.isin(['12:30','13:30','14:00','18:00','19:00','12:31','13:31','14:01']))]
print(f'\n(C) 金 イベント分(同時刻中央値の6倍超のレンジ・US指標/FOMC時刻) n={len(ev)}')
c=m1.close
for h in [5,15,30,60,120]:
    rows=[]
    for t,r in ev.iterrows():
        s=np.sign(r.close-r.open); e0=c.get(t+pd.Timedelta(minutes=1)); e1=c.get(t+pd.Timedelta(minutes=1+h))
        if e0 is None or e1 is None or s==0: continue
        rows.append(dict(t=t,follow=s*(e1-e0),first=abs(r.close-r.open)))
    x=pd.DataFrame(rows).set_index('t'); f=x.follow-0.26; y=f.groupby(f.index.year).sum()
    print(f'  初動方向に順張り 保有{h:3d}分: n={len(f)} net/回{f.mean():+.2f}$ t={f.mean()/f.std()*np.sqrt(len(f)):+.2f} PF={f[f>0].sum()/max(1e-9,-f[f<0].sum()):.2f} 勝ち年{int((y>0).sum())}/{len(y)} 前半{f[f.index.year<=2020].mean():+.2f} 後半{f[f.index.year>2020].mean():+.2f}  (逆張りなら符号反転)')
