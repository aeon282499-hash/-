"""GER40Cash 01:00→16:00JST(=欧州ローカルの引け後→翌寄り)・前夜≤0 の深掘り: 年別n・隣接窓・噪音床・EURJPY換算で現行ポートフォリオに足したE[log]。"""
import pandas as pd, numpy as np
JPY=148.0
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']; S=D['spec']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']
    out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
ger=jst(H['GER40Cash']); sp=S['GER40Cash']; spread_pct=sp['spread']/sp['bid']*100
ej=jst(H['EURUSD']).close.resample('1D').last().ffill()*jst(H['USDJPY']).close.resample('1D').last().ffill()   # EURJPY日次
def leg(df,h_in,h_out,dow_max=3):
    o=df.open; a=o[o.index.hour==h_in]; b=o[o.index.hour==h_out]; res=[]
    for t,pa in a.items():
        if t.dayofweek>dow_max: continue
        nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=26))]
        if len(nb)==0: continue
        res.append((t.normalize(),pa,nb.iloc[0],(nb.index[0]-t).total_seconds()/3600))
    L=pd.DataFrame(res,columns=['d','a','b','hrs']).set_index('d'); L['pct']=(L.b/L.a-1)*100
    L['net']=L.pct-spread_pct-sp['swap_long']/365*(L.hrs/24); return L
def stats(n):
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} net/回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝ち年{int((y>0).sum())}/{len(y)} 前半{n[n.index.year<=2020].mean():+.3f} 後半{n[n.index.year>2020].mean():+.3f} 最悪{n.min():+.1f}% sd{n.std():.2f}'
L=leg(ger,1,16); F=L[L.pct.shift(1)<=0]
print('01→16 全:',stats(L.net)); print('01→16 前夜≤0:',stats(F.net)); print('01→16 前夜>0:',stats(L[L.pct.shift(1)>0].net))
print('年別(前夜≤0): n / net合計%'); print(pd.DataFrame({'n':F.net.groupby(F.index.year).size(),'net%':F.net.groupby(F.index.year).sum().round(1),'全夜n':L.net.groupby(L.index.year).size()}).T.to_string())
print('曜日別(前夜≤0):'); print(F.net.groupby(F.index.dayofweek).agg(['count','mean']).round(3).T.to_string())
print('隣接窓(前夜≤0):')
for hi,ho in [(0,16),(1,15),(1,17),(2,16),(23,16),(1,18),(3,16)]:
    L2=leg(ger,hi,ho); F2=L2[L2.pct.shift(1)<=0]; print(f'  {hi:02d}→{ho:02d}: {stats(F2.net)}')
print('金曜も含む(前夜≤0・週末跨ぎ):', stats(leg(ger,1,16,4).pipe(lambda x:x[x.pct.shift(1)<=0]).net))
# 噪音床: 20営業日ブロックで符号反転(フィルタ後の系列)を1000回 → tの分布
rng=np.random.default_rng(11); x=F.net.values; nb=len(x)//20+1; ts=[]
for _ in range(1000):
    sgn=np.repeat(rng.choice([-1,1],nb),20)[:len(x)]; y=x*sgn; ts.append(y.mean()/y.std()*np.sqrt(len(y)))
ts=np.array(ts); print(f'噪音床: 実t={x.mean()/x.std()*np.sqrt(len(x)):.2f} vs 反転1000回の|t|95%点{np.percentile(np.abs(ts),95):.2f} 99%点{np.percentile(np.abs(ts),99):.2f}')
# 閾値高原
for q in [-1,-0.5,-0.25,0,0.25,0.5,1.0]:
    F3=L[L.pct.shift(1)<=q]; print(f'  前夜≤{q:+.2f}: {stats(F3.net)}')
# ポートフォリオ
g=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']; N=pd.read_pickle('_xm_night_legs.pkl')
def filt(X): X=X[X.pct.shift(1)<=0]; return X[X.index.dayofweek<4]
jp=filt(N['JP225Cash']); us=filt(N['US500Cash'])
gj=(F.a*F.net/100*0.1)*ej.reindex(F.index).ffill()     # 0.1lot=名目 index×0.1 EUR → 円
X=pd.DataFrame({'g':g,'j':jp.a*jp.net/100,'u':us.a*us.net/100*JPY*0.1,'d':gj}).fillna(0); X=X[X.index>='2016-05-26']
print('GER40 0.1lotの1晩sd=%.0f円(名目%.0f万円) / 日経1lot sd=%.0f円 / 相関 j-d %.2f, u-d %.2f'%(X.d[X.d!=0].std(),(F.a*0.1*ej.reindex(F.index)).mean()/1e4,X.j[X.j!=0].std(),X[(X.j!=0)&(X.d!=0)][['j','d']].corr().iloc[0,1],X[(X.u!=0)&(X.d!=0)][['u','d']].corr().iloc[0,1]))
def run(P,xd,B0=58966,stop=25000):
    B=B0;peak=B;dd=0;st=False; G=P.g.values;J=P.j.values;U=P.u.values;Dd=P.d.values
    for i in range(len(G)):
        if B<stop: st=True
        if not st:
            pnl=np.floor(B/50000)/100*100*G[i]*JPY+np.floor(B/7000*10)/10*J[i]+np.floor(B/40000)*U[i]+(np.floor(B/xd)*Dd[i] if xd>0 else 0)
            B=max(0,B+pnl)
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,dd,st
rng=np.random.default_rng(5); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; SS=[rng.choice(yrs,5) for _ in range(300)]
print('\n== 現行(金/日経7000/US500 40000)に GER40 01→16(前夜≤0) を足す: X円/0.1lot ==')
for xd in [0,200000,120000,80000,60000,40000]:
    e=[];st=0
    for s in SS:
        P=pd.concat([bl[y] for y in s]); B,dd,x=run(P,xd); e.append(B); st+=x
    e=np.array(e); print(f'  GER40 {xd:>7}円/0.1lot: 中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f}')
F.to_pickle('_ger40_night_legs.pkl')
