"""US100(ナスダック)夜レッグ(15→9JST・月〜木・直前≤0)を現行に足す/US500と入れ替える。USDJPY換算。"""
import pandas as pd, numpy as np, pickle
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
uj=jst(H['USDJPY']).close.resample('1D').last().ffill(); ej=jst(H['EURUSD']).close.resample('1D').last().ffill()*uj
legs=pickle.load(open('_xm_idx2_legs.pkl','rb')); NQ=legs[('US100Cash','15→9')]; NQ=NQ[NQ.prev<=0]
gold=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']; N=pd.read_pickle('_xm_night_legs.pkl')
def filt(X): X=X[X.pct.shift(1)<=0]; return X[X.index.dayofweek<4]
jp=filt(N['JP225Cash']); us=filt(N['US500Cash']); de=pd.read_pickle('_ger40_night_legs.pkl')
X=pd.DataFrame({'g':gold,'j':jp.a*jp.net/100,'u':us.a*us.net/100*0.1*uj.reindex(us.index).ffill(),'d':de.a*de.net/100*0.1*ej.reindex(de.index).ffill(),'q':NQ.a*NQ.net/100*0.1*uj.reindex(NQ.index).ffill()}).fillna(0)
X=X[X.index>='2016-05-26']
nz=X[(X.u!=0)&(X.q!=0)]; print('US100 0.1lot 1晩sd=%.0f円(名目直近%.0f万) US500 0.1lot sd=%.0f円 相関 q-u %.2f q-j %.2f q-d %.2f'%(X.q[X.q!=0].std(),23000*0.1*148/1e4,X.u[X.u!=0].std(),nz[['q','u']].corr().iloc[0,1],X[(X.q!=0)&(X.j!=0)][['q','j']].corr().iloc[0,1],X[(X.q!=0)&(X.d!=0)][['q','d']].corr().iloc[0,1]))
JPY=148.0
def run(P,xu,xq,xd=50000,xj=7000,B0=58966,stop=25000):
    B=B0;peak=B;dd=0;st=False; G=P.g.values;J=P.j.values;U=P.u.values;Dd=P.d.values;Q=P.q.values
    for i in range(len(G)):
        if B<stop: st=True
        if not st:
            pnl=np.floor(B/50000)/100*100*G[i]*JPY+np.floor(B/xj*10)/10*J[i]+(np.floor(B/xu)*U[i] if xu else 0)+np.floor(B/xd)*Dd[i]+(np.floor(B/xq)*Q[i] if xq else 0)
            B=max(0,B+pnl)
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,dd,st
rng=np.random.default_rng(5); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; SS=[rng.choice(yrs,5) for _ in range(300)]
print('== 5年BS300本: US500 X / US100 X (円/0.1lot・0=無し) ==')
for xu,xq in [(40000,0),(0,150000),(0,100000),(0,80000),(0,60000),(40000,150000),(40000,100000),(40000,80000),(80000,80000),(0,50000)]:
    e=[];st=0;dds=[]
    for s in SS:
        P=pd.concat([bl[y] for y in s]); B,dd,x=run(P,xu,xq); e.append(B); st+=x; dds.append(dd)
    e=np.array(e); print(f'  US500 {xu:>6} US100 {xq:>7}: 中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% DD中央{np.median(dds)*100:.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f}')
print('1年ごと(現行 vs US500→US100 80000 vs 両方):')
for yv in yrs:
    P=bl[yv]; a,_,_=run(P,40000,0); b,_,_=run(P,0,80000); c,_,_=run(P,40000,80000); print(f'  {yv}: 現行{a/1e4:5.1f} / 入替{b/1e4:5.1f} / 両方{c/1e4:5.1f}')
NQ.to_pickle('_us100_night_legs.pkl')
