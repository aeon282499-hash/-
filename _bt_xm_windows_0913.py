"""採用3レッグの窓(建て時刻/決済時刻)微調整・閾値・を実装可能な直前レッグ定義で。指標=per-leg t と ポートフォリオE[log]。"""
import pandas as pd, numpy as np, itertools
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']; S=D['spec']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
J=jst(H['JP225Cash']); U=jst(H['US500Cash']); G=jst(H['GER40Cash']); uj=jst(H['USDJPY']).close.resample('1D').last().ffill(); ej=jst(H['EURUSD']).close.resample('1D').last().ffill()*uj
def leg(df,h_in,h_out,dows,sym,thr=0.0):
    sp=S[sym]; spread_pct=sp['spread']/sp['bid']*100
    o=df.open; a=o[o.index.hour==h_in]; b=o[o.index.hour==h_out]; res=[]
    for t,pa in a.items():
        nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=30))]
        if len(nb)==0: continue
        res.append((t.normalize(),pa,nb.iloc[0],(nb.index[0]-t).total_seconds()/3600,t.dayofweek))
    L=pd.DataFrame(res,columns=['d','a','b','hrs','dow']).set_index('d'); L['pct']=(L.b/L.a-1)*100
    L['prev']=L.pct.shift(1); L=L[L.dow.isin(dows)&(L.prev<=thr)]
    L['net']=L.pct-spread_pct-sp['swap_long']/365*(L.hrs/24); return L
def stats(n):
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[n.index.year<=2020].mean():+.3f} 後{n[n.index.year>2020].mean():+.3f} 合計{n.sum():+.0f}% 最悪{n.min():+.1f}'
print('== JP225 窓 (月〜木・直前≤0) ==')
for hi,ho in [(15,9),(14,9),(16,9),(15,10),(15,8),(14,10),(13,9),(15,11)]:
    print(f'  {hi:02d}→{ho:02d}: {stats(leg(J,hi,ho,[0,1,2,3],"JP225Cash").net)}')
print('== US500 窓 (月〜木・直前≤0) ==')
for hi,ho in [(15,9),(14,9),(16,9),(15,10),(15,8),(17,9),(15,7),(13,9)]:
    print(f'  {hi:02d}→{ho:02d}: {stats(leg(U,hi,ho,[0,1,2,3],"US500Cash").net)}')
print('== GER40 窓 (火〜金・直前≤0) ==')
for hi,ho in [(1,16),(1,17),(0,17),(2,17),(1,18),(0,16),(23,17),(1,15)]:
    dows=[1,2,3,4] if hi>=1 else [0,1,2,3]
    print(f'  {hi:02d}→{ho:02d}: {stats(leg(G,hi,ho,dows,"GER40Cash").net)}')
print('== 閾値 (直前≤thr) ==')
for thr in [-0.5,-0.25,0,0.25,0.5]:
    print(f'  thr{thr:+.2f}: JP {stats(leg(J,15,9,[0,1,2,3],"JP225Cash",thr).net)}'); print(f'           US {stats(leg(U,15,9,[0,1,2,3],"US500Cash",thr).net)}'); print(f'           DE {stats(leg(G,1,16,[1,2,3,4],"GER40Cash",thr).net)}')
# ポートフォリオ: 変種を差し替えてE[log]
gold=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']; JPY=148.0
def build(jw,uw,dw,thr=(0,0,0)):
    j=leg(J,*jw,[0,1,2,3],'JP225Cash',thr[0]); u=leg(U,*uw,[0,1,2,3],'US500Cash',thr[1]); d=leg(G,*dw,[1,2,3,4],'GER40Cash',thr[2])
    X=pd.DataFrame({'g':gold,'j':j.a*j.net/100,'u':u.a*u.net/100*0.1*uj.reindex(u.index).ffill(),'d':d.a*d.net/100*0.1*ej.reindex(d.index).ffill()}).fillna(0); return X[X.index>='2016-05-26']
def run(P,xj=7000,xu=25000,xd=40000,B0=58966,stop=25000):
    B=B0;st=False; Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st: B=max(0,B+np.floor(B/50000)/100*100*Gg[i]*JPY+np.floor(B/xj*10)/10*Jj[i]+np.floor(B/xu)*Uu[i]+np.floor(B/xd)*Dd[i])
    return B,st
def evalp(X,seed=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}
    e=[];st=0
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x=run(P); e.append(B); st+=x
    e=np.array(e); return f'中央{np.median(e)/1e4:6.1f} 停止{st/3:3.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f}'
print('\n== ポートフォリオ E[log](現行サイズ固定) ==')
print('  現行 JP15→9 US15→9 DE01→16      :',evalp(build((15,9),(15,9),(1,16))))
print('  DE01→17                        :',evalp(build((15,9),(15,9),(1,17))))
print('  DE02→17                        :',evalp(build((15,9),(15,9),(2,17))))
print('  JP15→10                        :',evalp(build((15,10),(15,9),(1,16))))
print('  US14→9                         :',evalp(build((15,9),(14,9),(1,16))))
print('  US15→10                        :',evalp(build((15,9),(15,10),(1,16))))
print('  閾値 -0.25 全部                 :',evalp(build((15,9),(15,9),(1,16),(-0.25,-0.25,-0.25))))
print('  閾値 +0.25 全部                 :',evalp(build((15,9),(15,9),(1,16),(0.25,0.25,0.25))))
