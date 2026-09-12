"""②夜間の損切り/利確(H1高安で判定・滑り0.05%) ③ボラ連動サイズ(直近20レッグのsdで枚数を逆比例・0.5〜2倍) を現行3レッグ+金でE[log]比較"""
import pandas as pd, numpy as np
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']; S=D['spec']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
J=jst(H['JP225Cash']); U=jst(H['US500Cash']); G=jst(H['GER40Cash']); uj=jst(H['USDJPY']).close.resample('1D').last().ffill(); ej=jst(H['EURUSD']).close.resample('1D').last().ffill()*uj
def legs(df,h_in,h_out,dows,sym):
    sp=S[sym]; spread_pct=sp['spread']/sp['bid']*100
    o=df.open; a=o[o.index.hour==h_in]; b=o[o.index.hour==h_out]; res=[]
    for t,pa in a.items():
        nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=30))]
        if len(nb)==0: continue
        te=nb.index[0]; w=df[(df.index>=t)&(df.index<te)]
        res.append((t.normalize(),pa,nb.iloc[0],(te-t).total_seconds()/3600,t.dayofweek,w.low.min(),w.high.max()))
    L=pd.DataFrame(res,columns=['d','a','b','hrs','dow','lo','hi']).set_index('d'); L['pct']=(L.b/L.a-1)*100
    L['prev']=L.pct.shift(1); L=L[L.dow.isin(dows)&(L.prev<=0)]
    L['cost']=spread_pct+sp['swap_long']/365*(L.hrs/24); L['mdd']=(L.lo/L.a-1)*100; L['mfe']=(L.hi/L.a-1)*100; return L
LJ=legs(J,15,9,[0,1,2,3],'JP225Cash'); LU=legs(U,15,9,[0,1,2,3],'US500Cash'); LG=legs(G,1,16,[1,2,3,4],'GER40Cash')
def apply(L,sl=None,tp=None):
    p=L.pct.copy()
    if sl is not None: hit=L.mdd<=-sl; p[hit]=-sl-0.05
    if tp is not None: hit2=L.mfe>=tp; p[hit2 & ~(L.mdd<=-(sl if sl else 99))]=tp-0.05   # 先にSLに当たった玉は除く(順序不明なのでSL優先=保守的)
    return p-L.cost
def stats(n):
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 合計{n.sum():+.0f}% 最悪{n.min():+.1f} sd{n.std():.2f}'
print('== 損切り(夜間安値で判定) ==')
for nm,L in [('JP',LJ),('US',LU),('DE',LG)]:
    print(f' {nm} なし  : {stats(apply(L))}')
    for sl in [1.0,1.5,2.0,3.0]: print(f' {nm} SL{sl:.1f}%: {stats(apply(L,sl))}  発生{(L.mdd<=-sl).mean()*100:.0f}%')
    for tp in [1.0,1.5,2.0]: print(f' {nm} TP{tp:.1f}%: {stats(apply(L,None,tp))}  発生{(L.mfe>=tp).mean()*100:.0f}%')
gold=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']; JPY=148.0
def build(sl=None,tp=None,vol=False):
    j=apply(LJ,sl,tp); u=apply(LU,sl,tp); d=apply(LG,sl,tp)
    X=pd.DataFrame({'g':gold,'j':LJ.a*j/100,'u':LU.a*u/100*0.1*uj.reindex(LU.index).ffill(),'d':LG.a*d/100*0.1*ej.reindex(LG.index).ffill()}).fillna(0)
    if vol:
        for c,L in [('j',LJ),('u',LU),('d',LG)]:
            sd=L.pct.rolling(20).std().shift(1); m=(L.pct.std()/sd).clip(0.5,2.0).reindex(X.index).fillna(1.0)
            X[c]=X[c]*m
    return X[X.index>='2016-05-26']
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
print('\n== ポートフォリオ ==')
print('  現行           :',evalp(build()))
for sl in [1.5,2.0,3.0]: print(f'  SL{sl}%全レッグ   :',evalp(build(sl)))
print('  TP1.5%         :',evalp(build(None,1.5)))
print('  ボラ連動(0.5-2x):',evalp(build(vol=True)))
print('  ボラ連動+SL3%   :',evalp(build(3.0,None,True)))
