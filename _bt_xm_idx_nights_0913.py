"""他の指数(US30/GER40/UK100)の夜レッグ＝日経/US500型(15→9JST・前夜≤0)と、各市場ローカルの引け→翌寄り窓。実スペックのコスト。採用候補は現行(日経7000+US500 40000)に足してE[log]で判定。"""
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
def leg(df,h_in,h_out,spread_pct,swap_yr):
    o=df.open; a=o[o.index.hour==h_in]; b=o[o.index.hour==h_out]
    res=[]
    for t,pa in a.items():
        if t.dayofweek>3: continue
        nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=26))]
        if len(nb)==0: continue
        res.append((t.normalize(),pa,nb.iloc[0],(nb.index[0]-t).total_seconds()/3600))
    L=pd.DataFrame(res,columns=['d','a','b','hrs']).set_index('d'); L['pct']=(L.b/L.a-1)*100
    L['net']=L.pct-spread_pct-swap_yr/365*(L.hrs/24)*1.0
    return L
def stats(n):
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} net/回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝ち年{int((y>0).sum())}/{len(y)} 前半{n[n.index.year<=2020].mean():+.3f} 後半{n[n.index.year>2020].mean():+.3f} 年率{n.sum()/10.3:+.1f}% 最悪{n.min():+.1f}%'
legs={}
for sym,windows in [('US500Cash',[(15,9),(6,23),(5,22)]),('US30Cash',[(15,9),(6,23),(5,22)]),('GER40Cash',[(15,9),(1,16),(2,17),(15,1)]),('UK100Cash',[(15,9),(1,16),(2,17)]),('JP225Cash',[(15,9)])]:
    sp=S[sym]; spread_pct=sp['spread']/sp['bid']*100; df=jst(H[sym])
    print(f'\n== {sym} spread{spread_pct:.3f}% swap_long{sp["swap_long"]}%/年 ==')
    for hi,ho in windows:
        L=leg(df,hi,ho,spread_pct,sp['swap_long'])
        if len(L)<500: print(f'  {hi:02d}→{ho:02d}JST: n={len(L)} 不足'); continue
        print(f'  {hi:02d}→{ho:02d}JST 全夜   {stats(L.net)}')
        f=L[L.pct.shift(1)<=0]; print(f'  {hi:02d}→{ho:02d}JST 前夜≤0 {stats(f.net)}')
        legs[(sym,hi,ho)]=L
# ポートフォリオ: 現行(日経7000/lot・US500 40000/0.1lot)に候補を足す
g=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']
N=pd.read_pickle('_xm_night_legs.pkl')
def filt(L): L=L[L.pct.shift(1)<=0]; return L[L.index.dayofweek<4]
jp=N['JP225Cash']; jp=filt(jp); us=N['US500Cash']; us=filt(us)
base=pd.DataFrame({'g':g,'j':jp.a*jp.net/100,'u':us.a*us.net/100*JPY*0.1})
cands={'US30 15→9':(legs[('US30Cash',15,9)],0.1*JPY),'GER40 15→9':(legs[('GER40Cash',15,9)],0.1*JPY),'UK100 15→9':(legs[('UK100Cash',15,9)],0.1*JPY)}
for k,(L,mult) in cands.items(): 
    f=filt(L); base[k]=f.a*f.net/100*mult   # 0.1lot・USD/EUR/GBP建て→概算JPY(USD換算で代用)
X=base.fillna(0); X=X[X.index>='2016-05-26']
def run(P,xg,xj,xu,extra,B0=58966,stop=25000):
    B=B0;peak=B;dd=0;st=False; G=P.g.values;J=P.j.values;U=P.u.values; E=[(P[k].values,x) for k,x in extra.items()]
    for i in range(len(G)):
        if B<stop: st=True
        if not st:
            pnl=np.floor(B/xg)/100*100*G[i]*JPY+np.floor(B/xj*10)/10*J[i]+np.floor(B/xu)*U[i]
            for v,x in E: pnl+=np.floor(B/x)*v[i]
            B=max(0,B+pnl)
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,dd,st
rng=np.random.default_rng(5); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; SS=[rng.choice(yrs,5) for _ in range(300)]
def evalp(extra):
    e=[];st=0
    for s in SS:
        P=pd.concat([bl[y] for y in s]); B,dd,x=run(P,50000,7000,40000,extra); e.append(B); st+=x
    e=np.array(e); return np.median(e)/1e4,np.percentile(e,10)/1e4,st/3,np.mean(np.log(np.maximum(e,1)/58966))
print('\n== 現行(日経7000+US500 40000)に足す: 中央値(万)/下位10%/停止率/E[log] ==')
print('  現行のみ: 中央%.1f 下位%.1f 停止%.0f%% E[log]%+.2f'%evalp({}))
for k in cands:
    for x in [80000,40000,25000]:
        print(f'  +{k} {x}円/0.1lot: 中央%.1f 下位%.1f 停止%.0f%% E[log]%+.2f'%evalp({k:x}))
print('  相関:'); print(X[(X.j!=0)][['j','u']+list(cands)].replace(0,np.nan).corr().round(2))
