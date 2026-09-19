"""9/19 白紙スキャンを他銘柄へ: XM実データ(H1/M15/M5 ~4年)の JP225/US500/GER40/SILVER/XAUEUR/XPT を H1/H4/D1 に集計し、
MA(SMMA含む)/ドンチャン/RSI/BB/ROC/ボラフェード/連続足 の同族をコスト込みで判定。判定は金と同じ(t>3.5×前後半×勝ち年×コスト2倍×上位3日)。"""
import pandas as pd, numpy as np, time
t0=time.time()
COSTP={'JP225Cash':0.03,'US500Cash':0.02,'GER40Cash':0.02,'SILVER.':0.06,'XAUEUR.':0.03,'XPTUSD.':0.20}  # 往復%
def load():
    out={}
    for f in ['_xm_h1_indices.pkl','_xm_m5_recent.pkl','_xm_metals_m15_0919.pkl']:
        for k,v in pd.read_pickle(f).items():
            if k not in COSTP: continue
            v=v.copy(); v.index=pd.to_datetime(v['time'],unit='s')
            if k in out and (out[k].index[-1]-out[k].index[0])>=(v.index[-1]-v.index[0]): continue
            out[k]=v.rename(columns={'open':'o','high':'h','low':'l','close':'c','tick_volume':'v'})[['o','h','l','c','v']]
    return out
SYM=load()
def mk(m,rule):
    r=m.resample(rule)
    b=pd.DataFrame({'o':r.o.first(),'h':r.h.max(),'l':r.l.min(),'c':r.c.last(),'v':r.v.sum()}).dropna(); return b[b.v>0]
def sma(x,n): return x.rolling(n).mean()
def ema(x,n): return x.ewm(span=n,adjust=False).mean()
def smma(x,n): return x.ewm(alpha=1/n,adjust=False).mean()
def rsi(c,n):
    d=c.diff(); up=d.clip(lower=0); dn=-d.clip(upper=0)
    ru=up.ewm(alpha=1/n,adjust=False).mean(); rd=dn.ewm(alpha=1/n,adjust=False).mean(); return 100-100/(1+ru/rd.replace(0,np.nan))
def evaluate(b,pos,label,costp):
    pos=pos.fillna(0).astype(float); o=b.o; ret=(o.shift(-2)/o.shift(-1)-1)*100; ret=ret.fillna(0)
    pnl=pos*ret; chg=pos.diff().abs().fillna(pos.abs()); cp=chg*costp/2; net=pnl-cp
    mu=ret[ret!=0].mean(); alpha=(net-pos*mu).groupby(b.index.date).sum(); alpha=alpha[alpha!=0]
    day=net.groupby(b.index.date).sum(); day=day[day!=0]
    if len(day)<100: return None
    n=len(day); mid=n//2
    def tt(x): return x.mean()/x.std()*np.sqrt(len(x)) if x.std()>0 else 0
    yr=day.groupby(pd.to_datetime(day.index).year).sum(); g=day[day>0].sum(); l=-day[day<0].sum()
    net2=(pnl-2*cp).groupby(b.index.date).sum(); net2=net2[net2!=0]; top3=day.sort_values(ascending=False).iloc[3:]
    return dict(label=label,n_days=n,trades=int((chg>0).sum()),t=tt(day),t1=tt(day.iloc[:mid]),t2=tt(day.iloc[mid:]),pf=g/l if l>0 else 99,
                wy=int((yr>0).sum()),ny=len(yr),ann=yr.mean(),t_c2=tt(net2),t_top3=tt(top3),t_alpha=tt(alpha),expo=pos.abs().mean())
R=[]
def run(b,pos,label,costp):
    r=evaluate(b,pos,label,costp)
    if r: R.append(r)
def hold(sig,n): return sig.astype(float).rolling(n,min_periods=1).max()
for sym,m in SYM.items():
    cp=COSTP[sym]
    TF={'H1':mk(m,'1h'),'H4':mk(m,'4h'),'D1':mk(m,'24h')}
    print(sym,{k:(len(v),str(v.index[0])[:10]) for k,v in TF.items()})
    for tf,b in TF.items():
        c=b.c; h=b.h; l=b.l
        for mname,f in [('SMA',sma),('EMA',ema),('SMMA',smma)]:
            for fa,sl in [(5,20),(10,50),(20,100)]:
                sig=np.sign(f(c,fa)-f(c,sl))
                for side,s in [('両',sig),('買',sig.clip(lower=0)),('売',sig.clip(upper=0))]: run(b,s,f'{sym}|MAクロス{fa}/{sl}|{tf}|{mname}|{side}',cp)
            for n in (20,50,200):
                sig=np.sign(c-f(c,n))
                for side,s in [('両',sig),('買',sig.clip(lower=0)),('売',sig.clip(upper=0))]: run(b,s,f'{sym}|価格vsMA{n}|{tf}|{mname}|{side}',cp)
        for n in (10,20,55):
            hh=h.rolling(n).max().shift(1); ll=l.rolling(n).min().shift(1)
            sig=pd.Series(np.where(c>hh,1,np.where(c<ll,-1,np.nan)),index=b.index).ffill()
            for side,s in [('両',sig),('買',sig.clip(lower=0)),('売',sig.clip(upper=0))]: run(b,s,f'{sym}|ドンチャン{n}|{tf}|-|{side}',cp)
        for n in (2,14):
            r=rsi(c,n)
            for lo,hi in [(20,80),(30,70)]:
                for hd in (2,5,20):
                    run(b,hold(r<lo,hd),f'{sym}|RSI{n}<{lo}逆張り|{tf}|hold{hd}|買',cp); run(b,-hold(r>hi,hd),f'{sym}|RSI{n}>{hi}逆張り|{tf}|hold{hd}|売',cp)
                    run(b,hold(r>hi,hd),f'{sym}|RSI{n}>{hi}順張り|{tf}|hold{hd}|買',cp); run(b,-hold(r<lo,hd),f'{sym}|RSI{n}<{lo}順張り|{tf}|hold{hd}|売',cp)
        m_=c.rolling(20).mean(); s_=c.rolling(20).std(); ub=m_+2*s_; lb=m_-2*s_
        for hd in (5,20):
            run(b,hold(c<lb,hd),f'{sym}|BB下抜け逆張り|{tf}|hold{hd}|買',cp); run(b,-hold(c>ub,hd),f'{sym}|BB上抜け逆張り|{tf}|hold{hd}|売',cp)
            run(b,hold(c>ub,hd),f'{sym}|BB上抜け順張り|{tf}|hold{hd}|買',cp); run(b,-hold(c<lb,hd),f'{sym}|BB下抜け順張り|{tf}|hold{hd}|売',cp)
        for n in (10,20,50):
            sig=np.sign(c/c.shift(n)-1)
            for side,s in [('両',sig),('買',sig.clip(lower=0)),('売',sig.clip(upper=0))]: run(b,s,f'{sym}|ROC{n}|{tf}|-|{side}',cp)
        tr=pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1); atr=tr.rolling(14).mean().shift(1); mv=c-c.shift(1)
        for k in (2,3):
            for hd in (3,10):
                run(b,-hold(mv>k*atr,hd),f'{sym}|急騰{k}ATRフェード|{tf}|hold{hd}|売',cp); run(b,hold(mv<-k*atr,hd),f'{sym}|急落{k}ATRフェード|{tf}|hold{hd}|買',cp)
                run(b,hold(mv>k*atr,hd),f'{sym}|急騰{k}ATR追随|{tf}|hold{hd}|買',cp); run(b,-hold(mv<-k*atr,hd),f'{sym}|急落{k}ATR追随|{tf}|hold{hd}|売',cp)
        upb=(c>c.shift(1)).astype(int); dnb=(c<c.shift(1)).astype(int)
        for n in (3,4,5):
            su=(upb.rolling(n).sum()==n); sd=(dnb.rolling(n).sum()==n)
            for hd in (1,3):
                run(b,-hold(su,hd),f'{sym}|{n}連続陽線フェード|{tf}|hold{hd}|売',cp); run(b,hold(sd,hd),f'{sym}|{n}連続陰線フェード|{tf}|hold{hd}|買',cp)
                run(b,hold(su,hd),f'{sym}|{n}連続陽線追随|{tf}|hold{hd}|買',cp); run(b,-hold(sd,hd),f'{sym}|{n}連続陰線追随|{tf}|hold{hd}|売',cp)
    b=TF['D1']; run(b,pd.Series(1.0,index=b.index),f'{sym}|常時買い(ベンチ)|D1|-|買',cp)
R=pd.DataFrame(R); R.to_csv('_bt_xm_families_0919.csv',index=False); R['sym']=R.label.str.split('|').str[0]
N=len(R); print(f'\n総セル {N}・噪音床≈{np.sqrt(2*np.log(N)):.2f}・{time.time()-t0:.0f}s')
def show(df,k=15):
    for _,r in df.head(k).iterrows(): print(f"  {r.label:44s} 日{int(r.n_days):4d} 回{int(r.trades):5d} 年率{r.ann:+6.1f}% PF{r.pf:4.2f} t{r.t:+5.2f}(前{r.t1:+4.1f}/後{r.t2:+4.1f}) 勝年{int(r.wy)}/{int(r.ny)} α-t{r.t_alpha:+4.1f} 稼働{r.expo:.2f} コスト2倍t{r.t_c2:+4.1f} 上3除t{r.t_top3:+4.1f}")
ok=R[(R.t>3.5)&(R.t1>1.5)&(R.t2>1.5)&(R.wy>=R.ny*0.8)&(R.t_c2>2)&(R.t_top3>2.5)]
print(f'\n== 合格 {len(ok)} =='); show(ok.sort_values('t',ascending=False),30)
print(f'\n== 合格かつ α-t>3 (ベンチ買い持ちを引いても残る) {len(ok[ok.t_alpha>3])} =='); show(ok[ok.t_alpha>3].sort_values('t_alpha',ascending=False),20)
print('\n== 銘柄別: 最大t / ベンチ常時買いt / PF>1割合 ==')
for sym,g in R.groupby('sym'):
    bench=g[g.label.str.contains('ベンチ')]
    bt=bench.t.iloc[0] if len(bench) else float('nan'); ba=bench.ann.iloc[0] if len(bench) else 0
    print(f"  {sym:10s} セル{len(g)} 最大t{g.t.max():+.2f} α最大t{g.t_alpha.max():+.2f} ベンチt{bt:+.2f}(年率{ba:+.1f}%) PF>1 {(g.pf>1).mean():.2f}")
print('\n== α-t 上位10 (合格不問) =='); show(R.sort_values('t_alpha',ascending=False),10)
print('\n== SMMA vs SMA/EMA (全銘柄MAセル平均t) =='); A=R[R.label.str.contains('MAクロス|価格vsMA')].copy(); A['ma']=A.label.str.split('|').str[3]
print(A.groupby('ma').agg(cells=('t','size'),t_mean=('t','mean'),t_max=('t','max'),ann_mean=('ann','mean'),plus=('ann',lambda x:(x>0).mean())).round(2))
