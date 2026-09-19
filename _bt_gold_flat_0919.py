"""9/19 本人「移動平均をSMMAにしたらゴールド勝てる？」＋「フラットから隅々まで・時間軸は任せる」
→ 10年Dukascopy XAUUSD M1 を M5/M15/H1/H4/D1 に集計し、
   (A) MA型 SMA/EMA/SMMA/WMA × クロス/価格対MA/押し目 × 時間軸 × 売買
   (B) 白紙の全族: ドンチャン/RSI逆張り/RSI順張り/BB/ROC/ボラフェード/連続足/日足パターン
 を同じ土台(Zero実測コスト$0.21/oz往復・SL無し・翌足始値約定)で判定。判定は日次PnL(%)のt値・前後半・勝ち年・コスト2倍・上位3日除去。
 噪音床 = セル数Nから期待最大t≈sqrt(2 ln N) ＋ ランダム建玉30シードの実測最大t。
"""
import pandas as pd, numpy as np, sys, time
t0=time.time()
COST=0.21  # $/oz 往復(Zero: spread0.14+手数料0.07)
m1=pd.read_pickle('_fx_xauusd_m1.pkl'); m1=m1[m1.vol.notna()]
def mk(rule, offset=None):
    r=m1.resample(rule, offset=offset) if offset else m1.resample(rule)
    b=pd.DataFrame({'o':r.open.first(),'h':r.high.max(),'l':r.low.min(),'c':r.close.last(),'v':r.vol.sum()}).dropna()
    b=b[~((b.h==b.l)&(b.v==0))]   # 週末フラット足除去
    b=b[b.index.dayofweek<6]
    return b
TF={'M5':mk('5min'),'M15':mk('15min'),'H1':mk('1h'),'H4':mk('4h',offset='22h'),'D1':mk('24h',offset='22h')}
for k,v in TF.items(): print(k,len(v),v.index[0],v.index[-1])

# ---------- MA types ----------
def sma(x,n): return x.rolling(n).mean()
def ema(x,n): return x.ewm(span=n,adjust=False).mean()
def smma(x,n): return x.ewm(alpha=1/n,adjust=False).mean()   # Wilder/RMA = MT5のSMMA
def wma(x,n):
    w=np.arange(1,n+1,dtype=float); w/=w.sum()
    v=np.convolve(x.values,w[::-1],mode='full')[:len(x)]
    v[:n-1]=np.nan; return pd.Series(v,index=x.index)
MAS={'SMA':sma,'EMA':ema,'SMMA':smma,'WMA':wma}

# ---------- evaluator ----------
def evaluate(b,pos,label,cost=COST):
    """pos: 足の終値時点で決めた建玉(+1/-1/0)。次足の始値→その次の始値で実現(=翌足始値約定)。"""
    pos=pos.fillna(0).astype(float)
    o=b.o; ret=(o.shift(-2)/o.shift(-1)-1)*100  # 次足始値→次々足始値 の%
    ret=ret.fillna(0)
    pnl=pos*ret
    chg=pos.diff().abs().fillna(pos.abs())
    cpct=chg*(COST/2)/o.shift(-1)*100
    net=pnl-cpct.fillna(0)
    mu=ret[ret!=0].mean()                     # 同時間軸の1足平均ドリフト(買い持ちの取り分)
    alpha=(net-pos*mu).groupby(b.index.date).sum(); alpha=alpha[alpha!=0]
    expo=pos.abs().mean()
    day=net.groupby(b.index.date).sum(); day=day[day!=0]
    if len(day)<100: return None
    n=len(day); mid=n//2
    def tt(x): return x.mean()/x.std()*np.sqrt(len(x)) if x.std()>0 else 0
    yr=day.groupby(pd.to_datetime(day.index).year).sum()
    g=day[day>0].sum(); l=-day[day<0].sum()
    net2=(pnl-2*cpct.fillna(0)).groupby(b.index.date).sum(); net2=net2[net2!=0]
    top3=day.sort_values(ascending=False).iloc[3:]
    trades=int((chg>0).sum())
    return dict(label=label,n_days=n,trades=trades,t_alpha=tt(alpha),expo=expo,mean=day.mean(),t=tt(day),t1=tt(day.iloc[:mid]),t2=tt(day.iloc[mid:]),
                pf=g/l if l>0 else 99,wy=int((yr>0).sum()),ny=len(yr),ann=yr.mean(),t_c2=tt(net2),t_top3=tt(top3),
                gross_per_trade=(pnl.sum()/max(trades,1)),net_pct_total=day.sum())
R=[]
def run(b,pos,label):
    r=evaluate(b,pos,label)
    if r: R.append(r)

# ---------- (A) MA family ----------
for tf,b in TF.items():
    c=b.c
    for mname,f in MAS.items():
        for fa,sl in [(5,20),(10,50),(20,100),(50,200)]:
            m1_=f(c,fa); m2_=f(c,sl); sig=np.sign(m1_-m2_)
            run(b,sig,f'A|MAクロス|{tf}|{mname}|{fa}/{sl}|両')
            run(b,sig.clip(lower=0),f'A|MAクロス|{tf}|{mname}|{fa}/{sl}|買')
            run(b,sig.clip(upper=0),f'A|MAクロス|{tf}|{mname}|{fa}/{sl}|売')
        for n in (20,50,100,200):
            m_=f(c,n); sig=np.sign(c-m_)
            run(b,sig,f'A|価格vsMA|{tf}|{mname}|{n}|両')
            run(b,sig.clip(lower=0),f'A|価格vsMA|{tf}|{mname}|{n}|買')
            run(b,sig.clip(upper=0),f'A|価格vsMA|{tf}|{mname}|{n}|売')
        # 押し目/戻り: 200MAの向きでトレンド、20/50MAに触れて(低値<=MA & 終値>MA)→保有N足
        m200=f(c,200); up=(m200>m200.shift(5)); dn=(m200<m200.shift(5))
        for pb in (20,50):
            mp=f(c,pb)
            lsig=(up&(b.l<=mp)&(c>mp)).astype(float); ssig=(dn&(b.h>=mp)&(c<mp)).astype(float)
            for hold in (5,20):
                run(b,lsig.rolling(hold,min_periods=1).max(),f'A|押し目{pb}/トレンド200|{tf}|{mname}|hold{hold}|買')
                run(b,-ssig.rolling(hold,min_periods=1).max(),f'A|戻り{pb}/トレンド200|{tf}|{mname}|hold{hold}|売')
for tf,b in TF.items():
    c=b.c
    for n in (20,50,100,200):
        for lab,m_ in [('SMMA',smma(c,n)),(f'EMA{2*n-1}',ema(c,2*n-1)),(f'SMA{2*n}',sma(c,2*n)),(f'SMA{n}',sma(c,n))]:
            sig=np.sign(c-m_); run(b,sig.clip(lower=0),f'A2|価格vsMA{n}等価|{tf}|{lab}|買'); run(b,sig,f'A2|価格vsMA{n}等価|{tf}|{lab}|両')
    for fa,sl in [(10,50),(20,100)]:
        for lab,f1,f2 in [('SMMA',smma(c,fa),smma(c,sl)),(f'EMA{2*fa-1}/{2*sl-1}',ema(c,2*fa-1),ema(c,2*sl-1)),(f'SMA{2*fa}/{2*sl}',sma(c,2*fa),sma(c,2*sl))]:
            sig=np.sign(f1-f2); run(b,sig.clip(lower=0),f'A2|クロス{fa}/{sl}等価|{tf}|{lab}|買'); run(b,sig,f'A2|クロス{fa}/{sl}等価|{tf}|{lab}|両')
print('A done',len(R),round(time.time()-t0)); sys.stdout.flush()

# ---------- (B) flat scan ----------
def rsi(c,n):
    d=c.diff(); up=d.clip(lower=0); dn=-d.clip(upper=0)
    ru=up.ewm(alpha=1/n,adjust=False).mean(); rd=dn.ewm(alpha=1/n,adjust=False).mean()
    return 100-100/(1+ru/rd.replace(0,np.nan))
for tf,b in TF.items():
    c=b.c; h=b.h; l=b.l
    # ドンチャン
    for n in (10,20,55):
        hh=h.rolling(n).max().shift(1); ll=l.rolling(n).min().shift(1)
        sig=pd.Series(np.where(c>hh,1,np.where(c<ll,-1,np.nan)),index=b.index).ffill()
        run(b,sig,f'B|ドンチャン{n}|{tf}|両'); run(b,sig.clip(lower=0),f'B|ドンチャン{n}|{tf}|買'); run(b,sig.clip(upper=0),f'B|ドンチャン{n}|{tf}|売')
    # RSI 逆張り
    for n in (2,14):
        r=rsi(c,n)
        for lo,hi in [(20,80),(30,70),(10,90)]:
            for hold in (2,5,20):
                run(b,(r<lo).astype(float).rolling(hold,min_periods=1).max(),f'B|RSI{n}<{lo}逆張り|{tf}|hold{hold}|買')
                run(b,-(r>hi).astype(float).rolling(hold,min_periods=1).max(),f'B|RSI{n}>{hi}逆張り|{tf}|hold{hold}|売')
                run(b,(r>hi).astype(float).rolling(hold,min_periods=1).max(),f'B|RSI{n}>{hi}順張り|{tf}|hold{hold}|買')
                run(b,-(r<lo).astype(float).rolling(hold,min_periods=1).max(),f'B|RSI{n}<{lo}順張り|{tf}|hold{hold}|売')
        sig=np.sign(r-50); run(b,sig,f'B|RSI{n}中心線|{tf}|両')
    # BB
    for n,k in [(20,2.0),(20,2.5)]:
        m=c.rolling(n).mean(); s=c.rolling(n).std(); ub=m+k*s; lb=m-k*s
        for hold in (5,20):
            run(b,(c<lb).astype(float).rolling(hold,min_periods=1).max(),f'B|BB{n}/{k}下抜け逆張り|{tf}|hold{hold}|買')
            run(b,-(c>ub).astype(float).rolling(hold,min_periods=1).max(),f'B|BB{n}/{k}上抜け逆張り|{tf}|hold{hold}|売')
            run(b,(c>ub).astype(float).rolling(hold,min_periods=1).max(),f'B|BB{n}/{k}上抜け順張り|{tf}|hold{hold}|買')
            run(b,-(c<lb).astype(float).rolling(hold,min_periods=1).max(),f'B|BB{n}/{k}下抜け順張り|{tf}|hold{hold}|売')
    # ROC
    for n in (10,20,50,100):
        sig=np.sign(c/c.shift(n)-1)
        run(b,sig,f'B|ROC{n}|{tf}|両'); run(b,sig.clip(lower=0),f'B|ROC{n}|{tf}|買'); run(b,sig.clip(upper=0),f'B|ROC{n}|{tf}|売')
    # ボラフェード/追随
    tr=pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1); atr=tr.rolling(14).mean().shift(1)
    mv=c-c.shift(1)
    for k in (2,3):
        for hold in (3,10):
            big_up=(mv>k*atr).astype(float); big_dn=(mv<-k*atr).astype(float)
            run(b,-big_up.rolling(hold,min_periods=1).max(),f'B|急騰{k}ATRフェード|{tf}|hold{hold}|売')
            run(b,big_dn.rolling(hold,min_periods=1).max(),f'B|急落{k}ATRフェード|{tf}|hold{hold}|買')
            run(b,big_up.rolling(hold,min_periods=1).max(),f'B|急騰{k}ATR追随|{tf}|hold{hold}|買')
            run(b,-big_dn.rolling(hold,min_periods=1).max(),f'B|急落{k}ATR追随|{tf}|hold{hold}|売')
    # 連続足
    upb=(c>c.shift(1)).astype(int); dnb=(c<c.shift(1)).astype(int)
    for n in (3,4,5):
        su=(upb.rolling(n).sum()==n).astype(float); sd=(dnb.rolling(n).sum()==n).astype(float)
        for hold in (1,3):
            run(b,-su.rolling(hold,min_periods=1).max(),f'B|{n}連続陽線フェード|{tf}|hold{hold}|売')
            run(b,sd.rolling(hold,min_periods=1).max(),f'B|{n}連続陰線フェード|{tf}|hold{hold}|買')
            run(b,su.rolling(hold,min_periods=1).max(),f'B|{n}連続陽線追随|{tf}|hold{hold}|買')
            run(b,-sd.rolling(hold,min_periods=1).max(),f'B|{n}連続陰線追随|{tf}|hold{hold}|売')
    print(tf,'B done',len(R),round(time.time()-t0)); sys.stdout.flush()
# 日足パターン (曜日・前日陰陽・週足)
b=TF['D1']; c=b.c; dow=pd.Series(b.index.dayofweek,index=b.index)
for d in range(5):
    sig=(dow==d).astype(float); run(b,sig,f'B|曜日{d}買|D1|買'); run(b,-sig,f'B|曜日{d}売|D1|売')
prev=np.sign(c-b.o)
run(b,prev,f'B|前日陰陽追随|D1|両'); run(b,-prev,f'B|前日陰陽フェード|D1|両')
run(b,pd.Series(1.0,index=b.index),f'B|常時買い(ベンチ)|D1|買')
dom=pd.Series(b.index.day,index=b.index)
run(b,(dom<=5).astype(float),f'B|月初5日買|D1|買'); run(b,(dom>=25).astype(float),f'B|月末買|D1|買')

R=pd.DataFrame(R); R.to_csv('_bt_gold_flat_0919.csv',index=False)
N=len(R); floor=np.sqrt(2*np.log(N))
print(f'\n総セル {N}・理論噪音床(期待最大t)≈{floor:.2f}・所要{time.time()-t0:.0f}s')

# ---------- ランダム対照 ----------
print('\n== ランダム建玉対照(30シード・M5/H1/D1・平均保有20足・両方向) 最大t ==')
rng=np.random.default_rng(0)
for tf in ('M5','H1','D1'):
    b=TF[tf]; ts=[]
    for s in range(30):
        st=rng.integers(-1,2,size=len(b)//20+1); pos=pd.Series(np.repeat(st,20)[:len(b)],index=b.index,dtype=float)
        r=evaluate(b,pos,'rnd')
        if r: ts.append(r['t'])
    print(f'  {tf}: max t {max(ts):+.2f} / 95%点 {np.percentile(ts,95):+.2f} / 平均 {np.mean(ts):+.2f}')

def show(df,k=15):
    for _,r in df.head(k).iterrows():
        print(f"  {r.label:48s} 日{int(r.n_days):5d} 回{int(r.trades):6d} 年率{r.ann:+6.1f}% PF{r.pf:4.2f} t{r.t:+5.2f}(前{r.t1:+4.1f}/後{r.t2:+4.1f}) 勝年{int(r.wy)}/{int(r.ny)} α-t{r.t_alpha:+4.1f} 稼働{r.expo:.2f} コスト2倍t{r.t_c2:+4.1f} 上3除t{r.t_top3:+4.1f} グロス/回{r.gross_per_trade:+.3f}%")

print('\n== 合格判定: t>3.5 × 前後半t>1.5 × 勝ち年≥8/11 × コスト2倍でもt>2 × 上位3日除いてもt>2.5 ==')
ok=R[(R.t>3.5)&(R.t1>1.5)&(R.t2>1.5)&(R.wy>=8)&(R.t_c2>2)&(R.t_top3>2.5)]
print(f'合格 {len(ok)} / {N}'); show(ok.sort_values('t',ascending=False),30)
print('\n== ドリフト除去(α)判定: 上の合格条件 かつ α-t>3.5 (買い持ちの取り分を引いても残るか) ==')
ok2=ok[ok.t_alpha>3.5]; print(f'合格 {len(ok2)}'); show(ok2.sort_values('t_alpha',ascending=False),30)
print('\n== α-t 上位15 (合格不問) =='); show(R.sort_values('t_alpha',ascending=False),15)

print('\n== (A) SMMA質問: MA型別の集計(全戦略×全時間軸) ==')
A=R[R.label.str.startswith('A|')].copy(); A['ma']=A.label.str.split('|').str[3]; A['strat']=A.label.str.split('|').str[1]; A['tf']=A.label.str.split('|').str[2]
print(A.groupby('ma').agg(cells=('t','size'),t_mean=('t','mean'),t_max=('t','max'),pf_med=('pf','median'),plus=('ann',lambda x:(x>0).mean()),ann_mean=('ann','mean')).round(2))
print('\n  時間軸×MA型 平均t:'); print(A.pivot_table(index='tf',columns='ma',values='t',aggfunc='mean').round(2))
print('\n  戦略×MA型 平均年率%:'); print(A.pivot_table(index='strat',columns='ma',values='ann',aggfunc='mean').round(1))
print('\n  SMMAセル上位10:'); show(A[A.ma=='SMMA'].sort_values('t',ascending=False),10)
print('  SMMA vs SMA 同一セル差(t): 平均',round((A[A.ma=='SMMA'].set_index(A[A.ma=='SMMA'].label.str.replace('SMMA','X')).t - A[A.ma=='SMA'].set_index(A[A.ma=='SMA'].label.str.replace('SMA','X')).t).mean(),3))

print('\n== (B) 白紙スキャン t上位20(合格不問) ==')
B=R[R.label.str.startswith('B|')]; show(B.sort_values('t',ascending=False),20)
print('\n== 時間軸別 最大t / PF>1の割合 ==')
R['tf']=R.label.str.extract(r'\|(M5|M15|H1|H4|D1)\|')
print(R.groupby('tf').agg(cells=('t','size'),t_max=('t','max'),pf_gt1=('pf',lambda x:(x>1).mean()),ann_med=('ann','median')).round(2))
print('\n== 参考: 常時買い(ベンチ) =='); show(R[R.label.str.contains('ベンチ')])

print('\n== (A2) SMMA(n) と EMA(2n-1)/SMA(2n) の等価性(同一セルの年率%) ==')
A2=R[R.label.str.startswith('A2|')].copy(); A2['kind']=A2.label.str.split('|').str[1]; A2['tf']=A2.label.str.split('|').str[2]; A2['ma']=A2.label.str.split('|').str[3]; A2['side']=A2.label.str.split('|').str[4]
A2['mag']=A2.ma.str.replace(r'\d+.*','',regex=True)
print(A2.pivot_table(index=['kind','side'],columns='mag',values='ann',aggfunc='mean').round(1))
print('  t平均:'); print(A2.pivot_table(index=['side'],columns='mag',values='t',aggfunc='mean').round(2))

print('\n== 上位候補の年別ネット% (10年) ==')
def yearly(b,pos):
    pos=pos.fillna(0).astype(float); o=b.o; ret=(o.shift(-2)/o.shift(-1)-1)*100; ret=ret.fillna(0)
    chg=pos.diff().abs().fillna(pos.abs()); net=pos*ret-(chg*(COST/2)/o.shift(-1)*100).fillna(0)
    return net.groupby(b.index.year).sum().round(1)
b=TF['H4']; r2=rsi(b.c,2)
cand={'RSI2>80順張りH4 hold5 買':(r2>80).astype(float).rolling(5,min_periods=1).max(),
      'RSI2>80順張りH4 hold2 買':(r2>80).astype(float).rolling(2,min_periods=1).max(),
      'RSI2<20順張りH4 hold5 売(鏡像)':-(r2<20).astype(float).rolling(5,min_periods=1).max(),
      '常時買いH4':pd.Series(1.0,index=b.index)}
Y=pd.DataFrame({k:yearly(b,v) for k,v in cand.items()}); print(Y.T.to_string())

print('\n== XM実データ(GOLD. M15 2022-07〜2026-09)→H4で同ルール再現 ==')
xm=pd.read_pickle('_xm_metals_m15_0919.pkl')['GOLD.']; print('  cols',list(xm.columns)[:8], xm.index[0], xm.index[-1])
xm=xm.rename(columns={'open':'o','high':'h','low':'l','close':'c','tick_volume':'v'})
r=xm.resample('4h',offset='2h') if False else xm.resample('4h')
xb=pd.DataFrame({'o':r.o.first(),'h':r.h.max(),'l':r.l.min(),'c':r.c.last(),'v':r.v.sum()}).dropna(); xb=xb[xb.v>0]
xr=rsi(xb.c,2)
for lab,pos in [('RSI2>80 hold5 買',(xr>80).astype(float).rolling(5,min_periods=1).max()),('RSI2>80 hold2 買',(xr>80).astype(float).rolling(2,min_periods=1).max()),('常時買い',pd.Series(1.0,index=xb.index))]:
    e=evaluate(xb,pos,lab)
    if e: print(f"  {lab:20s} 日{e['n_days']} 回{e['trades']} 年率{e['ann']:+.1f}% PF{e['pf']:.2f} t{e['t']:+.2f} α-t{e['t_alpha']:+.2f} 勝年{e['wy']}/{e['ny']}")
# 同期間のDukascopy H4で比較
bd=TF['H4']; bd=bd[bd.index>=xb.index[0]]; rd=rsi(bd.c,2)
e=evaluate(bd,(rd>80).astype(float).rolling(5,min_periods=1).max(),'duka同期間')
print(f"  Duka同期間 hold5 買  日{e['n_days']} 回{e['trades']} 年率{e['ann']:+.1f}% PF{e['pf']:.2f} t{e['t']:+.2f} α-t{e['t_alpha']:+.2f}")
