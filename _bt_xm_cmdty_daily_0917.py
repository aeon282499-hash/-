"""2026-09-17 商品/指数CFD(現物型26本)の日足ルール30本(通貨と同じ判定)。スワップ: mode3=年%・mode1=points。"""
import pandas as pd, numpy as np, warnings; warnings.filterwarnings('ignore')
D=pd.read_pickle('_xm_multi_hist.pkl'); M=pd.read_pickle('_xm_more_idx_h1.pkl')
U={k:dict(h1=D['h1'][k],spec=D['spec'][k]) for k in D['h1'] if k not in ('USDJPY','EURUSD','GBPUSD','AUDUSD','USDCHF','NZDUSD')}
U.update({k:v for k,v in M.items()})
def daily(h):
    d=h.copy(); d['t']=pd.to_datetime(d['time'],unit='s'); d['date']=d.t.dt.normalize()
    g=d.groupby('date').agg(open=('open','first'),close=('close','last'),n=('close','size')); return g[(g.index.dayofweek<5)&(g.n>=1)]
rows=[]
for s,v in U.items():
    sp=v['spec']; Dd=daily(v['h1']); px=Dd.close; r=(px.pct_change()*100).dropna(); px=px.reindex(r.index)
    if len(r)<1500: continue
    cost_in=sp['spread']/sp['bid']*100
    if sp.get('swap_mode')==1: swl=sp['swap_long']*sp['point']/sp['bid']*100*1.4; sws=sp['swap_short']*sp['point']/sp['bid']*100*1.4
    else: swl=sp['swap_long']/365*1.4; sws=sp['swap_short']/365*1.4
    def pnl(p):
        p=p.reindex(r.index).fillna(0); prev=p.shift(1).fillna(0); opens=((p!=prev)&(p!=0)).astype(float)
        return p*r-opens*cost_in+np.where(p>0,swl,np.where(p<0,sws,0.0))
    def st(x):
        y=x.groupby(x.index.year).sum(); n=len(x); h=n//2; a=x.iloc[:h]; b=x.iloc[h:]
        return dict(n=n,mean=x.mean(),t=x.mean()/x.std()*np.sqrt(n),ta=a.mean()/a.std()*np.sqrt(len(a)),tb=b.mean()/b.std()*np.sqrt(len(b)),wy=(y>0).mean(),yrs=len(y),ann=y.mean())
    rules={}
    for f,sl in [(5,20),(10,50),(20,100),(50,200)]: rules[f'MA{f}/{sl}']=np.sign(px.rolling(f).mean()-px.rolling(sl).mean()).shift(1)
    for N in [20,60,120,250]: rules[f'モメ{N}']=np.sign(px-px.shift(N)).shift(1)
    z=(r/r.rolling(20).std())
    for H in [1,3,5]:
        sig=pd.Series(np.where(z<-1,1,np.where(z>1,-1,0)),index=r.index); rules[f'逆張z1_{H}日']=sig.rolling(H).apply(lambda w: w[w!=0][-1] if (w!=0).any() else 0,raw=True).shift(1)
        rules[f'順張z1_{H}日']=-rules[f'逆張z1_{H}日']
    for N in [20,55]:
        hi=px.rolling(N).max(); lo=px.rolling(N).min(); rules[f'ドンチャン{N}']=pd.Series(np.where(px>=hi,1,np.where(px<=lo,-1,np.nan)),index=r.index).ffill().shift(1)
    dow=r.index.dayofweek
    for d,nm in enumerate('月火水木金'): rules[f'{nm}曜買']=pd.Series((dow==d).astype(float),index=r.index); rules[f'{nm}曜売']=-rules[f'{nm}曜買']
    m=r.index.to_period('M'); pim=pd.Series(r.index,index=r.index).groupby(m).cumcount(); cnt=pd.Series(r.index,index=r.index).groupby(m).transform('size')
    rules['月末2日買']=(pim>=cnt-2).astype(float); rules['月末2日売']=-rules['月末2日買']; rules['月初2日買']=(pim<2).astype(float); rules['月初2日売']=-rules['月初2日買']
    rules['常時買']=pd.Series(1.0,index=r.index); rules['常時売']=-rules['常時買']
    for nm,p in rules.items():
        s1=st(pnl(p.fillna(0))); s1.update(sym=s,rule=nm,cost=cost_in,swl=swl,sws=sws,frm=r.index[0].date()); rows.append(s1)
R=pd.DataFrame(rows); R.to_csv('_bt_xm_cmdty_daily_0917.csv',index=False)
surv=R[(R.t>3)&(R.ta>1.5)&(R.tb>1.5)&(R.wy>=0.8)]
print(f'== 商品/指数CFD {R.sym.nunique()}本×{len(rules)}ルール={len(R)}セル → 生存{len(surv)} ==')
print(R.groupby('sym').agg(frm=('frm','first'),n=('n','first'),cost=('cost','first'),swl=('swl','first'),sws=('sws','first')).round(4).to_string())
print('\nルール別 平均t/最大t:'); print(R.groupby('rule').t.agg(['mean','max']).round(2).sort_values('max',ascending=False).to_string())
print('\nt上位15:'); print(R.sort_values('t',ascending=False).head(15)[['sym','rule','n','mean','t','ta','tb','wy','ann','cost']].round(3).to_string(index=False))
if len(surv): print('\n生存:'); print(surv[['sym','rule','n','mean','t','ta','tb','wy','ann','cost']].round(3).to_string(index=False))
