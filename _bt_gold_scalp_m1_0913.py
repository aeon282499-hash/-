"""金 XAUUSD 1分足スキャル総当たり 2016-2026(Dukascopy BID)・XM Zero実コスト $0.21/oz往復(+滑り$0.05)。
シグナル: 直近k分リターンのzスコア(200分窓)で逆張り/順張り・ボリンジャー(20,2σ)タッチ逆張り・RSI14極値逆張り。保有h分。セッション別も。"""
import pandas as pd, numpy as np, itertools, sys
m1=pd.read_pickle('_fx_xauusd_m1.pkl'); m1=m1[m1.index.dayofweek<5]
c=m1.close; o=m1.open
COST=0.21+0.05
hr=m1.index.hour
sess=pd.Series(np.where((hr>=0)&(hr<7),'Asia',np.where((hr>=7)&(hr<13),'London','NY')),index=m1.index)
# gap guard: 保有が日跨ぎ/週末を跨がないように、次のh本が連続分足であることを要求
t=m1.index.to_series(); 
def fwd_ret(h):
    nxt=o.shift(-1); ex=c.shift(-h)     # 建て=次足始値・決済=h本後終値
    ok=(t.shift(-h)-t)<=pd.Timedelta(minutes=h+3)
    return (ex-nxt).where(ok)
F={h:fwd_ret(h) for h in [1,3,5,15,30,60]}
sig={}
r=c.diff(); 
for k in [3,5,15,30]:
    rk=c-c.shift(k); z=rk/ (r.rolling(200).std()*np.sqrt(k))
    for q in [2,3,4]:
        sig[f'z{k}>{q} 逆張り']=-np.sign(z).where(z.abs()>q)
        sig[f'z{k}>{q} 順張り']= np.sign(z).where(z.abs()>q)
ma=c.rolling(20).mean(); sd=c.rolling(20).std()
sig['BB20上抜け売り/下抜け買い']=np.where(c>ma+2*sd,-1,np.where(c<ma-2*sd,1,np.nan)); sig['BB20上抜け売り/下抜け買い']=pd.Series(sig['BB20上抜け売り/下抜け買い'],index=c.index)
sig['BB20 3σ逆張り']=pd.Series(np.where(c>ma+3*sd,-1,np.where(c<ma-3*sd,1,np.nan)),index=c.index)
d=c.diff(); up=d.clip(lower=0).rolling(14).mean(); dn=(-d.clip(upper=0)).rolling(14).mean(); rsi=100-100/(1+up/dn)
sig['RSI14<20買い/>80売り']=pd.Series(np.where(rsi>80,-1,np.where(rsi<20,1,np.nan)),index=c.index)
sig['RSI14<10買い/>90売り']=pd.Series(np.where(rsi>90,-1,np.where(rsi<10,1,np.nan)),index=c.index)
rows=[]
for name,s in sig.items():
    s=s.dropna()
    for h,f in F.items():
        pnl=(s*f.reindex(s.index)).dropna()
        # 重複建て禁止: h分以内の再シグナルは無視
        idx=pnl.index; keep=[]; last=None
        for ts in idx:
            if last is None or (ts-last)>=pd.Timedelta(minutes=h): keep.append(ts); last=ts
        p=pnl.loc[keep]; 
        if len(p)<500: continue
        net=p-COST; y=net.groupby(net.index.year).sum()
        for lab,mask in [('全',np.ones(len(net),bool)),('Asia',sess.reindex(net.index).values=='Asia'),('London',sess.reindex(net.index).values=='London'),('NY',sess.reindex(net.index).values=='NY')]:
            n=net[mask]; g=p[mask]
            if len(n)<500: continue
            yy=n.groupby(n.index.year).sum()
            rows.append(dict(sig=name,h=h,sess=lab,n=len(n),gross=g.mean(),net=n.mean(),t=n.mean()/n.std()*np.sqrt(len(n)),pf=n[n>0].sum()/max(1e-9,-n[n<0].sum()),
                             yrs_pos=int((yy>0).sum()),yrs=len(yy),h1=n[n.index.year<=2020].mean(),h2=n[n.index.year>2020].mean(),net_yr=n.sum()/10.7))
df=pd.DataFrame(rows); df.to_csv('_log_gold_scalp_m1_0913.csv',index=False)
pd.set_option('display.width',250)
print('セル数',len(df),' net>0:',int((df.net>0).sum()),' net>0かつ前後半とも>0かつ勝ち年≥8/11:',int(((df.net>0)&(df.h1>0)&(df.h2>0)&(df.yrs_pos>=8)).sum()))
print(df.sort_values('t',ascending=False).head(25).round(3).to_string(index=False))
print('\n--- グロスの大きい順(コスト前の構造) ---')
print(df.sort_values('gross',ascending=False).head(10).round(3).to_string(index=False))
