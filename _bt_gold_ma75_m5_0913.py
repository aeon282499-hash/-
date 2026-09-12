"""金(XAUUSD) 5分足 75MA反発 BT 2016-2026。Zero実コスト$0.21/oz往復。
ルール: 上昇(終値>75MA が直前12本連続)で安値が75MAにタッチ→次足始値で買い。ミラーで売り。出口: N本後 / TP・SL($)。"""
import pandas as pd, numpy as np, itertools
m1=pd.read_pickle('_fx_xauusd_m1.pkl')
m5=m1.resample('5min').agg({'open':'first','high':'max','low':'min','close':'last'}).dropna()
m5=m5[m5.index.dayofweek<5]
ma=m5.close.rolling(75).mean(); m5['ma']=ma
above=(m5.close>ma); below=(m5.close<ma)
up=above.rolling(12).sum().shift(1)==12; dn=below.rolling(12).sum().shift(1)==12
touch_l=up&(m5.low<=ma)&(m5.close>ma)           # 上昇中に安値が75MAタッチ・終値は上に戻る
touch_s=dn&(m5.high>=ma)&(m5.close<ma)
COST=0.21
o=m5.open.values; h=m5.high.values; l=m5.low.values; c=m5.close.values
def sim(sig,side,hold,tp,sl):
    idx=np.where(sig.values)[0]; res=[]; last=-999
    for i in idx:
        if i<=last or i+1+hold>=len(o): continue
        e=o[i+1]; pnl=None
        for k in range(i+1,i+1+hold):
            if side>0:
                if l[k]<=e-sl: pnl=-sl;break
                if h[k]>=e+tp: pnl=tp;break
            else:
                if h[k]>=e+sl: pnl=-sl;break
                if l[k]<=e-tp: pnl=tp;break
        if pnl is None: pnl=side*(c[i+hold]-e)
        res.append((m5.index[i],pnl)); last=i+hold
    r=pd.DataFrame(res,columns=['t','g']).set_index('t'); r['n']=r.g-COST; return r
rows=[]
for side,sig,lab in [(1,touch_l,'買い反発'),(-1,touch_s,'売り反発')]:
    for hold,tp,sl in itertools.product([6,12,24,48],[2,4,8,999],[2,4,8,999]):
        r=sim(sig,side,hold,tp,sl)
        if len(r)<300: continue
        y=r.groupby(r.index.year).n.sum()
        rows.append(dict(side=lab,hold=hold,tp=tp,sl=sl,n=len(r),gross=r.g.mean(),net=r.n.mean(),win=(r.n>0).mean()*100,
                         pf=r.n[r.n>0].sum()/max(1e-9,-r.n[r.n<0].sum()),yrs_pos=int((y>0).sum()),yrs=len(y),
                         h1=r.n[r.index.year<=2020].mean(),h2=r.n[r.index.year>2020].mean(),net_yr=r.n.sum()/(len(y)-0.3)))
df=pd.DataFrame(rows).sort_values('net',ascending=False)
pd.set_option('display.width',200)
print('セル数',len(df),' net>0のセル',int((df.net>0).sum()),' 勝ち年過半かつnet>0',int(((df.net>0)&(df.yrs_pos>df.yrs/2)).sum()))
print(df.head(12).round(3).to_string(index=False))
print('--- 買い/売り別 gross平均$/回 ---'); print(df.groupby('side')[['gross','net','win','pf']].mean().round(3))
df.to_csv('_log_gold_ma75_m5_0913.csv',index=False)
