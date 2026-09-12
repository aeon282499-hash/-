"""金(XAUUSD)日足トレンドフォロー 2016-2026・Zero実コスト: spread+手数料$0.21/往復・スワップ long -0.97$/oz/泊(水3倍) short +0.13。
ドンチャン20/55・MA10/50・MA20/100・200日フィルタ・時系列モメンタム(60/120/250日)。ロング/ショート/両方。日次の終値(NY 22:00UTC相当=サーバー日)で判定・翌始値執行。"""
import pandas as pd, numpy as np, itertools
m1=pd.read_pickle('_fx_xauusd_m1.pkl')
# サーバー日(UTC+2/3)≈ NY引け基準: UTC+2で日付を切る
idx=m1.index+pd.Timedelta(hours=2)
d=m1.set_index(idx).resample('1D').agg({'open':'first','high':'max','low':'min','close':'last'}).dropna()
d=d[d.index.dayofweek<5]
COST=0.21; SWL=-0.97; SWS=0.13
c=d.close; o=d.open
def run(pos):   # pos: +1/-1/0 at close of day t → hold day t+1 (open t+1 → open t+2 実質 close-to-close近似)
    pos=pos.shift(1).fillna(0)
    ret=(c-c.shift(1))*pos
    sw=np.where(pos>0,SWL,np.where(pos<0,SWS,0))*np.where(d.index.dayofweek==2,3,1)
    trades=(pos!=pos.shift(1)).astype(int)*np.where(pos.shift(1).fillna(0)!=0,1,0)+((pos!=0)&(pos.shift(1).fillna(0)==0)).astype(int)
    cost=trades*COST/2*2  # 建て/決済で往復COST(片道ずつ)
    net=ret+sw*np.abs(pos)-cost*0.5
    return ret,net
sigs={}
for n1,n2 in [(20,10),(55,20),(100,50)]:
    hi=c.rolling(n1).max().shift(1); lo=c.rolling(n1).min().shift(1); xhi=c.rolling(n2).max().shift(1); xlo=c.rolling(n2).min().shift(1)
    p=pd.Series(0.0,index=c.index); cur=0
    for i in range(len(c)):
        if cur==0:
            if c.iloc[i]>hi.iloc[i]: cur=1
            elif c.iloc[i]<lo.iloc[i]: cur=-1
        elif cur==1 and c.iloc[i]<xlo.iloc[i]: cur=0
        elif cur==-1 and c.iloc[i]>xhi.iloc[i]: cur=0
        p.iloc[i]=cur
    sigs[f'ドンチャン{n1}/{n2}']=p
for f,s in [(10,50),(20,100),(50,200)]:
    sigs[f'MA{f}/{s}クロス']=np.sign(c.rolling(f).mean()-c.rolling(s).mean())
for k in [60,120,250]:
    sigs[f'モメンタム{k}日']=np.sign(c-c.shift(k))
sigs['200日MA上ロング/下ショート']=np.sign(c-c.rolling(200).mean())
rows=[]
for name,p in sigs.items():
    for side in ['両方','ロングのみ','ショートのみ']:
        q=p.copy()
        if side=='ロングのみ': q=q.clip(lower=0)
        if side=='ショートのみ': q=q.clip(upper=0)
        g,n=run(q); y=n.groupby(n.index.year).sum()
        rows.append(dict(sig=name,side=side,gross_yr=g.sum()/10.7,net_yr=n.sum()/10.7,yrs_pos=int((y>0).sum()),yrs=len(y),h1=n[n.index.year<=2020].sum()/5,h2=n[n.index.year>2020].sum()/5.7,
                         dd=(n.cumsum()-n.cumsum().cummax()).min(),sharpe=n.mean()/n.std()*np.sqrt(252),pct_yr=n.sum()/10.7/c.mean()*100))
df=pd.DataFrame(rows); pd.set_option('display.width',220)
print('金 日足トレンド (単位 $/oz/年・pct_yr=平均価格比%/年・dd=$/oz)'); print(df.sort_values('net_yr',ascending=False).round(2).to_string(index=False))
bh=(c.iloc[-1]-c.iloc[0])/10.7; print(f'\n参考: 買い持ち gross {bh:.0f}$/oz/年・スワップ込み {(bh+SWL*365*1.2/1):.0f}$/oz/年')
