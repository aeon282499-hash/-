"""③日経225 夜ギャップ条件付き日中(9→15JST) と 日経/US500 日足トレンド(実スワップ)。XM H1 2016-05〜。"""
import pandas as pd, numpy as np
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']; S=D['spec']
print('spec JP225Cash:',S['JP225Cash']); print('spec US500Cash:',S['US500Cash'])
jp=H['JP225Cash']; print(jp.columns.tolist(), jp.head(2))
def prep(df):
    df=df.copy()
    df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']
    return df
jp=prep(jp); us=prep(H['US500Cash'])
# サーバー時刻→JST: サーバーはUTC+2/+3(欧州DST)。既存 _xm_night_legs は jst index。ここでは既存pklのa(15:00)/b(9:00)に加え、H1から 9:00JST始値・15:00JST始値を取る
def jst(df):
    # サーバー時刻を UTC+2(冬)/UTC+3(夏, 3月最終日曜〜10月最終日曜)とみなしてJSTへ
    t=df.index; utc=[]
    for ts in t:
        y=ts.year; import calendar
        mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1)
        utc.append(ts-pd.Timedelta(hours=3 if dst else 2))
    df=df.copy(); df.index=pd.DatetimeIndex(utc)+pd.Timedelta(hours=9); return df
jp=jst(jp)
o9=jp[jp.index.hour==9].open; o15=jp[jp.index.hour==15].open
o9.index=o9.index.normalize(); o15.index=o15.index.normalize()
d=pd.DataFrame({'o9':o9,'o15':o15}).dropna()
d['prev15']=d.o15.shift(1)
d['night']=(d.o9/d.prev15-1)*100         # 前夜ギャップ(前日15→当日9)
d['day']=(d.o15/d.o9-1)*100              # 日中 9→15
sp=S['JP225Cash']; spread_pct=0.012
print('\n== 日経 日中(9:00→15:00) を前夜ギャップで条件付け・net=%-spread0.012 ==')
print(f'全日 n={len(d)} 日中平均{d.day.mean():+.3f}%  年率{d.day.mean()*245:+.1f}%')
for lab,m in [('前夜<-1%',d.night<-1),('前夜<-0.5%',d.night<-0.5),('前夜<0',d.night<0),('前夜>0',d.night>0),('前夜>+0.5%',d.night>0.5),('前夜>+1%',d.night>1)]:
    x=d.day[m]
    for side,sg in [('買い',1),('売り',-1)]:
        n=sg*x-spread_pct; y=n.groupby(n.index.year).sum()
        print(f'{lab:10s} {side} n={len(n):4d} net/回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝ち年{int((y>0).sum())}/{len(y)} 前半{n[n.index.year<=2020].mean():+.3f} 後半{n[n.index.year>2020].mean():+.3f} 年率{n.sum()/10.3:+.1f}%')
# 日足トレンド(終値=15:00始値近似・夜も持つ=持ち越し、スワップ年率)
def trend(px,swap_yr,spread_pct,name):
    c=px; rows=[]
    sigs={}
    for k in [20,60,120,250]: sigs[f'モメンタム{k}日']=np.sign(c-c.shift(k))
    for f,s in [(10,50),(20,100),(50,200)]: sigs[f'MA{f}/{s}']=np.sign(c.rolling(f).mean()-c.rolling(s).mean())
    sigs['200日MA']=np.sign(c-c.rolling(200).mean())
    for nm,p in sigs.items():
        for side in ['ロング','ショート','両方']:
            q=p.copy(); q=q.clip(lower=0) if side=='ロング' else (q.clip(upper=0) if side=='ショート' else q)
            pos=q.shift(1).fillna(0); r=(c.pct_change()*100)*pos
            sw=np.where(pos!=0,-swap_yr/245*np.sign(pos)*(1 if side!='ショート' else -1),0)  # ロングは負担、ショートは受取(概算同率)
            sw=np.where(pos>0,-swap_yr/245,np.where(pos<0,swap_yr/245*0.3,0))
            tr=(pos!=pos.shift(1)).astype(float)*spread_pct
            n=r+sw-tr; y=n.groupby(n.index.year).sum()
            rows.append(dict(sig=nm,side=side,net_yr=n.sum()/10.3,yrs_pos=int((y>0).sum()),yrs=len(y),h1=n[n.index.year<=2020].sum()/4.6,h2=n[n.index.year>2020].sum()/5.7,sharpe=n.mean()/n.std()*np.sqrt(245),dd=(n.cumsum()-n.cumsum().cummax()).min()))
    df=pd.DataFrame(rows).sort_values('net_yr',ascending=False); pd.set_option('display.width',200)
    print(f'\n== {name} 日足トレンド(%/年・スワップ年率{swap_yr}%) ==  買い持ち{(c.iloc[-1]/c.iloc[0]-1)*100/10.3:+.1f}%/年(スワップ前)')
    print(df.head(8).round(2).to_string(index=False)); print('...ショートのみ最良:'); print(df[df.side=='ショート'].head(2).round(2).to_string(index=False))
trend(d.o15,4.53,0.012,'日経225Cash')
us=jst(us); u=us[us.index.hour==15].open; u.index=u.index.normalize(); trend(u,1.21,0.010,'US500Cash')
