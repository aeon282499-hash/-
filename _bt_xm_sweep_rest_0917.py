"""未スキャン銘柄(_xm_rest_h1.pkl)の 建て時刻24×保有9×売買×{直前≤0,>0,全} 総当たり。9/13スイープと同じ判定だがスワップ符号は正しく(負=コスト)。メンテ跨ぎ人工物(JST4〜8時建て)は事前に除外して表示。"""
import pandas as pd, numpy as np, pickle
syms={k:(v['h1'],v['spec']) for k,v in pickle.load(open('_xm_rest_h1.pkl','rb')).items()}
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; idx=df.index
    off=pd.Series(2,index=idx)
    for yy in sorted(set(idx.year)):
        mar=max(d for d in range(25,32) if pd.Timestamp(yy,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(yy,10,d).dayofweek==6)
        m=(idx>=pd.Timestamp(yy,3,mar,1))&(idx<pd.Timestamp(yy,10,octb,1)); off[m]=3
    df.index=idx-pd.to_timedelta(off.values,unit='h')+pd.Timedelta(hours=9); return df[~df.index.duplicated()]
HOLDS=[1,2,3,4,6,9,12,15,18]; rows=[]
for s,(df,sp) in syms.items():
    d=jst(df)
    if len(d)<8000: print('short',s,len(d)); continue
    o=d.open; bid=sp['bid'] if sp['bid']>0 else float(d.close.iloc[-1]); pt=sp.get('point',0.01)
    spr=d['spread'].replace(0,np.nan).median(); spread_pct=(spr*pt/bid*100) if spr==spr else sp['spread']/bid*100
    if 'XMZero' in sp.get('path','') and s.endswith('.') and sp.get('contract',0)==100000: spread_pct+=0.007   # Zero口座FXの手数料 $3.5/lot/片道 ≈ 往復0.007%
    if sp['swap_mode']==3: swL=-sp['swap_long']/365; swS=-sp['swap_short']/365
    elif sp['swap_mode']==1: swL=-sp['swap_long']*pt/bid*100; swS=-sp['swap_short']*pt/bid*100
    else: swL=swS=0.0
    yrs=sorted(set(d.index.year)); print(f'{s}: {d.index[0].date()}〜 {len(d)}本 spread{spread_pct:.4f}% swapL{swL:+.4f}%/日 S{swS:+.4f}')
    for h in range(24):
        a=o[o.index.hour==h]
        if len(a)<500: continue
        for k in HOLDS:
            b=o.reindex(a.index+pd.Timedelta(hours=k)); L=pd.Series((b.values/a.values-1)*100,index=a.index).dropna()
            if len(L)<800: continue
            prev=L.shift(1)
            for side in (1,-1):
                n=(L-spread_pct-swL*(k/24)) if side==1 else (-L-spread_pct-swS*(k/24))
                for fl,mask in [('prev≤0',prev<=0),('prev>0',prev>0),('all',prev.notna())]:
                    x=n[mask]
                    if len(x)<300: continue
                    mid=x.index[len(x)//2]; h1=x[x.index<mid]; h2=x[x.index>=mid]
                    if len(h1)<100 or len(h2)<100: continue
                    y=x.groupby(x.index.year).sum()
                    rows.append(dict(sym=s,h=h,k=k,side='買' if side==1 else '売',flt=fl,n=len(x),mean=x.mean(),t=x.mean()/x.std()*np.sqrt(len(x)),t1=h1.mean()/h1.std()*np.sqrt(len(h1)),t2=h2.mean()/h2.std()*np.sqrt(len(h2)),wy=int((y>0).sum()),ny=len(y),tot=x.sum(),start=str(x.index[0].date())))
R=pd.DataFrame(rows); R.to_csv('_xm_sweep_rest_0917.csv',index=False); pd.set_option('display.width',230)
print('\nセル数',len(R),'銘柄',R.sym.nunique())
ok=R[(R.t>3)&(R.t1>1.5)&(R.t2>1.5)&(R.wy/R.ny>=0.8)].sort_values('t',ascending=False)
print(f'\n== t>3 かつ前後半t>1.5 かつ勝ち年8割: {len(ok)}セル (JST4〜8時建て=メンテ跨ぎ疑いに印) ==')
if len(ok):
    ok=ok.assign(疑=np.where(ok.h.between(4,8),'⚠メンテ',''))
    print(ok.head(40).round(3).to_string(index=False))
