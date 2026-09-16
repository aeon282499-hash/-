"""ペア v2: v1で「平均回帰」が全滅=「比率の順張り」が全勝に見えた。人工物(再開直後の始値・片方休場・スプレッド0のspec)を潰して順張り側を厳密に測る:
 - 執行はH1始値、判定はshift(1)
 - 両レッグとも「前バーと当バーのtick_volume>0」かつ「直前2時間にギャップ(バー欠落)なし」の時刻だけ建てる
 - スプレッドはバーのspread列の中央値(pt)×point/価格(specが0の銘柄対策)、スワップは符号を正しく(負=コスト)
 - 手仕舞い: |z|<0.25 か H時間後。順張り(z≥zin→long A/short B)"""
import pandas as pd, numpy as np, pickle
D=pd.read_pickle('_xm_multi_hist.pkl'); H=dict(D['h1']); S=dict(D['spec'])
for f in ['_xm_more_idx_h1.pkl','_xm_metals_fx2_h1.pkl']:
    for k,v in pickle.load(open(f,'rb')).items(): H[k]=v['h1']; S[k]=v['spec']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; ts=df.index
    def last_sun(yy,m): d=pd.Timestamp(yy,m,31); return d-pd.Timedelta(days=(d.dayofweek+1)%7)
    dst=np.array([last_sun(t.year,3)<=t<last_sun(t.year,10) for t in ts])
    df.index=ts+pd.to_timedelta(np.where(dst,6,7),unit='h'); return df[~df.index.duplicated()]
def costs(sym,df):
    sp=S[sym]; pt=sp.get('point',0.01); spr=df['spread'].replace(0,np.nan).median() if 'spread' in df and df['spread'].replace(0,np.nan).notna().any() else np.nan
    bid=sp['bid'] if sp['bid']>0 else float(df.close.iloc[-1])
    spread=(spr*pt/bid*100) if spr==spr else sp['spread']/bid*100
    if sp.get('swap_mode',3)==3: swL=-sp['swap_long']/365; swS=-sp['swap_short']/365
    else: swL=-sp['swap_long']*pt/bid*100; swS=-sp['swap_short']*pt/bid*100
    return spread,swL,swS
def stats(n):
    y=n.groupby(n.index.year).sum(); h=n.index.year<=2020
    return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[h].mean():+.3f} 後{n[~h].mean():+.3f} 合計{n.sum():+.0f}%'
PAIRS=[('US500Cash','US100Cash'),('US500Cash','US30Cash'),('US100Cash','US30Cash'),('US500Cash','US2000Cash'),('GER40Cash','EU50Cash'),('GER40Cash','FRA40Cash'),('EU50Cash','FRA40Cash'),('UK100Cash','EU50Cash'),('JP225Cash','HK50Cash'),('JP225Cash','US500Cash'),('GOLD.','SILVER.'),('OILCash','BRENTCash')]
out=[]
for A,B in PAIRS:
    a=jst(H[A]); b=jst(H[B]); df=pd.DataFrame({'a':a.open,'b':b.open,'va':a.tick_volume,'vb':b.tick_volume}).dropna()
    idx=df.index; gap=pd.Series((idx[1:]-idx[:-1]).total_seconds()/3600>1.0,index=idx[1:]).reindex(idx).fillna(True)
    okbar=(df.va>0)&(df.vb>0)&(df.va.shift(1)>0)&(df.vb.shift(1)>0)&(~gap)&(~gap.shift(1).fillna(True))&(~gap.shift(2).fillna(True))
    r=np.log(df.a/df.b); spA,swLA,swSA=costs(A,a); spB,swLB,swSB=costs(B,b)
    print(f'\n== {A}/{B} n={len(df)} 建て可バー{okbar.mean()*100:.0f}% spread {spA:.3f}+{spB:.3f}% ==')
    la=np.log(df.a.values); lb=np.log(df.b.values)
    for W in [24,72,168,336]:
        m=r.rolling(W).mean(); s=r.rolling(W).std(); z=((r-m)/s).shift(1); zz=z.values
        for zin in [1.5,2.0,2.5]:
            sig=np.where(z>=zin,1,np.where(z<=-zin,-1,0)); sig=pd.Series(sig,index=idx)   # 順張り: 比率が上に外れたらlong A
            ent=sig[(sig!=0)&(sig.shift(1)==0)&okbar]
            for Hh in [6,24,72]:
                res=[]; last=-1; pos=pd.Series(np.arange(len(idx)),index=idx)
                for t,d in ent.items():
                    i=pos[t]
                    if i<=last: continue
                    jmax=min(i+Hh,len(idx)-1); je=jmax
                    for k in range(i+1,jmax+1):
                        if abs(zz[k])<0.25: je=k; break
                    hrs=(idx[je]-idx[i]).total_seconds()/3600
                    gross=d*((la[je]-la[i])-(lb[je]-lb[i]))*100
                    cost=spA+spB+(hrs/24)*((swLA if d==1 else swSA)+(swSB if d==1 else swLB))
                    res.append((idx[i],gross-cost)); last=je
                if len(res)<30: continue
                n=pd.Series(dict(res)); tv=n.mean()/n.std()*np.sqrt(len(n)); f1=n[n.index.year<=2020]; f2=n[n.index.year>2020]
                y=n.groupby(n.index.year).sum()
                out.append((A,B,W,zin,Hh,len(n),n.mean(),tv,f1.mean(),f2.mean(),int((y>0).sum()),len(y),n.sum()))
                if tv>=3 and f1.mean()>0 and f2.mean()>0: print(f'  ★W{W:3d} z{zin} H{Hh:2d}: {stats(n)}')
R=pd.DataFrame(out,columns=['A','B','W','zin','H','n','mean','t','first','second','wy','ny','sum']); R.to_csv('_bt_xm_pairs2_0917.csv',index=False)
ok=R[(R.t>=3)&(R['first']>0)&(R.second>0)]
print('\n== 総括(順張り・厳密フィルタ): セル',len(R),' t≥3両半正:',len(ok),' うち勝ち年≥8割:',int((ok.wy/ok.ny>=0.8).sum()))
print(R.sort_values('t',ascending=False).head(15).round(3).to_string())
