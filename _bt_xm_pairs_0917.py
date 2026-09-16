"""ペアの平均回帰(未検証の新ファミリー): 指数同士/金銀の対数比のzスコアで両建て(等金額)・H1・JST・両レッグのスプレッド+スワップ(符号は正しく)込み"""
import pandas as pd, numpy as np, itertools, sys
D=pd.read_pickle('_xm_multi_hist.pkl'); H=dict(D['h1']); S=dict(D['spec'])
try:
    import pickle
    for f in ['_xm_more_idx_h1.pkl','_xm_metals_fx2_h1.pkl']:
        for k,v in pickle.load(open(f,'rb')).items(): H[k]=v['h1']; S[k]=v['spec']
except Exception as e: print('more_idx load:',e)
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']
    ts=df.index; y=ts.year
    # 簡易DST(欧州: 3月最終日曜〜10月最終日曜) → サーバーGMT+3/+2 → JST
    def last_sun(yy,m): d=pd.Timestamp(yy,m,31 if m==3 else 31); return d-pd.Timedelta(days=(d.dayofweek+1)%7)
    dst=np.array([last_sun(t.year,3)<=t.replace(tzinfo=None)<last_sun(t.year,10) for t in ts])
    df.index=ts+pd.to_timedelta(np.where(dst,6,7),unit='h'); return df
def cost_pct(sym):
    sp=S[sym]; spread=sp['spread']/sp['bid']*100
    if sp.get('swap_mode',3)==3: swL=-sp['swap_long']/365; swS=-sp['swap_short']/365   # %/日・正=コスト
    else: swL=-sp['swap_long']*sp.get('point',0.01)/sp['bid']*100; swS=-sp['swap_short']*sp.get('point',0.01)/sp['bid']*100
    return spread,swL,swS
def stats(n):
    if len(n)<20: return f'n={len(n)}'
    y=n.groupby(n.index.year).sum(); h=n.index.year<=2020
    return f'n={len(n):4d} 回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[h].mean():+.3f} 後{n[~h].mean():+.3f} 合計{n.sum():+.0f}%'
PAIRS=[('US500Cash','US100Cash'),('US500Cash','US30Cash'),('US100Cash','US30Cash'),('US500Cash','US2000Cash'),('GER40Cash','EU50Cash'),('GER40Cash','FRA40Cash'),('EU50Cash','FRA40Cash'),('UK100Cash','EU50Cash'),('JP225Cash','HK50Cash'),('JP225Cash','US500Cash'),('GOLD.','SILVER.'),('OILCash','BRENTCash')]
out=[]
for A,B in PAIRS:
    if A not in H or B not in H: print('skip',A,B); continue
    a=jst(H[A]); b=jst(H[B]); df=pd.DataFrame({'a':a.open,'b':b.open}).dropna()
    df=df[(df.index.hour>=0)]
    r=np.log(df.a/df.b); spA,swLA,swSA=cost_pct(A); spB,swLB,swSB=cost_pct(B)
    print(f'\n== {A}/{B} n={len(df)} spread {spA:.3f}+{spB:.3f}% swap/日 L{swLA:+.4f}/{swLB:+.4f} S{swSA:+.4f}/{swSB:+.4f} ==')
    for W in [24,72,168,336]:
        m=r.rolling(W).mean(); s=r.rolling(W).std(); z=((r-m)/s).shift(1)   # 判定は前バーまでで、今バー始値で執行
        for zin in [1.5,2.0,2.5]:
            for Hh in [6,24,72]:
                # エントリー: zが閾値を超えた最初のバー(連続は1回)、退出: |z|<0.25 か Hh時間後
                sig=np.where(z<=-zin,1,np.where(z>=zin,-1,0)); sig=pd.Series(sig,index=df.index)
                ent=sig[(sig!=0)&(sig.shift(1)==0)]
                res=[]; last_exit=None
                idx=df.index; pos=pd.Series(np.arange(len(idx)),index=idx)
                zz=z.values; la=np.log(df.a.values); lb=np.log(df.b.values)
                for t,d in ent.items():
                    i=pos[t]
                    if last_exit is not None and i<=last_exit: continue
                    jmax=min(i+Hh,len(idx)-1); je=jmax
                    for k in range(i+1,jmax+1):
                        if abs(zz[k])<0.25: je=k; break
                    hrs=(idx[je]-idx[i]).total_seconds()/3600
                    ra=(la[je]-la[i])*100; rb=(lb[je]-lb[i])*100
                    gross=d*(ra-rb)   # d=+1: long A short B
                    cost=spA+spB+(hrs/24)*((swLA if d==1 else swSA)+(swSB if d==1 else swLB))
                    res.append((idx[i],gross-cost)); last_exit=je
                if len(res)<30: continue
                n=pd.Series(dict(res)); st=stats(n); tval=n.mean()/n.std()*np.sqrt(len(n))
                out.append((A,B,W,zin,Hh,len(n),n.mean(),tval,n[n.index.year<=2020].mean(),n[n.index.year>2020].mean(),n.sum()))
                if abs(tval)>=2.5: print(f'  W{W:3d} z{zin} H{Hh:2d}: {st}')
R=pd.DataFrame(out,columns=['A','B','W','zin','H','n','mean','t','first','second','sum']); R.to_csv('_bt_xm_pairs_0917.csv',index=False)
print('\n== 総括: セル数',len(R),' t>=3:',int((R.t>=3).sum()),' t>=3かつ両半>0:',int(((R.t>=3)&(R['first']>0)&(R.second>0)).sum()),' t<=-3:',int((R.t<=-3).sum()))
print(R.sort_values('t',ascending=False).head(12).to_string())
