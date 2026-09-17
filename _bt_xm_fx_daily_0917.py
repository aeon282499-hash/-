"""2026-09-17 本人「ほかの通貨でない？XMができれば何でも」→ 未検証だった通貨の日足レベル(9/13・9/17スイープは保有≤18h・日足古典は3ペアのみ)を47ペア×2001〜で一括:
 (1)キャリー(今のスワップ+側の通貨をずっと持つ・100MAフィルタ) (2)日足ルール32本(MA/モメンタム/リバーサル/ドンチャン/曜日/月末月初) 判定=t>3×前後半同符号t>1.5×勝ち年8割。
 データ: 2001-2012は日足(1本/日)・2013-07〜はH1。コスト: Zero口座(スプレッド実spec+手数料0.007%)・スワップは今のspec(points)を%換算・水曜3倍≒×1.4/日。"""
import pandas as pd, numpy as np, warnings; warnings.filterwarnings('ignore')
R=pd.read_pickle('_xm_rest_h1.pkl')
syms=[k for k,v in R.items() if v['spec'].get('contract')==100000 and v['spec'].get('swap_mode')==1 and len(v['h1'])>40000 and k.endswith('.')]
print('対象ペア',len(syms),syms)
def daily(h):
    d=h.copy(); d['t']=pd.to_datetime(d['time'],unit='s'); d['date']=d.t.dt.normalize()
    g=d.groupby('date').agg(open=('open','first'),close=('close','last'),n=('close','size'))
    g=g[(g.index.dayofweek<5)&(g.n>=1)]; return g   # 2001-2012は日足1本/日で格納
rows=[]; carry=[]
for s in syms:
    sp=R[s]['spec']; D=daily(R[s]['h1']); px=D.close; r=px.pct_change()*100; r=r.dropna(); px=px.reindex(r.index)
    cost_in=sp['spread']/sp['bid']*100+0.007
    swl=sp['swap_long']*sp['point']/sp['bid']*100*1.4; sws=sp['swap_short']*sp['point']/sp['bid']*100*1.4   # %/日(正=収入)
    def pnl(p):  # p: 前日終値時点で決めた建玉(+1/-1/0)・当日リターンを得る
        p=p.reindex(r.index).fillna(0); prev=p.shift(1).fillna(0); opens=((p!=prev)&(p!=0)).astype(float)
        return p*r - opens*cost_in + np.where(p>0,swl,np.where(p<0,sws,0.0))
    def st(x):
        y=x.groupby(x.index.year).sum(); n=len(x); h=len(x)//2; a=x.iloc[:h]; b=x.iloc[h:]
        return dict(n=n,mean=x.mean(),t=x.mean()/x.std()*np.sqrt(n) if x.std()>0 else 0,ta=a.mean()/a.std()*np.sqrt(len(a)) if a.std()>0 else 0,tb=b.mean()/b.std()*np.sqrt(len(b)) if b.std()>0 else 0,wy=(y>0).mean(),yrs=len(y),ann=y.mean(),yr=y)
    # (1) キャリー
    side=1 if swl>sws else -1; swap_ann=(swl if side>0 else sws)/1.4*365
    spot=side*r; ys=spot.groupby(spot.index.year).sum()
    ma=px.rolling(100).mean(); trend=((px>ma) if side>0 else (px<ma)).shift(1).fillna(False)
    x=pnl(pd.Series(np.where(trend,side,0),index=r.index)); s1=st(x)
    carry.append(dict(sym=s,side='買い' if side>0 else '売り',swap_ann=swap_ann,spot_ann=ys.mean(),spot_wy=(ys>0).mean(),spot_sd=ys.std(),tot=ys.mean()+swap_ann,trend_ann=s1['ann'],trend_wy=s1['wy'],trend_t=s1['t']))
    # (2) 日足ルール
    rules={}
    for f,sl in [(5,20),(10,50),(20,100),(50,200)]: rules[f'MA{f}/{sl}']=np.sign(px.rolling(f).mean()-px.rolling(sl).mean()).shift(1)
    for N in [20,60,120,250]: rules[f'モメ{N}']=np.sign(px-px.shift(N)).shift(1)
    z=(r/r.rolling(20).std())
    for H in [1,3,5]:
        sig=pd.Series(np.where(z<-1,1,np.where(z>1,-1,0)),index=r.index); rules[f'逆張z1_{H}日']=sig.rolling(H).apply(lambda w: w[w!=0][-1] if (w!=0).any() else 0,raw=True).shift(1)
        sig2=pd.Series(np.where(z<-1,-1,np.where(z>1,1,0)),index=r.index); rules[f'順張z1_{H}日']=sig2.rolling(H).apply(lambda w: w[w!=0][-1] if (w!=0).any() else 0,raw=True).shift(1)
    for N in [20,55]:
        hi=px.rolling(N).max(); lo=px.rolling(N).min(); sig=pd.Series(np.where(px>=hi,1,np.where(px<=lo,-1,np.nan)),index=r.index).ffill(); rules[f'ドンチャン{N}']=sig.shift(1)
    dow=r.index.dayofweek
    for d,nm in enumerate('月火水木金'):
        rules[f'{nm}曜買']=pd.Series((dow==d).astype(float),index=r.index); rules[f'{nm}曜売']=-rules[f'{nm}曜買']
    m=r.index.to_period('M'); pos_in_m=pd.Series(r.index,index=r.index).groupby(m).cumcount(); cnt=pd.Series(r.index,index=r.index).groupby(m).transform('size'); last2=(pos_in_m>=cnt-2); first2=(pos_in_m<2)
    rules['月末2日買']=last2.astype(float); rules['月末2日売']=-rules['月末2日買']; rules['月初2日買']=first2.astype(float); rules['月初2日売']=-rules['月初2日買']
    for nm,p in rules.items():
        x=pnl(p.fillna(0)); s1=st(x); s1.update(sym=s,rule=nm,cost=cost_in); del s1['yr']; rows.append(s1)
C=pd.DataFrame(carry).sort_values('tot',ascending=False)
print('\n== (1) キャリー: スワップ+側をずっと持つ(スワップ=今のspec・スポットは2001〜の年平均%) ==')
print(C.round(2).to_string(index=False))
print(f"\n  スワップ年率が+1%超のペア: {(C.swap_ann>1).sum()}本  スポット+スワップ合計が+3%超かつスポット勝ち年60%超: {((C.tot>3)&(C.spot_wy>0.6)).sum()}本")
Rr=pd.DataFrame(rows); Rr.to_csv('_bt_xm_fx_daily_0917.csv',index=False)
surv=Rr[(Rr.t>3)&(Rr.ta>1.5)&(Rr.tb>1.5)&(Rr.wy>=0.8)]
print(f'\n== (2) 日足ルール {len(Rr)}セル({len(syms)}ペア×{len(rules)}ルール) 判定t>3×前後半t>1.5×勝ち年8割 → 生存{len(surv)} ==')
print('  ルール別の平均t:'); print(Rr.groupby('rule').t.agg(['mean','max']).round(2).sort_values('max',ascending=False).to_string())
print('\n  t上位15セル:'); print(Rr.sort_values('t',ascending=False).head(15)[['sym','rule','n','mean','t','ta','tb','wy','ann','cost']].round(3).to_string(index=False))
if len(surv):
    print('\n  生存セル:'); print(surv[['sym','rule','n','mean','t','ta','tb','wy','ann','cost']].round(3).to_string(index=False))
# 噪音床: 全セルのtの分布(ルールがランダムなら|t|>3は0.3%)
print(f"\n  |t|>3のセル数 {int((Rr.t.abs()>3).sum())}/{len(Rr)} ({(Rr.t.abs()>3).mean()*100:.1f}%・ランダムなら0.27%)  t>3かつ前後半>1.5: {int(((Rr.t>3)&(Rr.ta>1.5)&(Rr.tb>1.5)).sum())}")
