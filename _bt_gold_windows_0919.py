"""9/19 本人「金ほかの時間帯は？」→ 10年M1(Dukascopy UTC)で 96窓×保有{15,30,60}分×売買 をロンドン/NY時計で厳密判定。コスト=往復0.005%(Zero実測)。ロールオーバー帯(UTC21-01時)は人工物として別枠表示。"""
import pandas as pd, numpy as np
m1=pd.read_pickle('_fx_xauusd_m1.pkl'); m1=m1[m1.vol.notna()]
o=m1.open.resample('15min').first().dropna()
b=pd.DataFrame({'o':o}); b['utc']=b.index.tz_localize('UTC')
for k in (15,30,60): b[f'r{k}']=(b.o.shift(-k//15)/b.o-1)*100   # 買いのグロス%(次の窓の始値で決済)
b=b[b.index.dayofweek<5]; COST=0.005
res=[]
for clock,tz in [('ロンドン','Europe/London'),('NY','America/New_York'),('UTC','UTC')]:
    loc=b.utc.dt.tz_convert(tz); b['h']=loc.dt.hour; b['m']=loc.dt.minute; b['y']=b.index.year; b['uh']=b.index.hour
    for k in (15,30,60):
        for (h,m),g in b.groupby(['h','m']):
            x=g[f'r{k}'].dropna()
            if len(x)<1500: continue
            for side,s in [('買',1),('売',-1)]:
                v=x*s; n=len(v); mid=n//2; t=v.mean()/v.std()*np.sqrt(n); t1=v[:mid].mean()/v[:mid].std()*np.sqrt(mid); t2=v[mid:].mean()/v[mid:].std()*np.sqrt(n-mid)
                yr=v.groupby(g.loc[v.index,'y']).mean(); wy=(yr>0).sum()
                roll=g.uh.mode().iloc[0] in (21,22,23,0)
                res.append(dict(clock=clock,h=h,m=m,k=k,side=side,n=n,gross=v.mean(),net=v.mean()-COST,t=t,t1=t1,t2=t2,wy=wy,ny=len(yr),roll=roll))
R=pd.DataFrame(res); R.to_csv('_bt_gold_windows_0919.csv',index=False)
ok=R[(R.t>3)&(R.t1>1.5)&(R.t2>1.5)&(R.wy>=R.ny*0.8)&(R.net>0)]
def show(df):
    for _,r in df.iterrows(): print(f"  {r.clock:5s} {int(r.h):02d}:{int(r.m):02d} 保有{int(r.k):2d}分 {r.side} n{int(r.n)} グロス{r.gross:+.4f}% ネット{r.net:+.4f}% t{r.t:+.2f}(前{r.t1:+.1f}/後{r.t2:+.1f}) 勝ち年{int(r.wy)}/{int(r.ny)}{' ⚠ロールオーバー帯' if r.roll else ''}")
print(f'総セル{len(R)} 判定=t>3×前後半t>1.5×勝ち年8割×ネット>0(コスト{COST}%)')
print(f'\n== 合格 {len(ok)} (ロールオーバー帯を除くと {len(ok[~ok.roll])}) ==')
show(ok.sort_values('t',ascending=False))
print('\n== 参考: ロールオーバー帯を除いたt上位10(合格不問) ==')
show(R[~R.roll].sort_values('t',ascending=False).head(10))
print('\n== 4年XMデータで見えた候補の10年成績 ==')
for lab,c,h,m,side in [('ロンドン10:00買(値決め直前の前)','ロンドン',10,0,'買'),('ロンドン01:30買','ロンドン',1,30,'買'),('ロンドン02:45売','ロンドン',2,45,'売'),('ロンドン10:15売(実弾)','ロンドン',10,15,'売'),('UTC20:45売(NY引け前)','UTC',20,45,'売')]:
    print(' ',lab); show(R[(R.clock==c)&(R.h==h)&(R.m==m)&(R.side==side)].sort_values('k'))
