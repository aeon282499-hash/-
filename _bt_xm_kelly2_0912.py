"""2Dグリッド: 金X × 日経X。実パス(2016-05〜)＋年ブロックブートストラップ300本。"""
import pandas as pd, numpy as np
JPY=148.0
g=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']
jp=pd.read_pickle('_xm_night_legs.pkl')['JP225Cash']; jp=jp[jp.pct.shift(1)<=0]; jp=jp[jp.index.dayofweek<4]
D=pd.DataFrame({'g':g,'j':jp.a*jp.net/100}).fillna(0); D=D[D.index>='2016-05-26']
def run(G,J,xg,xj,B0=50000,stop=25000,gmax=0.20,jmax=50.0):
    B=B0; peak=B; dd=0.0; stopped=False
    for i in range(len(G)):
        if B<stop: stopped=True
        if not stopped:
            lot=min(gmax,np.floor(B/xg)/100) if xg>0 else 0.0
            jl=min(jmax,np.floor(B/xj*10)/10) if xj>0 else 0.0
            B=max(0.0,B+lot*100*G[i]*JPY+jl*J[i])
        if B>peak: peak=B
        d=B/peak-1
        if d<dd: dd=d
    return B,dd,stopped
XG=[0,50000,30000,20000,15000,10000,5000]; XJ=[0,50000,25000,15000,10000,7000,5000,3500]
G=D.g.to_numpy(); J=D.j.to_numpy()
print('== 実パス 2016-05→2026-09 終値(万円)/DD% ・ 行=X金/0.01・列=X日経/lot ==')
print('        '+''.join(f'{x:>14,}' for x in XJ))
for xg in XG:
    print(f'{xg:>7,} '+''.join((lambda r:f'{r[0]/1e4:>8.1f}{r[1]*100:>4.0f}%{"停" if r[2] else " "}')(run(G,J,xg,xj)) for xj in XJ))
print('\n== 2025-01→ 終値(万円)/DD% ==')
G2=D['2025':].g.to_numpy(); J2=D['2025':].j.to_numpy()
print('        '+''.join(f'{x:>14,}' for x in XJ))
for xg in XG:
    print(f'{xg:>7,} '+''.join((lambda r:f'{r[0]/1e4:>8.1f}{r[1]*100:>4.0f}%{"停" if r[2] else " "}')(run(G2,J2,xg,xj)) for xj in XJ))
rng=np.random.default_rng(1); yrs=sorted(set(D.index.year)); blocks={y:(D[D.index.year==y].g.to_numpy(),D[D.index.year==y].j.to_numpy()) for y in yrs}
samples=[rng.choice(yrs,5) for _ in range(300)]
print('\n== ブートストラップ(5年・300本): 中央値(万円) / 停止率% / E[log成長] ==')
print('        '+''.join(f'{x:>18,}' for x in XJ))
best=None
for xg in XG:
    row=f'{xg:>7,} '
    for xj in XJ:
        ends=[];st=0
        for s in samples:
            Gs=np.concatenate([blocks[y][0] for y in s]); Js=np.concatenate([blocks[y][1] for y in s])
            B,dd,stp=run(Gs,Js,xg,xj); ends.append(B); st+=stp
        e=np.array(ends); el=np.mean(np.log(np.maximum(e,1)/50000)); row+=f'{np.median(e)/1e4:>8.1f}{st/3:>5.0f}%{el:>+5.2f}'
        if best is None or el>best[0]: best=(el,xg,xj,np.median(e),st/3)
    print(row)
print('\nE[log]最大:',best)
