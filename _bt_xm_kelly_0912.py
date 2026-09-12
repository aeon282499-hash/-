"""5万円を「最大に増やす」＝成長最適(Kelly)ロットの探索。金15分S(SL$10・Zero実コスト)＋日経夜(前夜≤0)を残高連動で複利。"""
import pandas as pd, numpy as np
JPY=148.0
g=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']            # $/oz/回
jp=pd.read_pickle('_xm_night_legs.pkl')['JP225Cash']
jp=jp[jp.pct.shift(1)<=0]                                        # 前夜フィルタ
jp=jp[jp.index.dayofweek<4]                                      # 月〜木
jpj=(jp.a*jp.net/100)                                            # 1lotあたり円
D=pd.DataFrame({'g':g,'j':jpj}).fillna(0)
D=D[D.index>='2016-05-26']
def kelly(s):
    mu,var=s.mean(),s.var(); return mu/var
print('== 金 Kelly (oz per 円→5万円での lot) ==')
for lab,s in [('全期間16-26',g),('16-24',g[g.index.year<=2024]),('22-24',g[(g.index.year>=2022)&(g.index.year<=2024)]),('25-26',g[g.index.year>=2025]),('2026',g[g.index.year==2026])]:
    f=kelly(s)/JPY; print(f'{lab:10s} mean={s.mean():+.3f} sd={s.std():.2f}  full-Kelly lot@5万={f*50000/100:.3f}  (X円/0.01={100/f/100:,.0f})')
print('== 日経 Kelly (lot per 円→5万円での lot) ==')
for lab,s in [('全期間',jpj),('16-24',jpj[jpj.index.year<=2024]),('25-26',jpj[jpj.index.year>=2025])]:
    f=kelly(s); print(f'{lab:10s} mean={s.mean():+.0f}円/lot sd={s.std():.0f}  full-Kelly lot@5万={f*50000:.2f} (X円/lot={1/f:,.0f})')

def run(D,xg,xj,B0=50000,stop=25000,gmax=0.20,jmax=5.0):
    G=D.g.to_numpy(); J=D.j.to_numpy(); B=B0; peak=B; dd=0.0; stopped=False
    for i in range(len(G)):
        if B<stop: stopped=True
        if not stopped:
            lot=min(gmax,np.floor(B/xg)/100) if xg>0 else 0.0
            jl=min(jmax,np.floor(B/xj*10)/10) if xj>0 else 0.0
            B+=lot*100*G[i]*JPY + jl*J[i]
            B=max(B,0.0)
        if B>peak: peak=B
        d=B/peak-1
        if d<dd: dd=d
    return B,dd,stopped
print('\n== 実パス 5万スタート・残高連動・全停止2.5万 (終値残高/最大DD%/停止) ==')
grid=[(20000,200000),(10000,100000),(7500,75000),(5000,50000),(3500,35000),(2500,25000),(1500,15000),(1000,10000)]
per=[('2016-05〜2026-09',D),('2017-01〜2019-12(薄い年)',D['2017':'2019']),('2022-01〜2024-12',D['2022':'2024']),('2025-01〜',D['2025':]),('2026-01〜',D['2026':])]
hdr='X金/0.01  X日経/lot | '+' | '.join(p[0] for p in per); print(hdr)
for xg,xj in grid:
    row=f'{xg:>7,} {xj:>10,} |'
    for lab,P in per:
        B,dd,st=run(P,xg,xj); row+=f' {B:>10,.0f} {dd*100:5.0f}%{"停" if st else " "} |'
    print(row)
print('\n== 年ブロック・ブートストラップ(11年から5年を復元抽出・500本): 中央値/下位10%/停止率 ==')
rng=np.random.default_rng(0); yrs=sorted(set(D.index.year)); blocks={y:D[D.index.year==y] for y in yrs}
for xg,xj in grid:
    ends=[];stops=0
    for _ in range(500):
        P=pd.concat([blocks[y] for y in rng.choice(yrs,5)]).reset_index(drop=True)
        B,dd,st=run(P,xg,xj); ends.append(B); stops+=st
    e=np.array(ends); print(f'X金={xg:>6,} X日経={xj:>7,}: 中央値{np.median(e):>10,.0f} 下位10%{np.percentile(e,10):>9,.0f} 上位10%{np.percentile(e,90):>11,.0f} 停止率{stops/500*100:4.0f}%  E[log成長/5年]={np.mean(np.log(np.maximum(e,1)/50000)):+.2f}')
print('\n== 金だけ/日経だけの寄与(現行5000/50000・全期間) ==')
for lab,xg,xj in [('金のみ',5000,0),('日経のみ',0,50000),('両方',5000,50000)]:
    B,dd,st=run(D,xg,xj); print(f'{lab}: 終値{B:,.0f} DD{dd*100:.0f}% {"停止" if st else ""}')
