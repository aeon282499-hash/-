"""9/19 本人「ロット張る？ロスカットする？」→ 現行設定(8750/31250/50000・追加×2.0・金2.5万ゲート)を丸ごと×kした時の停止確率/DD/E[log]。"""
import pandas as pd, numpy as np, sys
src=open('_bt_xm_grid_0917.py',encoding='utf-8').read().split("print(f'== コスト=")[0]; exec(src)
X=Xm(2.0)
print(f'B0={B0} 全停止={25000} 期間 {X.index.min().date()}〜{X.index.max().date()} n={len(X)}')
print('倍率k | E[log] 5年中央 下位10% 停止% DD | 1年中央 下位10% | 1年停止%')
def ev2(X,units,xg,seed=5):
    r=ev(X,units,xg,seed)
    rng=np.random.default_rng(7); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; st=0
    for _ in range(400):
        P=bl[rng.choice(yrs)]; B,x,dd=run(P,units,xg); st+=x
    r['stop1y']=st/4; return r
for k in [1.0,1.25,1.5,2.0,2.5,3.0,4.0]:
    u=(8750/k,31250/k,50000/k); r=ev2(X,u,25000/k)
    print(f"×{k:<4.2f}| {fmt(r)} | 1年停止{r['stop1y']:3.0f}%")
print('\n参考: 最悪の1夜(現行×1・6.9枚時)と直近の実弾ペース')
# 直近1年の年別 実現額(現行×1・残高連動なし・B0固定)
for y in sorted(set(X.index.year)):
    P=X[X.index.year==y]; u=(8750,31250,50000)
    pnl=(np.floor(B0/25000)/100*100*P.g*JPY+np.floor(B0/8750*10)/10*P.j+np.floor(B0/31250)*P.u+np.floor(B0/50000)*P.d)
    print(f'  {y}: 合計{pnl.sum():+8.0f}円 最悪日{pnl.min():+7.0f} 最良日{pnl.max():+7.0f} 稼働日{(pnl!=0).sum()}')
