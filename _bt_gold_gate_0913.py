"""金15分ショートの自己判断ゲート: 直近N回(実約定)の平均がしきい値以上の時だけ次を撃つ。紙でも記録は続くので判定は常に更新される。"""
import pandas as pd, numpy as np
g=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']   # $/oz/回 net(2016-2026)
def stats(x,lab):
    y=x.groupby(x.index.year).sum(); print(f'{lab:28s} n={len(x):4d} 合計{x.sum():+7.1f}$ 勝ち年{int((y>0).sum())}/{len(y)} 2016-24合計{x[x.index.year<=2024].sum():+7.1f} 2025-26合計{x[x.index.year>=2025].sum():+7.1f} 最悪年{y.min():+.0f}')
stats(g,'ゲート無し')
for N in [20,40,60,120]:
    for thr in [0.0,0.1,0.2]:
        m=g.rolling(N).mean().shift(1)          # 直近N回の平均(紙で更新)
        on=m>thr; x=g[on.fillna(False)]
        stats(x,f'直近{N}回平均>{thr:.1f}$で稼働(稼働率{on.mean()*100:.0f}%)')
