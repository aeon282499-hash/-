# -*- coding: utf-8 -*-
"""極み買い(3枠×100万) と フェード(①100万/②50万) の年別損益・年利（2021-2026）。2026-09-04。"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
_src = open("_bt_kiwami_resid_vol_0903.py", encoding="utf-8").read().split('print("\n" + "=" * 122); print("① 業種指数')[0]
exec(_src)
cap_buy = 3_000_000
yb = R0.groupby("y").agg(n=("yen", "size"), yen=("yen", "sum"), win=("pnl", lambda x: (x > 0).mean() * 100))
print("■ 極み買い 3枠×100万（拘束300万）")
for y, r in yb.iterrows():
    print(f"  {y}: {int(r.n):>4}玉 勝率{r.win:5.1f}% 損益{r.yen/1e4:+8.1f}万 年利{r.yen/cap_buy*100:+6.1f}%")
print(f"  2021-2025計 {yb.loc[2021:2025].yen.sum()/1e4:+.1f}万 / 5年平均 年利{yb.loc[2021:2025].yen.sum()/5/cap_buy*100:+.1f}%")
exec(open("_bt_fade_untested_sweep.py", encoding="utf-8").read().split('print("=" * 118); print("■ 基準')[0])
d0 = select(P); cap = np.where(d0.rk == 1, S1, S2); sh = (cap / d0.px // 100 * 100).astype(int); sh = np.where((d0.rk == 2) & (d0.px * 100 > S2), 0, sh)
d0["yen"] = d0.pnl / 100 * sh * d0.o1
cap_fade = S1 + S2
yf = d0[d0.yen != 0].groupby("y").agg(n=("yen", "size"), yen=("yen", "sum"), win=("pnl", lambda x: (x > 0).mean() * 100))
print("\n■ フェード ①100万/②50万（1日の拘束150万・日計り）")
for y, r in yf.iterrows():
    print(f"  {y}: {int(r.n):>4}玉 勝率{r.win:5.1f}% 損益{r.yen/1e4:+8.1f}万 年利{r.yen/cap_fade*100:+6.1f}%")
print(f"  2021-2025計 {yf.loc[2021:2025].yen.sum()/1e4:+.1f}万 / 5年平均 年利{yf.loc[2021:2025].yen.sum()/5/cap_fade*100:+.1f}%")
print("\n[done]")
