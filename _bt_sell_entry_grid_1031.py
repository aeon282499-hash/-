# -*- coding: utf-8 -*-
"""_bt_sell_entry_grid_1031.py — 売りスイングの入口を厳しくする組み合わせ（2026-10-31 本人「急騰をさらに上げるとかRSI70以上とか」）
前日比 {3,4,5,7} × RSI {60,65,70,75} × 乖離 {4,6,8} の48通り。出口・枠・ゲートは本番のまま（_bt_sell_entry_surface_0922.py の面と同じ土台）。
前半(2001-16)で上位を選び後半(2017-26)で確認。検証のみ。
実行: python -X utf8 _bt_sell_entry_grid_1031.py > _log_sell_entry_grid_1031.txt 2>&1
"""
import numpy as np, pandas as pd, itertools, time
src = open("_bt_sell_entry_surface_0922.py", encoding="utf-8").read().split('CUR = GT & (CHG >= 3)')[0]
exec(src)
BASE = GT & ((RR >= 1.5) | (VR >= 2.0)) & (TOV >= 2e9) & (ATR <= 2.5) & (PX <= 10000)
def st2(R):
    d = st(R)
    if d is None: return None
    a, b = R[R.y <= 2016], R[R.y >= 2017]
    for nm, g in (("h1", a), ("h2", b)):
        d[nm + "_tot"] = g.yen.sum() / 1e4
        d[nm + "_pf"] = g.yen[g.yen > 0].sum() / max(-g.yen[g.yen <= 0].sum(), 1)
        d[nm + "_n"] = len(g)
    return d
rows = []
for chg, rsi, dev in itertools.product((3, 4, 5, 7), (60, 65, 70, 75), (4, 6, 8)):
    m = BASE & (CHG >= chg) & (RSI >= rsi) & (DEV >= dev)
    r = st2(run(m))
    if r is None: continue
    r.update(chg=chg, rsi=rsi, dev=dev); rows.append(r)
G = pd.DataFrame(rows)
cur = G[(G.chg == 3) & (G.rsi == 60) & (G.dev == 4)].iloc[0]
def line(r):
    return (f"| +{r.chg:g}% | {r.rsi:g} | +{r.dev:g}% | {int(r.n)} | {r.win:.0f}% | {r.pf:.2f} | {r.tot:+,.0f}万 | {r.dd:+.0f}万 | {r.worst:+.0f}万 | "
            f"{r.e1:+.0f}/{r.e2:+.0f}/{r.e3:+.0f}/{r.e4:+.0f} | {r.h1_pf:.2f} ({r.h1_tot:+.0f}万) | {r.h2_pf:.2f} ({r.h2_tot:+.0f}万) |")
HD = "| 前日比 | RSI | 乖離 | 玉 | 勝率 | PF | 26年合計 | 最大DD | 最悪年 | 時代01-08/09-16/17-21/22-26 | 前半PF(合計) | 後半PF(合計) |\n|---|---|---|---|---|---|---|---|---|---|---|---|"
log("# 売りスイング 入口を厳しくする48通り（出口・3枠×100万・日経25MAゲートは本番のまま）")
log(f"\n## 今の入口\n{HD}\n{line(cur)}")
log(f"\n## 本人の案\n{HD}")
for q in [(5, 60, 4), (7, 60, 4), (3, 70, 4), (3, 75, 4), (5, 70, 4), (5, 70, 6), (7, 70, 8)]:
    r = G[(G.chg == q[0]) & (G.rsi == q[1]) & (G.dev == q[2])]
    if len(r): log(line(r.iloc[0]))
log(f"\n## 全48通り 26年合計の順\n{HD}")
for _, r in G.sort_values("tot", ascending=False).iterrows(): log(line(r))
log(f"\n## 前半(2001-16)PFの上位8 → 後半(2017-26)で確認\n{HD}")
for _, r in G[G.h1_n >= 30].sort_values("h1_pf", ascending=False).head(8).iterrows(): log(line(r))
log(f"\n今の入口: 前半PF{cur.h1_pf:.2f} 後半PF{cur.h2_pf:.2f}。前半上位8の後半PF平均 {G[G.h1_n >= 30].sort_values('h1_pf', ascending=False).head(8).h2_pf.mean():.2f}")
log(f"\n## 合計÷DD（効率）の順 上位8\n{HD}")
G["eff"] = G.tot / -G.dd.clip(upper=-1)
for _, r in G.sort_values("eff", ascending=False).head(8).iterrows(): log(line(r))
log(f"\n[done] {time.time()-t0:.0f}s")
