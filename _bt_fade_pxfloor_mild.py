# -*- coding: utf-8 -*-
"""_bt_fade_pxfloor_mild.py — 低位株を抜くとどのくらいマイルドか（100万×1番・2026-08-06）。

7/31の株価下限検証は「儲け」の軸（旧構成・上位8相当）。今回は本人の問い
「メンタル的にマイルドになるか」を新構成(rank1×100万)で、傷の指標中心に測る。
下限を掛けたら日次で再ランク（本番がフロアを持った場合の挙動と同じ）。

実行: python -X utf8 _bt_fade_pxfloor_mild.py
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

D = pd.read_pickle("_fade_pool_v5_100.pkl")
BASE = (D.gain >= 7.0) & (D.vr < 6.0) & (D.atr >= 5.0) & (D.dev >= 12.0) \
       & (D.tov >= 3e8) & (D.vol_avg >= 100_000) & (D.rng > 5.0)


def run(floor: float | None):
    d = D[BASE & ((D.px >= floor) if floor else True)].copy()
    r = None
    for c in ("dev", "atr"):
        x = d.groupby("sig")[c].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    d = d[d.rk <= 1].copy()
    d["sh"] = (1_000_000 / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    d["ym"] = d.ent.str[:7]
    return d


print(f"{'下限':<10}{'件数':>6}{'10年計':>10}{'年平均':>9}{'前半':>9}{'後半':>9}"
      f"{'最悪月':>9}{'最悪1玉':>9}{'2番目':>9}{'3番目':>9}{'p1%玉':>8}{'勝ち月':>7}")
for floor, lab in ((None, "なし(現行)"), (100, "100円"), (300, "300円"), (500, "500円")):
    d = run(floor)
    ym = d.groupby("ym").yen.sum()
    e1 = d[d.y <= 2021].yen.sum(); e2 = d[d.y >= 2022].yen.sum()
    w = d.yen.sort_values()
    print(f"{lab:<10}{len(d):>6}{d.yen.sum()/1e4:>+9,.0f}万{d.yen.sum()/10/1e4:>+8,.0f}万"
          f"{e1/1e4:>+8,.0f}万{e2/1e4:>+8,.0f}万{ym.min()/1e4:>+8,.0f}万"
          f"{w.iloc[0]/1e4:>+8,.1f}万{w.iloc[1]/1e4:>+8,.1f}万{w.iloc[2]/1e4:>+8,.1f}万"
          f"{d.pnl.quantile(0.01):>+7.1f}%{(ym>0).mean()*100:>6.0f}%")

# 低位株玉そのものの素顔（rank1で選ばれた低位株）
cur = run(None)
low = cur[cur.px < 300]
print(f"\n[現行rank1のうち株価300円未満] {len(low)}件({len(low)/len(cur)*100:.1f}%) "
      f"計{low.yen.sum()/1e4:+,.0f}万 勝率{(low.pnl>0).mean()*100:.0f}% "
      f"最悪{low.yen.min()/1e4:+,.1f}万 最高{low.yen.max()/1e4:+,.1f}万")
ym_all = cur.groupby("ym").yen.sum()
worst3 = ym_all.nsmallest(3)
for ymm, v in worst3.items():
    sub = cur[cur.ym == ymm]
    lw = sub[sub.px < 300].yen.sum()
    print(f"  最悪月{ymm} {v/1e4:+,.0f}万 のうち低位株寄与 {lw/1e4:+,.0f}万")
