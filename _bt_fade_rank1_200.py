# -*- coding: utf-8 -*-
"""_bt_fade_rank1_200.py — 「#1だけを200万で打つ」の10年・年別・月別（2026-08-28 本人依頼）。
_bt_fade_ranktilt.py と同じプール/選定/約定前提（寄成→引成・値がさカット100万基準固定）。
比較: 100/100(現行) / 130/70 / 200/0 / 100/0(参考)。年利=年間円÷投入資金（200万 or 100万）。
実行: python -X utf8 _bt_fade_rank1_200.py
"""
import numpy as np, pandas as pd
CAP = 1_000_000
D = pd.read_pickle("_fade_pool_v5_100.pkl")
BASE = (D.gain >= 7.0) & (D.vr < 6.0) & (D.atr >= 5.0) & (D.dev >= 12.0) \
    & (D.tov >= 3e8) & (D.vol_avg >= 100_000) & (D.rng > 5.0) & (D.px * 100 <= CAP)
d = D[BASE].copy()
r = None
for c in ("dev", "atr"):
    x = d.groupby("sig")[c].rank(ascending=False, pct=True)
    r = x if r is None else r + x
d["mix"] = r / 2
d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
d["rk"] = d.groupby("sig").cumcount() + 1
d = d[d.rk <= 2].copy()
d["y"] = d.ent.str[:4].astype(int); d["ym"] = d.ent.str[:7]

def pf(x):
    n = -x[x <= 0].sum(); return x[x > 0].sum() / n if n else float("inf")

def sim(size1, size2):
    sh = np.where(d.rk == 1, size1 / d.px // 100 * 100, size2 / d.px // 100 * 100).astype(int)
    yen = pd.Series(d.pnl.to_numpy() / 100 * sh * d.o1.to_numpy(), index=d.index)
    return yen, pd.Series(sh, index=d.index)

CONFIGS = [("100/100 現行", 1_000_000, 1_000_000, 2_000_000),
           ("130/70", 1_300_000, 700_000, 2_000_000),
           ("200/0 ★#1だけ200万", 2_000_000, 0, 2_000_000),
           ("100/0 参考 #1だけ100万", 1_000_000, 0, 1_000_000)]
years = sorted(d.y.unique())
yearly = {}
for label, s1, s2, capital in CONFIGS:
    yen, sh = sim(s1, s2)
    t = pd.DataFrame({"y": d.y, "ym": d.ym, "ent": d.ent, "rk": d.rk, "yen": yen, "sh": sh, "px": d.px})
    t = t[t.yen != 0]
    yy = t.groupby("y").yen.sum(); mo = t.groupby("ym").yen.sum(); day = t.groupby("ent").yen.sum()
    eq = day.sort_index().cumsum(); dd = (eq - eq.cummax()).min()
    big = int((t[t.rk == 1].sh > 5000).sum())
    print("=" * 100)
    print(f"■ {label}  資金{capital//10000}万  10年 {t.yen.sum():+,.0f}円  PF{pf(t.yen):.2f}  玉{len(t)}  勝率{(t.yen>0).mean()*100:.1f}%")
    print(f"   年平均 {yy.mean():+,.0f}円 = 年利{yy.mean()/capital*100:+.1f}%  最悪日 {day.min():+,.0f}  最悪月 {mo.min():+,.0f}  "
          f"-20万超月 {(mo < -200_000).sum()}回  -40万超月 {(mo < -400_000).sum()}回  最大DD {dd:+,.0f}  勝ち年 {(yy>0).sum()}/{len(yy)}  "
          f"#1で5,000株超(51単元規制) {big}玉")
    print("   年別:  " + "  ".join(f"{y}:{yy.get(y,0)/10000:+.0f}万({yy.get(y,0)/capital*100:+.0f}%)" for y in years))
    yearly[label] = (yy, mo, capital)
# 月別表（200/0 と 現行）
for label in ("200/0 ★#1だけ200万", "100/100 現行"):
    yy, mo, capital = yearly[label]
    print("=" * 100); print(f"■ 月別（万円） {label}")
    print("   年   " + " ".join(f"{m:>5}月" for m in range(1, 13)) + "     年計   年利")
    for y in years:
        cells = [mo.get(f"{y}-{m:02d}", 0) / 10000 for m in range(1, 13)]
        print(f"   {y} " + " ".join(f"{c:>+6.0f}" for c in cells) + f"  {yy.get(y,0)/10000:>+7.0f}万 {yy.get(y,0)/capital*100:>+5.0f}%")
