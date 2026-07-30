# -*- coding: utf-8 -*-
"""_bt_fade_monthly.py — 売りフェード 現行モデルの月別損益（過去10年・2026-07-31）。

現行モデル＝daytrade_paper.py に実装済みの構成:
  前日+6%以上 × 貸借○ × 張り付き除外(レンジ>5%) × 出来高6倍未満 × 25MA乖離80%未満
  × **ATR5%以上 × 25MA乖離12%以上**（2026-07-31 10c1b29）× 株価5,000円以下
  → 25MA乖離とATR%の順位平均で上位2本 → 寄付成行で空売り → 引成買戻し ／ 1玉50万

集計月は **建てた月（ent）** を使う。シグナルは前日の大引けに出るので、月末のシグナルは
翌月の損益になる（口座の入出金と揃うのはこちら）。

実行: python -X utf8 _bt_fade_monthly.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SIZE, NPICK = 500_000, 2

D = pd.read_pickle("_fade_deep.pkl")
D = D[(D.gain >= 6.0) & (D.vr < 6.0) & (D.dev < 80.0)]
D = D[(D.atr >= 5.0) & (D.dev >= 12.0)].copy()          # 現行の下限
r = None
for c in ("dev", "atr"):
    x = D.groupby("sig")[c].rank(ascending=False, pct=True)
    r = x if r is None else r + x
D["mix"] = r / 2
D = D.sort_values(["sig", "mix"])
D["rk"] = D.groupby("sig").cumcount() + 1
D = D[D.rk <= NPICK].copy()
D["sh"] = (SIZE / D.o1 // 100 * 100).astype(int)
D = D[(D.sh > 0) & (D.px * 100 <= SIZE)].copy()
D["yen"] = (D.o1 - D.c1) * D.sh
D["ey"] = D.ent.str[:4].astype(int)
D["em"] = D.ent.str[5:7].astype(int)

print(f"[data] {len(D):,}玉 / {D.sig.nunique():,}日 / 建て月 {D.ent.min()}〜{D.ent.max()}")
print(f"[全期間] 勝率{(D.pnl>0).mean()*100:.1f}% ／ 総額{D.yen.sum():+,.0f}円 "
      f"／ 1玉平均{D.yen.mean():+,.0f}円\n")

M = D.pivot_table(index="ey", columns="em", values="yen", aggfunc="sum")
YS = sorted(D.ey.unique())

print("=" * 152)
print("① 月別損益（円・建てた月ベース）")
print("=" * 152)
print(f"  {'年':>6}" + "".join(f"{m:>11}月" for m in range(1, 13)) + f"{'年計':>14}")
for y in YS:
    line = f"  {y:>6}"
    for m in range(1, 13):
        v = M.loc[y, m] if (y in M.index and m in M.columns and pd.notna(M.loc[y, m])) else None
        line += f"{'—':>12}" if v is None else f"{v:>+12,.0f}"
    line += f"{D[D.ey == y].yen.sum():>+14,.0f}"
    print(line)
print("  " + "-" * 150)
line = f"  {'月計':>6}"
for m in range(1, 13):
    line += f"{D[D.em == m].yen.sum():>+12,.0f}"
print(line + f"{D.yen.sum():>+14,.0f}")

print("\n" + "=" * 152)
print("② 月ごとの性格（季節性・10年分をまとめたもの）")
print("=" * 152)
print(f"  {'月':>4}{'玉数':>7}{'勝率':>8}{'PF':>7}{'10年合計':>13}{'月平均':>12}{'勝ち月':>8}{'最悪の月':>13}{'最良の月':>13}")
for m in range(1, 13):
    g = D[D.em == m]
    if not len(g):
        continue
    mm = g.groupby("ey").yen.sum()
    p = g.pnl
    loss = -p[p < 0].sum()
    print(f"  {m:>3}月{len(g):>7}{(p>0).mean()*100:>7.1f}%"
          f"{(p[p>0].sum()/loss if loss else np.inf):>7.2f}{g.yen.sum():>+12,.0f}円"
          f"{mm.mean():>+11,.0f}円{int((mm>0).sum()):>5}/{len(mm):<2}"
          f"{mm.min():>+12,.0f}円{mm.max():>+12,.0f}円")

print("\n" + "=" * 152)
print("③ 分布")
print("=" * 152)
mo = D.groupby("ent").yen.sum().sort_values()
print(f"  月数 {len(mo)}ヶ月 ／ 勝ち月 {int((mo>0).sum())}ヶ月 ({(mo>0).mean()*100:.0f}%) "
      f"／ 中央値 {mo.median():+,.0f}円 ／ 月平均 {mo.mean():+,.0f}円")
print(f"  +20万超え {int((mo>200000).sum())}ヶ月 ／ プラス {int((mo>0).sum())} ／ "
      f"マイナス {int((mo<0).sum())} ／ -10万割れ {int((mo<-100000).sum())}ヶ月 ／ "
      f"-20万割れ {int((mo<-200000).sum())}ヶ月")
print(f"\n  ワースト10: " + " / ".join(f"{k} {v:+,.0f}" for k, v in mo.head(10).items()))
print(f"\n  ベスト10  : " + " / ".join(f"{k} {v:+,.0f}" for k, v in mo.tail(10)[::-1].items()))
print(f"\n  連続マイナス月の最長: ", end="")
run = best = 0
for v in D.groupby("ent").yen.sum().sort_index():
    run = run + 1 if v < 0 else 0
    best = max(best, run)
print(f"{best}ヶ月")
