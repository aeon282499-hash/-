# -*- coding: utf-8 -*-
"""_bt_earnings_yoy_gate.py — 「直近が増益の銘柄だけ買う」の頑健性検査（2026-07-31・本番無変更）。

_bt_earnings_fundamentals.py でファンダ軸を総当たりしたところ、ほぼ全部が
リターンを落として不採用だった。ただ1つだけ **リターンをほとんど落とさずに
DDを大きく下げる** ものが出た。決算持ち越しの実際の制約はリターンではなく
DD（10年最大DD -448万 = 資本800万の-56% で生存線-30%を突破）なので、
これは他の軸と性質が違う。採否を決めるための検査だけをまとめたのが本ファイル。

検査項目（記憶のバー）:
  ① 閾値の面が高原か（針でないか）
  ② 枠を振って符号が反転しないか
  ③ 素の層別と整合するか
  ④ 年別で何勝何敗か／どの年で稼ぎどの年で損するか

実行: python -X utf8 _bt_earnings_yoy_gate.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import _bt_earnings_fundamentals as B   # E（特徴量つき候補）と sim をそのまま使う

E = B.E
ORD = ["d0", "rsi"]
DD_BUDGET = 2_400_000        # 口座800万の-30%


def run(mask, slots=8):
    P = B.sim(E[mask].sort_values(ORD), slots=slots)
    y = P["pnl"] * 1_000_000 / 100
    c = y.cumsum()
    dd = float((c - c.cummax()).min())
    yr = (P.groupby("year")["pnl"].sum() * 1e4).reindex(range(2016, 2027), fill_value=0)
    return dict(n=len(P), tot=float(y.sum()), dd=dd, ratio=float(y.sum()) / abs(dd),
                worst_year=float(yr.min()), pos=int((yr > 0).sum()),
                mean=float(P.pnl.mean()), sd=float(P.pnl.std()), yr=yr,
                norm=float(y.sum()) * DD_BUDGET / abs(dd))   # 同DD予算に正規化した10年計


ALL = pd.Series(True, index=E.index)
BASE = run(ALL)
print(f"[基準] 現行 n={BASE['n']:,} 10年{BASE['tot']/1e4:+,.0f}万 DD{BASE['dd']/1e4:+,.0f}万 "
      f"比{BASE['ratio']:.2f} 同DD予算換算{BASE['norm']/1e4:+,.0f}万")
print(f"[カバー率] op_yoyが付いた候補 {E.op_yoy.notna().mean()*100:.0f}%"
      f"（欠測{int(E.op_yoy.isna().sum()):,}件はフェイルオープンで通過）")

print("\n① 閾値の面（高原か針か）")
print(f"  {'閾値':<12}{'件数':>7}{'10年計':>11}{'最大DD':>11}{'比':>7}{'最悪年':>10}{'同DD予算換算':>15}")
for thr in (-20, -10, 0, 5, 10, 15, 20, 30, 50):
    r = run(E.op_yoy.isna() | (E.op_yoy >= thr))
    print(f"  YoY>={thr:>+4}%{'':<4}{r['n']:>7}{r['tot']/1e4:>10,.0f}万{r['dd']/1e4:>10,.0f}万"
          f"{r['ratio']:>7.2f}{r['worst_year']/1e4:>9,.0f}万{r['norm']/1e4:>14,.0f}万")

print("\n② 枠を振る（符号が反転しないか）")
print(f"  {'枠':<5}{'現行 同DD換算':>16}{'YoY>=15% 同DD換算':>20}{'差':>12}")
M15 = E.op_yoy.isna() | (E.op_yoy >= 15)
for s in (5, 8, 10, 12, 16):
    a, b = run(ALL, s), run(M15, s)
    print(f"  {s:<5}{a['norm']/1e4:>15,.0f}万{b['norm']/1e4:>19,.0f}万{(b['norm']-a['norm'])/1e4:>+11,.0f}万")

print("\n③ 素の層別（選定前・U字なら機構の説明が弱い）")
d = E[E.op_yoy.notna()].copy()
d["b"] = pd.qcut(d.op_yoy, 5, labels=False, duplicates="drop")
for b, g in d.groupby("b"):
    neg = abs(g.gap[g.gap <= 0].sum())
    pos = g.gap[g.gap > 0].sum()
    print(f"  Q{int(b)+1} YoY{g.op_yoy.min():>+8.0f}〜{g.op_yoy.max():>+8.0f}%  n={len(g):>4}"
          f"  gap{g.gap.mean():>+6.2f}%  SD{g.gap.std():>5.2f}  PF{(pos/neg if neg else 9):>4.2f}")

print("\n④ 年別（何勝何敗か・どこで稼ぎどこで損するか）")
G = run(M15)
print(f"  {'':<10}" + "".join(f"{y:>7}" for y in range(2016, 2027)))
print(f"  {'現行':<10}" + "".join(f"{v/1e4:>+7.0f}" for v in BASE["yr"]))
print(f"  {'YoY>=15%':<10}" + "".join(f"{v/1e4:>+7.0f}" for v in G["yr"]))
diff = (G["yr"] - BASE["yr"]) / 1e4
print(f"  {'差':<10}" + "".join(f"{v:>+7.0f}" for v in diff))
print(f"  勝った年 {int((diff > 0).sum())}/11 ・ 最悪年 {BASE['worst_year']/1e4:+,.0f}万 → "
      f"{G['worst_year']/1e4:+,.0f}万 ・ 1件平均 {BASE['mean']:+.2f}% → {G['mean']:+.2f}% "
      f"・ SD {BASE['sd']:.2f} → {G['sd']:.2f}")

print("\n[読み方] リターンはほぼ同じでDDだけ下がる＝同じ痛みでより大きく張れる。")
print("         ただしDDと最悪年は極値統計でブレやすい。年別が5勝6敗である点は割り引くこと。")
