# -*- coding: utf-8 -*-
"""_bt_earnings_tail_check.py — 決算ボラゲートは «爆益» を切っていないか（2026-07-31・本番無変更）。

本人「第一工業製薬 ひろえないか 爆益なのに」。
4461は決算ボラ1.5%（＝普段まったく動かない銘柄）なのに 2026-07-29 の決算で
+16.65% ギャップし、PEAD延長で8/5まで保有中。ゲートを入れるとこれを買わない。

決算はテール依存（上位20玉で利益の84%）なので、「エッジがゼロの帯を切っている」
つもりが「テールごと切っている」なら致命傷になる。直接検査する。
あわせて救済条項（低ボラでも直前に大きく売られていたら拾う）も測る。

実行: python -X utf8 _bt_earnings_tail_check.py
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

import _bt_earnings_fundamentals as B
import _bt_earnings_vol_axis as V

E = V.E
RAW = pd.read_csv("_earnings_events_rich2.csv").sort_values(["ticker", "d0"])
RAW["vol"] = RAW.groupby("ticker")["gap"].transform(
    lambda s: s.abs().shift(1).expanding(min_periods=3).median())
C = RAW[(RAW.rsi <= 55) & (RAW.runup5 < -3.0) & (RAW.tov20 >= 7.5e8) & (RAW.price <= 10000)].copy()
C["pnl"] = np.where((C.gap > 8.0) & C.r5.notna(), C.r5, C.gap)
C = C[C.pnl.notna() & C.vol.notna()]

print("=" * 96)
print("① 大勝ち玉トップ20のうち何本が残るか（テールを切っていないかの直接検査）")
print("=" * 96)
top = C.nlargest(20, "pnl")
print(f"  {'閾値':<12}{'残る候補':>10}{'生の合計':>16}{'トップ20の残存':>16}{'その利益の残存率':>18}")
for t in (1.5, 2.0, 2.5, 3.0, 3.5):
    k = C[C.vol >= t]
    keep = top[top.vol >= t]
    print(f"  ボラ>={t:.1f}%{'':<3}{len(k):>10,}{k.pnl.sum()*1e4:>+15,.0f}円"
          f"{len(keep):>13}/20{keep.pnl.sum()/top.pnl.sum()*100:>16.0f}%")

print("\n② 低ボラ帯(<2.5%)の中身を全部開ける（«爆益を取り逃す» の実額）")
lo = C[C.vol < 2.5]
big, bad = lo[lo.pnl >= 8], lo[lo.pnl <= -8]
mid = lo[(lo.pnl > -8) & (lo.pnl < 8)]
print(f"  低ボラ帯 {len(lo):,}件 合計 {lo.pnl.sum()*1e4:>+12,.0f}円 ＝ ほぼゼロ")
print(f"    +8%以上の爆益 {len(big):>4}件 {big.pnl.sum()*1e4:>+12,.0f}円  ← 第一工業製薬型")
print(f"    -8%以下の爆損 {len(bad):>4}件 {bad.pnl.sum()*1e4:>+12,.0f}円  ← 同じ帯から出る")
print(f"    その他       {len(mid):>4}件 {mid.pnl.sum()*1e4:>+12,.0f}円")
hi = C[C.vol >= 2.5]
print(f"  高ボラ帯 {len(hi):,}件 合計 {hi.pnl.sum()*1e4:>+12,.0f}円")

print("\n③ 救済条項: 低ボラでも «直前5日で大きく売られていたら» 拾う")
print("  （第一工業製薬は runup5=-19.4%）まず素の層別:")
for a, b, tag in [(None, -15, "-15%より下"), (-15, -10, "-15〜-10%"),
                  (-10, -6, "-10〜-6%"), (-6, None, "-6〜-3%")]:
    m = pd.Series(True, index=lo.index)
    if a is not None:
        m &= lo.runup5 >= a
    if b is not None:
        m &= lo.runup5 < b
    g = lo[m]
    if len(g) < 20:
        continue
    print(f"    {tag:<12}{len(g):>5}件 平均{g.pnl.mean():>+6.2f}% 合計{g.pnl.sum()*1e4:>+12,.0f}円")


def run(mask, slots=8):
    P = B.sim(E[mask].sort_values(["d0", "rsi"]), slots=slots)
    y = P["pnl"] * 1_000_000 / 100
    c = y.cumsum()
    dd = float((c - c.cummax()).min())
    yr = (P.groupby("year")["pnl"].sum() * 1e4).reindex(range(2016, 2027), fill_value=0)
    return dict(n=len(P), tot=float(y.sum()), dd=dd, ratio=float(y.sum()) / abs(dd),
                pos=int((yr > 0).sum()), e1=float(yr[yr.index <= 2021].sum()),
                e2=float(yr[yr.index >= 2022].sum()))


print("\n  8枠シムに乗せると（素の層別で効いて見えても枠を食う）:")
print(f"    {'設定':<30}{'件数':>6}{'10年計':>11}{'最大DD':>10}{'比':>7}{'陽性':>7}")
V20 = E.egap_vol.isna() | (E.egap_vol >= 2.0)
for m, tag in [(pd.Series(True, index=E.index), "現行"),
               (V20, "ボラ>=2.0%のみ"),
               (V20 | (E.runup5 <= -8), "ボラ>=2.0% or 直前-8%超"),
               (V20 | (E.runup5 <= -10), "ボラ>=2.0% or 直前-10%超"),
               (V20 | (E.runup5 <= -12), "ボラ>=2.0% or 直前-12%超")]:
    r = run(m)
    print(f"    {tag:<30}{r['n']:>6}{r['tot']/1e4:>10,.0f}万{r['dd']/1e4:>9,.0f}万"
          f"{r['ratio']:>7.2f}{r['pos']:>5}/11")

print("\n[結論] 救済条項は全部マイナス＝良い玉を押しのけて枠を食う。")
print("       ただし閾値を2.0%まで下げればトップ20の利益の94%が残る（2.5%だと81%）。")
print("       → 最終推奨は ボラ>=2.0% 単独。第一工業製薬(1.5%)は諦めるコスト。")
