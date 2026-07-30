# -*- coding: utf-8 -*-
"""_bt_earnings_combo.py — 決算ボラゲート × 増益ゲートの併用効果（2026-07-31・本番無変更）。

今日の検証で生き残った候補は2つだけ:
  A. 決算ボラ >= 3.0%  … その銘柄が決算でよく動く体質か（全バー通過・本命）
  B. 営業利益YoY >= 15% … 直近が増益か（DDだけ大きく下げる・年別5勝6敗で保留）

両方とも「候補を削る」方向なので、重ねると枠が埋まらなくなる恐れがある
（決算はテール依存＝枠を埋め続けるのが生命線）。効くのか、削りすぎなのかを測る。

先に2軸が独立かを見る。相関していれば併用の意味は薄い。

実行: python -X utf8 _bt_earnings_combo.py
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

warnings.filterwarnings("ignore")

import _bt_earnings_fundamentals as B
import _bt_earnings_vol_axis as V

E = V.E.copy()                      # 決算ボラ + ファンダ特徴量つき候補
SLOTS = 8
DD_BUDGET = 2_400_000


def run(mask, slots=SLOTS):
    P = B.sim(E[mask].sort_values(["d0", "rsi"]), slots=slots)
    if len(P) < 50:
        return None
    y = P["pnl"] * 1_000_000 / 100
    c = y.cumsum()
    dd = float((c - c.cummax()).min())
    yr = (P.groupby("year")["pnl"].sum() * 1e4).reindex(range(2016, 2027), fill_value=0)
    return dict(n=len(P), tot=float(y.sum()), dd=dd, ratio=float(y.sum()) / abs(dd),
                norm=float(y.sum()) * DD_BUDGET / abs(dd), wr=(P.pnl > 0).mean() * 100,
                pos=int((yr > 0).sum()), worst=float(yr.min()),
                e1=float(yr[yr.index <= 2021].sum()), e2=float(yr[yr.index >= 2022].sum()),
                yr=yr)


HDR = (f"  {'設定':<26}{'件数':>6}{'10年計':>11}{'最大DD':>10}{'比':>7}{'勝率':>7}"
       f"{'陽性':>6}{'最悪年':>9}{'前半':>9}{'後半':>10}{'同DD換算':>11}")


def line(tag, r, b=None):
    if r is None:
        print(f"  {tag:<26}  — （件数不足）")
        return
    print(f"  {tag:<26}{r['n']:>6}{r['tot']/1e4:>10,.0f}万{r['dd']/1e4:>9,.0f}万{r['ratio']:>7.2f}"
          f"{r['wr']:>6.1f}%{r['pos']:>4}/11{r['worst']/1e4:>8,.0f}万{r['e1']/1e4:>8,.0f}万"
          f"{r['e2']/1e4:>9,.0f}万{r['norm']/1e4:>10,.0f}万")


ALL = pd.Series(True, index=E.index)
VOL = E["egap_vol"].isna() | (E["egap_vol"] >= 3.0)
YOY = E["op_yoy"].isna() | (E["op_yoy"] >= 15)

# ── 0. 2軸は独立か ──────────────────────────────────────
d = E[["egap_vol", "op_yoy"]].dropna()
rho = spearmanr(d["egap_vol"], d["op_yoy"]).statistic
both = int((VOL & YOY).sum())
print(f"[独立性] 決算ボラ と 営業利益YoY の順位相関 = {rho:+.3f}"
      f"（0に近いほど別々の情報＝重ねる価値がある）")
print(f"[件数]   全{len(E):,} / ボラのみ{int(VOL.sum()):,} / 増益のみ{int(YOY.sum()):,} / 両方{both:,}")

BASE = run(ALL)
print("\n" + "=" * 124)
print("① 単独 vs 併用")
print("=" * 124)
print(HDR)
line("現行（両方なし）", BASE)
line("A. 決算ボラ>=3.0%のみ", run(VOL), BASE)
line("B. 増益YoY>=15%のみ", run(YOY), BASE)
line("A+B 併用", run(VOL & YOY), BASE)

print("\n" + "=" * 124)
print("② 併用時の閾値の面（削りすぎになっていないか）")
print("=" * 124)
print(HDR)
for v in (2.0, 2.5, 3.0, 3.5):
    for y in (0, 15, 30):
        m = (E["egap_vol"].isna() | (E["egap_vol"] >= v)) & (E["op_yoy"].isna() | (E["op_yoy"] >= y))
        line(f"ボラ>={v:.1f}% × YoY>={y:+d}%", run(m), BASE)

print("\n" + "=" * 124)
print("③ 枠を振る（同DD予算-240万に正規化した10年計）")
print("=" * 124)
print(f"  {'枠':<5}{'現行':>11}{'Aのみ':>11}{'差':>10}{'A+B':>11}{'差':>10}{'A vs A+B':>12}")
for s in (5, 8, 10, 12, 16):
    a, b, c = run(ALL, s), run(VOL, s), run(VOL & YOY, s)
    if not all((a, b, c)):
        continue
    print(f"  {s:<5}{a['norm']/1e4:>10,.0f}万{b['norm']/1e4:>10,.0f}万{(b['norm']-a['norm'])/1e4:>+9,.0f}万"
          f"{c['norm']/1e4:>10,.0f}万{(c['norm']-a['norm'])/1e4:>+9,.0f}万{(c['norm']-b['norm'])/1e4:>+11,.0f}万")

print("\n" + "=" * 124)
print("④ 年別")
print("=" * 124)
print(f"  {'':<20}" + "".join(f"{y:>7}" for y in range(2016, 2027)))
for tag, m in (("現行", ALL), ("A ボラのみ", VOL), ("A+B 併用", VOL & YOY)):
    r = run(m)
    if r:
        print(f"  {tag:<20}" + "".join(f"{v/1e4:>+7.0f}" for v in r["yr"]))

print("\n[判定] A+B が A を明確に上回らないなら、増益ゲートは重ねない（削るコストが勝つ）。")
