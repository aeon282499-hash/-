# -*- coding: utf-8 -*-
"""_bt_fx_nightday.py — XM CFDで執行できる「夜/日中ドリフト」の10年分解（2026-08-06）。

XMのCFD（JP225/US500/GOLD）はほぼ24時間取引できるので、
  夜レッグ  = 東京引け(15:00/15:30)に買い→翌朝の東京寄り(9:00)に売り
  日中レッグ = 東京寄りに買い→同日引けに売り
が実際に執行できる。東証ETFの日足(寄り/引け)がそのままこの2レッグの10年データになる。
株の検証で「反発は夜に住む」だったのと同じ分解を、指数と金でやる。
プロキシ: JP225=1321 / US500=1557 / GOLD=1326(SPDR)・1540(純金信託)
コスト: XM実測前の概算バンド（スプレッド往復+スワップは後で実測）。
実行: python -X utf8 _bt_fx_nightday.py
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))


def merge(tk):
    dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk)) if d is not None and len(d)]
    d = pd.concat(dfs).sort_index()
    return d[~d.index.duplicated(keep="last")]


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


def legs(tk, label):
    d = merge(tk)
    o = d["Open"].astype(float)
    c = d["Close"].astype(float)
    df = pd.DataFrame({
        "y": d.index.year,
        "intra": (c / o - 1) * 100,                 # 寄り→引け
        "ovn": (o.shift(-1) / c - 1) * 100,         # 引け→翌寄り
    }).dropna()
    df = df[df.index >= "2016-08-01"]
    print(f"\n■ {label} ({tk}) {df.index[0].date()}〜{df.index[-1].date()} n={len(df):,}日")
    print(f"  {'レッグ':<14}{'平均/日':>9}{'勝率':>7}{'PF':>6}{'年率':>8}"
          f"{'前半16-21':>10}{'後半22-26':>10}{'勝ち年':>7}")
    for leg, lab in (("ovn", "夜(引け→翌寄り)"), ("intra", "日中(寄り→引け)")):
        x = df[leg]
        yr = df.groupby("y")[leg].sum()
        h1 = x[df.y <= 2021]; h2 = x[df.y >= 2022]
        ann = x.mean() * 245
        print(f"  {lab:<14}{x.mean():>+8.4f}%{(x>0).mean()*100:>6.1f}%{pf(x):>6.2f}"
              f"{ann:>+7.1f}%{h1.mean()*245:>+9.1f}%{h2.mean()*245:>+9.1f}%"
              f"{int((yr>0).sum()):>4}/{yr.index.nunique()}")
    # 年別（夜レッグ）
    yr_o = df.groupby("y").ovn.sum()
    yr_i = df.groupby("y").intra.sum()
    print("  年別合計%（夜/日中）: "
          + " ".join(f"{y}:{vo:+.0f}/{vi:+.0f}" for (y, vo), vi in zip(yr_o.items(), yr_i.values)))
    return df


PAIRS = [("1321.T", "日経225(JP225 CFD)"), ("1557.T", "S&P500(US500 CFD)"),
         ("1326.T", "金SPDR(GOLD CFD)"), ("1540.T", "純金信託(GOLD CFD)")]
res = {}
for tk, lab in PAIRS:
    res[tk] = legs(tk, lab)

print("\n" + "=" * 110)
print("コスト概算（XM実測前のバンド・1晩/1日の往復）と損益分岐")
print("=" * 110)
print("""  JP225: スプレッド往復 ~12pt(0.03%) + 買いスワップ ~-0.5〜-1.5pt/晩(-0.001〜-0.004%)
  US500: 往復 ~0.7pt(0.011%) + スワップ -0.01〜-0.02%/晩（米金利分・重い）
  GOLD : 往復 ~0.35$(0.013%) + スワップ -0.01〜-0.015%/晩（同上）
  → 「毎晩持つ」戦略はスワップ×245晩が年-2.5〜-5%の固定費。夜レッグの年率がこれを
     大きく超えているかが生死の分かれ目。日中レッグはスワップゼロ（スプレッドのみ）。""")

print("=" * 110)
print("夜レッグのコスト後の姿（スプレッド0.03%+スワップ3パターン・JP225の例）")
print("=" * 110)
d = res["1321.T"]
for sw, lab in ((0.0, "スワップ0(理論値)"), (0.005, "スワップ-0.005%/晩"), (0.010, "スワップ-0.010%/晩")):
    x = d.ovn - 0.03 - sw
    yr = x.groupby(d.y).sum()
    print(f"  {lab:<18} 平均{x.mean():+.4f}%/晩 年率{x.mean()*245:+.1f}%"
          f" 勝ち年{int((yr>0).sum())}/{yr.index.nunique()}"
          f" 前半{x[d.y<=2021].mean()*245:+.1f}%/後半{x[d.y>=2022].mean()*245:+.1f}%")
