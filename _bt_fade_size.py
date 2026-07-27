# -*- coding: utf-8 -*-
"""_bt_fade_size.py — デイトレ売り(フェード)の1建玉サイズ比較 50万 vs 100万（2026-07-27）。

サイズを変えると単純に損益が2倍になるのではなく、**買える銘柄の範囲が変わる**:
  50万  → 100株で買えるのは株価5,000円以下
  100万 → 株価10,000円以下（＝値がさ株も対象に入る）
さらに実際の投下額は100株単位で丸まるので、稼働率も変わる。そこを実株数ベースで測る。

本番ルール（daytrade_paper.py と同一）:
  前日比≥+12% / 貸借○ / 張り付き除外(信号日レンジ≤5%は除く) / 20日代金中央値≥3億 /
  株価≤サイズ/100 → 上昇率降順・上位8 → 寄指売り(寄り≥前日終値で約定) → 引成買戻し

実行: python -X utf8 _bt_fade_size.py
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

GAIN_MIN, TOV_MIN, STICKY_MIN, TOP_N = 12.0, 3e8, 0.05, 8
COST = 0.0                      # 手数料は一日信用0円。比較が目的なので両サイズ共通で0
SIZES = (500_000, 1_000_000)

print("[load] 読込中...", flush=True)
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
ISS = pickle.load(open("_iss_type_by_year.pkl", "rb"))
name_map = dict(old["name_map"]); name_map.update(new["name_map"])
from screener import is_etf_ticker

rows = []
for tk in set(old["all_data"]) | set(new["all_data"]):
    nm = name_map.get(tk)
    if nm is None or is_etf_ticker(tk, nm):
        continue
    dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk)) if d is not None and len(d)]
    if not dfs:
        continue
    df = pd.concat(dfs).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    if len(df) < 30:
        continue
    o = df["Open"].astype(float).to_numpy(); c = df["Close"].astype(float).to_numpy()
    h = df["High"].astype(float).to_numpy(); l = df["Low"].astype(float).to_numpy()
    v = df["Volume"].astype(float).to_numpy()
    tov = pd.Series(c * v).rolling(20).median().to_numpy()
    idx = df.index
    for t in range(21, len(c) - 1):
        if not (c[t - 1] > 0 and c[t] > 0):
            continue
        gain = (c[t] / c[t - 1] - 1) * 100
        if gain < GAIN_MIN or not np.isfinite(tov[t]) or tov[t] < TOV_MIN:
            continue
        rng = (h[t] - l[t]) / c[t] if c[t] > 0 else 0
        if rng <= STICKY_MIN:                      # ロックS高＝踏み上げ危険で除外
            continue
        y = idx[t + 1].year
        if ISS.get(y, {}).get(str(tk)[:4], "?") != "2":   # 貸借○のみ
            continue
        if not (o[t + 1] > 0 and c[t + 1] > 0):
            continue
        rows.append({"sig": idx[t].strftime("%Y-%m-%d"), "year": y, "ticker": tk,
                     "gain": gain, "prev_close": c[t],
                     "open1": o[t + 1], "close1": c[t + 1]})
D = pd.DataFrame(rows).sort_values(["sig", "gain"], ascending=[True, False])
print(f"[collect] 候補 {len(D):,}件（{D.sig.min()}〜{D.sig.max()}）", flush=True)


def run(size: int, top_n: int = TOP_N):
    A = D[D["prev_close"] * 100 <= size]           # 100株で買える株価のみ＝値がさカット
    A = A.groupby("sig").head(top_n)
    out = []
    for r in A.itertuples():
        if r.open1 < r.prev_close:                 # 寄指売り不成立（下寄りは見送り）
            continue
        sh = int(size / r.open1 / 100) * 100
        if sh <= 0:
            continue
        pnl_pct = (r.open1 - r.close1) / r.open1 * 100 - COST
        out.append({"year": r.year, "pnl_pct": pnl_pct,
                    "yen": pnl_pct / 100 * sh * r.open1, "used": sh * r.open1,
                    "rank": 0, "sig": r.sig})
    return pd.DataFrame(out), len(A)


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


print(f"\n{'サイズ':<8}{'候補':>7}{'執行':>7}{'平均%':>9}{'PF':>7}{'10年計':>12}{'稼働率':>8}")
res = {}
for size in SIZES:
    P, cand = run(size)
    res[size] = P
    print(f"{size // 10000}万{'':<5}{cand:>7}{len(P):>7}{P.pnl_pct.mean():>+8.3f}%{pf(P.pnl_pct):>7.2f}"
          f"{P.yen.sum():>+11,.0f}円{P.used.mean() / size * 100:>7.0f}%")

print(f"\n■ 年別（円）")
print(f"  {'年':>6}{'50万':>14}{'100万':>14}{'差':>12}")
for y in range(2017, 2027):
    a = res[500_000]; b = res[1_000_000]
    ya = a[a.year == y].yen.sum(); yb = b[b.year == y].yen.sum()
    if not len(a[a.year == y]) and not len(b[b.year == y]):
        continue
    print(f"  {y:>6}{ya:>+13,.0f}円{yb:>+13,.0f}円{yb - ya:>+11,.0f}円")
a, b = res[500_000], res[1_000_000]
print(f"  {'計':>6}{a.yen.sum():>+13,.0f}円{b.yen.sum():>+13,.0f}円{b.yen.sum() - a.yen.sum():>+11,.0f}円")
print(f"\n  1件あたり: 50万 {a.yen.mean():+,.0f}円 / 100万 {b.yen.mean():+,.0f}円")
print(f"  最悪の1件: 50万 {a.yen.min():+,.0f}円 / 100万 {b.yen.min():+,.0f}円")
print(f"  最悪の年 : 50万 {min(a.groupby('year').yen.sum()):+,.0f}円 / "
      f"100万 {min(b.groupby('year').yen.sum()):+,.0f}円")
