# -*- coding: utf-8 -*-
"""_bt_fade_daily.py — デイトレフェードを「毎日1件約定」に近づけられるか（2026-07-28・データ修正版）。

背景: 2026-07-23に「+8%まで下げれば毎日撃てるが増える分は限界平均+0.11%＝滑りで消える」と結論し
      本人が+12%据置を選んだ。だがあの測定は _iss_type_by_year.pkl に 2019/2024/2025 が
      無いことに気づかずその3年を落としていた。欠年を近隣年で補完して測り直す。

軸: 上昇率しきい値 × 寄りギャップ下限(GU) × 上位N
指標: 約定日率（毎日撃てるか）・PF・1件あたり・5年損益（1玉50万・実株数）

実行: python -X utf8 _bt_fade_daily.py
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

from screener import is_etf_ticker

CAP, TOV_MIN, STICKY_MIN, SINCE = 500_000, 3e8, 0.05, "2021-08"

old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
ISS = pickle.load(open("_iss_type_by_year.pkl", "rb"))
YEARS = sorted(ISS)
nm = dict(old["name_map"]); nm.update(new["name_map"])


def iss_for(y):
    return ISS[min(YEARS, key=lambda a: (abs(a - y), a))]


rows = []
for tk in set(old["all_data"]) | set(new["all_data"]):
    n = nm.get(tk)
    if n is None or is_etf_ticker(tk, n):
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
        if gain < 6.0 or not np.isfinite(tov[t]) or tov[t] < TOV_MIN:
            continue                                   # 最緩6%で拾ってグリッドは後段
        if (h[t] - l[t]) / c[t] <= STICKY_MIN:
            continue
        y = idx[t + 1].year
        if iss_for(y).get(str(tk)[:4], "?") != "2":
            continue
        if not (o[t + 1] > 0 and c[t + 1] > 0) or c[t] * 100 > CAP:
            continue
        rows.append({"sig": idx[t].strftime("%Y-%m-%d"), "ent": idx[t + 1].strftime("%Y-%m"),
                     "y": y, "gain": gain, "prev": c[t], "o1": o[t + 1], "c1": c[t + 1]})
B = pd.DataFrame(rows).sort_values(["sig", "gain"], ascending=[True, False])
B = B[B["ent"] >= SINCE]
NDAYS = B["sig"].nunique()
# 全営業日数（分母）
nk = pd.concat([x for x in (old["all_data"].get("1321.T"), new["all_data"].get("1321.T"))
                if x is not None]).sort_index()
nk = nk[~nk.index.duplicated(keep="last")]
TOTAL_DAYS = len(nk[nk.index >= pd.Timestamp("2021-08-01")])
print(f"[data] 候補{len(B):,}件 / シグナル日{NDAYS}日 / 全営業日{TOTAL_DAYS}日 "
      f"({B.sig.min()}〜{B.sig.max()})\n", flush=True)


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


def run(gain_min, gu_min, top_n):
    D = B[B["gain"] >= gain_min].groupby("sig").head(top_n)
    D = D[D["o1"] >= D["prev"] * (1 + gu_min / 100)].copy()
    if not len(D):
        return None
    D["sh"] = (CAP / D["o1"] // 100 * 100).astype(int)
    D = D[D["sh"] > 0]
    D["pnl"] = (D["o1"] - D["c1"]) / D["o1"] * 100
    D["yen"] = D["pnl"] / 100 * D["sh"] * D["o1"]
    return dict(n=len(D), days=D["sig"].nunique(), pf=pf(D["pnl"]),
                avg=D["pnl"].mean(), yen=D["yen"].sum(),
                worst_y=D.groupby("y")["yen"].sum().min())


print("■ しきい値ラダー（1玉50万・5年・実株数ベース）")
for top_n in (1, 3):
    print(f"\n  ── 上位{top_n}本 ──")
    print(f"  {'条件':<22}{'約定':>6}{'約定日率':>9}{'PF':>7}{'平均%':>8}{'5年損益':>12}{'最悪年':>11}")
    for gain in (6, 8, 10, 12, 15):
        for gu in (0.0, 1.0):
            r = run(gain, gu, top_n)
            if not r:
                continue
            lab = f"+{gain}%×GU≥{gu:.0f}%"
            mark = "  ←現行" if (gain == 12 and gu == 1.0 and top_n == 1) else ""
            print(f"  {lab:<22}{r['n']:>6}{r['days'] / TOTAL_DAYS * 100:>8.0f}%{r['pf']:>7.2f}"
                  f"{r['avg']:>+7.2f}%{r['yen']:>+11,.0f}円{r['worst_y']:>+10,.0f}円{mark}")
