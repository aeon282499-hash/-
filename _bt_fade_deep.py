# -*- coding: utf-8 -*-
"""_bt_fade_deep.py — デイトレフェードの徹底再検証（2026-07-28・年100万を目標に）。

前提の訂正:
  ・_iss_type_by_year.pkl に 2019/2024/2025 が無く、過去の検証は3年を落としていた。
    → 欠年は最も近い年の貸借区分で補完する。
  ・2026-07-27に入れた寄りギャップ下限(GU≥1%)は、上位1〜3本の実運用条件では
    総額を下げ最悪年を2〜5倍悪化させる（上位8本・10年でしか良く見えていなかった）。

やること: 候補に特徴量を大量に付けて単変量で総当たり → 両期間で頑健なものだけ残す。
出力: _fade_deep.pkl（以後の検証で使い回す）

実行: python -X utf8 _bt_fade_deep.py
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

from screener import is_etf_ticker

CAP, TOV_MIN, STICKY_MIN, GAIN_FLOOR = 500_000, 3e8, 0.05, 6.0

old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
ISS = pickle.load(open("_iss_type_by_year.pkl", "rb"))
YEARS = sorted(ISS)
nm = dict(old["name_map"]); nm.update(new["name_map"])


def iss_for(y):
    return ISS[min(YEARS, key=lambda a: (abs(a - y), a))]


# 日経（地合い判定用）
def _merge(tk):
    dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk)) if d is not None and len(d)]
    if not dfs:
        return None
    d = pd.concat(dfs).sort_index()
    return d[~d.index.duplicated(keep="last")]


nk = _merge("1321.T")
nkc = nk["Close"].astype(float)
nk_ma25 = nkc.rolling(25).mean()
nk_below = {d.strftime("%Y-%m-%d"): bool(v) for d, v in (nkc < nk_ma25).items()}
nk_chg = {d.strftime("%Y-%m-%d"): float(v) for d, v in (nkc.pct_change() * 100).items()}

rows = []
for tk in set(old["all_data"]) | set(new["all_data"]):
    n = nm.get(tk)
    if n is None or is_etf_ticker(tk, n):
        continue
    df = _merge(tk)
    if df is None or len(df) < 40:
        continue
    o = df["Open"].astype(float).to_numpy(); c = df["Close"].astype(float).to_numpy()
    h = df["High"].astype(float).to_numpy(); l = df["Low"].astype(float).to_numpy()
    v = df["Volume"].astype(float).to_numpy()
    tov = pd.Series(c * v).rolling(20).median().to_numpy()
    vma = pd.Series(v).rolling(20).mean().to_numpy()
    pc = pd.Series(c).shift(1)
    tr = pd.concat([pd.Series(h) - pd.Series(l), (pd.Series(h) - pc).abs(),
                    (pd.Series(l) - pc).abs()], axis=1).max(axis=1)
    atr = (tr.rolling(14).mean() / pd.Series(c) * 100).to_numpy()
    ma25 = pd.Series(c).rolling(25).mean().to_numpy()
    idx = df.index
    for t in range(26, len(c) - 1):
        if not (c[t - 1] > 0 and c[t] > 0):
            continue
        gain = (c[t] / c[t - 1] - 1) * 100
        if gain < GAIN_FLOOR or not np.isfinite(tov[t]) or tov[t] < TOV_MIN:
            continue
        rng = (h[t] - l[t]) / c[t]
        if rng <= STICKY_MIN:
            continue
        y = idx[t + 1].year
        if iss_for(y).get(str(tk)[:4], "?") != "2":
            continue
        if not (o[t + 1] > 0 and c[t + 1] > 0) or c[t] * 100 > CAP:
            continue
        d0 = idx[t].strftime("%Y-%m-%d")
        rows.append({
            "sig": d0, "ent": idx[t + 1].strftime("%Y-%m"), "y": y, "ticker": tk,
            "gain": gain, "prev": c[t], "o1": o[t + 1], "c1": c[t + 1],
            # ── 特徴量（すべてシグナル日の引けまでに確定）──
            "gu": (o[t + 1] / c[t] - 1) * 100,          # 翌朝の寄りギャップ（執行時に判明）
            "rng": rng * 100,                            # 当日の値幅
            "pos": (c[t] - l[t]) / (h[t] - l[t]) * 100 if h[t] > l[t] else 50,  # 終値の位置
            "vr": v[t] / vma[t] if vma[t] > 0 else 0,    # 出来高比
            "atr": atr[t],
            "tov": tov[t],
            "px": c[t],
            "dev": (c[t] / ma25[t] - 1) * 100 if ma25[t] > 0 else 0,   # 25MA乖離
            "gain2": (c[t] / c[t - 2] - 1) * 100 if c[t - 2] > 0 else 0,  # 2日騰落
            "prev_up": (c[t - 1] / c[t - 2] - 1) * 100 if c[t - 2] > 0 else 0,  # 前日の騰落
            "body": (c[t] - o[t]) / (h[t] - l[t]) * 100 if h[t] > l[t] else 0,  # 実体比
            "nk_below": nk_below.get(d0, False),
            "nk_chg": nk_chg.get(d0, 0.0),
            "dow": idx[t + 1].dayofweek,
        })
D = pd.DataFrame(rows).sort_values(["sig", "gain"], ascending=[True, False])
D["sh"] = (CAP / D["o1"] // 100 * 100).astype(int)
D = D[D["sh"] > 0]
D["pnl"] = (D["o1"] - D["c1"]) / D["o1"] * 100
D["yen"] = D["pnl"] / 100 * D["sh"] * D["o1"]
D.to_pickle("_fade_deep.pkl")
print(f"[save] _fade_deep.pkl {len(D):,}件 / {D.sig.nunique()}日 ({D.sig.min()}〜{D.sig.max()})")
print(f"  年別: " + " ".join(f"{y}:{v}" for y, v in D.groupby("y").size().items()))
