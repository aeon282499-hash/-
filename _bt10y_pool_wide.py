# -*- coding: utf-8 -*-
"""_bt10y_pool_wide.py — 買い入口閾値グリッド用の広域10年プール（2026-08-02）。

現行の入口(RSI≤45/乖離≤-1.5/(rr≥1.5|vr≥2.0)/代金20億/ATR≤3.0)は10年再グリッドを
一度もやっていない最後の未測定面。bt_10y_robustness.py の走査を**緩い側の境界**
  RSI≤52 / 乖離≤-0.5 / (rr≥1.2 | vr≥1.6) / 代金8億 / ATR%≤4.0
で回し、閾値判定に使う特徴量を全部保存する。決算±3日除外・寄指NOFILL・出口(実OCO+RSI50+
MAXHOLD3)は本番仕様のまま固定。買残 days_cover も _bt_margin_filter.py と同一式で結合
（公表ラグ4日安全側・欠損はNaN=フェイルオープン）。

出力: _bt10y_pool_wide.csv
実行: python -X utf8 _bt10y_pool_wide.py
"""
from __future__ import annotations

import json
import pickle
from datetime import timedelta

import numpy as np
import pandas as pd

from bt_10y_robustness import SINCE, load_earnings, sim
from screener import is_etf_ticker, yose_limit_price

RSI_W, DEV_W, RR_W, VR_W, TOV_W, ATR_W = 52.0, -0.5, 1.2, 1.6, 8e8, 4.0

old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
name_map = dict(old["name_map"]); name_map.update(new["name_map"])
data: dict = {}
for src in (old["all_data"], new["all_data"]):
    for tk, df in src.items():
        data.setdefault(tk, []).append(df)
merged = {}
for tk, dfs in data.items():
    df = pd.concat(dfs).sort_index() if len(dfs) > 1 else dfs[0].sort_index()
    merged[tk] = df[~df.index.duplicated(keep="last")]
print(f"[data] {len(merged)}銘柄", flush=True)

M: dict = pickle.load(open("_margin_10y.pkl", "rb"))
earn = load_earnings()
since_ts = pd.Timestamp(SINCE)

rows = []
nn = nofill = 0
for tk, df in merged.items():
    if df is None or len(df) < 140:
        continue
    name = name_map.get(tk)
    if name is None or is_etf_ticker(tk, name):
        continue
    o = df["Open"].astype(float); h = df["High"].astype(float)
    l = df["Low"].astype(float); cl = df["Close"].astype(float)
    v = df["Volume"].astype(float)
    nn += 1
    dlt = cl.diff()
    ag = dlt.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
    al = (-dlt).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
    rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).round(2)
    ma25 = cl.rolling(25).mean()
    dev = ((cl - ma25) / ma25 * 100).round(2)
    pc = cl.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    rr = ((h - l).shift(1) / atr.shift(1)).round(2)
    vr = (v.shift(1) / v.shift(2).rolling(20).mean()).round(2)
    tov = cl.shift(1) * v.shift(1)
    atr_pct = atr / cl * 100
    adv20 = (cl * v).rolling(20).mean()
    cand = ((rsi <= RSI_W) & (dev <= DEV_W) & ((rr >= RR_W) | (vr >= VR_W))
            & (tov >= TOV_W) & (atr_pct <= ATR_W))
    cand &= cl.index >= since_ts
    if not cand.fillna(False).any():
        continue
    on = o.to_numpy(); hn = h.to_numpy(); ln = l.to_numpy()
    cn = cl.to_numpy(); rn = rsi.to_numpy()
    idx = cl.index; n = len(cn)
    ewin = earn.get(tk) or earn.get(tk.replace(".T", "")) or set()
    mdf = M.get(tk[:4])
    for t in np.where(cand.fillna(False).to_numpy())[0]:
        if t + 2 >= n:
            continue
        entry_day = idx[t + 1].strftime("%Y-%m-%d")
        if entry_day < SINCE or entry_day in ewin:
            continue
        lp = yose_limit_price(cn[t])
        if lp and on[t + 1] > lp:
            nofill += 1
            continue
        res, exoff = sim(t + 1, on, hn, ln, cn, rn, n)
        if res is None:
            continue
        rsi_t, dev_t, tov_t = rn[t], dev.iloc[t], tov.iloc[t]
        score = (1 / (1 + ((rsi_t - 38) / 8) ** 2) * 0.30
                 + 1 / (1 + ((dev_t + 3) / 2) ** 2) * 0.30
                 + np.log10(max(tov_t, 1) / 1e9 + 1) / 3 * 0.40)
        days_cover = np.nan
        if mdf is not None and len(mdf):
            cutoff = idx[t + 1] - timedelta(days=4)
            m = mdf[mdf.index <= cutoff]
            if len(m):
                long_v = float(m.iloc[-1].get("LongVol") or np.nan)
                a_prev = adv20.iloc[t]
                if np.isfinite(long_v) and np.isfinite(a_prev) and a_prev > 0:
                    days_cover = long_v * cn[t] / a_prev
        rows.append({"entry": entry_day, "year": idx[t + 1].year, "ticker": tk,
                     "price": cn[t], "score": score, "pnl": res, "exoff": int(exoff),
                     "rsi": rsi_t, "dev": float(dev_t), "rr": float(rr.iloc[t]) if np.isfinite(rr.iloc[t]) else np.nan,
                     "vr": float(vr.iloc[t]) if np.isfinite(vr.iloc[t]) else np.nan,
                     "tov": float(tov_t), "atr_pct": float(atr_pct.iloc[t]),
                     "days_cover": days_cover})
D = pd.DataFrame(rows)
D.to_csv("_bt10y_pool_wide.csv", index=False)
print(f"[save] _bt10y_pool_wide.csv {len(D):,}件 / {nn}銘柄走査 / NOFILL {nofill:,}"
      f" / 現行条件の部分集合={((D.rsi<=45)&(D.dev<=-1.5)&((D.rr>=1.5)|(D.vr>=2.0))&(D.tov>=2e9)&(D.atr_pct<=3.0)).sum():,}件")
