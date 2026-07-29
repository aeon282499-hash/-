# -*- coding: utf-8 -*-
"""_bt_earnings_ret_ext.py — 決算持ち越し固有軸のために保有窓を全期間で拡張（2026-07-26）。

_earnings_events_rich.csv に ret1/ret3/ret8/ret10 を追加する（ret5は既存を検算）。
用途:
  ① 下側PEAD = 決算で叩き売られた玉（gap<-X%）を売らずに持つと戻るか（現行は翌寄り即売り）
  ② PEAD延長の保有日数グリッド（現行5営業日固定は未検証のツマミ）

出力: _earnings_events_rich2.csv
実行: python -X utf8 _bt_earnings_ret_ext.py
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

HORIZONS = [1, 3, 5, 8, 10]

print("[load] 読込中...", flush=True)
E = pd.read_csv("_earnings_events_rich.csv")
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))

close_map: dict[str, tuple[np.ndarray, dict[str, int]]] = {}
for tk in E["ticker"].unique():
    dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk)) if d is not None and len(d)]
    if not dfs:
        continue
    df = pd.concat(dfs).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    close_map[tk] = (df["Close"].astype(float).to_numpy(),
                     {s: i for i, s in enumerate(df.index.strftime("%Y-%m-%d"))},
                     df.index.to_numpy())

print(f"[calc] {len(close_map):,}銘柄の保有窓を計算中...", flush=True)
cols = {h: [] for h in HORIZONS}
for tk, d0, px in zip(E["ticker"], E["d0"], E["price"]):
    ent = close_map.get(tk)
    if ent is None or not (px > 0):
        for h in HORIZONS:
            cols[h].append(np.nan)
        continue
    cn, pos, idx = ent
    p = pos.get(d0)
    for h in HORIZONS:
        # 【2026-07-29 バグ修正】p+h は「h行後」であって「h営業日後」ではない。
        # キャッシュに穴がある銘柄では数年をまたぎ、その値上がりを保有リターンとして
        # 計上してしまう。暦日で連続性を確認する（h営業日は通常 h*1.5日以内、
        # 年末年始/GWの余裕を見て +10日）。
        if p is None or p + h >= len(cn) or not (cn[p + h] > 0):
            cols[h].append(np.nan)
        elif (idx[p + h] - idx[p]).astype("timedelta64[D]").astype(int) > h * 2 + 10:
            cols[h].append(np.nan)
        else:
            cols[h].append((cn[p + h] / px - 1) * 100)

for h in HORIZONS:
    E[f"r{h}"] = cols[h]

both = E["ret5"].notna() & E["r5"].notna()
diff = (E.loc[both, "ret5"] - E.loc[both, "r5"]).abs()
print(f"[check] 既存ret5との一致: n={both.sum():,} 最大差{diff.max():.4f}pt 平均{diff.mean():.6f}pt")

E.to_csv("_earnings_events_rich2.csv", index=False)
print(f"[save] _earnings_events_rich2.csv {len(E):,}件")
for h in HORIZONS:
    print(f"  r{h:<3} 欠損 {E[f'r{h}'].isna().mean() * 100:5.1f}%")
