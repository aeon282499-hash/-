# -*- coding: utf-8 -*-
"""_bt_crashshort_10y.py — 崩壊ショートを «本番想定の条件» で作り直す（2026-08-01・本番無変更）。

本人「はかってみて」＝ポートフォリオに足す4つ目の系統として測る。

既存の `_sellwatch_events_10y.csv` は検出を緩めた版（急騰15%・出来高1.3倍）で
PF1.09しかない。記憶に残っている候補の条件は
  急騰30% × 5MA割れ初日 × 当日-5%以下 × 出来高2倍 × 貸借 × 流動性
で PF1.52・陽性9/10年。閾値が違うので作り直す。

検出式は _bt_sellwatch_next.py と同一。閾値だけ本番想定に寄せ、
特徴量（runup/volx/tov/当日騰落）を残して後段でグリッドを振れるようにする。

⚠ 貸借銘柄の判定は入れていない（10年ぶんの制度信用区分が手元に無い）。
   空売りできない銘柄が混じるぶん、この結果は楽観側に出る。

出力: _crashshort_events.csv
実行: python -X utf8 _bt_crashshort_10y.py
"""
from __future__ import annotations

import pickle
import warnings

import numpy as np
import pandas as pd
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()
from screener import is_etf_ticker

OUT = "_crashshort_events.csv"


def build() -> pd.DataFrame:
    old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
    new = pickle.load(open("jquants_cache.pkl", "rb"))
    name_map = dict(old["name_map"])
    name_map.update(new["name_map"])
    data: dict[str, list] = {}
    for src in (old["all_data"], new["all_data"]):
        for tk, df in src.items():
            data.setdefault(tk, []).append(df)
    del old, new

    rows = []
    for tk, dfs in data.items():
        name = name_map.get(tk)
        if name is None or is_etf_ticker(tk, name):
            continue
        df = pd.concat(dfs).sort_index() if len(dfs) > 1 else dfs[0].sort_index()
        df = df[~df.index.duplicated(keep="last")]
        if len(df) < 40:
            continue
        o = df["Open"].astype(float)
        c = df["Close"].astype(float)
        v = df["Volume"].astype(float)
        ma5 = c.rolling(5).mean()
        below = c < ma5
        fresh = below & ~below.shift(1).fillna(False)          # 5MA割れ «初日» だけ
        r1 = c.pct_change() * 100
        volx = v / v.shift(1).rolling(20).mean()
        tov20 = (c * v).rolling(20).mean() / 1e8
        runup = (c.rolling(20).max() / c.shift(20) - 1) * 100
        sig = fresh & runup.notna() & (c >= 100)
        idx = np.where(sig.fillna(False).to_numpy())[0]
        on, cn = o.to_numpy(), c.to_numpy()
        rn, vx, tv, ru = r1.to_numpy(), volx.to_numpy(), tov20.to_numpy(), runup.to_numpy()
        n = len(cn)
        dates = df.index
        for t in idx:
            if t + 1 >= n:
                continue
            o1 = on[t + 1]
            if not (o1 > 0) or not np.isfinite(vx[t]):
                continue
            # 価格データの穴をまたいだ «翌営業日» を弾く
            if (dates[t + 1] - dates[t]).days > 10:
                continue
            rows.append({
                "ticker": tk, "d0": dates[t].strftime("%Y-%m-%d"), "year": dates[t].year,
                "entry": float(o1), "runup": float(ru[t]), "volx": float(vx[t]),
                "tov": float(tv[t]), "r1": float(rn[t]),
                # 翌寄り空売り→当日引け買戻し（プラス＝儲け）
                "s_d1": (o1 - cn[t + 1]) / o1 * 100,
            })
    D = pd.DataFrame(rows)
    D.to_csv(OUT, index=False)
    print(f"[crash] 検出 {len(D):,}件 / {D.d0.min()}〜{D.d0.max()}", flush=True)
    return D


import os

D = pd.read_csv(OUT) if os.path.exists(OUT) else build()

print("\n" + "=" * 100)
print("① 閾値グリッド（急騰 × 出来高倍率 × 当日下落）※翌寄り空売り→当日引け")
print("=" * 100)
print(f"  {'条件':<34}{'件数':>7}{'平均':>9}{'勝率':>8}{'PF':>7}{'前半':>9}{'後半':>9}")
best = []
for ru in (15, 20, 25, 30, 35):
    for vx in (1.3, 2.0):
        for r1 in (-2.0, -5.0):
            m = ((D.runup >= ru) & (D.volx >= vx) & (D.r1 <= r1)
                 & (D.tov >= 5.0) & D.s_d1.notna())
            g = D[m]
            if len(g) < 150:
                continue
            neg = abs(g.s_d1[g.s_d1 <= 0].sum())
            pf = g.s_d1[g.s_d1 > 0].sum() / neg if neg else np.inf
            e1 = g[g.year <= 2021].s_d1.mean()
            e2 = g[g.year >= 2022].s_d1.mean()
            tag = f"急騰{ru}%×出来高{vx}倍×当日{r1:.0f}%"
            print(f"  {tag:<34}{len(g):>7,}{g.s_d1.mean():>+8.2f}%{(g.s_d1>0).mean()*100:>7.1f}%"
                  f"{pf:>7.2f}{e1:>+8.2f}%{e2:>+8.2f}%")
            if pf > 1.2 and e1 > 0 and e2 > 0:
                best.append((tag, ru, vx, r1, pf))

print(f"\n  両期間プラス かつ PF>1.2 の条件: {len(best)}通り")
