# -*- coding: utf-8 -*-
"""_audit_earnings_parity.py — 決算持ち越し BT⇔本番のパリティ監査（2026-07-31）。

本人「バックテスト通りシグナルが運用されているか隅々までチェックして」。

監査で見つかった2つのズレを、BT側を本番に合わせて測り直して影響を確定させる。

  ズレ①: RSIの計算窓
    BT   … close.iloc[p-90:p+1]              ＝91営業日
    本番 … HIST_DAYS=60(暦日)の終値+現在値    ≒41営業日
    実測で RSI<=55 の判定が 2.06% 入れ替わる（最大34.3pt差）。
    本番の窓は14:55起動→大引けまでの時間制約から伸ばしにくいので、
    **BTを本番に合わせて**測り直すのが正しい向き。

  ズレ②: イベントの母集団
    BT   … earnings_calendar.json＝/fins/summary の全開示（業績修正・配当修正を含む）
    本番 … JPX公式の決算発表予定表＝四半期/本決算のみ（修正系は載らない）
    BT候補の9%は本番が構造的に撃てないイベントだった。

実行: python -X utf8 _audit_earnings_parity.py
"""
from __future__ import annotations

import json
import pickle
import sys
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

from screener import calc_rsi

PROD_BARS = 41          # 本番の実効窓（暦日60日≒41営業日ぶんの終値）
BT_BARS = 91            # 既存BTの窓
CACHES = ("jquants_cache_2016_2021.pkl", "jquants_cache.pkl")
OUT = "_earnings_rsi_prod.csv"


def load_prices() -> dict[str, pd.DataFrame]:
    frames: dict[str, list[pd.DataFrame]] = {}
    for p in CACHES:
        print(f"[audit] {p} 読み込み中…", flush=True)
        blob = pickle.load(open(p, "rb"))
        for tk, df in blob["all_data"].items():
            if df is not None and len(df):
                frames.setdefault(tk, []).append(df)
        del blob
    out = {}
    for tk, fs in frames.items():
        d = pd.concat(fs).sort_index()
        out[tk] = d[~d.index.duplicated(keep="last")]
    return out


def main() -> None:
    E = pd.read_csv("_earnings_events_rich2.csv")
    print(f"[audit] イベント表 {len(E):,}件", flush=True)
    prices = load_prices()

    rows = []
    need = E.groupby("ticker")["d0"].apply(list).to_dict()
    for n, (tk, ds) in enumerate(need.items(), 1):
        df = prices.get(tk)
        if df is None or len(df) < PROD_BARS + 2:
            continue
        idx = [str(i)[:10] for i in df.index]
        pos = {d: i for i, d in enumerate(idx)}
        cl = df["Close"].astype(float)
        for d in ds:
            p = pos.get(d)
            if p is None or p < PROD_BARS:
                continue
            r = calc_rsi(cl.iloc[p - (PROD_BARS - 1):p + 1].dropna())
            if r is not None:
                rows.append({"ticker": tk, "d0": d, "rsi_prod": r})
        if n % 800 == 0:
            print(f"  {n}/{len(need)}銘柄", flush=True)

    R = pd.DataFrame(rows)
    R.to_csv(OUT, index=False)
    print(f"[audit] {OUT} に {len(R):,}件", flush=True)

    M = E.merge(R, on=["ticker", "d0"], how="inner")
    d = M["rsi_prod"] - M["rsi"]
    flip = ((M["rsi"] <= 55) != (M["rsi_prod"] <= 55)).mean() * 100
    print(f"[audit] RSI差(本番窓-BT窓): 中央{np.median(d):+.2f} "
          f"|差|>1pt {(d.abs() > 1).mean()*100:.1f}% / 55判定の反転 {flip:.2f}%", flush=True)


if __name__ == "__main__":
    main()
