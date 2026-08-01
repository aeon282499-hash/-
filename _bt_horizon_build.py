# -*- coding: utf-8 -*-
"""_bt_horizon_build.py — 決算イベントに «長い保有期間» のリターンを付ける（2026-08-01）。

なぜやるか:
  現行4系統はすべて保有5営業日以内（決算1晩/スイング3日/崩壊ショート当日/フェード当日）。
  数週間〜数ヶ月の領域は完全な空白。
  そして今日、記事の「見極め5ヶ条」（進捗率/利益の質/上方修正/増配）が全部落ちたが、
  素の層別では効いて見えていた（上方修正PF1.45・利益の質Q5でPF1.31）。
  消えたのは全部「1晩の枠取り合い」に乗せた時。
  → **業績の良さは翌朝の寄り1回には出ないが、数ヶ月なら出るのでは** という仮説を測る。

作るもの: 各イベント(ticker, d0)について
  r20 / r40 / r60 … d0終値 → N営業日後の終値（%）
  m20 / m40 / m60 … 同区間の市場(1321.T)リターン（%）
  x20 / x40 / x60 … 市場超過（個別 − 市場）。長い期間ほど市場βが支配するので必須。

出力: _earnings_horizon.csv
実行: python -X utf8 _bt_horizon_build.py
"""
from __future__ import annotations

import pickle
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

HOR = (20, 40, 60)
GAP_MAX_DAYS = {20: 45, 40: 80, 60: 120}   # 暦日の上限（価格データの穴をまたがない）
CACHES = ("jquants_cache_2016_2021.pkl", "jquants_cache.pkl")


def load_prices() -> dict[str, pd.DataFrame]:
    frames: dict[str, list] = {}
    for p in CACHES:
        print(f"[hor] {p} 読み込み中…", flush=True)
        blob = pickle.load(open(p, "rb"))
        for tk, df in blob["all_data"].items():
            if df is not None and len(df):
                frames.setdefault(tk, []).append(df)
        del blob
    out = {}
    for tk, fs in frames.items():
        d = pd.concat(fs).sort_index() if len(fs) > 1 else fs[0].sort_index()
        out[tk] = d[~d.index.duplicated(keep="last")]
    return out


def main() -> None:
    E = pd.read_csv("_earnings_events_rich2.csv")
    print(f"[hor] イベント {len(E):,}件", flush=True)
    prices = load_prices()

    # 市場（1321.T）の日付→終値
    mk = prices.get("1321.T")
    m_idx = [str(i)[:10] for i in mk.index]
    m_pos = {d: i for i, d in enumerate(m_idx)}
    m_cl = mk["Close"].astype(float).to_numpy()

    rows = []
    need = E.groupby("ticker")["d0"].apply(list).to_dict()
    for n, (tk, ds) in enumerate(need.items(), 1):
        df = prices.get(tk)
        if df is None or len(df) < max(HOR) + 5:
            continue
        idx = [str(i)[:10] for i in df.index]
        pos = {d: i for i, d in enumerate(idx)}
        cl = df["Close"].astype(float).to_numpy()
        dts = df.index
        for d in ds:
            p = pos.get(d)
            if p is None:
                continue
            c0 = cl[p]
            if not (c0 > 0) or np.isnan(c0):
                continue
            rec = {"ticker": tk, "d0": d}
            mp = m_pos.get(d)
            for h in HOR:
                q = p + h
                rec[f"r{h}"] = np.nan
                rec[f"m{h}"] = np.nan
                if q < len(cl) and (dts[q] - dts[p]).days <= GAP_MAX_DAYS[h]:
                    cq = cl[q]
                    if cq > 0 and not np.isnan(cq):
                        rec[f"r{h}"] = (cq / c0 - 1) * 100
                if mp is not None and mp + h < len(m_cl):
                    a, b = m_cl[mp], m_cl[mp + h]
                    if a > 0 and b > 0:
                        rec[f"m{h}"] = (b / a - 1) * 100
                if np.isfinite(rec[f"r{h}"]) and np.isfinite(rec[f"m{h}"]):
                    rec[f"x{h}"] = rec[f"r{h}"] - rec[f"m{h}"]
                else:
                    rec[f"x{h}"] = np.nan
            rows.append(rec)
        if n % 800 == 0:
            print(f"  {n}/{len(need)}銘柄", flush=True)

    H = pd.DataFrame(rows)
    H.to_csv("_earnings_horizon.csv", index=False)
    print(f"[hor] _earnings_horizon.csv に {len(H):,}件", flush=True)
    for h in HOR:
        print(f"   r{h} 付与率 {H[f'r{h}'].notna().mean()*100:.0f}% / "
              f"平均{H[f'r{h}'].mean():+.2f}% / 市場超過 平均{H[f'x{h}'].mean():+.2f}%")


if __name__ == "__main__":
    main()
