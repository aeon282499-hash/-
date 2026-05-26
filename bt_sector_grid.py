"""
bt_sector_grid.py — Phase 1.1 セクター top_frac グリッドBT

bt_sector_filter.py の発展。top_frac を 0.33/0.4/0.5/0.6/0.75 で振って比較。
window=5 固定（Phase 1 と整合）。
"""
import math
import pickle
import time
from datetime import datetime as dt, timedelta
from pathlib import Path

import jpholiday
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from screener import is_etf_ticker
from sector_filter import fetch_sector33_map, build_sector_ranking, is_ticker_in_top_sector
from bt_sector_filter import (
    precompute_indicators, build_long_df, buy_score, trading_days_between,
    simulate, summarize, yearly_breakdown,
    CACHE_FILE, START, END, SECTOR_WINDOW,
)


GRID = [
    ("OFF",      None),    # ベースライン (フィルタなし)
    ("1/3=.33",  1/3),
    ("0.40",     0.40),
    ("0.50",     0.50),
    ("0.60",     0.60),
    ("0.75",     0.75),
]


def main():
    if not CACHE_FILE.exists():
        print(f"[grid] {CACHE_FILE} がありません"); return

    print("[grid] pickle 読込中...")
    t0 = time.time()
    with open(CACHE_FILE, "rb") as f:
        cache = pickle.load(f)
    all_data = cache["all_data"]
    print(f"[grid] all_data: {len(all_data)} 銘柄  ({time.time()-t0:.0f}s)")

    sector_map = fetch_sector33_map()

    # ランキングは top_frac ごとに再構築 (set 内容が変わるため)
    print("[grid] 指標事前計算...")
    t_pre = time.time()
    df_long = build_long_df(all_data)
    print(f"[grid] long DF: {len(df_long):,} 行 ({time.time()-t_pre:.0f}s)")

    all_trading_days = sorted({d for df in all_data.values() for d in df.index.strftime("%Y-%m-%d")})

    results = []
    yearly_rows = []
    for label, frac in GRID:
        print(f"\n[grid] ===== {label} =====")
        t_g = time.time()
        if frac is None:
            ranking = {}
            sector_on = False
        else:
            ranking, _ = build_sector_ranking(
                all_data, sector_map, window=SECTOR_WINDOW, top_frac=frac
            )
            sector_on = True
        trades = simulate(
            df_long, all_data, all_trading_days,
            sector_filter_on=sector_on, sector_map=sector_map, sector_ranking=ranking,
        )
        s = summarize(trades, label)
        s["frac"] = frac if frac is not None else float("nan")
        s["elapsed"] = time.time() - t_g
        print(f"[grid] {label}: {s['count']}件 / PF{s['pf']:.2f} / 累積{s['cum']:+.1f}% / MaxDD{s['maxdd']:+.1f}% ({s['elapsed']:.0f}s)")
        results.append(s)
        # 年別も保存
        yb = yearly_breakdown(trades)
        yb["filter"] = label
        yearly_rows.append(yb)

    print(f"\n{'='*78}")
    print(f"  Phase 1.1 セクターフィルタ top_frac グリッド ({START} 〜 {END})")
    print(f"{'='*78}")
    print(f"  {'frac':<10} {'件数':>6} {'勝率':>7} {'PF':>6} {'累積%':>9} {'平均':>9} {'MaxDD':>9}")
    print(f"  {'-'*76}")
    for s in results:
        print(
            f"  {s['label']:<10} {s['count']:>6} {s['wr']:>6.1f}% {s['pf']:>6.2f} "
            f"{s['cum']:>+8.1f}% {s['avg']:>+7.3f}% {s['maxdd']:>+8.1f}%"
        )
    print(f"{'='*78}\n")

    # 年別
    print(f"{'='*78}")
    print(f"  年別 PF / 累積%")
    print(f"{'='*78}")
    all_y = pd.concat(yearly_rows, ignore_index=True)
    pivot_pf = all_y.pivot_table(index="year", columns="filter", values="pf", aggfunc="first")
    pivot_cum = all_y.pivot_table(index="year", columns="filter", values="cum_pct", aggfunc="first")
    pivot_n = all_y.pivot_table(index="year", columns="filter", values="count", aggfunc="first")
    cols = [g[0] for g in GRID]
    pivot_pf = pivot_pf[cols] if all(c in pivot_pf.columns for c in cols) else pivot_pf
    pivot_cum = pivot_cum[cols] if all(c in pivot_cum.columns for c in cols) else pivot_cum
    pivot_n = pivot_n[cols] if all(c in pivot_n.columns for c in cols) else pivot_n
    print("\n[件数]");  print(pivot_n.to_string(float_format=lambda x: f"{x:.0f}"))
    print("\n[PF]");    print(pivot_pf.to_string(float_format=lambda x: f"{x:.2f}"))
    print("\n[累積%]"); print(pivot_cum.to_string(float_format=lambda x: f"{x:+.1f}"))

    # CSV
    df_summary = pd.DataFrame(results)
    df_summary.to_csv("bt_sector_grid_summary.csv", index=False, encoding="utf-8-sig")
    all_y.to_csv("bt_sector_grid_yearly.csv", index=False, encoding="utf-8-sig")
    print(f"\n[grid] 出力: bt_sector_grid_summary.csv / bt_sector_grid_yearly.csv")
    print(f"[grid] 総経過: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
