"""
bt_sector_window_grid.py — Phase 1.2 セクターランキング window 長グリッドBT

top_frac=0.50 固定（Phase 1.1 best）で window を振って最強値を探る。
"""
import pickle
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from sector_filter import fetch_sector33_map, build_sector_ranking
from bt_sector_filter import (
    build_long_df, simulate, summarize, yearly_breakdown,
    CACHE_FILE, START, END,
)


TOP_FRAC = 0.50
WINDOWS = [3, 5, 10, 15, 20]


def main():
    if not CACHE_FILE.exists():
        print(f"[wgrid] {CACHE_FILE} がありません"); return

    print("[wgrid] pickle 読込中...")
    t0 = time.time()
    with open(CACHE_FILE, "rb") as f:
        cache = pickle.load(f)
    all_data = cache["all_data"]
    print(f"[wgrid] all_data: {len(all_data)} 銘柄  ({time.time()-t0:.0f}s)")

    sector_map = fetch_sector33_map()

    print("[wgrid] 指標事前計算...")
    t_pre = time.time()
    df_long = build_long_df(all_data)
    print(f"[wgrid] long DF: {len(df_long):,} 行 ({time.time()-t_pre:.0f}s)")

    all_trading_days = sorted({d for df in all_data.values() for d in df.index.strftime("%Y-%m-%d")})

    results = []
    yearly_rows = []

    # OFF baseline
    print(f"\n[wgrid] ===== OFF (基準) =====")
    t_g = time.time()
    trades = simulate(
        df_long, all_data, all_trading_days,
        sector_filter_on=False, sector_map=sector_map, sector_ranking={},
    )
    s = summarize(trades, "OFF")
    s["window"] = None
    s["elapsed"] = time.time() - t_g
    print(f"[wgrid] OFF: {s['count']}件 / PF{s['pf']:.2f} / 累積{s['cum']:+.1f}% / MaxDD{s['maxdd']:+.1f}% ({s['elapsed']:.0f}s)")
    results.append(s)
    yb = yearly_breakdown(trades); yb["filter"] = "OFF"; yearly_rows.append(yb)

    for w in WINDOWS:
        label = f"w={w}"
        print(f"\n[wgrid] ===== {label} (frac=0.50) =====")
        t_g = time.time()
        ranking, _ = build_sector_ranking(
            all_data, sector_map, window=w, top_frac=TOP_FRAC
        )
        trades = simulate(
            df_long, all_data, all_trading_days,
            sector_filter_on=True, sector_map=sector_map, sector_ranking=ranking,
        )
        s = summarize(trades, label)
        s["window"] = w
        s["elapsed"] = time.time() - t_g
        print(f"[wgrid] {label}: {s['count']}件 / PF{s['pf']:.2f} / 累積{s['cum']:+.1f}% / MaxDD{s['maxdd']:+.1f}% ({s['elapsed']:.0f}s)")
        results.append(s)
        yb = yearly_breakdown(trades); yb["filter"] = label; yearly_rows.append(yb)

    print(f"\n{'='*80}")
    print(f"  Phase 1.2 window グリッド (top_frac=0.50固定, {START} 〜 {END})")
    print(f"{'='*80}")
    print(f"  {'window':<8} {'件数':>6} {'勝率':>7} {'PF':>6} {'累積%':>9} {'平均':>9} {'MaxDD':>9}")
    print(f"  {'-'*78}")
    for s in results:
        print(
            f"  {s['label']:<8} {s['count']:>6} {s['wr']:>6.1f}% {s['pf']:>6.2f} "
            f"{s['cum']:>+8.1f}% {s['avg']:>+7.3f}% {s['maxdd']:>+8.1f}%"
        )
    print(f"{'='*80}\n")

    # 年別
    print(f"{'='*80}")
    print(f"  年別PF / 累積% (window別)")
    print(f"{'='*80}")
    all_y = pd.concat(yearly_rows, ignore_index=True)
    pivot_pf = all_y.pivot_table(index="year", columns="filter", values="pf", aggfunc="first")
    pivot_cum = all_y.pivot_table(index="year", columns="filter", values="cum_pct", aggfunc="first")
    pivot_n = all_y.pivot_table(index="year", columns="filter", values="count", aggfunc="first")
    cols = ["OFF"] + [f"w={w}" for w in WINDOWS]
    cols = [c for c in cols if c in pivot_pf.columns]
    pivot_pf = pivot_pf[cols]; pivot_cum = pivot_cum[cols]; pivot_n = pivot_n[cols]
    print("\n[件数]");   print(pivot_n.to_string(float_format=lambda x: f"{x:.0f}"))
    print("\n[PF]");     print(pivot_pf.to_string(float_format=lambda x: f"{x:.2f}"))
    print("\n[累積%]");  print(pivot_cum.to_string(float_format=lambda x: f"{x:+.1f}"))

    pd.DataFrame(results).to_csv("bt_sector_window_summary.csv", index=False, encoding="utf-8-sig")
    all_y.to_csv("bt_sector_window_yearly.csv", index=False, encoding="utf-8-sig")
    print(f"\n[wgrid] 出力: bt_sector_window_summary.csv / bt_sector_window_yearly.csv")
    print(f"[wgrid] 総経過: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
