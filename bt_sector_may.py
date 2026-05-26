"""
bt_sector_may.py — 2026年5月（キャッシュ範囲5/11まで）の Sec_OR_Theme 実績
"""
import pickle
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

import bt_sector_filter as bsf
from sector_filter import fetch_sector33_map, build_sector_ranking
from bt_sector_phase2 import simulate_mode
from bt_sector_phase3_theme import build_theme_universe

# END を 2026-05-11 まで延長 (キャッシュ最終日)
bsf.END = "2026-05-11"


def main():
    print("[may] pickle 読込中...")
    t0 = time.time()
    with open(bsf.CACHE_FILE, "rb") as f:
        cache = pickle.load(f)
    all_data = cache["all_data"]
    name_map = cache["name_map"]
    print(f"[may] all_data: {len(all_data)} 銘柄 ({time.time()-t0:.0f}s)")

    sector_map = fetch_sector33_map()
    theme_universe = build_theme_universe(name_map)

    print("[may] セクターランキング構築 (window=20, frac=0.5)...")
    sector_ranking, _ = build_sector_ranking(all_data, sector_map, window=20, top_frac=0.5)

    print("[may] long DF 構築...")
    df_long = bsf.build_long_df(all_data)
    all_trading_days = sorted({d for df in all_data.values() for d in df.index.strftime("%Y-%m-%d")})

    print("[may] Sec_OR_Theme シミュレーション (END=2026-05-11)...")
    trades = simulate_mode(
        df_long, all_data, all_trading_days,
        mode="Sec_OR_White",
        sector_map=sector_map,
        sector_ranking=sector_ranking,
        whitelist=theme_universe,
    )
    df = pd.DataFrame(trades)
    df["sig_month"] = df["entry_date"].str[:7]
    print(f"[may] trades total: {len(df)} 件")

    # 2026年5月分抽出 (entry_date)
    may = df[df["entry_date"].str.startswith("2026-05")].copy()
    print(f"\n[may] 2026-05 (5/1〜5/11): {len(may)} 件")

    if not may.empty:
        wins = (may["pnl_pct"] > 0).sum()
        gw = may[may["pnl_pct"] > 0]["pnl_pct"].sum()
        gl = -may[may["pnl_pct"] < 0]["pnl_pct"].sum()
        pf = gw / gl if gl > 0 else float("inf")
        cum = may["pnl_pct"].sum()
        avg = may["pnl_pct"].mean()
        print(f"  勝率: {wins/len(may)*100:.1f}%")
        print(f"  PF: {pf:.2f}")
        print(f"  累積%: {cum:+.2f}%")
        print(f"  平均/件: {avg:+.3f}%")

        print(f"\n  詳細(entry_date順):")
        cols = ["entry_date", "exit_date", "ticker", "pnl_pct", "exit_type"]
        print(may[cols].sort_values("entry_date").to_string(index=False))

    # 比較: 素のスイングの5月件数
    print("\n[may] (比較) 素のスイング 5月集計...")
    trades_off = simulate_mode(
        df_long, all_data, all_trading_days,
        mode="OFF",
        sector_map=sector_map,
        sector_ranking=sector_ranking,
        whitelist=theme_universe,
    )
    df_off = pd.DataFrame(trades_off)
    off_may = df_off[df_off["entry_date"].str.startswith("2026-05")]
    print(f"\n  素のスイング 5月 (5/1〜5/11): {len(off_may)} 件")
    if not off_may.empty:
        owins = (off_may["pnl_pct"] > 0).sum()
        ogw = off_may[off_may["pnl_pct"] > 0]["pnl_pct"].sum()
        ogl = -off_may[off_may["pnl_pct"] < 0]["pnl_pct"].sum()
        opf = ogw / ogl if ogl > 0 else float("inf")
        ocum = off_may["pnl_pct"].sum()
        print(f"  素: 勝率{owins/len(off_may)*100:.1f}% / PF{opf:.2f} / 累積{ocum:+.2f}%")

    print(f"\n[may] 注意: BTキャッシュは 2026-05-11 まで。5/12以降のデータは未取得。")
    print(f"[may] 経過: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
