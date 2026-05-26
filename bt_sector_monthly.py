"""
bt_sector_monthly.py — Sec_OR_Theme BTの月別シグナル件数 (2026年中心)
"""
import pickle
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from sector_filter import fetch_sector33_map, build_sector_ranking
from bt_sector_filter import build_long_df, CACHE_FILE
from bt_sector_phase2 import simulate_mode
from bt_sector_phase3_theme import build_theme_universe


def main():
    print("[monthly] pickle 読込中...")
    t0 = time.time()
    with open(CACHE_FILE, "rb") as f:
        cache = pickle.load(f)
    all_data = cache["all_data"]
    name_map = cache["name_map"]
    print(f"[monthly] all_data: {len(all_data)} 銘柄 ({time.time()-t0:.0f}s)")

    sector_map = fetch_sector33_map()
    theme_universe = build_theme_universe(name_map)
    print(f"[monthly] theme universe: {len(theme_universe)} 銘柄")

    print("[monthly] セクターランキング構築 (window=20, frac=0.5)...")
    sector_ranking, _ = build_sector_ranking(all_data, sector_map, window=20, top_frac=0.5)

    print("[monthly] long DF 構築...")
    df_long = build_long_df(all_data)
    all_trading_days = sorted({d for df in all_data.values() for d in df.index.strftime("%Y-%m-%d")})

    print("[monthly] Sec_OR_Theme シミュレーション...")
    trades = simulate_mode(
        df_long, all_data, all_trading_days,
        mode="Sec_OR_White",
        sector_map=sector_map,
        sector_ranking=sector_ranking,
        whitelist=theme_universe,
    )
    df = pd.DataFrame(trades)
    df["sig_month"] = df["signal_date"].str[:7]
    df["year"] = df["signal_date"].str[:4]
    print(f"[monthly] trades: {len(df)} 件")

    # 月別集計 (全期間)
    rows = []
    for month, g in df.groupby("sig_month"):
        n = len(g)
        wins = (g["pnl_pct"] > 0).sum()
        gw = g[g["pnl_pct"] > 0]["pnl_pct"].sum()
        gl = -g[g["pnl_pct"] < 0]["pnl_pct"].sum()
        pf = gw / gl if gl > 0 else float("inf")
        cum = g["pnl_pct"].sum()
        rows.append({
            "month": month,
            "n": n,
            "win_rate": wins / n * 100,
            "pf": pf,
            "cum_pct": cum,
        })
    monthly = pd.DataFrame(rows)

    print("\n" + "=" * 60)
    print("  Sec_OR_Theme 月別シグナル件数 (全期間)")
    print("=" * 60)
    print(monthly.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    # 2026年抽出
    df_2026 = df[df["year"] == "2026"]
    print("\n" + "=" * 60)
    print(f"  2026年詳細 (1〜5月)・合計 {len(df_2026)} 件")
    print("=" * 60)
    m2026 = monthly[monthly["month"].str.startswith("2026")]
    print(m2026.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    # 2026月平均
    if len(m2026) > 0:
        print(f"\n  2026月平均: {m2026['n'].mean():.1f} 件/月  "
              f"PF平均: {m2026['pf'].mean():.2f}  "
              f"累積%平均: {m2026['cum_pct'].mean():+.1f}/月")

    # 比較: OFF (素のスイング) も同様に月別件数
    print("\n[monthly] (比較用) OFF=素のスイングも実行...")
    trades_off = simulate_mode(
        df_long, all_data, all_trading_days,
        mode="OFF",
        sector_map=sector_map,
        sector_ranking=sector_ranking,
        whitelist=theme_universe,
    )
    df_off = pd.DataFrame(trades_off)
    df_off["sig_month"] = df_off["signal_date"].str[:7]
    off_2026_monthly = df_off[df_off["signal_date"].str[:4] == "2026"].groupby("sig_month").size()

    print("\n" + "=" * 60)
    print("  2026年: 素のスイング vs Sec_OR_Theme 月別件数比較")
    print("=" * 60)
    print(f"  {'month':<10} {'素':>6} {'Sec_OR_Theme':>14} {'残存率':>8}")
    print(f"  {'-'*45}")
    for m, n in off_2026_monthly.items():
        n_new = int(m2026[m2026["month"] == m]["n"].iloc[0]) if (m2026["month"] == m).any() else 0
        ratio = (n_new / n * 100) if n > 0 else 0
        print(f"  {m:<10} {n:>6} {n_new:>14} {ratio:>7.1f}%")

    monthly.to_csv("bt_sector_monthly.csv", index=False, encoding="utf-8-sig")
    print(f"\n[monthly] 出力: bt_sector_monthly.csv ({time.time()-t0:.0f}s)")


if __name__ == "__main__":
    main()
