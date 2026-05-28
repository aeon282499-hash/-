"""bt_sector_compare_earnings.py — 決算除外 ON/OFF 比較 (3-5月)"""
import pickle
import time

from dotenv import load_dotenv
load_dotenv()

import bt_sector_filter as bsf
import bt_sector_phase2 as bsp2
import pandas as pd

from sector_filter import fetch_sector33_map, build_sector_ranking
from bt_sector_phase2 import simulate_mode
from bt_sector_phase3_theme import build_theme_universe

bsf.END = "2026-05-26"
bsp2.END = "2026-05-26"


def stats(df, month):
    sub = df[df["entry_date"].str.startswith(month)]
    if sub.empty:
        return {"month": month, "n": 0, "wr": 0, "pf": 0, "cum": 0, "yen": 0}
    wins = (sub["pnl_pct"] > 0).sum()
    gw = sub[sub["pnl_pct"] > 0]["pnl_pct"].sum()
    gl = -sub[sub["pnl_pct"] < 0]["pnl_pct"].sum()
    pf = gw / gl if gl > 0 else float("inf")
    return {
        "month": month, "n": len(sub),
        "wr": wins / len(sub) * 100,
        "pf": pf,
        "cum": sub["pnl_pct"].sum(),
        "yen": round(sub["pnl_pct"].sum() / 100 * 1_000_000),
    }


def main():
    t0 = time.time()
    with open(bsf.CACHE_FILE, "rb") as f:
        cache = pickle.load(f)
    all_data = cache["all_data"]
    name_map = cache["name_map"]

    sector_map = fetch_sector33_map()
    theme_universe = build_theme_universe(name_map)
    sector_ranking, _ = build_sector_ranking(all_data, sector_map, window=20, top_frac=0.5)
    df_long = bsf.build_long_df(all_data)
    all_trading_days = sorted({d for df in all_data.values() for d in df.index.strftime("%Y-%m-%d")})

    print("\n=== 決算除外 OFF (旧BT) ===")
    trades_off = simulate_mode(
        df_long, all_data, all_trading_days,
        mode="Sec_OR_White",
        sector_map=sector_map, sector_ranking=sector_ranking,
        whitelist=theme_universe,
        apply_earnings_exclusion=False,
    )
    df_off = pd.DataFrame(trades_off)

    print("\n=== 決算除外 ON (本番準拠) ===")
    trades_on = simulate_mode(
        df_long, all_data, all_trading_days,
        mode="Sec_OR_White",
        sector_map=sector_map, sector_ranking=sector_ranking,
        whitelist=theme_universe,
        apply_earnings_exclusion=True,
    )
    df_on = pd.DataFrame(trades_on)

    print(f"\n総件数: OFF={len(df_off)} / ON={len(df_on)} (除外 {len(df_off)-len(df_on)} 件)")

    print(f"\n{'='*90}")
    print(f"  {'month':<10} {'mode':<7} {'件数':>6} {'勝率%':>7} {'PF':>6} {'累積%':>9} {'円換算':>12}")
    print(f"{'='*90}")
    for m in ["2026-03", "2026-04", "2026-05"]:
        s_off = stats(df_off, m)
        s_on = stats(df_on, m)
        print(f"  {m:<10} {'OFF':<7} {s_off['n']:>6} {s_off['wr']:>6.1f}% {s_off['pf']:>6.2f} {s_off['cum']:>+8.2f}% {s_off['yen']:>+12,}円")
        print(f"  {m:<10} {'ON':<7} {s_on['n']:>6} {s_on['wr']:>6.1f}% {s_on['pf']:>6.2f} {s_on['cum']:>+8.2f}% {s_on['yen']:>+12,}円")
        print(f"  {'-'*88}")

    # 3-5月通算
    off_3m = df_off[df_off["entry_date"].str.match(r"2026-0[345]")]
    on_3m = df_on[df_on["entry_date"].str.match(r"2026-0[345]")]
    for name, sub in [("OFF合計", off_3m), ("ON合計", on_3m)]:
        wins = (sub["pnl_pct"] > 0).sum()
        gw = sub[sub["pnl_pct"] > 0]["pnl_pct"].sum()
        gl = -sub[sub["pnl_pct"] < 0]["pnl_pct"].sum()
        pf = gw / gl if gl > 0 else float("inf")
        cum = sub["pnl_pct"].sum()
        print(f"  3-5月{name:<7} {len(sub):>6} {wins/len(sub)*100:>6.1f}% {pf:>6.2f} {cum:>+8.2f}% {round(cum/100*1_000_000):>+12,}円")

    # 除外された銘柄サンプル (4月)
    off_apr = df_off[df_off["entry_date"].str.startswith("2026-04")]
    on_apr_keys = set(zip(df_on["entry_date"], df_on["ticker"]))
    excluded = off_apr[~off_apr.apply(lambda r: (r["entry_date"], r["ticker"]) in on_apr_keys, axis=1)].copy()
    excluded["name"] = excluded["ticker"].map(name_map)
    print(f"\n[4月 決算除外された銘柄: {len(excluded)} 件]")
    print(excluded[["entry_date", "ticker", "name", "pnl_pct", "exit_type"]].to_string(index=False))

    print(f"\n経過: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
