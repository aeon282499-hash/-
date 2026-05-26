"""
bt_sector_phase2.py — Phase 2 セクター×ホワイトリスト 比較BT

Phase 1 best (window=20, top_frac=0.50) を固定し、
ホワイトリスト 28銘柄との組合せロジック別に比較する。

モード:
  M0 OFF                — フィルタなし (ベースライン)
  M1 Sector             — セクター上位のみ (Phase 1 best)
  M2 Sector OR White    — セクター上位 もしくは ホワイトリスト
  M3 Sector AND White   — ホワイトリスト中、セクター上位のものだけ
  M4 White only         — ホワイトリストのみ
"""
import json
import pickle
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from sector_filter import fetch_sector33_map, build_sector_ranking, is_ticker_in_top_sector
from bt_sector_filter import (
    build_long_df, summarize, yearly_breakdown,
    CACHE_FILE, START, END,
)
from bt_sector_filter import (
    RSI_MAX, DEV_MAX, ATR_CAP, TURNOVER_MIN, RANGE_MULT, VOL_MULT,
    MAX_SIGNALS, MAX_HOLD, STOP_LOSS, TAKE_PROFIT, RSI_PERIOD, buy_score,
)
import math
import numpy as np

WINDOW = 20
TOP_FRAC = 0.50
WHITELIST_PATH = Path("whitelist_world_top.json")


def load_whitelist() -> set[str]:
    with open(WHITELIST_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return set(data["tickers"].keys())


def simulate_mode(
    df_long: pd.DataFrame,
    all_data: dict,
    all_trading_days: list[str],
    *,
    mode: str,
    sector_map: dict[str, str],
    sector_ranking: dict[str, set[str]],
    whitelist: set[str],
) -> list[dict]:
    base = df_long[
        (df_long["RSI"] <= RSI_MAX)
        & (df_long["DEV"] <= DEV_MAX)
        & (df_long["ATR_PCT"] <= ATR_CAP)
        & (df_long["TURNOVER_PREV"] >= TURNOVER_MIN)
        & (
            (df_long["RANGE_RATIO_PREV"] >= RANGE_MULT)
            | (df_long["VOL_RATIO_PREV"] >= VOL_MULT)
        )
    ].copy()

    base["score"] = base.apply(
        lambda r: buy_score(r["RSI"], r["DEV"], r["TURNOVER_PREV"]), axis=1
    )
    base = base.sort_values(["DateStr", "score"], ascending=[True, False])

    def pass_sector(row) -> bool:
        return is_ticker_in_top_sector(
            row["Ticker"], row["DateStr"], sector_map, sector_ranking, all_trading_days
        )

    def pass_white(row) -> bool:
        return row["Ticker"] in whitelist

    if mode == "OFF":
        pass
    elif mode == "Sector":
        base = base[base.apply(pass_sector, axis=1)]
    elif mode == "Sec_OR_White":
        base = base[base.apply(lambda r: pass_sector(r) or pass_white(r), axis=1)]
    elif mode == "Sec_AND_White":
        base = base[base.apply(lambda r: pass_sector(r) and pass_white(r), axis=1)]
    elif mode == "White_only":
        base = base[base["Ticker"].isin(whitelist)]
    else:
        raise ValueError(f"unknown mode: {mode}")

    candidates_by_sig: dict[str, list[str]] = {}
    for sig_date, grp in base.groupby("DateStr"):
        candidates_by_sig[sig_date] = list(grp["Ticker"].head(MAX_SIGNALS))

    trades = []
    open_until: dict[str, str] = {}
    atd_index = {d: i for i, d in enumerate(all_trading_days)}

    for sig_date in sorted(candidates_by_sig.keys()):
        if sig_date not in atd_index:
            continue
        i = atd_index[sig_date]
        if i + 1 >= len(all_trading_days):
            continue
        trade_date = all_trading_days[i + 1]
        if trade_date < START or trade_date > END:
            continue
        for ticker in candidates_by_sig[sig_date]:
            if ticker in open_until and open_until[ticker] >= trade_date:
                continue
            full_df = all_data.get(ticker)
            if full_df is None:
                continue
            entry_rows = full_df[full_df.index.strftime("%Y-%m-%d") == trade_date]
            if entry_rows.empty:
                continue
            entry_open = float(entry_rows["Open"].iloc[0])
            if entry_open <= 0 or np.isnan(entry_open):
                continue
            stop_price = entry_open * (1 - STOP_LOSS / 100)
            tp_price = entry_open * (1 + TAKE_PROFIT / 100)
            pnl_pct = exit_date = exit_type = None
            for hold_day in range(1, MAX_HOLD + 1):
                day_idx = atd_index[trade_date] + (hold_day - 1)
                if day_idx >= len(all_trading_days):
                    break
                check_date = all_trading_days[day_idx]
                day_rows = full_df[full_df.index.strftime("%Y-%m-%d") == check_date]
                if day_rows.empty:
                    continue
                day_open = float(day_rows["Open"].iloc[0])
                day_high = float(day_rows["High"].iloc[0])
                day_low = float(day_rows["Low"].iloc[0])
                day_close = float(day_rows["Close"].iloc[0])
                if any(v <= 0 or np.isnan(v) for v in [day_open, day_high, day_low, day_close]):
                    continue
                if day_low <= stop_price:
                    pnl_pct, exit_date, exit_type = -STOP_LOSS, check_date, "STOP"
                    break
                if day_high >= tp_price:
                    pnl_pct, exit_date, exit_type = +TAKE_PROFIT, check_date, "TP"
                    break
                pre = full_df.loc[full_df.index.strftime("%Y-%m-%d") <= check_date]
                cl = pre["Close"].dropna()
                if len(cl) >= RSI_PERIOD + 1:
                    delta = cl.diff()
                    g = delta.clip(lower=0)
                    l = -delta.clip(upper=0)
                    ag = g.ewm(alpha=1/RSI_PERIOD, min_periods=RSI_PERIOD).mean()
                    al = l.ewm(alpha=1/RSI_PERIOD, min_periods=RSI_PERIOD).mean()
                    rs = ag / al.replace(0, np.nan)
                    rsi_now = (100 - 100/(1+rs)).iloc[-1]
                    rsi_exit = pd.notna(rsi_now) and rsi_now >= 50
                else:
                    rsi_exit = False
                if rsi_exit or hold_day == MAX_HOLD:
                    pnl_pct = (day_close - entry_open) / entry_open * 100
                    exit_date = check_date
                    exit_type = "RSI" if rsi_exit else "MAXHOLD"
                    break
            if pnl_pct is None:
                continue
            trades.append({"signal_date": sig_date, "entry_date": trade_date,
                           "exit_date": exit_date, "ticker": ticker,
                           "pnl_pct": pnl_pct, "exit_type": exit_type})
            open_until[ticker] = exit_date
    return trades


def main():
    if not CACHE_FILE.exists():
        print(f"[ph2] {CACHE_FILE} がありません"); return

    print("[ph2] pickle 読込中...")
    t0 = time.time()
    with open(CACHE_FILE, "rb") as f:
        cache = pickle.load(f)
    all_data = cache["all_data"]
    print(f"[ph2] all_data: {len(all_data)} 銘柄  ({time.time()-t0:.0f}s)")

    sector_map = fetch_sector33_map()
    whitelist = load_whitelist()
    print(f"[ph2] ホワイトリスト: {len(whitelist)} 銘柄")

    print(f"[ph2] セクターランキング構築 (window={WINDOW}, frac={TOP_FRAC})...")
    sector_ranking, _ = build_sector_ranking(
        all_data, sector_map, window=WINDOW, top_frac=TOP_FRAC
    )

    print("[ph2] 指標事前計算...")
    t_pre = time.time()
    df_long = build_long_df(all_data)
    print(f"[ph2] long DF: {len(df_long):,} 行 ({time.time()-t_pre:.0f}s)")

    all_trading_days = sorted({d for df in all_data.values() for d in df.index.strftime("%Y-%m-%d")})

    modes = ["OFF", "Sector", "Sec_OR_White", "Sec_AND_White", "White_only"]
    results = []
    yearly_rows = []
    for m in modes:
        print(f"\n[ph2] ===== {m} =====")
        t_g = time.time()
        trades = simulate_mode(
            df_long, all_data, all_trading_days,
            mode=m,
            sector_map=sector_map,
            sector_ranking=sector_ranking,
            whitelist=whitelist,
        )
        s = summarize(trades, m)
        s["elapsed"] = time.time() - t_g
        print(f"[ph2] {m}: {s['count']}件 / PF{s['pf']:.2f} / 累積{s['cum']:+.1f}% / MaxDD{s['maxdd']:+.1f}% ({s['elapsed']:.0f}s)")
        results.append(s)
        yb = yearly_breakdown(trades); yb["mode"] = m; yearly_rows.append(yb)

    print(f"\n{'='*82}")
    print(f"  Phase 2 セクター×ホワイトリスト ({START} 〜 {END})")
    print(f"  Phase1 best: window={WINDOW} / top_frac={TOP_FRAC} / whitelist={len(whitelist)}銘柄")
    print(f"{'='*82}")
    print(f"  {'mode':<16} {'件数':>6} {'勝率':>7} {'PF':>6} {'累積%':>9} {'平均':>9} {'MaxDD':>9}")
    print(f"  {'-'*80}")
    for s in results:
        print(
            f"  {s['label']:<16} {s['count']:>6} {s['wr']:>6.1f}% {s['pf']:>6.2f} "
            f"{s['cum']:>+8.1f}% {s['avg']:>+7.3f}% {s['maxdd']:>+8.1f}%"
        )
    print(f"{'='*82}\n")

    print(f"{'='*82}")
    print(f"  年別 PF / 累積% / 件数 (モード別)")
    print(f"{'='*82}")
    all_y = pd.concat(yearly_rows, ignore_index=True)
    pivot_pf = all_y.pivot_table(index="year", columns="mode", values="pf", aggfunc="first")
    pivot_cum = all_y.pivot_table(index="year", columns="mode", values="cum_pct", aggfunc="first")
    pivot_n = all_y.pivot_table(index="year", columns="mode", values="count", aggfunc="first")
    cols = [m for m in modes if m in pivot_pf.columns]
    pivot_pf = pivot_pf[cols]; pivot_cum = pivot_cum[cols]; pivot_n = pivot_n[cols]
    print("\n[件数]");   print(pivot_n.to_string(float_format=lambda x: f"{x:.0f}"))
    print("\n[PF]");     print(pivot_pf.to_string(float_format=lambda x: f"{x:.2f}"))
    print("\n[累積%]");  print(pivot_cum.to_string(float_format=lambda x: f"{x:+.1f}"))

    pd.DataFrame(results).to_csv("bt_sector_phase2_summary.csv", index=False, encoding="utf-8-sig")
    all_y.to_csv("bt_sector_phase2_yearly.csv", index=False, encoding="utf-8-sig")
    print(f"\n[ph2] 出力: bt_sector_phase2_summary.csv / bt_sector_phase2_yearly.csv")
    print(f"[ph2] 総経過: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
