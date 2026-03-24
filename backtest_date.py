"""
backtest_date.py — 特定日付のシグナルを再現する
使い方: python backtest_date.py 2024-08-06
"""

import sys
import time
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import numpy as np

from screener import (
    fetch_tse_universe,
    judge_signal_pre,
    check_gap_entry,
    batch_download,
    LOOKBACK_DAYS,
    MAX_SIGNALS,
)


def run_backtest(target_date_str: str) -> None:
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()

    # 前日データの取得期間（target_date を含まない = 前日終値まで）
    pre_end   = target_date.strftime("%Y-%m-%d")
    pre_start = (target_date - timedelta(days=LOOKBACK_DAYS + 10)).strftime("%Y-%m-%d")

    # 当日データの取得（始値・終値）
    day_start = target_date.strftime("%Y-%m-%d")
    day_end   = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n{'='*55}")
    print(f"  バックテスト: {target_date}")
    print(f"  条件①〜⑦ 全適用")
    print(f"{'='*55}")

    universe = fetch_tse_universe()
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    print(f"\n[backtest] {len(universe)} 銘柄のデータ取得中...")

    # 前日までデータ
    pre_data = batch_download(tickers, start=pre_start, end=pre_end)
    # 当日データ（始値・終値の確認用）
    day_data = batch_download(tickers, start=day_start, end=day_end)

    print(f"[backtest] 取得完了。判定中...\n")

    signals_pre = []   # ①〜⑥通過
    trades      = []   # ①〜⑦通過（実際のトレード）

    for ticker, pre_df in pre_data.items():
        if len(pre_df) < 30:
            continue
        try:
            name   = name_map.get(ticker, ticker)
            signal = judge_signal_pre(ticker, name, pre_df)
            if signal is None:
                continue

            signals_pre.append(signal)

            # ── 当日の始値・終値を取得 ──────────────────────
            today_df = day_data.get(ticker)
            if today_df is None or today_df.empty:
                print(f"  [{ticker}] {name} → ①〜⑥OK / 当日データなし")
                continue

            today_open  = float(today_df["Open"].iloc[0])
            today_close = float(today_df["Close"].iloc[0])

            if any(v <= 0 or np.isnan(v) for v in [today_open, today_close]):
                print(f"  [{ticker}] {name} → ①〜⑥OK / 始値/終値異常 → スキップ")
                continue

            # ── 条件⑦ ギャップ判定 ──────────────────────────
            gap_ok = check_gap_entry(signal, today_open)
            gap_str = "✅ ギャップOK" if gap_ok else "❌ ギャップNG"

            if signal["direction"] == "BUY":
                pnl_pct = (today_close - today_open) / today_open * 100
            else:
                pnl_pct = (today_open - today_close) / today_open * 100

            result_icon = "✅" if pnl_pct > 0 else "❌"

            print(f"  {result_icon} {name}（{ticker}）")
            print(f"     方向: {signal['direction']}  {gap_str}")
            print(f"     始値: {today_open:.0f}円  終値: {today_close:.0f}円  "
                  f"前日終値: {signal['prev_close']:.0f}円")
            print(f"     損益: {pnl_pct:+.2f}%")
            for r in signal["reason"]:
                print(f"     {r}")
            print()

            if gap_ok:
                trades.append({
                    "ticker":    ticker,
                    "name":      name,
                    "direction": signal["direction"],
                    "open":      today_open,
                    "close":     today_close,
                    "pnl_pct":   round(pnl_pct, 3),
                })

        except Exception as e:
            continue

    # ── 結果サマリー ──────────────────────────────────
    print(f"{'='*55}")
    print(f"  ①〜⑥クリア銘柄: {len(signals_pre)} 件")
    print(f"  ⑦ギャップOK（実際のトレード）: {len(trades)} 件")

    if trades:
        wins    = sum(1 for t in trades if t["pnl_pct"] > 0)
        avg_pnl = sum(t["pnl_pct"] for t in trades) / len(trades)
        print(f"\n  勝率: {wins}/{len(trades)} = {wins/len(trades)*100:.0f}%")
        print(f"  平均損益: {avg_pnl:+.2f}%")
        print(f"\n  銘柄別:")
        for t in trades:
            icon = "✅" if t["pnl_pct"] > 0 else "❌"
            print(f"    {icon} {t['name']}({t['ticker']}) "
                  f"{t['direction']} {t['pnl_pct']:+.2f}%")
    else:
        print(f"\n  トレード対象なし（ノートレード）")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2024-08-06"
    run_backtest(date_str)
