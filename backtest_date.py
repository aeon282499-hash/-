"""
backtest_date.py — 特定日付のシグナルを再現する
使い方: python backtest_date.py 2025-03-19
"""

import sys
import time
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from screener import (
    fetch_tse_universe, judge_signal,
    RSI_BUY_MAX, RSI_SELL_MIN, DEV_BUY_MAX, DEV_SELL_MIN,
    RANGE_MULT, VOL_MULT, MAX_SIGNALS, BATCH_SIZE
)

def run_backtest(target_date_str: str):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    end_date    = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    start_date  = (target_date - timedelta(days=120)).strftime("%Y-%m-%d")

    print(f"\n{'='*55}")
    print(f"  バックテスト: {target_date}")
    print(f"{'='*55}")

    universe = fetch_tse_universe()
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    print(f"[backtest] {len(universe)} 銘柄を {start_date} 〜 {end_date} で取得中...")

    # バッチダウンロード
    data = {}
    batches = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    for idx, batch in enumerate(batches):
        print(f"  [batch {idx+1}/{len(batches)}] {len(batch)} 銘柄...")
        try:
            raw = yf.download(
                batch, start=start_date, end=end_date,
                interval="1d", auto_adjust=True,
                progress=False, group_by="ticker"
            )
            for ticker in batch:
                try:
                    df = raw[ticker].copy() if len(batch) > 1 else raw.copy()
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(0)
                    if not df.empty:
                        data[ticker] = df
                except Exception:
                    pass
        except Exception as e:
            print(f"  エラー: {e}")
        time.sleep(0.5)

    print(f"\n[backtest] {len(data)} 銘柄のデータ取得完了。判定中...\n")

    signals = []
    for ticker, df in data.items():
        if len(signals) >= MAX_SIGNALS:
            break
        name   = name_map.get(ticker, ticker)
        result = judge_signal(ticker, name, df)
        if result:
            signals.append(result)
            print(f"  ✅ {name}（{ticker}）→ {result['direction']}")
            for r in result["reason"]:
                print(f"       {r}")

    print(f"\n{'='*55}")
    if signals:
        print(f"  結果: {len(signals)} 銘柄がシグナルを満たしました")
        for s in signals:
            print(f"  → {s['name']}（{s['ticker']}）{s['direction']}")
    else:
        print("  結果: 0 件（ノートレード）")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2025-03-19"
    run_backtest(date_str)
