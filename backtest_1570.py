"""
backtest_1570.py — 1570 ETF デイトレバックテスト
=================================================

使い方:
  python backtest_1570.py 2024-01-01 2025-12-31

戦略:
  エントリー : シグナル翌営業日の始値
  エグジット : 同日の終値（15:30大引け）
  損切り     : 始値 -3%
  利確       : 始値 +5%
"""

import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

import jpholiday
import pandas as pd
import numpy as np

from screener import batch_download_jquants, _jquants_id_token
from screener_1570 import judge_signal_1570, TICKER_1570, TICKER_SP500P, LOOKBACK_DAYS

STOP_LOSS   = 3.0
TAKE_PROFIT = 5.0


def get_trading_days(start: str, end: str) -> list[str]:
    days = []
    cur  = datetime.strptime(start, "%Y-%m-%d").date()
    end_ = datetime.strptime(end,   "%Y-%m-%d").date()
    while cur <= end_:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


def run_backtest(start: str, end: str) -> None:
    trading_days = get_trading_days(start, end)
    print(f"\n{'='*60}")
    print(f"  1570 ETF バックテスト期間: {start} 〜 {end}")
    print(f"  営業日数: {len(trading_days)} 日")
    print(f"{'='*60}\n")

    fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d")

    print(f"[backtest_1570] データ取得中（{fetch_start} 〜 {end}）...")
    token         = _jquants_id_token()
    all_data      = batch_download_jquants(token, start=fetch_start, end=end, tickers=[TICKER_1570, TICKER_SP500P])
    df_1570_full  = all_data.get(TICKER_1570)
    df_sp500_full = all_data.get(TICKER_SP500P)
    print(f"[backtest_1570] データ取得完了\n")

    if df_1570_full is None or df_sp500_full is None:
        print("データ取得失敗")
        return
    sp500_dates = sorted(df_sp500_full.index.strftime("%Y-%m-%d").tolist())

    all_trading_days = get_trading_days(fetch_start, end)
    trades: list[dict] = []

    for signal_date in trading_days:
        pre_1570 = df_1570_full[df_1570_full.index.strftime("%Y-%m-%d") <= signal_date].copy()
        if len(pre_1570) < 20:
            continue

        pre_sp500 = df_sp500_full[df_sp500_full.index.strftime("%Y-%m-%d") <= signal_date].copy()
        if len(pre_sp500) < 2:
            continue
        sp500_ret = (float(pre_sp500["Close"].iloc[-1]) - float(pre_sp500["Close"].iloc[-2])) / float(pre_sp500["Close"].iloc[-2]) * 100

        signal = judge_signal_1570(pre_1570, sp500_ret)
        if signal["direction"] == "PASS":
            continue

        idx = all_trading_days.index(signal_date) if signal_date in all_trading_days else -1
        if idx < 0 or idx + 1 >= len(all_trading_days):
            continue
        entry_date = all_trading_days[idx + 1]

        entry_rows = df_1570_full[df_1570_full.index.strftime("%Y-%m-%d") == entry_date]
        if entry_rows.empty:
            continue

        entry_open  = float(entry_rows["Open"].iloc[0])
        entry_close = float(entry_rows["Close"].iloc[0])
        entry_high  = float(entry_rows["High"].iloc[0])
        entry_low   = float(entry_rows["Low"].iloc[0])

        if any(v <= 0 or np.isnan(v) for v in [entry_open, entry_close]):
            continue

        direction = signal["direction"]
        if direction == "BUY":
            stop_p = entry_open * (1 - STOP_LOSS   / 100)
            tp_p   = entry_open * (1 + TAKE_PROFIT / 100)
            if entry_low <= stop_p:
                pnl, etype = -STOP_LOSS, "STOP"
            elif entry_high >= tp_p:
                pnl, etype = +TAKE_PROFIT, "TP"
            else:
                pnl   = (entry_close - entry_open) / entry_open * 100
                etype = "CLOSE"
        else:
            stop_p = entry_open * (1 + STOP_LOSS   / 100)
            tp_p   = entry_open * (1 - TAKE_PROFIT / 100)
            if entry_high >= stop_p:
                pnl, etype = -STOP_LOSS, "STOP"
            elif entry_low <= tp_p:
                pnl, etype = +TAKE_PROFIT, "TP"
            else:
                pnl   = (entry_open - entry_close) / entry_open * 100
                etype = "CLOSE"

        trades.append({
            "signal_date": signal_date,
            "entry_date":  entry_date,
            "direction":   direction,
            "sp500_ret":   signal.get("sp500_ret"),
            "rsi":         signal.get("rsi"),
            "entry_open":  entry_open,
            "entry_close": entry_close,
            "pnl_pct":     round(pnl, 3),
            "exit_type":   etype,
            "win":         pnl > 0,
        })

    _print_results(trades, start, end)


def _print_results(trades: list[dict], start: str, end: str) -> None:
    print(f"\n{'='*60}")
    print(f"  1570 ETF バックテスト結果 ({start} 〜 {end})")
    print(f"{'='*60}")

    if not trades:
        print("  取引回数: 0 件")
        print(f"{'='*60}\n")
        return

    df       = pd.DataFrame(trades)
    total    = len(df)
    wins     = df["win"].sum()
    losses   = total - wins
    win_rate = wins / total * 100
    avg_pnl  = df["pnl_pct"].mean()
    avg_win  = df[df["win"]]["pnl_pct"].mean()  if wins   > 0 else 0
    avg_loss = df[~df["win"]]["pnl_pct"].mean() if losses > 0 else 0
    profit_factor = (
        df[df["win"]]["pnl_pct"].sum() / abs(df[~df["win"]]["pnl_pct"].sum())
        if losses > 0 and df[~df["win"]]["pnl_pct"].sum() != 0 else float("inf")
    )
    cumulative = df.sort_values("entry_date")["pnl_pct"].cumsum()
    peak       = cumulative.cummax()
    max_dd     = (cumulative - peak).min()

    print(f"  取引回数      : {total} 件")
    print(f"  勝ち          : {int(wins)} 件")
    print(f"  負け          : {int(losses)} 件")
    print(f"  勝率          : {win_rate:.1f}%")
    print(f"  平均損益      : {avg_pnl:+.3f}%")
    print(f"  平均利益      : {avg_win:+.3f}%")
    print(f"  平均損失      : {avg_loss:+.3f}%")
    print(f"  プロフィットF : {profit_factor:.2f}")
    print(f"  最大DD        : {max_dd:+.2f}%")

    print(f"\n  ── エグジット種別 ──────────────────────")
    for etype in ["STOP", "TP", "CLOSE"]:
        sub = df[df["exit_type"] == etype]
        if len(sub) > 0:
            wr = sub["win"].sum() / len(sub) * 100
            print(f"  [{etype:5s}] {len(sub):4d}件 / 勝率{wr:5.1f}% / 平均{sub['pnl_pct'].mean():+.3f}%")

    for d in ["BUY", "SELL"]:
        sub = df[df["direction"] == d]
        if len(sub) == 0:
            continue
        wr = sub["win"].sum() / len(sub) * 100
        print(f"\n  [{d}] {len(sub)}件 / 勝率{wr:.1f}% / 平均{sub['pnl_pct'].mean():+.3f}%")

    print(f"\n  ── 上位5件（利益）──────────────")
    for _, r in df.nlargest(5, "pnl_pct").iterrows():
        print(f"    {r['entry_date']} {r['direction']} "
              f"S&P500={r['sp500_ret']:+.1f}% RSI={r['rsi']:.0f} → {r['pnl_pct']:+.2f}% [{r['exit_type']}]")

    print(f"\n  ── 下位5件（損失）──────────────")
    for _, r in df.nsmallest(5, "pnl_pct").iterrows():
        print(f"    {r['entry_date']} {r['direction']} "
              f"S&P500={r['sp500_ret']:+.1f}% RSI={r['rsi']:.0f} → {r['pnl_pct']:+.2f}% [{r['exit_type']}]")

    print(f"\n{'='*60}\n")

    out_path = f"backtest_1570_{start}_{end}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  詳細結果を {out_path} に保存しました。\n")


if __name__ == "__main__":
    if len(sys.argv) == 3:
        s, e = sys.argv[1], sys.argv[2]
    else:
        e = datetime.today().strftime("%Y-%m-%d")
        s = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        print(f"[info] 引数省略: デフォルト期間 {s} 〜 {e} を使用します")

    run_backtest(s, e)
