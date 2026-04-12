"""
backtest_day.py — デイトレバックテスト（寄り買い/売り → 引け決済）
====================================================================

使い方:
  python backtest_day.py 2024-01-01 2024-12-31

戦略:
  エントリー : シグナル翌営業日の始値
  エグジット : 同日の終値（15:30大引け）
  損切り     : 始値 -3%（日中安値/高値が到達した場合）
  利確       : 始値 +5%（日中高値/安値が到達した場合）
"""

import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

import jpholiday
import pandas as pd
import numpy as np

from screener import batch_download, _nikkei225_universe, calc_atr, batch_download_jquants, _jquants_id_token, fetch_tse_universe
from screener_day import (
    judge_signal_day,
    LOOKBACK_DAYS,
    ATR_VOL_CAP,
    GOOD_MONTHS,
)

STOP_LOSS      = 3.0   # %
TAKE_PROFIT    = 3.0   # %
ATR_VOL_CAP    = 4.0   # ATR/終値(%)がこれを超える高ボラ銘柄は除外（screener_dayと統一）
SP500_DROP_MAX = -1.5  # S&P500プロキシ前日下落率の下限


def get_trading_days(start: str, end: str) -> list[str]:
    days = []
    cur  = datetime.strptime(start, "%Y-%m-%d").date()
    end_ = datetime.strptime(end,   "%Y-%m-%d").date()
    while cur <= end_:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


def run_day_backtest(start: str, end: str) -> None:
    trading_days = get_trading_days(start, end)
    print(f"\n{'='*60}")
    print(f"  デイトレ バックテスト期間: {start} 〜 {end}")
    print(f"  営業日数: {len(trading_days)} 日")
    print(f"  戦略: 前日大幅変動逆張り（寄り成り → 引け決済）")
    print(f"{'='*60}\n")

    fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d")
    token    = _jquants_id_token()
    universe = fetch_tse_universe(token)
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}
    for proxy in ["1321.T", "1655.T"]:
        if proxy not in tickers:
            tickers.append(proxy)

    print(f"[backtest_day] {len(universe)} 銘柄のデータ取得中（{fetch_start} 〜 {end}）...")
    all_data = batch_download_jquants(token, start=fetch_start, end=end, tickers=tickers)
    print(f"[backtest_day] J-Quants: {len(all_data)} 銘柄のデータ取得完了\n")

    # 日経225プロキシ（1321.T）を取得済みデータから使用（stooq不要）
    nk_df = all_data.get("1321.T")
    if nk_df is not None and len(nk_df) > 25:
        nk_df["MA25"]  = nk_df["Close"].rolling(25).mean()
        nk_df["ATR14"] = (nk_df["High"] - nk_df["Low"]).rolling(14).mean()
    else:
        nk_df = None

    # S&P500プロキシ（1655.T）
    sp_df = all_data.get("1655.T")

    all_trading_days = get_trading_days(
        (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d"),
        end
    )

    trades: list[dict] = []
    stop_cooldown: dict[str, str] = {}  # ticker → 直近STOP発生の entry_date

    for signal_date in trading_days:
        # 月フィルター（良い月のみ）
        if int(signal_date[5:7]) not in GOOD_MONTHS:
            continue

        # 前日までのデータでシグナル判定
        for ticker, full_df in all_data.items():
            if ticker in ("1321.T", "1655.T"):
                continue
            try:
                pre_df = full_df[full_df.index.strftime("%Y-%m-%d") <= signal_date].copy()
                if len(pre_df) < 30:
                    continue

                name   = name_map.get(ticker, ticker)
                signal = judge_signal_day(ticker, name, pre_df)
                if signal is None:
                    continue

                # 連続STOP防止: 直近2営業日以内にSTOPした銘柄はスキップ
                if ticker in stop_cooldown:
                    last_stop = stop_cooldown[ticker]
                    last_idx  = all_trading_days.index(last_stop) if last_stop in all_trading_days else -1
                    cur_idx   = all_trading_days.index(signal_date) if signal_date in all_trading_days else -1
                    if last_idx >= 0 and cur_idx >= 0 and cur_idx - last_idx <= 2:
                        continue

                # 市場フィルター③ 日経ボラフィルター（横横除外）
                # 日経の前日値幅 < ATR14 の場合（動意なし）はスキップ
                if nk_df is not None:
                    nk_rows = nk_df[nk_df.index.strftime("%Y-%m-%d") <= signal_date]
                    if len(nk_rows) >= 15:
                        nk_range = float(nk_rows["High"].iloc[-1] - nk_rows["Low"].iloc[-1])
                        nk_atr   = float(nk_rows["ATR14"].iloc[-1])
                        if not np.isnan(nk_atr) and nk_atr > 0 and nk_range < nk_atr * 0.8:
                            continue  # 日経が動いていない日はスキップ

                # 市場フィルター① 日経ETF(1321.T): 終値 ≥ 25日MA
                if signal["direction"] == "BUY" and nk_df is not None:
                    nk_rows = nk_df[nk_df.index.strftime("%Y-%m-%d") <= signal_date]
                    if len(nk_rows) >= 25:
                        nk_close = float(nk_rows["Close"].iloc[-1])
                        nk_ma25  = float(nk_rows["MA25"].iloc[-1])
                        if not np.isnan(nk_ma25) and nk_close < nk_ma25:
                            continue

                # 市場フィルター② S&P500プロキシ(1655.T): 前日下落率 ≥ SP500_DROP_MAX
                if signal["direction"] == "BUY" and sp_df is not None:
                    sp_rows = sp_df[sp_df.index.strftime("%Y-%m-%d") <= signal_date]
                    if len(sp_rows) >= 2:
                        sp_prev = float(sp_rows["Close"].iloc[-2])
                        sp_last = float(sp_rows["Close"].iloc[-1])
                        if sp_prev > 0:
                            sp_ret = (sp_last - sp_prev) / sp_prev * 100
                            if sp_ret < SP500_DROP_MAX:
                                continue

                # 高ボラ除外（ATR/終値 > ATR_VOL_CAP%）
                atr = calc_atr(pre_df)
                last_close = float(pre_df["Close"].iloc[-1])
                if atr is not None and last_close > 0:
                    if (atr / last_close * 100) > ATR_VOL_CAP:
                        continue

                # エントリー日（シグナル翌営業日）
                idx = all_trading_days.index(signal_date) if signal_date in all_trading_days else -1
                if idx < 0 or idx + 1 >= len(all_trading_days):
                    continue
                entry_date = all_trading_days[idx + 1]

                entry_rows = full_df[full_df.index.strftime("%Y-%m-%d") == entry_date]
                if entry_rows.empty:
                    continue

                entry_open  = float(entry_rows["Open"].iloc[0])
                entry_close = float(entry_rows["Close"].iloc[0])
                entry_high  = float(entry_rows["High"].iloc[0])
                entry_low   = float(entry_rows["Low"].iloc[0])

                if any(v <= 0 or np.isnan(v) for v in [entry_open, entry_close]):
                    continue

                # 損益計算（損切り・利確チェック）
                direction = signal["direction"]
                if direction == "BUY":
                    stop_price = entry_open * (1 - STOP_LOSS   / 100)
                    tp_price   = entry_open * (1 + TAKE_PROFIT / 100)
                    if entry_low <= stop_price:
                        pnl_pct  = -STOP_LOSS
                        exit_type = "STOP"
                    elif entry_high >= tp_price:
                        pnl_pct  = +TAKE_PROFIT
                        exit_type = "TP"
                    else:
                        pnl_pct  = (entry_close - entry_open) / entry_open * 100
                        exit_type = "CLOSE"
                else:
                    stop_price = entry_open * (1 + STOP_LOSS   / 100)
                    tp_price   = entry_open * (1 - TAKE_PROFIT / 100)
                    if entry_high >= stop_price:
                        pnl_pct  = -STOP_LOSS
                        exit_type = "STOP"
                    elif entry_low <= tp_price:
                        pnl_pct  = +TAKE_PROFIT
                        exit_type = "TP"
                    else:
                        pnl_pct  = (entry_open - entry_close) / entry_open * 100
                        exit_type = "CLOSE"

                if exit_type == "STOP":
                    stop_cooldown[ticker] = entry_date

                trades.append({
                    "signal_date":  signal_date,
                    "entry_date":   entry_date,
                    "ticker":       ticker,
                    "name":         name,
                    "direction":    direction,
                    "prev_return":  signal["prev_return"],
                    "entry_open":   entry_open,
                    "entry_close":  entry_close,
                    "pnl_pct":      round(pnl_pct, 3),
                    "exit_type":    exit_type,
                    "win":          pnl_pct > 0,
                })

            except Exception:
                continue

    _print_results(trades, start, end)


def _print_results(trades: list[dict], start: str, end: str) -> None:
    print(f"\n{'='*60}")
    print(f"  デイトレ バックテスト結果 ({start} 〜 {end})")
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
        print(f"    {r['entry_date']} {r['name']}({r['ticker']}) "
              f"{r['direction']} 前日{r['prev_return']:+.1f}% → {r['pnl_pct']:+.2f}% [{r['exit_type']}]")

    print(f"\n  ── 下位5件（損失）──────────────")
    for _, r in df.nsmallest(5, "pnl_pct").iterrows():
        print(f"    {r['entry_date']} {r['name']}({r['ticker']}) "
              f"{r['direction']} 前日{r['prev_return']:+.1f}% → {r['pnl_pct']:+.2f}% [{r['exit_type']}]")

    print(f"\n{'='*60}\n")

    out_path = f"backtest_day_{start}_{end}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  詳細結果を {out_path} に保存しました。\n")


if __name__ == "__main__":
    if len(sys.argv) == 3:
        s, e = sys.argv[1], sys.argv[2]
    else:
        e = datetime.today().strftime("%Y-%m-%d")
        s = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        print(f"[info] 引数省略: デフォルト期間 {s} 〜 {e} を使用します")

    run_day_backtest(s, e)
