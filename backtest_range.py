"""
backtest_range.py — スイングトレード期間バックテスト（勝率・損益集計）
=======================================================================

使い方:
  python backtest_range.py 2024-01-01 2024-12-31

戦略:
  エントリー : シグナル翌営業日の始値
  エグジット : 以下のいずれか早い方
    1. 損切り  -3%（固定）
    2. 利確    +5%（固定）
    3. RSI回復  BUY→RSI≧50 / SELL→RSI≦50 の終値
    4. 最大保有 5営業日後の終値
  除外: ATR/終値 > 3% の高ボラ銘柄はスキップ
"""

import sys
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

import jpholiday
import pandas as pd
import numpy as np
import requests

from screener import (
    _nikkei225_universe,
    fetch_tse_prime_universe,
    judge_signal_pre,
    batch_download_stooq,
    batch_download_jquants,
    _jquants_id_token,
    calc_rsi,
    calc_atr,
    LOOKBACK_DAYS,
    MAX_SIGNALS,
)

STOP_LOSS       = 2.0   # % 固定損切り
TAKE_PROFIT     = 5.0   # % 固定利確
MAX_HOLD        = 3     # 最大保有営業日数
ATR_VOL_CAP     = 2.5   # ATR/終値(%)がこれを超える高ボラ銘柄は除外


def get_trading_days(start: str, end: str) -> list[str]:
    """指定期間内の日本株営業日リスト（文字列）を返す。"""
    days = []
    cur  = datetime.strptime(start, "%Y-%m-%d").date()
    end_ = datetime.strptime(end,   "%Y-%m-%d").date()
    while cur <= end_:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


def run_range_backtest(start: str, end: str) -> None:
    trading_days = get_trading_days(start, end)
    print(f"\n{'='*60}")
    print(f"  バックテスト期間: {start} 〜 {end}")
    print(f"  営業日数: {len(trading_days)} 日")
    print(f"  戦略: スイング（最大{MAX_HOLD}日・損切{STOP_LOSS}%・利確{TAKE_PROFIT}%・RSI回復・高ボラ除外ATR>{ATR_VOL_CAP}%）")
    print(f"{'='*60}\n")

    # ── 銘柄リスト取得（東証プライム全銘柄）────────────
    universe = fetch_tse_prime_universe()
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    # ── 期間全体のデータをstooqで取得 ─────────────────
    fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d")
    # エグジット用に終了日を少し延ばす（最大保有日数分）
    fetch_end = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=MAX_HOLD * 3)).strftime("%Y-%m-%d")
    print(f"[backtest] {len(universe)} 銘柄のデータを取得中（{fetch_start} 〜 {fetch_end}）...")
    try:
        token    = _jquants_id_token()
        all_data = batch_download_jquants(token, start=fetch_start, end=fetch_end, tickers=tickers)
        print(f"[backtest] J-Quants: {len(all_data)} 銘柄のデータ取得完了\n")
    except Exception as e:
        print(f"[backtest] J-Quants失敗({e})→stooqで再試行...")
        all_data = batch_download_stooq(tickers, start=fetch_start, end=fetch_end)
        print(f"[backtest] stooq: {len(all_data)} 銘柄のデータ取得完了\n")

    # ── 日経225データ取得（市場フィルター用）─────────
    print("[backtest] 日経225データ取得中...")
    nk_data = batch_download_stooq(["^NKX"], start=fetch_start, end=fetch_end)
    nk_df = nk_data.get("^NKX")
    if nk_df is not None and len(nk_df) > 25:
        nk_df["MA25"] = nk_df["Close"].rolling(25).mean()
        print(f"[backtest] 日経225データ取得完了（{len(nk_df)}日分）")
    else:
        nk_df = None
        print("[backtest] 日経225データ取得失敗 → 市場フィルターOFF")

    # 全営業日インデックス（エグジット日探索用）
    all_trading_days = get_trading_days(fetch_start, fetch_end)

    # ── 各営業日でシグナル判定 ────────────────────────
    trades: list[dict] = []

    for trade_date in trading_days:
        # 当日オープン中のティッカーを収集（重複エントリー防止）
        open_tickers = {
            t["ticker"] for t in trades
            if t["exit_date"] is None or t["exit_date"] > trade_date
        }

        for ticker, full_df in all_data.items():
            if ticker in open_tickers:
                continue
            try:
                # 前日までのデータ（ルックアヘッド防止）
                pre_df = full_df[full_df.index.strftime("%Y-%m-%d") < trade_date].copy()
                if len(pre_df) < 30:
                    continue

                name   = name_map.get(ticker, ticker)
                signal = judge_signal_pre(ticker, name, pre_df)
                if signal is None:
                    continue

                # ── 市場フィルター（BUYは日経が25MA以上の時のみ）──
                if signal["direction"] == "BUY" and nk_df is not None:
                    nk_rows = nk_df[nk_df.index.strftime("%Y-%m-%d") < trade_date]
                    if len(nk_rows) >= 25:
                        nk_close = float(nk_rows["Close"].iloc[-1])
                        nk_ma25  = float(nk_rows["MA25"].iloc[-1])
                        if not np.isnan(nk_ma25) and nk_close < nk_ma25:
                            continue

                # ── エントリー日（シグナル翌営業日）──────────────
                idx = all_trading_days.index(trade_date) if trade_date in all_trading_days else -1
                if idx < 0 or idx + 1 >= len(all_trading_days):
                    continue
                entry_date = all_trading_days[idx + 1]

                entry_rows = full_df[full_df.index.strftime("%Y-%m-%d") == entry_date]
                if entry_rows.empty:
                    continue
                entry_open = float(entry_rows["Open"].iloc[0])
                if entry_open <= 0 or np.isnan(entry_open):
                    continue

                # ── 高ボラ銘柄フィルター（ATR/終値 > 3% は除外）──────
                direction  = signal["direction"]
                atr = calc_atr(pre_df)
                last_close = float(pre_df["Close"].iloc[-1])
                if atr is not None and last_close > 0:
                    if (atr / last_close * 100) > ATR_VOL_CAP:
                        continue  # 高ボラ銘柄をスキップ

                # ── 固定ストップ・利確計算 ─────────────────────────
                if direction == "BUY":
                    stop_price = entry_open * (1 - STOP_LOSS   / 100)
                    tp_price   = entry_open * (1 + TAKE_PROFIT / 100)
                else:
                    stop_price = entry_open * (1 + STOP_LOSS   / 100)
                    tp_price   = entry_open * (1 - TAKE_PROFIT / 100)

                pnl_pct   = None
                exit_date = None
                exit_type = None

                for hold_day in range(1, MAX_HOLD + 1):
                    day_idx  = idx + 1 + hold_day  # entry_date の翌日から
                    if day_idx >= len(all_trading_days):
                        break
                    check_date = all_trading_days[day_idx]

                    day_rows = full_df[full_df.index.strftime("%Y-%m-%d") == check_date]
                    if day_rows.empty:
                        continue

                    day_open  = float(day_rows["Open"].iloc[0])
                    day_high  = float(day_rows["High"].iloc[0])
                    day_low   = float(day_rows["Low"].iloc[0])
                    day_close = float(day_rows["Close"].iloc[0])

                    if any(v <= 0 or np.isnan(v) for v in [day_open, day_high, day_low, day_close]):
                        continue

                    # 損切り・利確チェック（日中値）
                    if direction == "BUY":
                        if day_low <= stop_price:
                            pnl_pct   = -STOP_LOSS
                            exit_date = check_date
                            exit_type = "STOP"
                            break
                        if day_high >= tp_price:
                            pnl_pct   = +TAKE_PROFIT
                            exit_date = check_date
                            exit_type = "TP"
                            break
                    else:
                        if day_high >= stop_price:
                            pnl_pct   = -STOP_LOSS
                            exit_date = check_date
                            exit_type = "STOP"
                            break
                        if day_low <= tp_price:
                            pnl_pct   = +TAKE_PROFIT
                            exit_date = check_date
                            exit_type = "TP"
                            break

                    # RSI回復チェック（終値ベースで計算）
                    hist_df = full_df[full_df.index.strftime("%Y-%m-%d") <= check_date]
                    rsi_now = calc_rsi(hist_df["Close"].dropna())
                    rsi_exit = (rsi_now is not None and (
                        (direction == "BUY"  and rsi_now >= 50) or
                        (direction == "SELL" and rsi_now <= 50)
                    ))

                    # 最終日または RSI回復 → 終値で決済
                    if rsi_exit or hold_day == MAX_HOLD:
                        if direction == "BUY":
                            pnl_pct = (day_close - entry_open) / entry_open * 100
                        else:
                            pnl_pct = (entry_open - day_close) / entry_open * 100
                        exit_date = check_date
                        exit_type = "RSI" if rsi_exit else "MAXHOLD"
                        break

                if pnl_pct is None:
                    continue

                trades.append({
                    "signal_date": trade_date,
                    "entry_date":  entry_date,
                    "exit_date":   exit_date,
                    "exit_type":   exit_type,
                    "ticker":      ticker,
                    "name":        name,
                    "direction":   direction,
                    "entry_open":  entry_open,
                    "pnl_pct":     round(pnl_pct, 3),
                    "win":         pnl_pct > 0,
                })

            except Exception:
                continue

    _print_results(trades, start, end)


def _print_results(trades: list[dict], start: str, end: str) -> None:
    print(f"\n{'='*60}")
    print(f"  バックテスト結果 ({start} 〜 {end})  [スイング戦略]")
    print(f"{'='*60}")

    if not trades:
        print("  取引回数: 0 件（条件を満たす銘柄なし）")
        print(f"{'='*60}\n")
        return

    df       = pd.DataFrame(trades)
    total    = len(df)
    wins     = df["win"].sum()
    losses   = total - wins
    win_rate = wins / total * 100
    avg_pnl  = df["pnl_pct"].mean()
    avg_win  = df[df["win"]]["pnl_pct"].mean() if wins > 0 else 0
    avg_loss = df[~df["win"]]["pnl_pct"].mean() if losses > 0 else 0
    profit_factor = (
        df[df["win"]]["pnl_pct"].sum() / abs(df[~df["win"]]["pnl_pct"].sum())
        if losses > 0 and df[~df["win"]]["pnl_pct"].sum() != 0 else float("inf")
    )

    print(f"  取引回数      : {total} 件")
    print(f"  勝ち          : {int(wins)} 件")
    print(f"  負け          : {int(losses)} 件")
    print(f"  勝率          : {win_rate:.1f}%")
    print(f"  平均損益      : {avg_pnl:+.3f}%")
    print(f"  平均利益      : {avg_win:+.3f}%")
    print(f"  平均損失      : {avg_loss:+.3f}%")
    print(f"  プロフィットF : {profit_factor:.2f}")

    # エグジット種別集計
    print(f"\n  ── エグジット種別 ──────────────────────")
    for etype in ["STOP", "TP", "RSI", "MAXHOLD"]:
        sub = df[df["exit_type"] == etype]
        if len(sub) > 0:
            wr = sub["win"].sum() / len(sub) * 100
            print(f"  [{etype:7s}] {len(sub):4d}件 / 勝率{wr:5.1f}% / 平均{sub['pnl_pct'].mean():+.3f}%")

    # 売買方向別
    for d in ["BUY", "SELL"]:
        sub = df[df["direction"] == d]
        if len(sub) == 0:
            continue
        wr = sub["win"].sum() / len(sub) * 100
        print(f"\n  [{d}] {len(sub)}件 / 勝率{wr:.1f}% / 平均{sub['pnl_pct'].mean():+.3f}%")

    # 上位損益銘柄
    print(f"\n  ── 上位5件（利益）──────────────")
    for _, r in df.nlargest(5, "pnl_pct").iterrows():
        print(f"    {r['entry_date']} {r['name']}({r['ticker']}) "
              f"{r['direction']} {r['pnl_pct']:+.2f}% [{r['exit_type']}]")

    print(f"\n  ── 下位5件（損失）──────────────")
    for _, r in df.nsmallest(5, "pnl_pct").iterrows():
        print(f"    {r['entry_date']} {r['name']}({r['ticker']}) "
              f"{r['direction']} {r['pnl_pct']:+.2f}% [{r['exit_type']}]")

    print(f"\n{'='*60}\n")

    # CSV 出力
    out_path = f"backtest_{start}_{end}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  詳細結果を {out_path} に保存しました。\n")


if __name__ == "__main__":
    if len(sys.argv) == 3:
        s, e = sys.argv[1], sys.argv[2]
    else:
        e = datetime.today().strftime("%Y-%m-%d")
        s = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        print(f"[info] 引数省略: デフォルト期間 {s} 〜 {e} を使用します")

    run_range_backtest(s, e)
