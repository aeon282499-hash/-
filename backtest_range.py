"""
backtest_range.py — 期間バックテスト（勝率・損益集計）
=======================================================

使い方:
  python backtest_range.py 2024-01-01 2024-12-31

期間内の全営業日に対してシグナル判定①〜⑦を適用し、
  エントリー: 当日始値
  エグジット: 当日終値（15:30大引け）
として損益を計算し、勝率・期待値などを表示する。

ルックアヘッドバイアス防止:
  条件⑦の判定に使うのは当日の「始値（Open）」のみ。
  当日の高値・安値・終値はエグジット計算にのみ使用し、
  エントリー判断には絶対に混入させない。
"""

import sys
import time
from datetime import datetime, timedelta

import jpholiday
import yfinance as yf
import pandas as pd
import numpy as np

from screener import (
    _nikkei225_universe,
    judge_signal_pre,
    check_gap_entry,
    batch_download_stooq,
    LOOKBACK_DAYS,
    MAX_SIGNALS,
)


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
    """
    start 〜 end の期間でバックテストを実行し、結果を表示する。

    Parameters
    ----------
    start : "YYYY-MM-DD"
    end   : "YYYY-MM-DD"
    """
    trading_days = get_trading_days(start, end)
    print(f"\n{'='*60}")
    print(f"  バックテスト期間: {start} 〜 {end}")
    print(f"  営業日数: {len(trading_days)} 日")
    print(f"{'='*60}\n")

    # ── 銘柄リスト取得 ────────────────────────────────
    universe = _nikkei225_universe()
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    # ── 期間全体のデータをstooqで取得 ─────────────────
    fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d")
    print(f"[backtest] {len(universe)} 銘柄のデータを取得中（{fetch_start} 〜 {end}）...")
    all_data = batch_download_stooq(tickers, start=fetch_start, end=end)
    print(f"[backtest] {len(all_data)} 銘柄のデータ取得完了\n")

    # ── 日経225データ取得（市場フィルター用）─────────
    print("[backtest] 日経225データ取得中（市場フィルター用）...")
    nk_data = batch_download_stooq(["^NKX"], start=fetch_start, end=end)
    nk_df = nk_data.get("^NKX")
    if nk_df is not None and len(nk_df) > 25:
        nk_df["MA25"] = nk_df["Close"].rolling(25).mean()
        print(f"[backtest] 日経225データ取得完了（{len(nk_df)}日分）")
    else:
        nk_df = None
        print("[backtest] 日経225データ取得失敗 → 市場フィルターOFF")

    # ── 各営業日でシグナル判定 ────────────────────────
    trades: list[dict] = []

    for trade_date in trading_days:
        # 当日のデータを含む DataFrame は条件①〜⑥の判定に使わない
        # → trade_date の「前日終値」までのデータ（exclusive end = trade_date）
        pre_end = trade_date   # yfinance の end は「その日を含まない」

        for ticker, full_df in all_data.items():
            try:
                # ── 前日までのデータを切り出す（ルックアヘッド防止）──
                # 文字列比較にしてタイムゾーン問題を回避する
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
                            continue  # 日経下落トレンド中はBUYしない

                # ── 当日データを取得（始値・終値のみ使用）──────────
                today_rows = full_df[full_df.index.strftime("%Y-%m-%d") == trade_date]
                if today_rows.empty:
                    continue   # 当日データなし（休場・上場廃止等）

                today_open  = float(today_rows["Open"].iloc[0])
                today_close = float(today_rows["Close"].iloc[0])
                today_low   = float(today_rows["Low"].iloc[0])
                today_high  = float(today_rows["High"].iloc[0])

                # ── 始値・終値が異常値（0やNaN）の場合はスキップ ──
                if any(v <= 0 or np.isnan(v) for v in [today_open, today_close]):
                    continue

                # ── 損益計算（損切り-3% / 利確+5%）──────────────
                STOP_LOSS   = 3.0  # %
                TAKE_PROFIT = 5.0  # %
                if signal["direction"] == "BUY":
                    stop_price = today_open * (1 - STOP_LOSS   / 100)
                    tp_price   = today_open * (1 + TAKE_PROFIT / 100)
                    if today_low <= stop_price:
                        pnl_pct = -STOP_LOSS    # 損切り発動
                    elif today_high >= tp_price:
                        pnl_pct = +TAKE_PROFIT  # 利確発動
                    else:
                        pnl_pct = (today_close - today_open) / today_open * 100
                else:
                    stop_price = today_open * (1 + STOP_LOSS   / 100)
                    tp_price   = today_open * (1 - TAKE_PROFIT / 100)
                    if today_high >= stop_price:
                        pnl_pct = -STOP_LOSS    # 損切り発動
                    elif today_low <= tp_price:
                        pnl_pct = +TAKE_PROFIT  # 利確発動
                    else:
                        pnl_pct = (today_open - today_close) / today_open * 100

                trades.append({
                    "date":      trade_date,
                    "ticker":    ticker,
                    "name":      name,
                    "direction": signal["direction"],
                    "open":      today_open,
                    "close":     today_close,
                    "pnl_pct":   round(pnl_pct, 3),
                    "win":       pnl_pct > 0,
                })

            except Exception as e:
                # 個別銘柄のエラーでバックテスト全体を止めない
                continue

        # MAX_SIGNALS を超えた日はシグナル数を上限で打ち切る処理は
        # 期間バックテストでは全件記録（成績評価のため）

    # ── 結果集計 ──────────────────────────────────────
    _print_results(trades, start, end)


def _print_results(trades: list[dict], start: str, end: str) -> None:
    """バックテスト結果を表示する。"""
    print(f"\n{'='*60}")
    print(f"  バックテスト結果 ({start} 〜 {end})")
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
        print(f"    {r['date']} {r['name']}({r['ticker']}) "
              f"{r['direction']} {r['pnl_pct']:+.2f}%")

    print(f"\n  ── 下位5件（損失）──────────────")
    for _, r in df.nsmallest(5, "pnl_pct").iterrows():
        print(f"    {r['date']} {r['name']}({r['ticker']}) "
              f"{r['direction']} {r['pnl_pct']:+.2f}%")

    print(f"\n{'='*60}\n")

    # CSV 出力
    out_path = f"backtest_{start}_{end}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  詳細結果を {out_path} に保存しました。\n")


if __name__ == "__main__":
    if len(sys.argv) == 3:
        s, e = sys.argv[1], sys.argv[2]
    else:
        # デフォルト: 直近1年
        e = datetime.today().strftime("%Y-%m-%d")
        s = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        print(f"[info] 引数省略: デフォルト期間 {s} 〜 {e} を使用します")

    run_range_backtest(s, e)
