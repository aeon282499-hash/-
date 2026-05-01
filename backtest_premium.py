"""
backtest_premium.py — 至高版バックテスト
==========================================

使い方:
  python backtest_premium.py 2024-01-01 2024-12-31

【戦略】
  エントリー : 至高シグナル発生翌営業日の始値（=signal当日寄り付き）
  エグジット : 以下のいずれか早い方
    1. 損切り  -3%（OCO・ザラ場ヒット）
    2. 利確    +5%（OCO・ザラ場ヒット）
    3. RSI回復 終値ベースで RSI≧50 → 当日大引け（=終値）で決済
    4. 最大保有 5営業日目強制大引け（=終値）

【至高フィルター（screener_premium.judge_signal_premium と同一）】
  - RSI ≦ 25
  - 25MA乖離 ≦ -4.0%
  - 売買代金 ≧ 30億円
  - ATR/終値 ≦ 2.0%
  - ボラ/出来高 条件
  - 日経-2σ超下落日はゼロ件
  - 多因子スコア降順 → 上位2銘柄
"""

import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

import jpholiday
import pandas as pd
import numpy as np

from screener import (
    fetch_tse_universe,
    batch_download_jquants,
    _jquants_id_token,
    calc_rsi,
    LOOKBACK_DAYS,
)
from screener_premium import (
    judge_signal_premium,
    premium_score,
    is_nikkei_panic,
    MAX_SIGNALS_PRM,
    NK_PANIC_SIGMA,
)

STOP_LOSS_PRM   = 3.0
TAKE_PROFIT_PRM = 3.0   # v2: 5.0→3.0
MAX_HOLD_PRM    = 5


def get_trading_days(start: str, end: str) -> list[str]:
    days: list[str] = []
    cur  = datetime.strptime(start, "%Y-%m-%d").date()
    end_ = datetime.strptime(end,   "%Y-%m-%d").date()
    while cur <= end_:
        if cur.weekday() < 5 and not jpholiday.is_holiday(cur):
            days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


def run_premium_backtest(start: str, end: str) -> None:
    trading_days = get_trading_days(start, end)
    print(f"\n{'='*60}")
    print(f"  至高バックテスト期間: {start} 〜 {end}")
    print(f"  営業日数: {len(trading_days)} 日")
    print(f"  戦略: 至高（最大{MAX_HOLD_PRM}日・損切{STOP_LOSS_PRM}%・利確{TAKE_PROFIT_PRM}%・RSI≦25・乖離≦-4%・MAX{MAX_SIGNALS_PRM}件・日経{NK_PANIC_SIGMA}σ超下落日停止）")
    print(f"{'='*60}\n")

    fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d")
    fetch_end   = (datetime.strptime(end,   "%Y-%m-%d") + timedelta(days=MAX_HOLD_PRM * 3)).strftime("%Y-%m-%d")

    token    = _jquants_id_token()
    universe = fetch_tse_universe(token)
    name_map = {t: n for t, n in universe}

    print(f"[bt_prm] データ取得中（{fetch_start} 〜 {fetch_end}）...")
    all_data = batch_download_jquants(token, start=fetch_start, end=fetch_end, tickers=None)
    print(f"[bt_prm] J-Quants: {len(all_data)} 銘柄のデータ取得完了\n")

    nk_df = all_data.get("1321.T")
    if nk_df is None or len(nk_df) < 25:
        print("[bt_prm] 日経225データ不足 → パニック日フィルターOFF")
        nk_df = None

    all_trading_days = get_trading_days(fetch_start, fetch_end)
    trades: list[dict] = []
    panic_skips = 0

    for trade_date in trading_days:
        # 当日オープン中銘柄
        open_tickers = {
            t["ticker"] for t in trades
            if t["exit_date"] is None or t["exit_date"] > trade_date
        }

        # ── パニック日チェック（trade_date 前日終値で判定）──
        if nk_df is not None:
            nk_pre = nk_df[nk_df.index.strftime("%Y-%m-%d") < trade_date]
            if is_nikkei_panic(nk_pre, sigma=NK_PANIC_SIGMA) is True:
                panic_skips += 1
                continue

        # ── 候補抽出 ──────────────────────────────
        candidates: list[dict] = []
        signal_cache: dict = {}
        for ticker, full_df in all_data.items():
            if ticker in open_tickers:
                continue
            try:
                pre = full_df[full_df.index.strftime("%Y-%m-%d") < trade_date]
                if len(pre) < 30:
                    continue
                name = name_map.get(ticker, ticker)
                sig  = judge_signal_premium(ticker, name, pre)
                if sig is None:
                    continue
                signal_cache[ticker] = (sig, pre)
                candidates.append(sig)
            except Exception:
                continue

        # 多因子スコア降順 → 上位 MAX_SIGNALS_PRM
        candidates.sort(key=premium_score, reverse=True)
        selected = candidates[:MAX_SIGNALS_PRM]

        for signal in selected:
            ticker = signal["ticker"]
            try:
                _, pre_df = signal_cache[ticker]
                full_df   = all_data[ticker]
                name      = name_map.get(ticker, ticker)

                if trade_date not in all_trading_days:
                    continue
                idx = all_trading_days.index(trade_date)

                entry_date = trade_date
                entry_rows = full_df[full_df.index.strftime("%Y-%m-%d") == entry_date]
                if entry_rows.empty:
                    continue
                entry_open = float(entry_rows["Open"].iloc[0])
                if entry_open <= 0 or np.isnan(entry_open):
                    continue

                stop_price = entry_open * (1 - STOP_LOSS_PRM   / 100)
                tp_price   = entry_open * (1 + TAKE_PROFIT_PRM / 100)

                pnl_pct   = None
                exit_date = None
                exit_type = None

                for hold_day in range(1, MAX_HOLD_PRM + 1):
                    day_idx = idx + (hold_day - 1)
                    if day_idx >= len(all_trading_days):
                        break
                    check_date = all_trading_days[day_idx]
                    day_rows   = full_df[full_df.index.strftime("%Y-%m-%d") == check_date]
                    if day_rows.empty:
                        continue
                    day_high  = float(day_rows["High"].iloc[0])
                    day_low   = float(day_rows["Low"].iloc[0])
                    day_close = float(day_rows["Close"].iloc[0])
                    if any(v <= 0 or np.isnan(v) for v in [day_high, day_low, day_close]):
                        continue

                    # OCO（BUYのみ）
                    if day_low <= stop_price:
                        pnl_pct, exit_date, exit_type = -STOP_LOSS_PRM, check_date, "STOP"
                        break
                    if day_high >= tp_price:
                        pnl_pct, exit_date, exit_type = +TAKE_PROFIT_PRM, check_date, "TP"
                        break

                    # RSI回復
                    hist_df  = full_df[full_df.index.strftime("%Y-%m-%d") <= check_date]
                    rsi_now  = calc_rsi(hist_df["Close"].dropna())
                    rsi_exit = (rsi_now is not None and rsi_now >= 50)

                    if rsi_exit or hold_day == MAX_HOLD_PRM:
                        pnl_pct   = (day_close - entry_open) / entry_open * 100
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
                    "entry_open":  entry_open,
                    "pnl_pct":     round(pnl_pct, 3),
                    "win":         pnl_pct > 0,
                    "score":       round(premium_score(signal), 4),
                    "rsi":         signal["rsi"],
                    "deviation":   signal["deviation"],
                    "turnover":    signal["turnover"],
                })
            except Exception:
                continue

    _print_results(trades, start, end, panic_skips)


def _summary_stats(df: pd.DataFrame) -> dict:
    total = len(df)
    if total == 0:
        return {"total": 0, "win_rate": 0, "avg_pnl": 0, "pf": 0, "max_dd": 0, "cum_pnl": 0}
    wins   = int(df["win"].sum())
    losses = total - wins
    avg_pnl = df["pnl_pct"].mean()
    cum     = df.sort_values("entry_date")["pnl_pct"].cumsum()
    max_dd  = (cum - cum.cummax()).min()
    pf = (df[df["win"]]["pnl_pct"].sum() / abs(df[~df["win"]]["pnl_pct"].sum())
          if losses > 0 and df[~df["win"]]["pnl_pct"].sum() != 0 else float("inf"))
    return {
        "total":    total,
        "win_rate": wins / total * 100,
        "avg_pnl":  avg_pnl,
        "pf":       pf,
        "max_dd":   max_dd,
        "cum_pnl":  df["pnl_pct"].sum(),
    }


def _print_results(trades: list[dict], start: str, end: str, panic_skips: int) -> None:
    print(f"\n{'='*60}")
    print(f"  至高バックテスト結果 ({start} 〜 {end})")
    print(f"  パニック日スキップ: {panic_skips} 日")
    print(f"{'='*60}")

    if not trades:
        print("  取引回数: 0 件（至高条件を満たす銘柄なし）")
        print(f"{'='*60}\n")
        return

    df = pd.DataFrame(trades)
    s  = _summary_stats(df)
    print(f"\n  【至高サマリー】")
    print(f"  取引回数 : {s['total']} 件")
    print(f"  勝率     : {s['win_rate']:.1f}%")
    print(f"  平均損益 : {s['avg_pnl']:+.3f}%")
    print(f"  PF       : {s['pf']:.2f}")
    print(f"  累積PnL  : {s['cum_pnl']:+.2f}%")
    print(f"  MaxDD    : {s['max_dd']:+.2f}%")

    # ── 年別 ──
    print(f"\n  {'='*56}")
    print(f"  年別")
    print(f"  {'='*56}")
    print(f"  {'年':>4}  {'件数':>6}  {'勝率':>7}  {'PF':>6}  {'平均':>8}  {'累積':>8}")
    print(f"  {'-'*56}")
    years = sorted(df["exit_date"].str[:4].unique())
    for yr in years:
        sub = df[df["exit_date"].str[:4] == yr]
        sa  = _summary_stats(sub)
        print(f"  {yr}  {sa['total']:>6}件  {sa['win_rate']:>6.1f}%  {sa['pf']:>6.2f}  {sa['avg_pnl']:>+7.2f}%  {sa['cum_pnl']:>+7.2f}%")

    # ── 月別 ──
    print(f"\n  {'='*56}")
    print(f"  月別")
    print(f"  {'='*56}")
    print(f"  {'年月':>7}  {'件数':>4}  {'勝率':>7}  {'PF':>6}  {'平均':>8}  {'累積':>8}")
    print(f"  {'-'*56}")
    months = sorted(df["exit_date"].str[:7].unique())
    for ym in months:
        sub = df[df["exit_date"].str[:7] == ym]
        sa  = _summary_stats(sub)
        print(f"  {ym}  {sa['total']:>4}件  {sa['win_rate']:>6.1f}%  {sa['pf']:>6.2f}  {sa['avg_pnl']:>+7.2f}%  {sa['cum_pnl']:>+7.2f}%")

    print(f"\n{'='*60}\n")

    out_path = f"backtest_premium_{start}_{end}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  詳細結果を {out_path} に保存しました。\n")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) >= 2:
        s, e = args[0], args[1]
    else:
        e = datetime.today().strftime("%Y-%m-%d")
        s = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        print(f"[info] 引数省略: デフォルト期間 {s} 〜 {e} を使用します")

    run_premium_backtest(s, e)
