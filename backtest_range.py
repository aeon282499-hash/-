"""
backtest_range.py — スイングトレード期間バックテスト（勝率・損益集計）
=======================================================================

使い方:
  python backtest_range.py 2024-01-01 2024-12-31

戦略:
  エントリー : シグナル当日の始値（当日寄り付き成行）
  エグジット : 以下のいずれか早い方
    1. 損切り  -3%（OCO・ザラ場ヒット）
    2. 利確    +5%（OCO・ザラ場ヒット）
    3. RSI回復  終値で判定 → 当日大引け成売り（15:00通知 → 15:25-15:30クロージングオークション）
    4. 最大保有 3営業日目強制大引け（終値≒クロージングオークション成売り）
  除外: ATR/終値 > 3% の高ボラ銘柄はスキップ
"""

import json
import os
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
    fetch_tse_universe,
    judge_signal_pre,
    judge_sell_signal_pre,
    batch_download_jquants,
    _jquants_id_token,
    calc_rsi,
    calc_atr,
    LOOKBACK_DAYS,
    MAX_SIGNALS,
)

STOP_LOSS       = 3.0   # % 固定損切り
TAKE_PROFIT     = 5.0   # % 固定利確 ※2026-04-29: 3.0→5.0に修正（tracker.pyと整合）
MAX_HOLD        = 3     # 最大保有営業日数
ATR_VOL_CAP     = 2.5   # ATR/終値(%)がこれを超える高ボラ銘柄は除外（売り用）
ATR_VOL_CAP_BUY = 3.0   # 買いは2026-05-07 BT検証で3.0が最適
BUY_ONLY        = True  # TrueにするとBUYシグナルのみ対象
SELL_ONLY       = False # TrueにするとSELLシグナルのみ（信用売り専用）

# 決算日除外フィルタ（BT専用・2026-05-21追加）
# 「決算/IRで下げた銘柄を逆張りで買って更に下がる」を回避するため、
# 各銘柄の決算開示日（DiscDate）±N営業日のシグナルをBUYから除外する。
# earnings_calendar.json は build_earnings_calendar.py で J-Quants /fins/summary から事前生成。
EARNINGS_EXCLUSION_DAYS = 3
_EARNINGS_EXCLUDED_DATES: dict[str, set[str]] = {}


def _load_earnings_calendar(days: int = EARNINGS_EXCLUSION_DAYS) -> None:
    """earnings_calendar.json から (ticker → 除外日set) を構築する。
    決算日±N日（カレンダー日ベース）をシグナル日として除外対象にする。
    """
    path = "earnings_calendar.json"
    if not os.path.exists(path):
        print(f"[backtest] {path} が見つかりません → 決算除外フィルタOFF")
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            cal = json.load(f)
    except Exception as e:
        print(f"[backtest] {path} 読込失敗: {e} → 決算除外フィルタOFF")
        return
    total_excl = 0
    for ticker, dates in cal.items():
        excl: set[str] = set()
        for d_str in dates:
            try:
                d = datetime.strptime(d_str, "%Y-%m-%d").date()
                for offset in range(-days, days + 1):
                    excl.add((d + timedelta(days=offset)).strftime("%Y-%m-%d"))
            except Exception:
                continue
        if excl:
            _EARNINGS_EXCLUDED_DATES[ticker] = excl
            total_excl += len(excl)
    print(f"[backtest] 決算カレンダー: {len(_EARNINGS_EXCLUDED_DATES)}銘柄 / "
          f"延べ除外日{total_excl}件（±{days}日窓）")


def _is_near_earnings(ticker: str, sig_date: str) -> bool:
    return sig_date in _EARNINGS_EXCLUDED_DATES.get(ticker, set())

# Phase 2: 3階層独立BT定義（main.py の TIERS と整合）
TIERS_BT = [
    {"key": "main",  "label": "大100万", "size": 1_000_000},
    {"key": "mid",   "label": "中 50万", "size":   500_000},
    {"key": "small", "label": "小 30万", "size":   300_000},
]


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
    print(f"  戦略: スイング（最大{MAX_HOLD}日・損切{STOP_LOSS}%・利確{TAKE_PROFIT}%・RSI回復・高ボラ除外ATR>{ATR_VOL_CAP}%・市場フィルターOFF）")
    print(f"{'='*60}\n")

    # 決算日除外フィルタ用のカレンダーをロード
    _load_earnings_calendar()

    # ── 銘柄リスト取得（J-Quants）────────────────────
    fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d")
    # エグジット用に終了日を少し延ばす（最大保有日数分）
    fetch_end = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=MAX_HOLD * 3)).strftime("%Y-%m-%d")
    token    = _jquants_id_token()
    universe = fetch_tse_universe(token)
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}

    print(f"[backtest] データ取得中（{fetch_start} 〜 {fetch_end}）...")
    all_data = batch_download_jquants(token, start=fetch_start, end=fetch_end, tickers=tickers)
    print(f"[backtest] J-Quants: {len(all_data)} 銘柄のデータ取得完了\n")

    # ── 日経225プロキシ（1321.T）を取得済みデータから使用 ─
    nk_df = all_data.get("1321.T")
    if nk_df is not None and len(nk_df) > 25:
        nk_df["MA25"] = nk_df["Close"].rolling(25).mean()
        print(f"[backtest] 日経ETF(1321.T)取得完了（{len(nk_df)}日分）")
    else:
        nk_df = None
        print("[backtest] 日経225データ取得失敗 → 市場フィルターOFF")

    # 全営業日インデックス（エグジット日探索用）
    all_trading_days = get_trading_days(fetch_start, fetch_end)

    # ── 各営業日でシグナル判定（Phase 2: 3階層独立）────────────────────
    trades_per_tier: dict[str, list[dict]] = {t["key"]: [] for t in TIERS_BT}

    import math
    def _buy_score(sig: dict) -> float:
        rsi = sig["rsi"]
        dev = sig["deviation"]
        turn = sig["turnover"]
        rsi_score  = 1.0 / (1.0 + ((rsi - 38.0) / 8.0) ** 2)
        dev_score  = 1.0 / (1.0 + ((dev + 3.0) / 2.0) ** 2)
        turn_score = math.log10(max(turn, 1) / 1e9 + 1.0) / 3.0
        return rsi_score * 0.30 + dev_score * 0.30 + turn_score * 0.40

    for trade_date in trading_days:
        # 階層別のオープン中ティッカー（同一階層内で重複エントリー防止）
        open_per_tier: dict[str, set] = {}
        for tier in TIERS_BT:
            open_per_tier[tier["key"]] = {
                t["ticker"] for t in trades_per_tier[tier["key"]]
                if t["exit_date"] is None or t["exit_date"] > trade_date
            }
        # 全階層の和集合（シグナル判定スキップ判定用）
        any_open = set().union(*open_per_tier.values())

        # ── 当日シグナル候補を1パスで集計 ─────────────────────
        signal_cache: dict = {}                     # ticker -> (signal, pre_df, prev_close)
        buy_cands: list = []                        # (score, ticker, prev_close)
        sell_cands: list = []                       # (turnover, ticker, prev_close)
        for _ticker, _full_df in all_data.items():
            if _ticker not in name_map:
                continue
            try:
                _pre = _full_df[_full_df.index.strftime("%Y-%m-%d") < trade_date].copy()
                if len(_pre) < 30:
                    continue
                _name = name_map[_ticker]
                if SELL_ONLY:
                    _sig = judge_sell_signal_pre(_ticker, _name, _pre)
                else:
                    _sig = judge_signal_pre(_ticker, _name, _pre)
                if _sig is None:
                    continue
                # 決算日除外フィルタ（BUYのみ・BT専用）
                # シグナル日が銘柄の決算開示日±N日内なら、決算/IRショックでの逆張りを回避
                if _sig["direction"] == "BUY" and _is_near_earnings(_ticker, trade_date):
                    continue
                _prev_close = float(_sig.get("prev_close") or _pre["Close"].iloc[-1])
                signal_cache[_ticker] = (_sig, _pre, _prev_close)
                if _sig["direction"] == "BUY":
                    buy_cands.append((_buy_score(_sig), _ticker, _prev_close))
                else:
                    sell_cands.append((_sig["turnover"], _ticker, _prev_close))
            except Exception:
                continue

        buy_cands.sort(reverse=True, key=lambda x: x[0])
        sell_cands.sort(reverse=True, key=lambda x: x[0])

        # ── 階層別に上位MAX_SIGNALS銘柄を選定（サイズ&open除外） ──
        selected_per_tier: dict[str, set] = {}
        for tier in TIERS_BT:
            size = tier["size"]
            buy_pool = [
                tk for (_, tk, pc) in buy_cands
                if pc * 100 <= size and tk not in open_per_tier[tier["key"]]
            ]
            sell_pool = [
                tk for (_, tk, pc) in sell_cands
                if pc * 100 <= size and tk not in open_per_tier[tier["key"]]
            ]
            selected_per_tier[tier["key"]] = (
                set(buy_pool[:MAX_SIGNALS]) | set(sell_pool[:MAX_SIGNALS])
            )

        # シミュレーション対象 = どこかの階層で選ばれた銘柄の和集合
        selected_tickers = set().union(*selected_per_tier.values())

        for ticker, full_df in all_data.items():
            if ticker not in selected_tickers:
                continue
            try:
                signal, pre_df, _prev_close = signal_cache[ticker]
                name = name_map[ticker]

                # ── 日経MA25状態を記録（フィルター比較用）──────────
                nk_above = None
                if nk_df is not None:
                    nk_rows = nk_df[nk_df.index.strftime("%Y-%m-%d") < trade_date]
                    if len(nk_rows) >= 25:
                        nk_close = float(nk_rows["Close"].iloc[-1])
                        nk_ma25  = float(nk_rows["MA25"].iloc[-1])
                        if not np.isnan(nk_ma25):
                            nk_above = (nk_close >= nk_ma25)

                # ── エントリー日（シグナル当日）──────────────────
                idx = all_trading_days.index(trade_date) if trade_date in all_trading_days else -1
                if idx < 0:
                    continue
                entry_date = all_trading_days[idx]

                entry_rows = full_df[full_df.index.strftime("%Y-%m-%d") == entry_date]
                if entry_rows.empty:
                    continue
                entry_open = float(entry_rows["Open"].iloc[0])
                if entry_open <= 0 or np.isnan(entry_open):
                    continue

                # ── 高ボラ銘柄フィルター（買い:3.0% / 売り:2.5%）──
                direction = signal["direction"]
                if BUY_ONLY and direction != "BUY":
                    continue
                if SELL_ONLY and direction != "SELL":
                    continue
                atr = calc_atr(pre_df)
                last_close = float(pre_df["Close"].iloc[-1])
                if atr is not None and last_close > 0:
                    cap = ATR_VOL_CAP_BUY if direction == "BUY" else ATR_VOL_CAP
                    if (atr / last_close * 100) > cap:
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
                    day_idx  = idx + (hold_day - 1)  # hold_day=1 は entry_date 当日
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

                    # RSI回復 or 最大保有日 → 当日大引け成売り（=終値）
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

                trade_record = {
                    "signal_date": trade_date,
                    "entry_date":  entry_date,
                    "exit_date":   exit_date,
                    "exit_type":   exit_type,
                    "ticker":      ticker,
                    "name":        name,
                    "direction":   direction,
                    "entry_open":  entry_open,
                    "prev_close":  _prev_close,
                    "pnl_pct":     round(pnl_pct, 3),
                    "win":         pnl_pct > 0,
                    "nk_above_ma25": nk_above,
                }
                # この銘柄を選んだ階層 全てに記録（同じシミュレーション結果を共有）
                for tier in TIERS_BT:
                    if ticker in selected_per_tier[tier["key"]]:
                        trades_per_tier[tier["key"]].append(trade_record)

            except Exception:
                continue

    _print_results_per_tier(trades_per_tier, start, end)


def _summary_stats(df: pd.DataFrame) -> dict:
    total = len(df)
    if total == 0:
        return {"total": 0, "win_rate": 0, "avg_pnl": 0, "pf": 0, "max_dd": 0, "cum_pnl": 0}
    wins   = df["win"].sum()
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


def _print_results_per_tier(trades_per_tier: dict[str, list[dict]], start: str, end: str) -> None:
    """3階層分のBT結果を順次表示＋CSV出力。"""
    print(f"\n{'#'*60}")
    print(f"#  Phase 2: 3階層独立BT 結果まとめ ({start} 〜 {end})")
    print(f"{'#'*60}")
    summary_rows = []
    for tier in TIERS_BT:
        key = tier["key"]
        trades = trades_per_tier[key]
        print(f"\n\n{'#'*60}")
        print(f"#  [{tier['label']} / 1件{tier['size']//10000}万円]  trades={len(trades)}件")
        print(f"{'#'*60}")
        _print_results(trades, start, end, tier_suffix=f"_{key}")

        if trades:
            df = pd.DataFrame(trades)
            s = _summary_stats(df)
            summary_rows.append({
                "tier":     tier["label"],
                "size_万":  tier["size"]//10000,
                "件数":     s["total"],
                "勝率":     f"{s['win_rate']:.1f}%",
                "平均":     f"{s['avg_pnl']:+.3f}%",
                "PF":       f"{s['pf']:.2f}",
                "累積%":    f"{s['cum_pnl']:+.1f}%",
                "MaxDD":    f"{s['max_dd']:+.1f}%",
            })

    # 階層別サマリ表
    print(f"\n\n{'='*72}")
    print(f"  Phase 2 階層別サマリ ({start} 〜 {end})")
    print(f"{'='*72}")
    print(f"  {'階層':<8} {'1件':>5} {'件数':>6} {'勝率':>7} {'平均':>8} {'PF':>5} {'累積%':>9} {'MaxDD':>8}")
    print(f"  {'-'*70}")
    for r in summary_rows:
        print(f"  {r['tier']:<8} {r['size_万']:>3}万 {r['件数']:>6} {r['勝率']:>7} "
              f"{r['平均']:>8} {r['PF']:>5} {r['累積%']:>9} {r['MaxDD']:>8}")
    print(f"{'='*72}\n")


def _print_results(trades: list[dict], start: str, end: str, *, tier_suffix: str = "") -> None:
    print(f"\n{'='*60}")
    print(f"  バックテスト結果 ({start} 〜 {end})  [スイング戦略]")
    print(f"{'='*60}")

    if not trades:
        print("  取引回数: 0 件（条件を満たす銘柄なし）")
        print(f"{'='*60}\n")
        return

    df_all      = pd.DataFrame(trades)
    # BUY: 上昇トレンド時のみ / SELL: 下降トレンド時のみ（逆向きフィルター）
    if SELL_ONLY:
        df_filtered = df_all[df_all["nk_above_ma25"] == False].copy()
    else:
        df_filtered = df_all[df_all["nk_above_ma25"] == True].copy()

    # ── 全体サマリー ──────────────────────────────────
    s = _summary_stats(df_all)
    print(f"\n  【フィルターなし】 {s['total']}件 / 勝率{s['win_rate']:.1f}% / 平均{s['avg_pnl']:+.3f}% / PF{s['pf']:.2f} / MaxDD{s['max_dd']:+.2f}%")
    sf = _summary_stats(df_filtered)
    print(f"  【フィルターあり】 {sf['total']}件 / 勝率{sf['win_rate']:.1f}% / 平均{sf['avg_pnl']:+.3f}% / PF{sf['pf']:.2f} / MaxDD{sf['max_dd']:+.2f}%")

    # ── 年別比較 ──────────────────────────────────────
    print(f"\n  {'='*56}")
    print(f"  年別比較（月次平均損益%）")
    print(f"  {'='*56}")
    print(f"  {'年':>4}  {'なし件数':>6}  {'なし勝率':>7}  {'なしPF':>6}  {'あり件数':>6}  {'あり勝率':>7}  {'ありPF':>6}")
    print(f"  {'-'*56}")

    years = sorted(df_all["exit_date"].str[:4].unique())
    for yr in years:
        sub_all = df_all[df_all["exit_date"].str[:4] == yr]
        sub_flt = df_filtered[df_filtered["exit_date"].str[:4] == yr]
        sa = _summary_stats(sub_all)
        sf2 = _summary_stats(sub_flt)
        print(f"  {yr}  {sa['total']:>6}件  {sa['win_rate']:>6.1f}%  {sa['pf']:>6.2f}  {sf2['total']:>6}件  {sf2['win_rate']:>6.1f}%  {sf2['pf']:>6.2f}")

    # ── 月別比較 ──────────────────────────────────────
    print(f"\n  {'='*56}")
    print(f"  月別比較")
    print(f"  {'='*56}")
    print(f"  {'年月':>7}  {'なし':>10}  {'なし勝率':>8}  {'あり':>10}  {'あり勝率':>8}")
    print(f"  {'-'*56}")

    months = sorted(df_all["exit_date"].str[:7].unique())
    for ym in months:
        sub_all = df_all[df_all["exit_date"].str[:7] == ym]
        sub_flt = df_filtered[df_filtered["exit_date"].str[:7] == ym]
        sa = _summary_stats(sub_all)
        sf2 = _summary_stats(sub_flt)
        flag = " <<" if sf2["pf"] > sa["pf"] else ""
        print(f"  {ym}  {sa['total']:>4}件{sa['avg_pnl']:>+7.2f}%  {sa['win_rate']:>6.1f}%  {sf2['total']:>4}件{sf2['avg_pnl']:>+7.2f}%  {sf2['win_rate']:>6.1f}%{flag}")

    print(f"\n{'='*60}\n")

    # CSV 出力（tier別: backtest_{start}_{end}{_tier}.csv）
    out_path = f"backtest_{start}_{end}{tier_suffix}.csv"
    df_all.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  詳細結果を {out_path} に保存しました。\n")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if "--sell" in sys.argv:
        BUY_ONLY  = False
        SELL_ONLY = True
        print("[info] SELLモードで実行します")
    if "--buy" in sys.argv:
        BUY_ONLY  = True
        SELL_ONLY = False

    if len(args) >= 2:
        s, e = args[0], args[1]
    else:
        e = datetime.today().strftime("%Y-%m-%d")
        s = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        print(f"[info] 引数省略: デフォルト期間 {s} 〜 {e} を使用します")

    run_range_backtest(s, e)
