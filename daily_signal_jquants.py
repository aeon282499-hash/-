"""
daily_signal_jquants.py — 明朝の寄り付き売買シグナル出力（J-Quants のみ）
=========================================================
使い方:
  python daily_signal_jquants.py
"""

import os
from datetime import date, timedelta
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import numpy as np

from screener import _jquants_id_token, batch_download_jquants, calc_rsi
from backtest_jquants import (
    UNIVERSE, SP500_PROXY, _FETCH_TICKERS,
    VOL_LIMIT, SP500_BUY_MAX, SP500_SELL_MIN,
    RSI_BUY_MAX, RSI_SELL_MIN, ROE_MIN, PBR_MAX, PER_SELL_MIN,
    STOP_PCT,
    fetch_fins_statements, get_latest_fin, calc_fundamentals, build_sp500_proxy,
)


def run_daily_signal() -> None:
    today     = date.today()
    end_str   = today.strftime("%Y-%m-%d")
    start_str = (today - timedelta(days=90)).strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"  デイリーシグナル  判定日: {today}")
    print(f"  S&P500代替: {SP500_PROXY}  VIX代替: ATR/終値%")
    print(f"  ※ 明朝の寄り付きでエントリー → 当日引けで決済")
    print(f"  ※ Open -{STOP_PCT}% で強制ロスカット")
    print(f"{'='*60}\n")

    # ── 価格データ（プロキシETF含む）────────────────────
    print("[1/2] J-Quants 価格データ取得中...")
    token      = _jquants_id_token()
    price_data = batch_download_jquants(token, start=start_str, end=end_str,
                                        tickers=_FETCH_TICKERS)
    print(f"  {len(price_data)} 銘柄取得完了\n")

    # マクロ指標（1655.T ベース）
    sp_raw = price_data.get(SP500_PROXY)
    if sp_raw is not None and len(sp_raw) >= 5:
        macro_df  = build_sp500_proxy(sp_raw)
        # 今日より前の最新行
        past_mac  = macro_df[macro_df.index.strftime("%Y-%m-%d") < end_str]
        if len(past_mac) >= 2:
            last      = past_mac.iloc[-1]
            sp500_ret = float(last["sp500_ret"]) if not np.isnan(last["sp500_ret"]) else 0.0
            vol_pct   = float(last["vol_pct"])   if not np.isnan(last["vol_pct"])   else 0.0
            sp500_date = past_mac.index[-1].strftime("%Y-%m-%d")
            print(f"  {SP500_PROXY} ({sp500_date}) 前日比: {sp500_ret:+.2f}%  "
                  f"ボラ(ATR/終値): {vol_pct:.2f}%\n")
        else:
            sp500_ret, vol_pct = 0.0, 0.0
    else:
        print(f"  WARNING: {SP500_PROXY} データなし → マクロフィルター無効\n")
        sp500_ret, vol_pct = 0.0, 0.0

    if vol_pct >= VOL_LIMIT:
        print(f"  ⚠️  ボラ = {vol_pct:.2f}% ≥ {VOL_LIMIT}% → 高ボラ環境 → 本日はノートレード\n")
        return

    macro_bias = "neutral"
    if sp500_ret <= SP500_BUY_MAX:
        macro_bias = "BUY_BIAS"
    elif sp500_ret >= SP500_SELL_MIN:
        macro_bias = "SELL_BIAS"

    if macro_bias == "neutral":
        print(f"  {SP500_PROXY} 前日比 {sp500_ret:+.2f}% は BUY/SELL いずれの閾値にも非該当\n"
              f"  → 本日はシグナルなし（マクロ条件を満たさず）\n")
        return

    print(f"  マクロバイアス: {macro_bias}\n")

    # ── 財務データ ───────────────────────────────────────
    print("[2/2] J-Quants 財務データ取得中...")
    fin_data = fetch_fins_statements(token, UNIVERSE)
    print()

    # ── シグナル判定 ─────────────────────────────────────
    signals = []

    for ticker in UNIVERSE:
        price_df = price_data.get(ticker)
        fin_df   = fin_data.get(ticker)
        if price_df is None or len(price_df) < 16:
            continue

        price_df = price_df.copy()
        price_df.index = pd.to_datetime(price_df.index).normalize()

        # 今日分は未確定なので除外（昨日以前のデータで判定）
        price_df = price_df[price_df.index.strftime("%Y-%m-%d") < end_str]
        if len(price_df) < 16:
            continue

        last_row    = price_df.iloc[-1]
        signal_date = price_df.index[-1].strftime("%Y-%m-%d")

        rsi = calc_rsi(price_df["Close"].dropna())
        if rsi is None:
            continue

        fin_row = get_latest_fin(fin_df, signal_date)
        if fin_row is None:
            continue
        fund = calc_fundamentals(fin_row, float(last_row["Close"]))

        direction = None
        reason    = []

        if (macro_bias == "BUY_BIAS" and
                rsi <= RSI_BUY_MAX and
                fund["roe"] is not None and fund["roe"] >= ROE_MIN and
                fund["pbr"] is not None and fund["pbr"] <= PBR_MAX):
            direction = "BUY"
            reason = [
                f"S&P500 前日{sp500_ret:+.2f}% ≤ {SP500_BUY_MAX}%",
                f"RSI={rsi:.0f} ≤ {RSI_BUY_MAX}（売られすぎ）",
                f"ROE={fund['roe']:.1f}% ≥ {ROE_MIN}%（優良）",
                f"PBR={fund['pbr']:.2f} ≤ {PBR_MAX}（割安）",
            ]

        elif (macro_bias == "SELL_BIAS" and
                  rsi >= RSI_SELL_MIN and
                  ((fund["per"] is not None and fund["per"] >= PER_SELL_MIN) or
                   (fund["eps"] is not None and fund["eps"] < 0))):
            direction = "SELL"
            reason = [
                f"S&P500 前日{sp500_ret:+.2f}% ≥ +{SP500_SELL_MIN}%",
                f"RSI={rsi:.0f} ≥ {RSI_SELL_MIN}（買われすぎ）",
                f"PER={fund['per']:.0f}倍以上 or 赤字（割高）" if fund["per"] else "EPS赤字（割高）",
            ]

        if direction:
            signals.append({
                "ticker":    ticker,
                "direction": direction,
                "prev_close": float(last_row["Close"]),
                "rsi":       round(rsi, 1),
                "roe":       round(fund["roe"], 1) if fund["roe"] is not None else None,
                "pbr":       round(fund["pbr"], 2) if fund["pbr"] is not None else None,
                "per":       round(fund["per"], 1) if fund["per"] is not None else None,
                "reason":    reason,
            })

    # ── 出力 ─────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  明日の寄り付きシグナル  ({today} 判定)")
    print(f"  S&P500前日: {sp500_ret:+.2f}%  VIX: {vix_val:.1f}  バイアス: {macro_bias}")
    print(f"{'='*60}")

    if not signals:
        print("  シグナルなし（テクニカル・ファンダ条件を満たす銘柄なし）\n")
        return

    buy_signals  = [s for s in signals if s["direction"] == "BUY"]
    sell_signals = [s for s in signals if s["direction"] == "SELL"]

    if buy_signals:
        print(f"\n  ▲ BUY シグナル ({len(buy_signals)} 件) ─────────────────────")
        print(f"  {'銘柄':12s}  {'前日終値':>8s}  {'RSI':>6s}  {'ROE':>7s}  {'PBR':>6s}")
        print(f"  {'-'*52}")
        for s in buy_signals:
            print(f"  {s['ticker']:12s}  {s['prev_close']:>8.0f}円  "
                  f"{s['rsi']:>6.1f}  {str(s['roe'])+'%':>7s}  {str(s['pbr']):>6s}")
        print()
        for s in buy_signals:
            print(f"  [{s['ticker']}] 根拠:")
            for r in s["reason"]:
                print(f"    ・{r}")

    if sell_signals:
        print(f"\n  ▼ SELL シグナル ({len(sell_signals)} 件) ────────────────────")
        print(f"  {'銘柄':12s}  {'前日終値':>8s}  {'RSI':>6s}  {'PER':>7s}")
        print(f"  {'-'*40}")
        for s in sell_signals:
            print(f"  {s['ticker']:12s}  {s['prev_close']:>8.0f}円  "
                  f"{s['rsi']:>6.1f}  {str(s['per'])+'倍' if s['per'] else '赤字':>7s}")
        print()
        for s in sell_signals:
            print(f"  [{s['ticker']}] 根拠:")
            for r in s["reason"]:
                print(f"    ・{r}")

    print(f"\n  ─────────────────────────────────────────────────────")
    print(f"  ※ 明朝の寄り付き（Open）でエントリー")
    print(f"  ※ 当日引け（Close 15:00）で全決済")
    print(f"  ※ Open から -{STOP_PCT}% 到達で強制ロスカット")
    print(f"  ※ データソース: J-Quants のみ（IPブロック回避）\n")


if __name__ == "__main__":
    run_daily_signal()
