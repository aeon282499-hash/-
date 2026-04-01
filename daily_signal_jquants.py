"""
daily_signal_jquants.py — 明朝の寄り付き買いシグナル出力（J-Quants のみ・全銘柄）
=========================================================
使い方:
  python daily_signal_jquants.py
"""

from datetime import date, timedelta
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import numpy as np

from screener import (
    _jquants_id_token, batch_download_jquants,
    fetch_tse_prime_universe, calc_rsi, calc_atr,
)
from backtest_jquants import (
    SP500_PROXY, NIKKEI_PROXY,
    SP500_BUY_MAX, VOL_CAP, RSI_BUY_MAX, TURNOVER_MIN, STOP_PCT,
    build_macro,
)

MAX_SIGNALS = 10  # 最大出力件数（RSI 低い順）


def run_daily_signal() -> None:
    today     = date.today()
    end_str   = today.strftime("%Y-%m-%d")
    start_str = (today - timedelta(days=90)).strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"  デイリーシグナル  判定日: {today}")
    print(f"  戦略: 寄り引け逆張り（BUY only）")
    print(f"  ※ 明朝の寄り付きでエントリー -> 当日引けで決済")
    print(f"  ※ Open -{STOP_PCT}% で強制ロスカット")
    print(f"{'='*60}\n")

    # ── データ取得 ────────────────────────────────────────
    print("[1/2] 銘柄リスト & 価格データ取得中...")
    universe = fetch_tse_prime_universe()
    tickers  = [t for t, _ in universe]
    name_map = {t: n for t, n in universe}
    for proxy in [SP500_PROXY, NIKKEI_PROXY]:
        if proxy not in tickers:
            tickers.append(proxy)

    token      = _jquants_id_token()
    price_data = batch_download_jquants(token, start=start_str, end=end_str, tickers=tickers)
    print(f"  {len(price_data)} 銘柄取得完了\n")

    # ── マクロ確認（1655.T）──────────────────────────────
    print("[2/2] マクロ確認中...")
    sp_raw = price_data.get(SP500_PROXY)
    if sp_raw is not None and len(sp_raw) >= 5:
        macro_df  = build_macro(sp_raw)
        past_mac  = macro_df[macro_df.index.strftime("%Y-%m-%d") < end_str]
        if len(past_mac) >= 2:
            last      = past_mac.iloc[-1]
            sp500_ret = float(last["sp500_ret"]) if not np.isnan(last["sp500_ret"]) else 0.0
            sp_date   = past_mac.index[-1].strftime("%Y-%m-%d")
            print(f"  {SP500_PROXY} ({sp_date}) 前日比: {sp500_ret:+.2f}%\n")
        else:
            sp500_ret = 0.0
            print("  WARNING: マクロデータ不足\n")
    else:
        sp500_ret = 0.0
        print(f"  WARNING: {SP500_PROXY} データなし\n")

    if sp500_ret > SP500_BUY_MAX:
        print(f"  {SP500_PROXY} 前日比 {sp500_ret:+.2f}% > {SP500_BUY_MAX}%")
        print("  -> 米株が下がっていない -> 本日はシグナルなし\n")
        return

    print(f"  米株急落確認 ({sp500_ret:+.2f}%) -> シグナルスキャン開始\n")

    # ── シグナルスキャン ──────────────────────────────────
    signals = []
    for ticker, price_df in price_data.items():
        if ticker in (SP500_PROXY, NIKKEI_PROXY):
            continue
        if price_df is None or len(price_df) < 20:
            continue

        price_df = price_df.copy()
        price_df.index = pd.to_datetime(price_df.index).normalize()
        price_df = price_df[price_df.index.strftime("%Y-%m-%d") < end_str]
        if len(price_df) < 20:
            continue

        last_row   = price_df.iloc[-1]
        last_close = float(last_row["Close"])
        if last_close <= 0:
            continue

        # ATR フィルター
        atr = calc_atr(price_df.iloc[-21:])
        if atr is None or (atr / last_close * 100) > VOL_CAP:
            continue

        # RSI フィルター
        rsi = calc_rsi(price_df["Close"].dropna())
        if rsi is None or rsi > RSI_BUY_MAX:
            continue

        # 売買代金フィルター
        turnover = last_close * float(last_row.get("Volume", 0))
        if turnover < TURNOVER_MIN:
            continue

        signals.append({
            "ticker":    ticker,
            "name":      name_map.get(ticker, ticker),
            "close":     round(last_close, 1),
            "rsi":       round(rsi, 1),
            "atr_pct":   round(atr / last_close * 100, 2),
            "turnover":  round(turnover / 1e8, 1),
        })

    # RSI 低い順に並べて上位 MAX_SIGNALS 件
    signals.sort(key=lambda x: x["rsi"])
    signals = signals[:MAX_SIGNALS]

    # ── 出力 ─────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  明日の寄り付きシグナル  ({today} 判定)")
    print(f"  {SP500_PROXY} 前日比: {sp500_ret:+.2f}%")
    print(f"{'='*60}")

    if not signals:
        print("  シグナルなし（RSI<=30 & ATR<=2.5% & 売買代金>=30億 を満たす銘柄なし）\n")
        return

    print(f"\n  -- BUY シグナル ({len(signals)} 件 / RSI低い順) --")
    print(f"  {'銘柄':10s} {'名前':20s} {'終値':>8s} {'RSI':>6s} {'ATR%':>6s} {'売買代金':>8s}")
    print(f"  {'-'*65}")
    for s in signals:
        print(f"  {s['ticker']:10s} {s['name'][:18]:20s} {s['close']:>8.0f}円 "
              f"{s['rsi']:>6.1f} {s['atr_pct']:>6.2f}% {s['turnover']:>7.1f}億")

    print(f"\n  ※ 明朝の寄り付き（Open）でBUY")
    print(f"  ※ 当日引け（Close 15:00）で決済")
    print(f"  ※ Open -{STOP_PCT}% に逆指値を置く\n")


if __name__ == "__main__":
    run_daily_signal()
