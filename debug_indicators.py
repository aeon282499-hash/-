"""
debug_indicators.py — 指標の実値を確認して0件の原因を調べる
"""
import yfinance as yf
import pandas as pd
import numpy as np
from screener import (
    calc_rsi, calc_ma_deviation, calc_range_ratio,
    calc_volume_ratio, calc_turnover,
    RSI_PERIOD, MA_PERIOD, ATR_PERIOD,
    RSI_BUY_MAX, RSI_SELL_MIN, DEV_BUY_MAX, DEV_SELL_MIN,
    RANGE_MULT, VOL_MULT, TURNOVER_MIN,
)

# 代表銘柄で指標値を確認
SAMPLE = [
    ("7203.T", "トヨタ"),
    ("9984.T", "SBG"),
    ("6758.T", "ソニー"),
    ("8306.T", "三菱UFJ"),
    ("8035.T", "東京エレク"),
    ("9983.T", "ファストリ"),
    ("6861.T", "キーエンス"),
    ("4568.T", "第一三共"),
]

print(f"\n{'='*80}")
print(f" 指標デバッグ（RSI_PERIOD={RSI_PERIOD}, MA_PERIOD={MA_PERIOD}）")
print(f" 閾値: RSI買い≦{RSI_BUY_MAX} / 売り≧{RSI_SELL_MIN}")
print(f"       乖離率買い≦{DEV_BUY_MAX}% / 売り≧{DEV_SELL_MIN}%")
print(f"{'='*80}")
print(f"{'銘柄':<14} {'RSI(2)':>8} {'5MA乖離%':>10} {'値幅/ATR':>10} {'出来高比':>10} {'売買代金':>12}")
print(f"{'-'*80}")

for ticker, name in SAMPLE:
    try:
        df = yf.download(ticker, period="60d", interval="1d",
                         auto_adjust=True, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        if df.empty:
            continue

        close = df["Close"].dropna()
        rsi  = calc_rsi(close)
        dev  = calc_ma_deviation(close)
        rr   = calc_range_ratio(df)
        vr   = calc_volume_ratio(df)
        to   = calc_turnover(df)

        rsi_flag = "←BUY" if rsi and rsi <= RSI_BUY_MAX else ("←SELL" if rsi and rsi >= RSI_SELL_MIN else "")
        dev_flag = "←BUY" if dev and dev <= DEV_BUY_MAX else ("←SELL" if dev and dev >= DEV_SELL_MIN else "")

        print(f"{name:<14} "
              f"{rsi:>8.1f}{rsi_flag:<6} "
              f"{dev:>+8.2f}%{dev_flag:<6} "
              f"{rr:>8.2f}  "
              f"{vr:>8.2f}  "
              f"{to/1e8:>10.1f}億")
    except Exception as e:
        print(f"{name:<14} エラー: {e}")

print(f"{'='*80}")
print(f"\n【判定】")
print(f"  RSI(2) が {RSI_BUY_MAX} 以下 or {RSI_SELL_MIN} 以上の銘柄が条件①クリア")
print(f"  5MA乖離率 が {DEV_BUY_MAX}% 以下 or {DEV_SELL_MIN}% 以上の銘柄が条件②クリア")
print(f"  両方同時に満たす銘柄がシグナル候補\n")
