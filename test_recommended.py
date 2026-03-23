"""
推奨設定で本日（2026-03-23）のシグナルをテストする
"""
import yfinance as yf
import pandas as pd
import numpy as np
from screener import (
    UNIVERSE, calc_rsi, calc_ma_deviation, calc_range_ratio, calc_volume_ratio
)

# ── 推奨設定 ──────────────────────────────
RSI_BUY_MAX   = 32
RSI_SELL_MIN  = 68
DEV_BUY_MAX   = -4.0
DEV_SELL_MIN  = +4.0
RANGE_MULT    = 1.3
VOL_MULT      = 2.0
MAX_SIGNALS   = 5
# ──────────────────────────────────────────

def judge(ticker, name, df):
    close = df["Close"].dropna()
    if len(close) < 30:
        return None
    rsi         = calc_rsi(close)
    deviation   = calc_ma_deviation(close)
    range_ratio = calc_range_ratio(df)
    vol_ratio   = calc_volume_ratio(df)
    if any(v is None for v in [rsi, deviation, range_ratio, vol_ratio]):
        return None

    if rsi <= RSI_BUY_MAX:       rsi_dir = "BUY"
    elif rsi >= RSI_SELL_MIN:    rsi_dir = "SELL"
    else:                        return None

    if deviation <= DEV_BUY_MAX:     dev_dir = "BUY"
    elif deviation >= DEV_SELL_MIN:  dev_dir = "SELL"
    else:                            return None

    if rsi_dir != dev_dir:       return None
    if range_ratio < RANGE_MULT: return None
    if vol_ratio   < VOL_MULT:   return None

    return {
        "ticker": ticker, "name": name,
        "direction": rsi_dir,
        "rsi": rsi, "deviation": deviation,
        "range_ratio": range_ratio, "vol_ratio": vol_ratio,
    }

print("\n" + "="*55)
print("  推奨設定テスト｜2026-03-23")
print("="*55)

signals = []
for ticker, name in UNIVERSE:
    try:
        df = yf.download(ticker, period="80d", interval="1d",
                         auto_adjust=True, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        result = judge(ticker, name, df)
        if result:
            signals.append(result)
            print(f"  ✅ {name}（{ticker}）→ {result['direction']}")
            print(f"     RSI={result['rsi']}  乖離率={result['deviation']:+.1f}%  "
                  f"値幅比={result['range_ratio']}  出来高比={result['vol_ratio']}")
        else:
            print(f"  ✗  {name}（{ticker}）")
    except Exception as e:
        print(f"  [{ticker}] エラー: {e}")

print("="*55)
if signals:
    print(f"  シグナル: {len(signals)} 銘柄")
    for s in signals[:MAX_SIGNALS]:
        print(f"  → {s['name']} {s['direction']}")
else:
    print("  シグナル: 0 件（ノートレード）")
print("="*55 + "\n")
