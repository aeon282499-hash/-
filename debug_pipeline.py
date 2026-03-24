"""
debug_pipeline.py — データ取得〜シグナル判定の全段階を確認する
"""
import yfinance as yf
import pandas as pd
import numpy as np
from screener import (
    calc_rsi, calc_ma_deviation, calc_range_ratio,
    calc_volume_ratio, calc_turnover, calc_trend,
    RSI_BUY_MAX, RSI_SELL_MIN, DEV_BUY_MAX, DEV_SELL_MIN,
    RANGE_MULT, VOL_MULT, TURNOVER_MIN, USE_TREND_FILTER,
    RSI_PERIOD, MA_SHORT,
)

SAMPLE = [
    ("7203.T", "トヨタ"),
    ("9984.T", "SBG"),
    ("6758.T", "ソニー"),
    ("8306.T", "三菱UFJ"),
    ("8035.T", "東京エレク"),
    ("6098.T", "リクルート"),
]

print(f"\n設定値: RSI≦{RSI_BUY_MAX}/≧{RSI_SELL_MIN}  5MA乖離≦{DEV_BUY_MAX}%/≧{DEV_SELL_MIN}%  200MAフィルター={USE_TREND_FILTER}\n")

for ticker, name in SAMPLE:
    from screener import _SESSION
    dl_kwargs = dict(period="300d", interval="1d", auto_adjust=True, progress=False)
    if _SESSION:
        dl_kwargs["session"] = _SESSION
    df = yf.download(ticker, **dl_kwargs)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    close = df["Close"].dropna()
    rsi   = calc_rsi(close)
    dev   = calc_ma_deviation(close)
    rr    = calc_range_ratio(df)
    vr    = calc_volume_ratio(df)
    to    = calc_turnover(df)
    trend = calc_trend(close)
    rows  = len(close)

    rsi_ok  = rsi is not None and (rsi <= RSI_BUY_MAX or rsi >= RSI_SELL_MIN)
    dev_ok  = dev is not None and (dev <= DEV_BUY_MAX or dev >= DEV_SELL_MIN)
    vol_ok  = (rr is not None and rr >= RANGE_MULT) or (vr is not None and vr >= VOL_MULT)
    turn_ok = to is not None and to >= TURNOVER_MIN

    print(f"{'─'*60}")
    print(f"{name}({ticker})  データ行数:{rows}")
    if rsi is None:
        print(f"  ⚠️ データ取得失敗（レート制限 or エラー）")
        continue
    print(f"  RSI({RSI_PERIOD})={rsi:.1f}  {'✅' if rsi_ok else '❌(条件①未達)'}")
    print(f"  5MA乖離率={dev:+.2f}%  {'✅' if dev_ok else '❌(条件②未達)'}")
    print(f"  値幅/ATR={rr:.2f}  出来高比={vr:.2f}  {'✅' if vol_ok else '❌(条件③未達)'}")
    print(f"  売買代金={to/1e8:.1f}億  {'✅' if turn_ok else '❌(条件④未達)'}")
    print(f"  200MAトレンド={trend}")
    print(f"  → 全条件クリア: {'✅' if (rsi_ok and dev_ok and vol_ok and turn_ok) else '❌'}")

print(f"\n{'─'*60}")
print("※ RSI条件と5MA乖離率条件の両方が✅にならないとシグナル出ません")
