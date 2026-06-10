"""
screener_sell_day.py - デイトレv2 SELL側シグナル（急騰翌日売り）
====================================================================

【戦略】信用売り（空売り）
  前日大幅上昇銘柄を翌日寄成売り → 同日大引け買戻し（1日完結）

【シグナル条件】
  A: 前日終値が前々日比 +DAILY_GAIN_MIN% 以上の急騰
  B: 前日出来高が20日平均出来高の VOL_RATIO_MIN 倍以上

【MIN指値運用】
  翌朝の寄り値が前日終値以上なら執行（ギャップアップ歓迎・売り建てやすい）
  寄り < 前日終値（ギャップダウン）= 既に下げ始め＝エッジ消失 → 見送り

【BT実証】
  2022年: n=13 / PF 6.61 / 平均+5.39%
  2023-2025年: n=100 / PF 3.60 / 平均+3.44%
  両期間で勝率60%+ / 3年連続+

【基本フィルター】
  価格 >= 300円
  平均出来高 >= 10万株
  ATR/終値 <= 5%
"""

import time
import ssl

import pandas as pd
import numpy as np

ssl._create_default_https_context = ssl._create_unverified_context

# ================================================================
# パラメーター設定
# ================================================================

DAILY_GAIN_MIN   = 25.0   # 前日比 +25% 以上（極限の過熱）
VOL_RATIO_MIN    = 5.0    # 出来高比 5倍以上
MIN_PRICE        = 300
MIN_AVG_VOLUME   = 100_000
ATR_VOL_CAP      = 5.0
MAX_SIGNALS      = 3
LOOKBACK_DAYS    = 60

from screener import (
    fetch_tse_prime_universe,
    batch_download_jquants,
    _jquants_id_token,
    calc_atr,
    fetch_macro,
)


def judge_sell_signal_day(ticker: str, name: str, df: pd.DataFrame) -> dict | None:
    """SELL側シグナル判定（急騰翌日売り）。"""
    if len(df) < 25:
        return None

    close  = df["Close"]
    volume = df["Volume"]

    last_close = float(close.iloc[-1])
    prev_close = float(close.iloc[-2])
    last_volume = float(volume.iloc[-1])

    # 基本フィルター
    if last_close < MIN_PRICE or prev_close <= 0:
        return None

    vol_avg = float(volume.iloc[:-1].tail(20).mean())
    if vol_avg < MIN_AVG_VOLUME:
        return None

    atr = calc_atr(df)
    if atr is None or last_close == 0:
        return None
    atr_pct = atr / last_close * 100
    if atr_pct > ATR_VOL_CAP:
        return None

    # 条件A: 前日比 +DAILY_GAIN_MIN% 以上
    daily_gain = (last_close - prev_close) / prev_close * 100
    if daily_gain < DAILY_GAIN_MIN:
        return None

    # 条件B: 出来高 N 倍
    vol_ratio = last_volume / vol_avg if vol_avg > 0 else 0.0
    if vol_ratio < VOL_RATIO_MIN:
        return None

    # MIN指値: 前日終値 = ギャップダウン寄りを除外（エッジ消失）
    min_entry_price = last_close

    reason = [
        f"前日急騰 +{daily_gain:.1f}%（≧{DAILY_GAIN_MIN}%）",
        f"出来高 = 20日平均の {vol_ratio:.1f}倍（≧{VOL_RATIO_MIN}倍）",
        f"ATR/終値 = {atr_pct:.1f}%（≦{ATR_VOL_CAP}%）",
        "→ 翌日寄り高で売り、引けで買戻し",
    ]

    return {
        "ticker":          ticker,
        "name":            name,
        "direction":       "SELL",
        "prev_close":      last_close,
        "daily_gain":      round(daily_gain, 2),
        "vol_ratio":       round(vol_ratio, 2),
        "atr_pct":         round(atr_pct, 2),
        "min_entry_price": round(min_entry_price, 1),
        "reason":          reason,
    }


def run_screener_sell_day() -> tuple[list[dict], dict]:
    """SELL側スクリーニングを実行し (signals, macro) を返す。"""

    macro = fetch_macro()

    universe = fetch_tse_prime_universe()
    name_map = {t: n for t, n in universe}
    tickers  = [t for t, _ in universe]
    print(f"[screener_sell] ユニバース: {len(tickers)} 銘柄")

    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    # JST基準（GitHubランナーはUTC。朝8時台は date.today() がUTC前日を返し、
    # 「今日」が昨日になって前日確定足の窓が1日ズレる。2026-06-10修正）
    _today = _dt.now(_tz(_td(hours=9))).date()
    today_str  = _today.strftime("%Y-%m-%d")
    start_str  = (_today - _td(days=90)).strftime("%Y-%m-%d")
    token = _jquants_id_token()
    data  = batch_download_jquants(token, start=start_str, end=today_str, tickers=tickers)
    if not data:
        print("[screener_sell] データ取得失敗")
        return [], macro

    data = {
        t: df[df.index.strftime("%Y-%m-%d") < today_str]
        for t, df in data.items()
    }

    candidates: list[dict] = []
    for ticker, df in data.items():
        if len(df) < 25:
            continue
        name = name_map.get(ticker, ticker)
        result = judge_sell_signal_day(ticker, name, df)
        if result is None:
            continue
        candidates.append(result)
        print(f"  [SELL HIT] {ticker} {name} 前日+{result['daily_gain']}% "
              f"出来高{result['vol_ratio']}倍 MIN指値¥{result['min_entry_price']:,.0f}")

    # daily_gain の大きい順に上位 MAX_SIGNALS 件
    candidates.sort(key=lambda x: x["daily_gain"], reverse=True)
    signals = candidates[:MAX_SIGNALS]

    print(f"[screener_sell] 候補{len(candidates)}銘柄 → 最終{len(signals)}銘柄")
    return signals, macro
