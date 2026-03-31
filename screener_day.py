"""
screener_day.py — デイトレ用シグナルロジック（前日大幅変動 逆張り）
====================================================================

【戦略】
  前日に -3%〜-8% 下落した銘柄 → 翌日寄りで買い（当日引けで決済）
  前日に +3%〜+8% 上昇した銘柄 → 翌日寄りで売り（当日引けで決済）

  根拠: 大きなギャップは6〜7割当日中に部分回帰する傾向がある。
  -8%/+8% 超えは決算・不祥事等の「戻らないニュース」の可能性があるため除外。

【フィルター】
  ① 前日騰落率   BUY: -8% 〜 -3% / SELL: +3% 〜 +8%
  ② RSI(14)      BUY: ≦40 / SELL: ≧50
  ③ 出来高比     前日出来高 ≧ 20日平均の1.5倍（動きが本物）
  ④ 売買代金     ≧ 30億円（流動性確保）
  ⑤ 高ボラ除外   ATR/終値 > 3% の銘柄はスキップ
"""

import time
import ssl

import requests
import pandas as pd
import numpy as np

ssl._create_default_https_context = ssl._create_unverified_context

# ================================================================
# パラメーター設定
# ================================================================

PREV_RETURN_BUY_MIN  = -8.0   # 前日騰落率の下限（これより下はニュース系除外）
PREV_RETURN_BUY_MAX  = -3.0   # 前日騰落率の上限
PREV_RETURN_SELL_MIN =  3.0   # 前日騰落率の下限
PREV_RETURN_SELL_MAX =  8.0   # 前日騰落率の上限（これより上はニュース系除外）

RSI_BUY_MAX    = 50
RSI_SELL_MIN   = 50
VOL_MULT       = 1.5
TURNOVER_MIN   = 3_000_000_000   # 30億円
ATR_VOL_CAP    = 2.5             # ATR/終値(%)上限
MAX_SIGNALS    = 5
RSI_PERIOD     = 14
VOL_AVG_PERIOD = 20
ATR_PERIOD     = 14
LOOKBACK_DAYS  = 60


# ================================================================
# 共通モジュールからインポート
# ================================================================

from screener import (
    fetch_tse_prime_universe,
    _nikkei225_universe,
    batch_download_stooq,
    batch_download_jquants,
    _jquants_id_token,
    calc_rsi,
    calc_atr,
    fetch_macro,
)


# ================================================================
# テクニカル指標（デイトレ専用）
# ================================================================

def calc_prev_return(df: pd.DataFrame) -> float | None:
    """前日（最新行）の終値騰落率(%)を返す。"""
    close = df["Close"].dropna()
    if len(close) < 2:
        return None
    prev2 = float(close.iloc[-2])
    prev1 = float(close.iloc[-1])
    if prev2 <= 0:
        return None
    return round((prev1 - prev2) / prev2 * 100, 2)


def calc_volume_ratio_day(df: pd.DataFrame, period: int = VOL_AVG_PERIOD) -> float | None:
    """シグナル当日出来高 / 過去N日平均出来高を返す。"""
    vol = df["Volume"].dropna()
    if len(vol) < period + 2:
        return None
    avg_vol  = float(vol.iloc[-(period + 1):-1].mean())
    prev_vol = float(vol.iloc[-1])
    if avg_vol <= 0:
        return None
    return round(prev_vol / avg_vol, 2)


def calc_turnover_day(df: pd.DataFrame) -> float | None:
    """シグナル当日の売買代金を返す。"""
    if len(df) < 1:
        return None
    prev_close  = float(df["Close"].iloc[-1])
    prev_volume = float(df["Volume"].iloc[-1])
    if prev_volume <= 0:
        return None
    return prev_close * prev_volume


# ================================================================
# シグナル判定
# ================================================================

def judge_signal_day(ticker: str, name: str, df: pd.DataFrame) -> dict | None:
    """デイトレ用シグナル判定（前日大幅変動 逆張り）。"""
    if len(df) < RSI_PERIOD + VOL_AVG_PERIOD + 5:
        return None

    close = df["Close"].dropna()

    prev_return = calc_prev_return(df)
    rsi         = calc_rsi(close)
    vol_ratio   = calc_volume_ratio_day(df)
    turnover    = calc_turnover_day(df)
    atr         = calc_atr(df)

    if any(v is None for v in [prev_return, rsi, turnover]):
        return None

    # ── ⑤ 高ボラ除外 ──────────────────────────────────────
    last_close = float(close.iloc[-1])
    if atr is not None and last_close > 0:
        if (atr / last_close * 100) > ATR_VOL_CAP:
            return None

    # ── ① 前日騰落率で方向判定 ────────────────────────────
    if PREV_RETURN_BUY_MIN <= prev_return <= PREV_RETURN_BUY_MAX:
        direction = "BUY"
    elif PREV_RETURN_SELL_MIN <= prev_return <= PREV_RETURN_SELL_MAX:
        direction = "SELL"
    else:
        return None

    # ── ② RSIフィルター ───────────────────────────────────
    if direction == "BUY"  and rsi > RSI_BUY_MAX:
        return None
    if direction == "SELL" and rsi < RSI_SELL_MIN:
        return None

    # ── ③ 出来高フィルター ────────────────────────────────
    vol_ok = (vol_ratio is not None) and (vol_ratio >= VOL_MULT)
    if not vol_ok:
        return None

    # ── ④ 流動性フィルター ────────────────────────────────
    if turnover < TURNOVER_MIN:
        return None

    if direction == "BUY":
        reason = [
            f"前日騰落率 = {prev_return:+.1f}%（{PREV_RETURN_BUY_MIN}〜{PREV_RETURN_BUY_MAX}%の急落）",
            f"RSI({RSI_PERIOD}) = {rsi}（≦{RSI_BUY_MAX}：売られすぎ）",
            f"出来高比 = {vol_ratio:.1f}（≧{VOL_MULT}：出来高急増で本物の動き）",
            f"売買代金 = {turnover/1e8:.0f}億円",
        ]
    else:
        reason = [
            f"前日騰落率 = {prev_return:+.1f}%（{PREV_RETURN_SELL_MIN}〜{PREV_RETURN_SELL_MAX}%の急騰）",
            f"RSI({RSI_PERIOD}) = {rsi}（≧{RSI_SELL_MIN}：買われすぎ）",
            f"出来高比 = {vol_ratio:.1f}（≧{VOL_MULT}：出来高急増で本物の動き）",
            f"売買代金 = {turnover/1e8:.0f}億円",
        ]

    return {
        "ticker":      ticker,
        "name":        name,
        "direction":   direction,
        "prev_return": prev_return,
        "rsi":         rsi,
        "vol_ratio":   vol_ratio,
        "turnover":    turnover,
        "prev_close":  last_close,
        "reason":      reason,
    }


# ================================================================
# メインスクリーニング
# ================================================================

def run_screener_day() -> tuple[list[dict], dict]:
    """デイトレスクリーニングを実行し (signals, macro) を返す。"""

    macro    = fetch_macro()
    universe = fetch_tse_prime_universe()
    name_map = {t: n for t, n in universe}
    tickers  = [t for t, _ in universe]
    print(f"[screener_day] ユニバース: {len(tickers)} 銘柄")

    from datetime import date as _date, timedelta as _td
    today_str  = _date.today().strftime("%Y-%m-%d")
    start_str  = (_date.today() - _td(days=180)).strftime("%Y-%m-%d")
    try:
        token = _jquants_id_token()
        data  = batch_download_jquants(token, start=start_str, end=today_str, tickers=tickers)
    except Exception as e:
        print(f"[screener_day] J-Quants失敗({e})→stooqで再試行...")
        data = batch_download_stooq(tickers, start=start_str, end=today_str)
    if not data:
        print("[screener_day] データ取得失敗")
        return [], macro
    data = {
        t: df[df.index.strftime("%Y-%m-%d") < today_str]
        for t, df in data.items()
    }

    candidates: list[dict] = []
    for ticker, df in data.items():
        if len(df) < RSI_PERIOD + VOL_AVG_PERIOD + 5:
            continue
        name   = name_map.get(ticker, ticker)
        result = judge_signal_day(ticker, name, df)
        if result:
            candidates.append(result)
            print(f"  [HIT] {ticker} {name} {result['direction']} "
                  f"前日{result['prev_return']:+.1f}% RSI={result['rsi']}")

    # マクロバイアス（参考表示のみ・絞り込みは行わない）
    bias = macro.get("bias", "neutral")
    print(f"[screener_day] マクロバイアス: {bias}（参考）")

    # 騰落率の絶対値が大きい順（より極端な動きを優先）
    candidates.sort(key=lambda x: abs(x["prev_return"]), reverse=True)
    signals = candidates[:MAX_SIGNALS]

    print(f"[screener_day] 候補{len(candidates)}銘柄 → 最終{len(signals)}銘柄")
    return signals, macro
