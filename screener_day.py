"""
screener_day.py — デイトレ用シグナルロジック（順張りモメンタム）
====================================================================

【戦略】
  前日に +2%〜+8% 急騰 かつ 出来高急増した銘柄 → 翌日寄りで買い（当日引けで決済）

  根拠: 強い出来高を伴う急騰銘柄はモメンタムが継続しやすい。
  +8% 超えは決算・テーマ相場等の特殊要因の可能性があるため除外。

【フィルター】
  ① 前日騰落率   +2% 〜 +8%（出来高急増を伴う急騰）
  ② RSI(14)      50 〜 75（モメンタムゾーン・過熱しすぎない）
  ③ 出来高比     前日出来高 ≧ 20日平均の2.0倍（本物の急騰）
  ④ 売買代金     ≧ 30億円（流動性確保）
  ⑤ 高ボラ除外   ATR/終値 > 4% の銘柄はスキップ
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

PREV_RETURN_BUY_MIN  =  2.0   # 前日騰落率の下限
PREV_RETURN_BUY_MAX  =  8.0   # 前日騰落率の上限（これより上はニュース系除外）

RSI_BUY_MIN    = 50    # RSI下限（モメンタムゾーン）
RSI_BUY_MAX    = 75    # RSI上限（過熱しすぎ除外）
VOL_MULT       = 2.0
TURNOVER_MIN   = 3_000_000_000   # 30億円
ATR_VOL_CAP    = 4.0             # ATR/終値(%)上限
MAX_SIGNALS    = 5
RSI_PERIOD     = 14
VOL_AVG_PERIOD = 20
ATR_PERIOD     = 14
LOOKBACK_DAYS  = 60
HIGH_PERIOD    = 20              # 20日高値ブレイクアウト判定期間
SP500_DROP_MAX = -1.5            # S&P500プロキシ(1655.T)前日下落率の下限（これ以下はスキップ）


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
    """デイトレ用シグナル判定（順張りモメンタム）。"""
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

    # ── ① 前日騰落率フィルター（BUYのみ）────────────────
    if not (PREV_RETURN_BUY_MIN <= prev_return <= PREV_RETURN_BUY_MAX):
        return None
    direction = "BUY"

    # ── ② RSIフィルター（モメンタムゾーン）──────────────
    if rsi < RSI_BUY_MIN or rsi > RSI_BUY_MAX:
        return None

    # ── ⑥ 20日高値ブレイクアウト ─────────────────────────
    if len(close) >= HIGH_PERIOD + 2:
        high_20 = float(close.iloc[-(HIGH_PERIOD + 1):-1].max())
        if float(close.iloc[-1]) < high_20:
            return None

    # ── ③ 出来高フィルター ────────────────────────────────
    if vol_ratio is None or vol_ratio < VOL_MULT:
        return None

    # ── ④ 流動性フィルター ────────────────────────────────
    if turnover < TURNOVER_MIN:
        return None

    high_20 = float(close.iloc[-(HIGH_PERIOD + 1):-1].max()) if len(close) >= HIGH_PERIOD + 2 else None
    reason = [
        f"前日騰落率 = {prev_return:+.1f}%（{PREV_RETURN_BUY_MIN}〜{PREV_RETURN_BUY_MAX}%の急騰）",
        f"RSI({RSI_PERIOD}) = {rsi:.0f}（{RSI_BUY_MIN}〜{RSI_BUY_MAX}：モメンタムゾーン）",
        f"出来高比 = {vol_ratio:.1f}（≧{VOL_MULT}：出来高急増で本物の動き）",
        f"売買代金 = {turnover/1e8:.0f}億円",
        f"20日高値 = {high_20:.0f}（ブレイクアウト確認済）" if high_20 else "20日高値ブレイクアウト確認",
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
    # マクロプロキシを追加（取得済みデータから利用）
    for proxy in ["1321.T", "1655.T"]:
        if proxy not in tickers:
            tickers.append(proxy)
    print(f"[screener_day] ユニバース: {len(tickers)} 銘柄（マクロETF含む）")

    from datetime import date as _date, timedelta as _td
    today_str  = _date.today().strftime("%Y-%m-%d")
    start_str  = (_date.today() - _td(days=90)).strftime("%Y-%m-%d")
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

    # ── マクロフィルター ───────────────────────────────────────
    # ① 日経ETF(1321.T): 終値が25日MAを上回っているか
    nikkei_ok = True
    nk_df = data.get("1321.T")
    if nk_df is not None and len(nk_df) >= 25:
        nk_close = float(nk_df["Close"].iloc[-1])
        nk_ma25  = float(nk_df["Close"].rolling(25).mean().iloc[-1])
        if not np.isnan(nk_ma25) and nk_close < nk_ma25:
            nikkei_ok = False
            print(f"[screener_day] 日経フィルター: NG（終値{nk_close:.0f} < MA25 {nk_ma25:.0f}）→ BUYシグナルなし")
        else:
            print(f"[screener_day] 日経フィルター: OK（終値{nk_close:.0f} ≥ MA25 {nk_ma25:.0f}）")
    else:
        print("[screener_day] 日経ETFデータ不足 → 日経フィルターOFF")

    # ② S&P500プロキシ(1655.T): 前日の騰落率が SP500_DROP_MAX% 以上か
    sp500_ok = True
    sp_df = data.get("1655.T")
    if sp_df is not None and len(sp_df) >= 2:
        sp_close_prev = float(sp_df["Close"].iloc[-2])
        sp_close_last = float(sp_df["Close"].iloc[-1])
        if sp_close_prev > 0:
            sp_ret = (sp_close_last - sp_close_prev) / sp_close_prev * 100
            if sp_ret < SP500_DROP_MAX:
                sp500_ok = False
                print(f"[screener_day] S&P500フィルター: NG（前日{sp_ret:+.1f}% < {SP500_DROP_MAX}%）→ BUYシグナルなし")
            else:
                print(f"[screener_day] S&P500フィルター: OK（前日{sp_ret:+.1f}%）")
    else:
        print("[screener_day] S&P500 ETFデータ不足 → S&P500フィルターOFF")

    # どちらかのマクロフィルターがNGなら全シグナルをスキップ
    if not nikkei_ok or not sp500_ok:
        print("[screener_day] マクロ環境NG → 本日はシグナルなし")
        return [], macro

    candidates: list[dict] = []
    for ticker, df in data.items():
        if ticker in ("1321.T", "1655.T"):
            continue
        if len(df) < RSI_PERIOD + VOL_AVG_PERIOD + 5:
            continue
        name   = name_map.get(ticker, ticker)
        result = judge_signal_day(ticker, name, df)
        if result:
            candidates.append(result)
            print(f"  [HIT] {ticker} {name} {result['direction']} "
                  f"前日{result['prev_return']:+.1f}% RSI={result['rsi']:.0f}")

    bias = macro.get("bias", "neutral")
    print(f"[screener_day] マクロバイアス: {bias}（参考）")

    # 出来高比が高い順に優先
    candidates.sort(key=lambda x: x["vol_ratio"], reverse=True)
    signals = candidates[:MAX_SIGNALS]

    print(f"[screener_day] 候補{len(candidates)}銘柄 → 最終{len(signals)}銘柄")
    return signals, macro
