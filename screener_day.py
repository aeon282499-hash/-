"""
screener_day.py — デイトレv2シグナルロジック（ブレイクアウト+出来高+ギャップ）
================================================================================

【戦略】
  前日大引け確定後にスクリーニング。翌日寄り成り（MAX指値）→ 同日大引け決済。
  当日終値が20日高値を更新 + 出来高爆発 + 終値が日中レンジ上位 の銘柄を選ぶ。

【シグナル条件】
  A: 当日終値が過去20日高値を更新（終値 >= 過去20日の最高値）
  B: 当日出来高が20日平均出来高の VOL_RATIO_MIN 倍以上（10倍）
  C: 当日終値が日中レンジの上位30%以内
     (Close - Low) / (High - Low) >= CLOSE_RANGE_MIN

【MAX指値運用】
  翌朝の寄り値が 20日高値 × (1 + GAP_MAX/100) 以下なら執行・超えたら見送り
  実装: 配信時に max_entry_price を計算して表示、利用者がその値で指値発注。

【BTで実証された数字（2023-2025）】
  n=68 / PF 2.81 / 勝率57% / 平均+1.0% / 3年連続プラス
  月収（400万信用1ポジ）約7.5万円見込み

【フィルター（追加）】
  日経225 ETF (1321.T) 終値 >= 25日MA（地合いフィルター）
  価格 >= 300円
  平均出来高 >= 10万株
  ATR/終値 <= ATR_VOL_CAP%
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

BREAKOUT_DAYS    = 20
VOL_RATIO_MIN    = 10.0   # 出来高比 20日平均の10倍以上
CLOSE_RANGE_MIN  = 0.7    # 終値が日中レンジ上位30%以内
GAP_MAX          = 20.0   # 翌朝寄り値の上限: 20日高値+20%まで
MIN_PRICE        = 300
MIN_AVG_VOLUME   = 100_000
ATR_VOL_CAP      = 3.0    # ATR/終値(%)上限
MAX_SIGNALS      = 3
LOOKBACK_DAYS    = 60
USE_MARKET_FILTER = True  # 日経25MA割れの日は買いシグナル出さない


# ================================================================
# 共通モジュールからインポート
# ================================================================

from screener import (
    fetch_tse_prime_universe,
    batch_download_jquants,
    _jquants_id_token,
    calc_atr,
    fetch_macro,
)


# ================================================================
# シグナル判定
# ================================================================

def judge_signal_day(ticker: str, name: str, df: pd.DataFrame) -> dict | None:
    """デイトレv2シグナル判定（ブレイクアウト+出来高+終値日中上位）。"""
    if len(df) < BREAKOUT_DAYS + 5:
        return None

    close  = df["Close"]
    volume = df["Volume"]
    high   = df["High"]
    low    = df["Low"]

    last_close  = float(close.iloc[-1])
    last_high   = float(high.iloc[-1])
    last_low    = float(low.iloc[-1])
    last_volume = float(volume.iloc[-1])

    # 基本フィルター
    if last_close < MIN_PRICE:
        return None

    vol_avg = float(volume.iloc[:-1].tail(BREAKOUT_DAYS).mean())
    if vol_avg < MIN_AVG_VOLUME:
        return None

    atr = calc_atr(df)
    if atr is None or last_close == 0:
        return None
    atr_pct = atr / last_close * 100
    if atr_pct > ATR_VOL_CAP:
        return None

    # 条件A: 当日終値が過去20日の最高値を更新
    high_20 = float(high.iloc[:-1].tail(BREAKOUT_DAYS).max())
    if last_close < high_20:
        return None

    # 条件B: 当日出来高が20日平均の VOL_RATIO_MIN 倍以上
    vol_ratio = last_volume / vol_avg if vol_avg > 0 else 0.0
    if vol_ratio < VOL_RATIO_MIN:
        return None

    # 条件C: 終値が日中レンジ上位30%以内
    day_range = last_high - last_low
    if day_range == 0:
        return None
    close_position = (last_close - last_low) / day_range
    if close_position < CLOSE_RANGE_MIN:
        return None

    # MAX指値価格（翌朝の寄り値上限）
    max_entry_price = high_20 * (1 + GAP_MAX / 100)

    reason = [
        f"20日高値ブレイク（前日終値 {last_close:,.0f}円 ≧ 20日高値 {high_20:,.0f}円）",
        f"出来高 = 20日平均の {vol_ratio:.1f}倍（≧{VOL_RATIO_MIN}倍）",
        f"終値が日中レンジ上位 {close_position*100:.0f}%（≧{CLOSE_RANGE_MIN*100:.0f}%）",
        f"ATR/終値 = {atr_pct:.1f}%（≦{ATR_VOL_CAP}%）",
    ]

    return {
        "ticker":          ticker,
        "name":            name,
        "direction":       "BUY",
        "prev_close":      last_close,
        "high_20":         high_20,
        "vol_ratio":       round(vol_ratio, 2),
        "close_position":  round(close_position, 3),
        "atr_pct":         round(atr_pct, 2),
        "max_entry_price": round(max_entry_price, 1),
        "reason":          reason,
    }


# ================================================================
# メインスクリーニング
# ================================================================

def run_screener_day() -> tuple[list[dict], dict]:
    """デイトレv2スクリーニングを実行し (signals, macro) を返す。"""

    macro = fetch_macro()

    universe = fetch_tse_prime_universe()
    name_map = {t: n for t, n in universe}
    tickers  = [t for t, _ in universe]
    if "1321.T" not in tickers:
        tickers.append("1321.T")
    print(f"[screener_day] ユニバース: {len(tickers)} 銘柄")

    from datetime import date as _date, timedelta as _td
    today_str  = _date.today().strftime("%Y-%m-%d")
    start_str  = (_date.today() - _td(days=90)).strftime("%Y-%m-%d")
    token = _jquants_id_token()
    data  = batch_download_jquants(token, start=start_str, end=today_str, tickers=tickers)
    if not data:
        print("[screener_day] データ取得失敗")
        return [], macro

    # 当日のデータは含めない（前日大引け確定までを判定対象に）
    data = {
        t: df[df.index.strftime("%Y-%m-%d") < today_str]
        for t, df in data.items()
    }

    # 地合いフィルター: 日経ETF(1321.T) 終値 >= 25日MA
    market_ok = True
    if USE_MARKET_FILTER:
        nk_df = data.get("1321.T")
        if nk_df is not None and len(nk_df) >= 25:
            ma25 = nk_df["Close"].rolling(25).mean().iloc[-1]
            nk_close = float(nk_df["Close"].iloc[-1])
            if not np.isnan(ma25):
                market_ok = (nk_close >= ma25)
                print(f"[screener_day] 地合い: 日経終値 {nk_close:,.0f} vs 25MA {ma25:,.0f} → {'OK' if market_ok else 'NG（買い見送り）'}")
        else:
            print("[screener_day] 日経データ不足 → 地合いチェックOFF")

    if not market_ok:
        bias = macro.get("bias", "neutral")
        print(f"[screener_day] 地合いNG → シグナル0件で終了")
        return [], macro

    candidates: list[dict] = []
    for ticker, df in data.items():
        if ticker == "1321.T":
            continue
        if len(df) < BREAKOUT_DAYS + 5:
            continue
        name   = name_map.get(ticker, ticker)
        result = judge_signal_day(ticker, name, df)
        if result is None:
            continue
        candidates.append(result)
        print(f"  [HIT] {ticker} {name} 出来高{result['vol_ratio']}倍 "
              f"高値{result['high_20']:,.0f}→終値{result['prev_close']:,.0f} MAX指値{result['max_entry_price']:,.0f}")

    bias = macro.get("bias", "neutral")
    print(f"[screener_day] マクロバイアス: {bias}（参考）")

    # 出来高比の高い順に上位 MAX_SIGNALS 件
    candidates.sort(key=lambda x: x["vol_ratio"], reverse=True)
    signals = candidates[:MAX_SIGNALS]

    print(f"[screener_day] 候補{len(candidates)}銘柄 → 最終{len(signals)}銘柄")
    return signals, macro
