"""
screener_1570.py — 1570（日経レバレッジETF）デイトレシグナル判定
=================================================================

【戦略】
  S&P500(SPY)の前日騰落率 と 1570のRSIで判断

  BUY : S&P500前日 ≥+0.7% かつ 1570のRSI ≦ 50
  SELL: S&P500前日 ≤-0.7% かつ 1570のRSI ≧ 50
  PASS: それ以外

  エントリー: 翌営業日の寄り（始値）
  エグジット: 当日引け（15:30）または 損切-3% / 利確+5%
"""

SP500_BUY_MIN  =  0.7   # S&P500前日騰落率（BUY条件）
SP500_SELL_MAX = -0.7   # S&P500前日騰落率（SELL条件）
RSI_BUY_MAX    = 50     # BUY時のRSI上限
RSI_SELL_MIN   = 50     # SELL時のRSI下限
RSI_PERIOD     = 14
LOOKBACK_DAYS  = 60

TICKER_1570    = "1570.T"
TICKER_SP500P  = "1655.T"  # フォールバック用（互換性のため残す）


import os
from screener import batch_download_jquants, _jquants_id_token, calc_rsi, _fetch_av_daily_return


def judge_signal_1570(df_1570, sp500_ret: float) -> dict:
    """1570のシグナルを判定して返す。sp500_ret: S&P500前日騰落率(%)"""
    close = df_1570["Close"].dropna()
    if len(close) < RSI_PERIOD + 2:
        return {"direction": "PASS", "reason": "データ不足"}

    rsi        = calc_rsi(close)
    prev_close = float(close.iloc[-1])

    if rsi is None:
        return {"direction": "PASS", "reason": "RSI計算不可"}

    if sp500_ret >= SP500_BUY_MIN and rsi <= RSI_BUY_MAX:
        direction = "BUY"
    elif sp500_ret <= SP500_SELL_MAX and rsi >= RSI_SELL_MIN:
        direction = "SELL"
    else:
        direction = "PASS"

    return {
        "direction":  direction,
        "rsi":        round(rsi, 1),
        "sp500_ret":  round(sp500_ret, 2),
        "prev_close": prev_close,
        "reason": [
            f"S&P500(SPY) 前日 {sp500_ret:+.2f}%",
            f"1570 RSI({RSI_PERIOD}) = {rsi:.0f}",
            f"1570 前日終値 {prev_close:,.0f}円",
        ],
    }


def run_screener_1570() -> dict:
    """1570シグナルを取得して返す。"""
    from datetime import date, timedelta
    today_str = date.today().strftime("%Y-%m-%d")
    start_str = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")

    # Alpha VantageでS&P500前日騰落率を取得
    api_key   = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()
    sp500_ret = _fetch_av_daily_return("SPY", api_key) if api_key else None
    if sp500_ret is None:
        print("[screener_1570] Alpha Vantage取得失敗 → シグナルなし")
        return {"direction": "PASS", "reason": "S&P500データ取得失敗"}

    # 1570のRSI計算用データ取得
    token    = _jquants_id_token()
    data     = batch_download_jquants(token, start=start_str, end=today_str,
                                       tickers=[TICKER_1570])
    df_1570  = data.get(TICKER_1570)
    if df_1570 is None:
        print("[screener_1570] 1570データ取得失敗")
        return {"direction": "PASS", "reason": "1570データ取得失敗"}

    df_1570 = df_1570[df_1570.index.strftime("%Y-%m-%d") < today_str]

    signal = judge_signal_1570(df_1570, sp500_ret)
    print(f"[screener_1570] シグナル: {signal['direction']} "
          f"(S&P500={sp500_ret:+.2f}% / RSI={signal.get('rsi', '?')})")
    return signal
