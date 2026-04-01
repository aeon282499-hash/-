"""
screener_1570.py — 1570（日経レバレッジETF）デイトレシグナル判定
=================================================================

【戦略】
  S&P500プロキシ(1655.T)の前日騰落率 と 1570のRSIで判断

  BUY : S&P500前日 ≥+1.0% かつ 1570のRSI ≦ 60
  SELL: S&P500前日 ≤-1.0% かつ 1570のRSI ≧ 40
  PASS: それ以外

  エントリー: 翌営業日の寄り（始値）
  エグジット: 当日引け（15:30）または 損切-3% / 利確+5%
"""

SP500_BUY_MIN  =  1.0   # S&P500前日騰落率（BUY条件）
SP500_SELL_MAX = -1.0   # S&P500前日騰落率（SELL条件）
RSI_BUY_MAX    = 60     # BUY時のRSI上限
RSI_SELL_MIN   = 40     # SELL時のRSI下限
RSI_PERIOD     = 14
LOOKBACK_DAYS  = 60

TICKER_1570    = "1570.T"
TICKER_SP500P  = "1655.T"


from screener import batch_download_jquants, _jquants_id_token, calc_rsi


def judge_signal_1570(df_1570, df_sp500) -> dict:
    """1570のシグナルを判定して返す。direction は BUY / SELL / PASS。"""
    close = df_1570["Close"].dropna()
    if len(close) < RSI_PERIOD + 2:
        return {"direction": "PASS", "reason": "データ不足"}

    rsi        = calc_rsi(close)
    prev_close = float(close.iloc[-1])

    # S&P500プロキシの前日騰落率
    sp_close = df_sp500["Close"].dropna()
    if len(sp_close) < 2:
        return {"direction": "PASS", "reason": "S&P500データ不足"}
    sp_ret = (float(sp_close.iloc[-1]) - float(sp_close.iloc[-2])) / float(sp_close.iloc[-2]) * 100

    if rsi is None:
        return {"direction": "PASS", "reason": "RSI計算不可"}

    if sp_ret >= SP500_BUY_MIN and rsi <= RSI_BUY_MAX:
        direction = "BUY"
    elif sp_ret <= SP500_SELL_MAX and rsi >= RSI_SELL_MIN:
        direction = "SELL"
    else:
        direction = "PASS"

    return {
        "direction":  direction,
        "rsi":        round(rsi, 1),
        "sp500_ret":  round(sp_ret, 2),
        "prev_close": prev_close,
        "reason": [
            f"S&P500(1655.T) 前日 {sp_ret:+.2f}%",
            f"1570 RSI({RSI_PERIOD}) = {rsi:.0f}",
            f"1570 前日終値 {prev_close:,.0f}円",
        ],
    }


def run_screener_1570() -> dict:
    """1570シグナルを取得して返す。"""
    from datetime import date, timedelta
    today_str = date.today().strftime("%Y-%m-%d")
    start_str = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")

    tickers = [TICKER_1570, TICKER_SP500P]
    token   = _jquants_id_token()
    data    = batch_download_jquants(token, start=start_str, end=today_str, tickers=tickers)

    df_1570  = data.get(TICKER_1570)
    df_sp500 = data.get(TICKER_SP500P)

    if df_1570 is None or df_sp500 is None:
        print("[screener_1570] データ取得失敗")
        return {"direction": "PASS", "reason": "データ取得失敗"}

    # 今日より前のデータのみ使用
    df_1570  = df_1570[df_1570.index.strftime("%Y-%m-%d") < today_str]
    df_sp500 = df_sp500[df_sp500.index.strftime("%Y-%m-%d") < today_str]

    signal = judge_signal_1570(df_1570, df_sp500)
    print(f"[screener_1570] シグナル: {signal['direction']} "
          f"(S&P500={signal.get('sp500_ret', '?'):+}% / RSI={signal.get('rsi', '?')})")
    return signal
