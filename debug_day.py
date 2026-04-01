"""
debug_day.py — デイトレ条件の通過率を調べる診断スクリプト
使い方: python debug_day.py
"""
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from datetime import date, timedelta
from screener import (
    fetch_tse_prime_universe, batch_download_jquants, _jquants_id_token,
    calc_rsi, calc_atr,
)
from screener_day import (
    calc_prev_return, calc_volume_ratio_day, calc_turnover_day,
    PREV_RETURN_BUY_MIN, PREV_RETURN_BUY_MAX,
    PREV_RETURN_SELL_MIN, PREV_RETURN_SELL_MAX,
    RSI_BUY_MAX, RSI_SELL_MIN,
    VOL_MULT, TURNOVER_MIN, ATR_VOL_CAP,
    RSI_PERIOD, VOL_AVG_PERIOD,
)

today_str = date.today().strftime("%Y-%m-%d")
start_str = (date.today() - timedelta(days=120)).strftime("%Y-%m-%d")

print(f"診断期間: {start_str} 〜 {today_str}")
print("データ取得中...")

token = _jquants_id_token()
universe = fetch_tse_prime_universe()
name_map = {t: n for t, n in universe}
data = batch_download_jquants(token, start=start_str, end=today_str)
data = {t: df[df.index.strftime("%Y-%m-%d") < today_str] for t, df in data.items()}

print(f"取得銘柄数: {len(data)}\n")

counters = {
    "total": 0,
    "short_data": 0,
    "none_values": 0,
    "atr_cap": 0,
    "prev_return_range": 0,
    "rsi_filter": 0,
    "vol_filter": 0,
    "turnover_filter": 0,
    "pass": 0,
}

samples = {"atr": [], "prev_return_ok": [], "rsi": [], "vol": [], "turnover": []}

for ticker, df in data.items():
    counters["total"] += 1

    if len(df) < RSI_PERIOD + VOL_AVG_PERIOD + 5:
        counters["short_data"] += 1
        continue

    close = df["Close"].dropna()
    prev_return = calc_prev_return(df)
    rsi         = calc_rsi(close)
    vol_ratio   = calc_volume_ratio_day(df)
    turnover    = calc_turnover_day(df)
    atr         = calc_atr(df)

    if any(v is None for v in [prev_return, rsi, turnover]):
        counters["none_values"] += 1
        continue

    last_close = float(close.iloc[-1])
    atr_pct = (atr / last_close * 100) if (atr and last_close > 0) else 0
    samples["atr"].append(atr_pct)

    if atr is not None and last_close > 0 and atr_pct > ATR_VOL_CAP:
        counters["atr_cap"] += 1
        continue

    is_buy_range  = PREV_RETURN_BUY_MIN  <= prev_return <= PREV_RETURN_BUY_MAX
    is_sell_range = PREV_RETURN_SELL_MIN <= prev_return <= PREV_RETURN_SELL_MAX
    if not (is_buy_range or is_sell_range):
        counters["prev_return_range"] += 1
        continue

    direction = "BUY" if is_buy_range else "SELL"
    samples["prev_return_ok"].append((ticker, prev_return, direction))

    if direction == "BUY" and rsi > RSI_BUY_MAX:
        counters["rsi_filter"] += 1
        samples["rsi"].append((ticker, rsi, direction))
        continue
    if direction == "SELL" and rsi < RSI_SELL_MIN:
        counters["rsi_filter"] += 1
        samples["rsi"].append((ticker, rsi, direction))
        continue

    if vol_ratio is None or vol_ratio < VOL_MULT:
        counters["vol_filter"] += 1
        continue

    if turnover < TURNOVER_MIN:
        counters["turnover_filter"] += 1
        continue

    counters["pass"] += 1
    print(f"  [PASS] {ticker} {name_map.get(ticker, '')} {direction} "
          f"騰落{prev_return:+.1f}% RSI={rsi:.0f} vol={vol_ratio:.1f} "
          f"代金{turnover/1e8:.0f}億 ATR={atr_pct:.1f}%")

print(f"\n{'='*50}")
print(f"総銘柄数      : {counters['total']}")
print(f"データ不足    : {counters['short_data']}")
print(f"値None        : {counters['none_values']}")
print(f"ATRキャップ除外: {counters['atr_cap']}  (>{ATR_VOL_CAP}%)")
print(f"騰落率範囲外  : {counters['prev_return_range']}")
print(f"RSIフィルター : {counters['rsi_filter']}")
print(f"出来高フィルター: {counters['vol_filter']}")
print(f"売買代金フィルター: {counters['turnover_filter']}")
print(f"通過           : {counters['pass']}")
print(f"{'='*50}")

if samples["atr"]:
    import statistics
    atr_vals = samples["atr"]
    print(f"\nATR分布: 平均{statistics.mean(atr_vals):.2f}% / "
          f"中央値{statistics.median(atr_vals):.2f}% / "
          f">{ATR_VOL_CAP}%の割合: {sum(1 for v in atr_vals if v > ATR_VOL_CAP)/len(atr_vals)*100:.1f}%")

if samples["prev_return_ok"]:
    print(f"\n騰落率条件通過サンプル（最大10件）:")
    for t, r, d in samples["prev_return_ok"][:10]:
        print(f"  {t} {name_map.get(t, '')} {d} {r:+.1f}%")

if samples["rsi"]:
    print(f"\nRSIで落ちたサンプル（最大5件）:")
    for t, r, d in samples["rsi"][:5]:
        print(f"  {t} {name_map.get(t, '')} {d} RSI={r:.0f}")
