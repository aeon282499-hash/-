"""
close_check.py — 大引け前のRSI判定とDiscord通知
==================================================
毎営業日 15:00 JST に GitHub Actions から呼ばれる。

実行フロー:
  1. 営業日チェック・時間外スキップ
  2. positions.json をロード（status=pending/open のBUYポジションのみ対象）
  3. 各ポジションについて:
     - yfinance で当日 current price 取得（~14:45データ・15分遅延）
     - 過去終値（J-Quants）+ current price で RSI(14) 計算
     - 判定:
       * 当日 hold_day == MAX_HOLD: 強制MAXHOLD大引け処分
       * else if RSI ≥ 50 (BUY): RSI回復で大引け処分推奨
  4. 該当銘柄があれば notifier.send_close_signals() でDiscord通知

ユーザーは通知を受けて15:25-15:30のクロージングオークションでSBI証券アプリから成売り発注。
"""

import json
import os
from datetime import datetime, date
import zoneinfo

import jpholiday
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")

POSITIONS_FILE = "positions.json"
MAX_HOLD = 3
RSI_EXIT_THRESHOLD = 50


def is_trading_day(d) -> bool:
    if d.weekday() >= 5:
        return False
    if jpholiday.is_holiday(d):
        return False
    return True


def calc_today_hold_day(pos: dict, today: date) -> int:
    """positions.jsonのhold_days（朝tracker処理後の累計）から当日が第何日目か計算。

    - entry_date == today: 当日エントリー → 1日目
    - それ以外: 朝tracker.pyが「前日までの完了日数」をhold_daysにセットしている → +1で当日
    """
    entry_date_str = pos["entry_date"]
    today_str = today.strftime("%Y-%m-%d")
    if entry_date_str == today_str:
        return 1
    return pos.get("hold_days", 0) + 1


def main():
    now = datetime.now(JST)
    today = now.date()
    print(f"[close_check] 実行: {now.strftime('%Y-%m-%d %H:%M JST')}")

    if not is_trading_day(today):
        print("[close_check] 休場日のためスキップ")
        return

    # 14:00〜15:30 JST 以外は誤トリガーとしてスキップ
    if not (14 <= now.hour <= 15):
        print(f"[close_check] 時間外スキップ（実行時刻={now.strftime('%H:%M')}）")
        return

    if not os.path.exists(POSITIONS_FILE):
        print("[close_check] positions.json なし")
        return
    with open(POSITIONS_FILE, encoding="utf-8") as f:
        positions = json.load(f)

    open_positions = [p for p in positions
                      if p.get("status") in ("pending", "open")
                      and p.get("direction") == "BUY"]
    if not open_positions:
        print("[close_check] BUY オープンポジションなし")
        return

    print(f"[close_check] 対象 {len(open_positions)} 件")

    import yfinance as yf
    from datetime import timedelta
    from screener import calc_rsi, fetch_ticker_ohlcv, _jquants_id_token

    token = _jquants_id_token()
    # 銘柄ごとに過去30営業日分（暦日換算 約45日）取得
    end_str   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    start_str = (today - timedelta(days=45)).strftime("%Y-%m-%d")
    historical_data: dict = {}
    for ticker in {p["ticker"] for p in open_positions}:
        code4 = ticker.replace(".T", "")
        df = fetch_ticker_ohlcv(token, code4, start_str, end_str)
        if df is not None and not df.empty:
            historical_data[ticker] = df

    sell_targets = []

    for pos in open_positions:
        ticker = pos["ticker"]
        name = pos["name"]
        today_hold = calc_today_hold_day(pos, today)
        entry_open = pos.get("entry_open") or pos.get("prev_close")

        print(f"[close_check] {ticker} {name} - day {today_hold}")

        # MAXHOLD: 強制処分（RSI判定スキップ）
        if today_hold >= MAX_HOLD:
            sell_targets.append({
                "ticker":        ticker,
                "name":          name,
                "reason_type":   "MAXHOLD",
                "reason":        f"保有{today_hold}日目・強制大引け処分",
                "today_hold":    today_hold,
                "rsi_now":       None,
                "current_price": None,
                "entry_open":    entry_open,
            })
            continue

        # RSI判定（hold_day < MAX_HOLD）
        try:
            yf_obj = yf.Ticker(ticker)
            intraday = yf_obj.history(period="1d", interval="5m")
            if intraday.empty:
                print(f"  [yfinance] {ticker} intraday 空 → スキップ")
                continue
            current_price = float(intraday["Close"].iloc[-1])
        except Exception as e:
            print(f"  [yfinance] {ticker} 失敗: {e}")
            continue

        if ticker not in historical_data:
            print(f"  [J-Quants] {ticker} データなし")
            continue
        df = historical_data[ticker]
        closes = df["Close"].dropna().tolist()
        closes.append(current_price)  # 当日tentative close
        rsi_now = calc_rsi(pd.Series(closes))

        if rsi_now is None:
            print(f"  [RSI] {ticker} 計算失敗")
            continue

        print(f"  RSI={rsi_now:.1f} / current_price={current_price:.0f}")

        if rsi_now >= RSI_EXIT_THRESHOLD:
            sell_targets.append({
                "ticker":        ticker,
                "name":          name,
                "reason_type":   "RSI",
                "reason":        f"RSI回復（RSI={rsi_now:.1f} ≥ 50）",
                "today_hold":    today_hold,
                "rsi_now":       rsi_now,
                "current_price": current_price,
                "entry_open":    entry_open,
            })

    if not sell_targets:
        print("[close_check] 大引け処分対象なし")
        return

    print(f"[close_check] 大引け処分対象 {len(sell_targets)} 件")

    from notifier import send_close_signals
    send_close_signals(sell_targets, today)


if __name__ == "__main__":
    main()
