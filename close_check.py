"""
close_check.py — 大引け前のRSI判定とDiscord通知
==================================================
毎営業日 15:00 JST に GitHub Actions から呼ばれる。

実行フロー:
  1. 営業日チェック・時間外スキップ
  2. positions.json + positions_sell.json をロード
  3. status=pending/open のポジションについて:
     - yfinance で当日 current price 取得（~14:45データ・15分遅延）
     - 過去終値（J-Quants）+ current price で RSI(14) 計算
     - 判定:
       * 当日 hold_day == MAX_HOLD: 強制MAXHOLD大引け処分
       * BUY  かつ RSI ≥ 50: RSI回復で大引け処分推奨
       * SELL かつ RSI ≤ 50: RSI回復で大引け買戻し推奨
  4. 該当銘柄があれば Discord 通知:
     - BUY  → DISCORD_WEBHOOK_URL (BUYチャンネル)
     - SELL → DISCORD_WEBHOOK_SELL_URL (SELLチャンネル)

ユーザーは通知を受けて15:25-15:30のクロージングオークションでSBI証券アプリから成行発注。
"""

import json
import os
from datetime import datetime, date, timedelta
import zoneinfo

import jpholiday
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")

POSITIONS_FILE      = "positions.json"
SELL_POSITIONS_FILE = "positions_sell.json"
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


def collect_targets(open_positions: list[dict], direction: str, today: date) -> list[dict]:
    """指定directionのオープンポジションから大引け処分対象を抽出する。

    direction: "BUY" or "SELL"
    """
    if not open_positions:
        return []

    import yfinance as yf
    from screener import calc_rsi, fetch_ticker_ohlcv, _jquants_id_token

    token = _jquants_id_token()
    end_str   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    start_str = (today - timedelta(days=45)).strftime("%Y-%m-%d")
    historical_data: dict = {}
    for ticker in {p["ticker"] for p in open_positions}:
        code4 = ticker.replace(".T", "")
        df = fetch_ticker_ohlcv(token, code4, start_str, end_str)
        if df is not None and not df.empty:
            historical_data[ticker] = df

    targets = []

    for pos in open_positions:
        ticker = pos["ticker"]
        name = pos["name"]
        today_hold = calc_today_hold_day(pos, today)
        entry_open = pos.get("entry_open") or pos.get("prev_close")

        print(f"[close_check] [{direction}] {ticker} {name} - day {today_hold}")

        # MAXHOLD: 強制処分（RSI判定スキップ）
        if today_hold >= MAX_HOLD:
            targets.append({
                "ticker":        ticker,
                "name":          name,
                "direction":     direction,
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
        closes.append(current_price)
        rsi_now = calc_rsi(pd.Series(closes))

        if rsi_now is None:
            print(f"  [RSI] {ticker} 計算失敗")
            continue

        print(f"  RSI={rsi_now:.1f} / current_price={current_price:.0f}")

        rsi_exit = (
            (direction == "BUY"  and rsi_now >= RSI_EXIT_THRESHOLD) or
            (direction == "SELL" and rsi_now <= RSI_EXIT_THRESHOLD)
        )
        if rsi_exit:
            cmp = "≥" if direction == "BUY" else "≤"
            targets.append({
                "ticker":        ticker,
                "name":          name,
                "direction":     direction,
                "reason_type":   "RSI",
                "reason":        f"RSI回復（RSI={rsi_now:.1f} {cmp} 50）",
                "today_hold":    today_hold,
                "rsi_now":       rsi_now,
                "current_price": current_price,
                "entry_open":    entry_open,
            })

    return targets


def main():
    now = datetime.now(JST)
    today = now.date()
    print(f"[close_check] 実行: {now.strftime('%Y-%m-%d %H:%M JST')}")

    if not is_trading_day(today):
        print("[close_check] 休場日のためスキップ")
        return

    # 14:00〜17:59 JST 以外は誤トリガーとしてスキップ
    # （GitHub Actions cron の遅延に備えて上限を17:59まで緩和。15:00定刻が理想だが
    #  遅延しても通知ゼロを避けるため。15:30以降は大引けに間に合わない可能性あり）
    if not (14 <= now.hour <= 17):
        print(f"[close_check] 時間外スキップ（実行時刻={now.strftime('%H:%M')}）")
        return

    # ── BUY 処理 ───────────────────────────────────────
    buy_open: list[dict] = []
    if os.path.exists(POSITIONS_FILE):
        with open(POSITIONS_FILE, encoding="utf-8") as f:
            buy_open = [p for p in json.load(f)
                        if p.get("status") in ("pending", "open")
                        and p.get("direction") == "BUY"]
    print(f"[close_check] BUY オープン {len(buy_open)} 件")
    buy_targets = collect_targets(buy_open, "BUY", today)

    # ── SELL 処理 ──────────────────────────────────────
    sell_open: list[dict] = []
    if os.path.exists(SELL_POSITIONS_FILE):
        with open(SELL_POSITIONS_FILE, encoding="utf-8") as f:
            sell_open = [p for p in json.load(f)
                         if p.get("status") in ("pending", "open")
                         and p.get("direction") == "SELL"]
    print(f"[close_check] SELL オープン {len(sell_open)} 件")
    sell_targets = collect_targets(sell_open, "SELL", today)

    # ── Discord 通知 ───────────────────────────────────
    if buy_targets:
        from notifier import send_close_signals
        send_close_signals(buy_targets, today)
        print(f"[close_check] BUY 大引け処分通知: {len(buy_targets)} 件")
    else:
        print("[close_check] BUY 大引け処分対象なし")

    if sell_targets:
        from notifier import send_close_signals_sell
        send_close_signals_sell(sell_targets, today)
        print(f"[close_check] SELL 大引け処分通知: {len(sell_targets)} 件")
    else:
        print("[close_check] SELL 大引け処分対象なし")


if __name__ == "__main__":
    main()
