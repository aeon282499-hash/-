"""
main_1570.py — 1570 ETF デイトレシグナル配信
=============================================
毎営業日 8:25 JST に GitHub Actions から呼ばれる。

実行フロー:
  1. 今日が営業日か判定
  2. 前日シグナルの結果を確認 → Discord に送信
  3. 本日のシグナル（BUY / SELL / PASS）を判定
  4. Discord に送信
  5. signals_1570.json に保存
"""

import sys
import json
import os
from datetime import datetime, date, timedelta
import zoneinfo

import jpholiday
from dotenv import load_dotenv

load_dotenv()

JST              = zoneinfo.ZoneInfo("Asia/Tokyo")
SIGNALS_FILE     = "signals_1570.json"
STOP_LOSS        = 3.0
TAKE_PROFIT      = 5.0


def is_trading_day(d) -> bool:
    if d.weekday() >= 5:
        return False
    if jpholiday.is_holiday(d):
        return False
    return True


def load_signals() -> list[dict]:
    if os.path.exists(SIGNALS_FILE):
        with open(SIGNALS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_signals(data: list[dict]) -> None:
    with open(SIGNALS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_webhook_url() -> str:
    return (os.getenv("DISCORD_WEBHOOK_URL_1570") or os.getenv("DISCORD_WEBHOOK_URL", "")).strip()


def check_yesterday_result(yesterday_signal: dict, today: date) -> dict | None:
    """前日シグナルの損益を計算して返す。"""
    if not yesterday_signal or yesterday_signal.get("direction") == "PASS":
        return None

    from screener import batch_download_jquants, _jquants_id_token
    end_str   = today.strftime("%Y-%m-%d")
    start_str = (today - timedelta(days=10)).strftime("%Y-%m-%d")

    token    = _jquants_id_token()
    data     = batch_download_jquants(token, start=start_str, end=end_str, tickers=["1570.T"])
    df       = data.get("1570.T")
    if df is None or df.empty:
        return None

    entry_date = yesterday_signal.get("entry_date")
    if not entry_date:
        return None

    rows = df[df.index.strftime("%Y-%m-%d") == entry_date]
    if rows.empty:
        return None

    entry_open  = float(rows["Open"].iloc[0])
    entry_close = float(rows["Close"].iloc[0])
    entry_high  = float(rows["High"].iloc[0])
    entry_low   = float(rows["Low"].iloc[0])
    direction   = yesterday_signal["direction"]

    if direction == "BUY":
        stop_p = entry_open * (1 - STOP_LOSS   / 100)
        tp_p   = entry_open * (1 + TAKE_PROFIT / 100)
        if entry_low <= stop_p:
            pnl, etype = -STOP_LOSS, "STOP"
        elif entry_high >= tp_p:
            pnl, etype = +TAKE_PROFIT, "TP"
        else:
            pnl   = (entry_close - entry_open) / entry_open * 100
            etype = "CLOSE"
    else:
        stop_p = entry_open * (1 + STOP_LOSS   / 100)
        tp_p   = entry_open * (1 - TAKE_PROFIT / 100)
        if entry_high >= stop_p:
            pnl, etype = -STOP_LOSS, "STOP"
        elif entry_low <= tp_p:
            pnl, etype = +TAKE_PROFIT, "TP"
        else:
            pnl   = (entry_open - entry_close) / entry_open * 100
            etype = "CLOSE"

    return {**yesterday_signal, "pnl_pct": round(pnl, 3), "exit_type": etype, "win": pnl > 0}


def send_result(result: dict, today: date) -> None:
    """前日結果をDiscordに送信。"""
    import requests
    url = get_webhook_url()
    if not url:
        return

    pnl   = result["pnl_pct"]
    etype = result["exit_type"]
    emoji = "✅" if pnl > 0 else "❌"
    dir_str = "買い" if result["direction"] == "BUY" else "売り"
    date_str = today.strftime("%Y年%m月%d日")

    payload = {
        "embeds": [{
            "title":       f"📋【1570結果】{date_str}",
            "description": f"{emoji} **1570 ETF** {dir_str} → **{pnl:+.2f}%** [{etype}]",
            "color":       0x43A047 if pnl > 0 else 0xE53935,
        }]
    }
    requests.post(url, json=payload, timeout=10)
    print(f"[main_1570] 結果送信: {pnl:+.2f}% [{etype}]")


def send_signal(signal: dict, today: date) -> None:
    """本日シグナルをDiscordに送信。"""
    import requests
    url = get_webhook_url()
    if not url:
        return

    direction = signal["direction"]
    date_str  = today.strftime("%Y年%m月%d日")
    time_str  = datetime.now(JST).strftime("%H:%M JST")

    if direction == "PASS":
        payload = {
            "embeds": [{
                "title":       f"🏳️【1570 ETF】{date_str} — PASS",
                "description": "本日は条件を満たしません。ノーポジ推奨。",
                "color":       0x757575,
                "footer":      {"text": f"配信時刻: {time_str}"},
            }]
        }
    else:
        prev_close = signal.get("prev_close", 0)
        if direction == "BUY":
            action_str = "🔴 **寄り成り 買い**（9:00エントリー → 15:30大引け決済）"
            color      = 0xE53935
            stop_str   = f"**{prev_close * 0.97:,.0f}円**（始値-3%）"
            tp_str     = f"**{prev_close * 1.05:,.0f}円**（始値+5%）"
        else:
            action_str = "🔵 **寄り成り 売り（空売り）**（9:00エントリー → 15:30大引け決済）"
            color      = 0x1E88E5
            stop_str   = f"**{prev_close * 1.03:,.0f}円**（始値+3%）"
            tp_str     = f"**{prev_close * 0.95:,.0f}円**（始値-5%）"

        reason_text = "\n".join(f"・{r}" for r in signal["reason"])

        payload = {
            "embeds": [{
                "title": f"⚡【1570 ETF】{date_str} — {direction}",
                "color": color,
                "fields": [
                    {"name": "📌 アクション",     "value": action_str,  "inline": False},
                    {"name": "🛑 損切り（目安）", "value": stop_str,    "inline": True},
                    {"name": "✅ 利確（目安）",   "value": tp_str,      "inline": True},
                    {"name": "⚠️ 必ず当日決済",  "value": "**15:30大引けで必ず決済**（翌日持ち越し禁止）", "inline": False},
                    {"name": "📊 シグナル根拠",   "value": reason_text, "inline": False},
                ],
                "footer": {"text": f"配信時刻: {time_str}"},
            }]
        }

    requests.post(url, json=payload, timeout=10)
    print(f"[main_1570] シグナル送信: {direction}")


def main() -> None:
    today = datetime.now(JST).date()
    print(f"[main_1570] 実行日: {today}")

    if not is_trading_day(today):
        print("[main_1570] 休場日 → スキップ")
        sys.exit(0)

    from screener_1570 import run_screener_1570

    try:
        # ── ① 前日シグナルの結果チェック ──────────────────
        all_saved = load_signals()
        yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_signal = next((s for s in all_saved if s.get("signal_date") == yesterday), None)

        if prev_signal and prev_signal.get("direction") != "PASS":
            result = check_yesterday_result(prev_signal, today)
            if result:
                send_result(result, today)

        # ── ② 本日のシグナル判定 ──────────────────────────
        signal = run_screener_1570()

        # ── ③ 保存 ────────────────────────────────────────
        entry_date = today + timedelta(days=1)
        while not is_trading_day(entry_date):
            entry_date += timedelta(days=1)

        record = {
            "signal_date": today.strftime("%Y-%m-%d"),
            "entry_date":  entry_date.strftime("%Y-%m-%d"),
            "direction":   signal["direction"],
            "rsi":         signal.get("rsi"),
            "sp500_ret":   signal.get("sp500_ret"),
            "prev_close":  signal.get("prev_close"),
        }
        cutoff = (today - timedelta(days=60)).strftime("%Y-%m-%d")
        kept   = [s for s in all_saved if s.get("signal_date", "") >= cutoff]
        save_signals(kept + [record])

        # ── ④ Discord 送信 ────────────────────────────────
        send_signal(signal, today)
        print("[main_1570] 正常終了")

    except Exception as e:
        import traceback, requests as req
        err_msg = traceback.format_exc()
        print(f"[main_1570] エラー:\n{err_msg}", file=sys.stderr)
        url = get_webhook_url()
        if url:
            req.post(url, json={"content": f"[1570] エラー発生:\n```{err_msg[:1500]}```"}, timeout=10)
        sys.exit(1)


if __name__ == "__main__":
    main()
