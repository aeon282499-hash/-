"""
report.py — 夕方15:40 JST に朝のシグナル結果を Discord に送信する
=================================================================
  - 朝の main.py が保存した today_signals.json を読み込む
  - 各銘柄の当日 始値・終値 を J-Quants で取得して損益を計算
  - Discord に結果サマリーを送信する
"""

import json
import os
import sys
import time
import requests
from datetime import datetime, date
import zoneinfo

from dotenv import load_dotenv

load_dotenv()
JST = zoneinfo.ZoneInfo("Asia/Tokyo")

_JQUANTS_BASE = "https://api.jquants.com/v2"


def _jquants_get(path: str, params: dict | None = None) -> dict:
    api_key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise ValueError("JQUANTS_API_KEY が未設定です")
    resp = requests.get(
        f"{_JQUANTS_BASE}{path}",
        headers={"x-api-key": api_key},
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_today_ohlc(tickers: list[str]) -> dict[str, dict]:
    """J-Quantsで当日の始値・終値を取得する。"""
    if not tickers:
        return {}

    today_str = date.today().strftime("%Y-%m-%d")
    result = {}

    for ticker in tickers:
        code4 = ticker.replace(".T", "")
        code5 = code4.zfill(5)
        try:
            data = _jquants_get(
                "/equities/daily_quotes",
                {"code": code5, "from": today_str, "to": today_str},
            )
            quotes = data.get("daily_quotes", [])
            if not quotes:
                print(f"  [report] {ticker}: 本日データなし")
                continue
            q = quotes[0]
            o = q.get("AdjustmentOpen") or q.get("Open")
            c = q.get("AdjustmentClose") or q.get("Close")
            if o and c and float(o) > 0 and float(c) > 0:
                result[ticker] = {"open": float(o), "close": float(c)}
            time.sleep(0.5)
        except Exception as e:
            print(f"  [report] {ticker} 取得失敗: {e}")

    return result


def calc_pnl(direction: str, open_price: float, close_price: float) -> float:
    if direction == "BUY":
        return (close_price - open_price) / open_price * 100
    else:
        return (open_price - close_price) / open_price * 100


def send_report(results: list[dict], signal_date: str) -> None:
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("[report] DISCORD_WEBHOOK_URL が未設定です")
        return

    date_str = datetime.strptime(signal_date, "%Y-%m-%d").strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    wins  = [r for r in results if r["pnl"] > 0]
    total = len(results)
    win_rate = len(wins) / total * 100 if total > 0 else 0
    avg_pnl  = sum(r["pnl"] for r in results) / total if total > 0 else 0

    lines = []
    for r in results:
        mark      = "✅" if r["pnl"] > 0 else "❌"
        dir_label = "🔴BUY" if r["direction"] == "BUY" else "🔵SELL"
        lines.append(
            f"{mark} **{r['name']}**（{r['ticker']}）{dir_label}\n"
            f"　始値 {r['open']:,.0f}円 → 終値 {r['close']:,.0f}円　**{r['pnl']:+.2f}%**"
        )

    body = "\n".join(lines) if lines else "（データ取得できませんでした）"

    if total > 0:
        summary = (
            f"**{total}銘柄** エントリー｜"
            f"勝率 {len(wins)}/{total}（{win_rate:.0f}%）｜"
            f"平均損益 **{avg_pnl:+.2f}%**"
        )
        color = 0x43A047 if avg_pnl >= 0 else 0xE53935
    else:
        summary = "本日シグナルなし（ノートレード）"
        color   = 0x757575

    payload = {
        "content": f"## 📊 本日の売買結果｜{date_str}",
        "embeds": [
            {
                "description": f"{summary}\n\n{body}",
                "color": color,
                "footer": {"text": f"集計時刻: {time_str}（大引け後）"},
            }
        ],
    }

    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"[report] Discord 送信失敗: HTTP {resp.status_code}")
    else:
        print(f"[report] 結果レポートを Discord に送信しました（{total}件）")


def main() -> None:
    today_str = date.today().strftime("%Y-%m-%d")

    if not os.path.exists("today_signals.json"):
        print("[report] today_signals.json が見つかりません → スキップ")
        sys.exit(0)

    with open("today_signals.json", encoding="utf-8") as f:
        data = json.load(f)

    signal_date = data.get("date", "")
    signals     = data.get("signals", [])

    if signal_date != today_str:
        print(f"[report] シグナル日付 {signal_date} ≠ 今日 {today_str} → スキップ")
        sys.exit(0)

    if not signals:
        print("[report] 本日シグナルなし → 通知スキップ")
        sys.exit(0)

    tickers = [s["ticker"] for s in signals]
    print(f"[report] {len(tickers)} 銘柄の終値をJ-Quantsで取得中...")
    ohlc = fetch_today_ohlc(tickers)

    results = []
    for s in signals:
        t = s["ticker"]
        if t not in ohlc:
            print(f"  [skip] {t}: データなし")
            continue
        o   = ohlc[t]["open"]
        c   = ohlc[t]["close"]
        pnl = calc_pnl(s["direction"], o, c)
        results.append({
            "ticker":    t,
            "name":      s["name"],
            "direction": s["direction"],
            "open":      o,
            "close":     c,
            "pnl":       round(pnl, 2),
        })

    send_report(results, signal_date)


if __name__ == "__main__":
    main()
