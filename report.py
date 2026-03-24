"""
report.py — 夕方15:40 JST に朝のシグナル結果を Discord に送信する
=================================================================
  - 朝の main.py が保存した today_signals.json を読み込む
  - 各銘柄の当日 始値・終値 を取得して損益を計算
  - Discord に結果サマリーを送信する
"""

import json
import os
import sys
from datetime import datetime, date
import zoneinfo

import yfinance as yf
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
JST = zoneinfo.ZoneInfo("Asia/Tokyo")


def fetch_today_ohlc(tickers: list[str]) -> dict[str, dict]:
    """当日の始値・終値を取得する。"""
    if not tickers:
        return {}
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context

    result = {}
    try:
        raw = yf.download(
            tickers,
            period="2d",
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
        )
        today_str = date.today().strftime("%Y-%m-%d")
        for ticker in tickers:
            try:
                df = raw[ticker].copy() if len(tickers) > 1 else raw.copy()
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                rows = df[df.index.strftime("%Y-%m-%d") == today_str]
                if rows.empty:
                    continue
                o = float(rows["Open"].iloc[0])
                c = float(rows["Close"].iloc[0])
                if o > 0 and c > 0:
                    result[ticker] = {"open": o, "close": c}
            except Exception:
                pass
    except Exception as e:
        print(f"[report] データ取得エラー: {e}")
    return result


def calc_pnl(direction: str, open_price: float, close_price: float) -> float:
    if direction == "BUY":
        return (close_price - open_price) / open_price * 100
    else:
        return (open_price - close_price) / open_price * 100


def send_report(results: list[dict], signal_date: str) -> None:
    import requests
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("[report] DISCORD_WEBHOOK_URL が未設定です")
        return

    date_str = datetime.strptime(signal_date, "%Y-%m-%d").strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")

    wins   = [r for r in results if r["pnl"] > 0]
    losses = [r for r in results if r["pnl"] <= 0]
    total  = len(results)
    win_rate = len(wins) / total * 100 if total > 0 else 0
    avg_pnl  = sum(r["pnl"] for r in results) / total if total > 0 else 0

    lines = []
    for r in results:
        mark = "✅" if r["pnl"] > 0 else "❌"
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
        color = 0x757575

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

    # today_signals.json を読み込む
    if not os.path.exists("today_signals.json"):
        print("[report] today_signals.json が見つかりません → スキップ")
        sys.exit(0)

    with open("today_signals.json", encoding="utf-8") as f:
        data = json.load(f)

    signal_date = data.get("date", "")
    signals     = data.get("signals", [])

    # 日付が今日でなければスキップ（古いファイルが残っている場合）
    if signal_date != today_str:
        print(f"[report] シグナル日付 {signal_date} ≠ 今日 {today_str} → スキップ")
        sys.exit(0)

    if not signals:
        print("[report] 本日シグナルなし → 通知スキップ")
        # 0件の場合も一応通知
        send_report([], signal_date)
        sys.exit(0)

    tickers = [s["ticker"] for s in signals]
    print(f"[report] {len(tickers)} 銘柄の終値を取得中...")
    ohlc = fetch_today_ohlc(tickers)

    results = []
    for s in signals:
        t = s["ticker"]
        if t not in ohlc:
            print(f"  [skip] {t}: データなし")
            continue
        o = ohlc[t]["open"]
        c = ohlc[t]["close"]
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
