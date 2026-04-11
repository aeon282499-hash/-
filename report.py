"""
report.py — 夕方15:40 JST に朝のシグナル結果を Discord に送信する
=================================================================
  - 朝の main.py が保存した today_signals.json を読み込む
  - 各銘柄の当日 始値・終値 を J-Quants で取得して損益を計算
  - 結果を trade_history.json に蓄積
  - 本日結果 + 月別累計 + 年間累計 を Discord に送信する
"""

import json
import os
import sys
import time
import requests
from collections import defaultdict
from datetime import datetime, date
import zoneinfo

from dotenv import load_dotenv

load_dotenv()
JST              = zoneinfo.ZoneInfo("Asia/Tokyo")
_JQUANTS_BASE    = "https://api.jquants.com/v2"
HISTORY_FILE     = "trade_history.json"


# ================================================================
# J-Quants
# ================================================================

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
        code5 = ticker.replace(".T", "").zfill(5)
        try:
            data   = _jquants_get("/equities/daily_quotes",
                                   {"code": code5, "from": today_str, "to": today_str})
            quotes = data.get("daily_quotes", [])
            if not quotes:
                print(f"  [report] {ticker}: 本日データなし")
                continue
            q = quotes[0]
            o = q.get("AdjustmentOpen")  or q.get("Open")
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


# ================================================================
# 履歴管理
# ================================================================

def load_history() -> list[dict]:
    if not os.path.exists(HISTORY_FILE):
        return []
    with open(HISTORY_FILE, encoding="utf-8") as f:
        return json.load(f).get("trades", [])


def save_history(trades: list[dict]) -> None:
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump({"trades": trades}, f, ensure_ascii=False, indent=2)


def append_today_results(results: list[dict], trade_date: str) -> list[dict]:
    """今日の結果を履歴に追加（同日の重複は上書き）。"""
    trades = load_history()
    # 同日分を一旦除去
    trades = [t for t in trades if t["date"] != trade_date]
    for r in results:
        trades.append({
            "date":      trade_date,
            "ticker":    r["ticker"],
            "name":      r["name"],
            "direction": r["direction"],
            "open":      r["open"],
            "close":     r["close"],
            "pnl":       r["pnl"],
            "win":       r["pnl"] > 0,
        })
    save_history(trades)
    return trades


# ================================================================
# 集計
# ================================================================

def calc_stats(trades: list[dict]) -> dict:
    if not trades:
        return {"count": 0, "wins": 0, "win_rate": 0, "avg_pnl": 0, "pf": 0}
    pnls  = [t["pnl"] for t in trades]
    wins  = sum(1 for p in pnls if p > 0)
    gain  = sum(p for p in pnls if p > 0)
    loss  = abs(sum(p for p in pnls if p < 0))
    return {
        "count":    len(trades),
        "wins":     wins,
        "win_rate": round(wins / len(trades) * 100, 1),
        "avg_pnl":  round(sum(pnls) / len(pnls), 2),
        "pf":       round(gain / loss, 2) if loss > 0 else 999,
    }


def build_monthly_summary(trades: list[dict]) -> list[str]:
    """年間の月別累計テキストを生成する。"""
    current_year = date.today().year
    year_trades  = [t for t in trades if t["date"].startswith(str(current_year))]

    monthly = defaultdict(list)
    for t in year_trades:
        month = t["date"][:7]  # "2026-04"
        monthly[month].append(t)

    lines = []
    for month in sorted(monthly.keys()):
        s    = calc_stats(monthly[month])
        sign = "+" if s["avg_pnl"] >= 0 else ""
        lines.append(
            f"`{month}` {s['count']}件 "
            f"勝率{s['win_rate']}% "
            f"平均{sign}{s['avg_pnl']}% "
            f"PF{s['pf']}"
        )
    return lines


# ================================================================
# Discord 送信
# ================================================================

def _post(url: str, payload: dict) -> None:
    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"[report] Discord 送信失敗: HTTP {resp.status_code}")
    time.sleep(0.5)


def send_report(results: list[dict], signal_date: str, all_trades: list[dict]) -> None:
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("[report] DISCORD_WEBHOOK_URL が未設定です")
        return

    date_str = datetime.strptime(signal_date, "%Y-%m-%d").strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    today_s  = calc_stats(results)

    # ── 本日結果 ────────────────────────────────────
    lines = []
    for r in results:
        mark      = "✅" if r["pnl"] > 0 else "❌"
        dir_label = "🔴BUY" if r["direction"] == "BUY" else "🔵SELL"
        lines.append(
            f"{mark} **{r['name']}**（{r['ticker']}）{dir_label}\n"
            f"　始値 {r['open']:,.0f}円 → 終値 {r['close']:,.0f}円　**{r['pnl']:+.2f}%**"
        )

    if today_s["count"] > 0:
        summary = (
            f"**{today_s['count']}銘柄** エントリー｜"
            f"勝率 {today_s['wins']}/{today_s['count']}（{today_s['win_rate']}%）｜"
            f"平均損益 **{today_s['avg_pnl']:+.2f}%**"
        )
        color = 0x43A047 if today_s["avg_pnl"] >= 0 else 0xE53935
    else:
        summary = "本日シグナルなし（ノートレード）"
        color   = 0x757575

    _post(url, {
        "content": f"## 📊 本日の売買結果｜{date_str}",
        "embeds": [{
            "description": f"{summary}\n\n" + "\n".join(lines),
            "color":  color,
            "footer": {"text": f"集計時刻: {time_str}（大引け後）"},
        }],
    })

    # ── 月別 + 年間累計 ─────────────────────────────
    current_year  = date.today().year
    year_trades   = [t for t in all_trades if t["date"].startswith(str(current_year))]
    year_s        = calc_stats(year_trades)
    monthly_lines = build_monthly_summary(all_trades)

    if not monthly_lines:
        return

    monthly_text = "\n".join(monthly_lines)
    year_sign    = "+" if year_s["avg_pnl"] >= 0 else ""
    year_text    = (
        f"**{current_year}年合計** {year_s['count']}件｜"
        f"勝率{year_s['win_rate']}%｜"
        f"平均{year_sign}{year_s['avg_pnl']}%｜"
        f"PF **{year_s['pf']}**"
    )

    _post(url, {
        "embeds": [{
            "title":       "📈 月別・年間累計",
            "description": f"{monthly_text}\n\n{year_text}",
            "color":       0x1565C0,
            "footer":      {"text": f"{current_year}年 実績（本番稼働分）"},
        }],
    })

    print(f"[report] レポート送信完了（本日{today_s['count']}件 / 年間{year_s['count']}件）")


# ================================================================
# メイン
# ================================================================

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
        print("[report] 本日シグナルなし → スキップ")
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

    # 履歴に保存して累計集計
    all_trades = append_today_results(results, signal_date)
    send_report(results, signal_date, all_trades)


if __name__ == "__main__":
    main()
