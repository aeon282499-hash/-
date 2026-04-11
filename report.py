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
HISTORY_FILE      = "trade_history.json"
SELL_HISTORY_FILE = "trade_history_sell.json"


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

def load_history(path: str = HISTORY_FILE) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f).get("trades", [])


def save_history(trades: list[dict], path: str = HISTORY_FILE) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"trades": trades}, f, ensure_ascii=False, indent=2)


def append_today_results(results: list[dict], trade_date: str,
                         history_path: str = HISTORY_FILE) -> list[dict]:
    """今日の結果を履歴に追加（同日の重複は上書き）。"""
    trades = load_history(history_path)
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
    save_history(trades, history_path)
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


CAPITAL     = 3_000_000   # 総資金（円）
PER_TRADE   = 1_000_000   # 1トレード投入額（円）
WEIGHT      = PER_TRADE / CAPITAL  # 資金比率 = 1/3


def monthly_return(trades: list[dict]) -> float:
    """月利（資金300万円・1トレード100万円基準）。"""
    return round(sum(t["pnl"] for t in trades) * WEIGHT, 2)


def build_monthly_summary(trades: list[dict]) -> list[str]:
    """当年のみ月別累計テキストを生成する。"""
    current_year = date.today().year
    year_trades  = [t for t in trades if t["date"].startswith(str(current_year))]

    monthly = defaultdict(list)
    for t in year_trades:
        monthly[t["date"][:7]].append(t)

    lines = []
    for month in sorted(monthly.keys()):
        s    = calc_stats(monthly[month])
        mr   = monthly_return(monthly[month])
        yen  = mr / 100 * CAPITAL
        sign = "+" if mr >= 0 else ""
        lines.append(
            f"`{month}` {s['count']}件 "
            f"勝率{s['win_rate']}% "
            f"平均{'+' if s['avg_pnl']>=0 else ''}{s['avg_pnl']}% "
            f"PF{s['pf']} "
            f"**月利{sign}{mr}%**（{sign}{yen/10000:.1f}万円）"
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

    # 年利 = 当年全トレードの合計pnl × 資金比率
    annual_return = round(sum(t["pnl"] for t in year_trades) * WEIGHT, 2)
    annual_yen    = annual_return / 100 * CAPITAL
    ar_sign       = "+" if annual_return >= 0 else ""
    yr_sign       = "+" if year_s["avg_pnl"] >= 0 else ""

    year_text = (
        f"**{current_year}年合計** {year_s['count']}件｜"
        f"勝率{year_s['win_rate']}%｜"
        f"平均{yr_sign}{year_s['avg_pnl']}%｜"
        f"PF **{year_s['pf']}**｜"
        f"**年利{ar_sign}{annual_return}%**（{ar_sign}{annual_yen/10000:.1f}万円）"
    )

    _post(url, {
        "embeds": [{
            "title":       f"📈 {current_year}年 月別・年間累計",
            "description": "\n".join(monthly_lines) + f"\n\n{year_text}",
            "color":       0x1565C0,
            "footer":      {"text": f"※資金300万円・1トレード100万円基準"},
        }],
    })

    print(f"[report] レポート送信完了（本日{today_s['count']}件 / 年間{year_s['count']}件）")


def send_sell_report(results: list[dict], signal_date: str, all_trades: list[dict]) -> None:
    """空売り結果をSELL専用Webhookに送信する。"""
    url = os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip()
    if not url:
        print("[report] DISCORD_WEBHOOK_SELL_URL が未設定 → SELLレポートスキップ")
        return

    date_str = datetime.strptime(signal_date, "%Y-%m-%d").strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    today_s  = calc_stats(results)

    lines = []
    for r in results:
        mark = "✅" if r["pnl"] > 0 else "❌"
        lines.append(
            f"{mark} **{r['name']}**（{r['ticker']}）空売り\n"
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
        summary = "本日空売りシグナルなし（ノートレード）"
        color   = 0x757575

    _post(url, {
        "content": f"## 📉 本日の空売り結果｜{date_str}",
        "embeds": [{
            "description": f"{summary}\n\n" + "\n".join(lines),
            "color":  color,
            "footer": {"text": f"集計時刻: {time_str}（大引け後）"},
        }],
    })

    # 月別・年間累計
    current_year  = date.today().year
    year_trades   = [t for t in all_trades if t["date"].startswith(str(current_year))]
    year_s        = calc_stats(year_trades)
    monthly_lines = build_monthly_summary(all_trades)

    if not monthly_lines:
        return

    annual_return = round(sum(t["pnl"] for t in year_trades) * WEIGHT, 2)
    annual_yen    = annual_return / 100 * CAPITAL
    ar_sign       = "+" if annual_return >= 0 else ""
    yr_sign       = "+" if year_s["avg_pnl"] >= 0 else ""

    year_text = (
        f"**{current_year}年合計** {year_s['count']}件｜"
        f"勝率{year_s['win_rate']}%｜"
        f"平均{yr_sign}{year_s['avg_pnl']}%｜"
        f"PF **{year_s['pf']}**｜"
        f"**年利{ar_sign}{annual_return}%**（{ar_sign}{annual_yen/10000:.1f}万円）"
    )

    _post(url, {
        "embeds": [{
            "title":       f"📉 {current_year}年 月別・年間累計（空売り）",
            "description": "\n".join(monthly_lines) + f"\n\n{year_text}",
            "color":       0x1E88E5,
            "footer":      {"text": "※資金300万円・1トレード100万円基準"},
        }],
    })

    print(f"[report] SELL レポート送信完了（本日{today_s['count']}件 / 年間{year_s['count']}件）")


# ================================================================
# メイン
# ================================================================

def _load_signals_file(filepath: str, today_str: str) -> list[dict] | None:
    """シグナルファイルを読み込んで検証する。Noneは処理スキップ。"""
    if not os.path.exists(filepath):
        print(f"[report] {filepath} が見つかりません → スキップ")
        return None
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)
    signal_date = data.get("date", "")
    if signal_date != today_str:
        print(f"[report] {filepath} 日付 {signal_date} ≠ 今日 {today_str} → スキップ")
        return None
    return data.get("signals", [])


def _process_signals(signals: list[dict], ohlc: dict, signal_date: str,
                     history_path: str) -> tuple[list[dict], list[dict]]:
    """シグナルリストから損益を計算して履歴に保存。"""
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
    all_trades = append_today_results(results, signal_date, history_path)
    return results, all_trades


def main() -> None:
    today_str = date.today().strftime("%Y-%m-%d")

    # BUY シグナル処理
    buy_signals = _load_signals_file("today_signals.json", today_str)
    if buy_signals is not None:
        all_tickers = [s["ticker"] for s in buy_signals]
        print(f"[report] BUY {len(all_tickers)} 銘柄の終値をJ-Quantsで取得中...")
        ohlc = fetch_today_ohlc(all_tickers)
        results, all_trades = _process_signals(buy_signals, ohlc, today_str, HISTORY_FILE)
        send_report(results, today_str, all_trades)
    else:
        ohlc = None

    # SELL シグナル処理
    sell_signals = _load_signals_file("today_sell_signals.json", today_str)
    if sell_signals is not None:
        # OHLCデータの取得（BUYで未取得の銘柄だけ追加取得）
        sell_tickers = [s["ticker"] for s in sell_signals]
        missing = [t for t in sell_tickers if ohlc is None or t not in ohlc]
        if missing:
            print(f"[report] SELL {len(missing)} 銘柄の終値をJ-Quantsで取得中...")
            sell_ohlc = fetch_today_ohlc(missing)
            combined_ohlc = {**(ohlc or {}), **sell_ohlc}
        else:
            combined_ohlc = ohlc or {}
        results_sell, all_sell_trades = _process_signals(
            sell_signals, combined_ohlc, today_str, SELL_HISTORY_FILE)
        send_sell_report(results_sell, today_str, all_sell_trades)


if __name__ == "__main__":
    main()
