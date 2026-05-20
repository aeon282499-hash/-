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
from datetime import datetime, date
import zoneinfo

from dotenv import load_dotenv

load_dotenv()
JST              = zoneinfo.ZoneInfo("Asia/Tokyo")
HISTORY_FILE      = "trade_history.json"
SELL_HISTORY_FILE = "trade_history_sell.json"

# 階層定義（main.py / close_check.py の TIERS と整合）
TIERS = [
    {
        "key":              "main",
        "label":            "大資金",
        "emoji":            "",
        "size":             1_000_000,
        "buy_sig_file":     "today_signals.json",
        "sell_sig_file":    "today_sell_signals.json",
        "buy_history_file": "trade_history.json",
        "sell_history_file": "trade_history_sell.json",
        "buy_webhook":      os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
        "sell_webhook":     os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip(),
    },
    {
        "key":              "mid",
        "label":            "中資金",
        "emoji":            "🔵",
        "size":             500_000,
        "buy_sig_file":     "today_signals_mid.json",
        "sell_sig_file":    "today_sell_signals_mid.json",
        "buy_history_file": "trade_history_mid.json",
        "sell_history_file": "trade_history_sell_mid.json",
        "buy_webhook":      os.getenv("DISCORD_WEBHOOK_BUY_MID_URL", "").strip(),
        "sell_webhook":     os.getenv("DISCORD_WEBHOOK_SELL_MID_URL", "").strip(),
    },
    {
        "key":              "small",
        "label":            "小資金",
        "emoji":            "🟢",
        "size":             300_000,
        "buy_sig_file":     "today_signals_small.json",
        "sell_sig_file":    "today_sell_signals_small.json",
        "buy_history_file": "trade_history_small.json",
        "sell_history_file": "trade_history_sell_small.json",
        "buy_webhook":      os.getenv("DISCORD_WEBHOOK_BUY_SMALL_URL", "").strip(),
        "sell_webhook":     os.getenv("DISCORD_WEBHOOK_SELL_SMALL_URL", "").strip(),
    },
]


# ================================================================
# J-Quants（screener.batch_download_jquants と同じ日付ベース全銘柄取得方式）
# Light プランでは個別銘柄取得 (?code=) が制限されるため、close_check.py と
# 同じく当日全銘柄を一括取得→ticker フィルタで処理する。
# ================================================================

def fetch_today_ohlc(tickers: list[str]) -> dict[str, dict]:
    """当日の {ticker: {"open": float, "close": float}} を返す。"""
    if not tickers:
        return {}
    from screener import batch_download_jquants, _jquants_id_token

    today_str = date.today().strftime("%Y-%m-%d")
    print(f"[report] J-Quantsで {today_str} の全銘柄を取得中...")
    token    = _jquants_id_token()
    all_data = batch_download_jquants(token, start=today_str, end=today_str)

    result: dict[str, dict] = {}
    for ticker in tickers:
        df = all_data.get(ticker)
        if df is None or df.empty:
            print(f"  [report] {ticker}: 本日データなし")
            continue
        try:
            o = float(df["Open"].iloc[-1])
            c = float(df["Close"].iloc[-1])
            if o > 0 and c > 0:
                result[ticker] = {"open": o, "close": c}
        except Exception as e:
            print(f"  [report] {ticker} パース失敗: {e}")
    print(f"[report] {len(result)}/{len(tickers)} 銘柄のOHLC取得完了")
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


# ================================================================
# Discord 送信
# ================================================================

def _post(url: str, payload: dict) -> None:
    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"[report] Discord 送信失敗: HTTP {resp.status_code}")
    time.sleep(0.5)


def _tier_prefix(tier: dict) -> str:
    if tier["key"] == "main":
        return ""
    return f"{tier['emoji']}【{tier['label']}】"


def send_report(results: list[dict], signal_date: str, all_trades: list[dict],
                tier: dict | None = None) -> None:
    """夕方の1日目スナップショット結果をDiscord送信。"""
    if tier is None:
        tier = TIERS[0]
    url = tier["buy_webhook"]
    if not url:
        print(f"[report-{tier['label']}] buy_webhook未設定 → スキップ")
        return

    date_str = datetime.strptime(signal_date, "%Y-%m-%d").strftime("%Y年%m月%d日")
    time_str = datetime.now(JST).strftime("%H:%M JST")
    today_s  = calc_stats(results)

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
            f"平均 **{today_s['avg_pnl']:+.2f}%**"
        )
        color = 0x43A047 if today_s["avg_pnl"] >= 0 else 0xE53935
    else:
        summary = "本日シグナルなし（ノートレード）"
        color   = 0x757575

    _post(url, {
        "content": f"## {_tier_prefix(tier)}📊 本日のシグナル日中結果｜{date_str}",
        "embeds": [{
            "description": (
                f"※1日目寄付→終値スナップショット（参考値・実現損益とは別）\n\n"
                f"{summary}\n\n" + "\n".join(lines)
            ),
            "color":  color,
            "footer": {"text": f"集計時刻: {time_str}（大引け後）/ 1件{tier['size']//10000}万円枠"},
        }],
    })
    print(f"[report-{tier['label']}] BUY {today_s['count']}件 送信")


def send_sell_report(results: list[dict], signal_date: str, all_trades: list[dict],
                     tier: dict | None = None) -> None:
    """空売り1日目スナップショット結果。"""
    if tier is None:
        tier = TIERS[0]
    url = tier["sell_webhook"]
    if not url:
        print(f"[report-{tier['label']}] sell_webhook未設定 → SELLスキップ")
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
            f"平均 **{today_s['avg_pnl']:+.2f}%**"
        )
        color = 0x43A047 if today_s["avg_pnl"] >= 0 else 0xE53935
    else:
        summary = "本日空売りシグナルなし（ノートレード）"
        color   = 0x757575

    _post(url, {
        "content": f"## {_tier_prefix(tier)}📉 本日の空売り日中結果｜{date_str}",
        "embeds": [{
            "description": (
                f"※1日目寄付→終値スナップショット（参考値・実現損益とは別）\n\n"
                f"{summary}\n\n" + "\n".join(lines)
            ),
            "color":  color,
            "footer": {"text": f"集計時刻: {time_str}（大引け後）/ 1件{tier['size']//10000}万円枠"},
        }],
    })
    print(f"[report-{tier['label']}] SELL {today_s['count']}件 送信")


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

    # 全階層のsignalsをまずロードしてticker集合を作る
    tier_signals = {}
    union_tickers = set()
    for tier in TIERS:
        if not tier["buy_webhook"] and tier["key"] != "main":
            continue
        buy_sigs  = _load_signals_file(tier["buy_sig_file"],  today_str)
        sell_sigs = _load_signals_file(tier["sell_sig_file"], today_str)
        tier_signals[tier["key"]] = (buy_sigs, sell_sigs)
        for sigs in (buy_sigs or [], sell_sigs or []):
            union_tickers.update(s["ticker"] for s in sigs)

    if not union_tickers:
        print("[report] 本日シグナル0件 → 取得スキップ")
        ohlc = {}
    else:
        ohlc = fetch_today_ohlc(sorted(union_tickers))

    for tier in TIERS:
        key = tier["key"]
        if key not in tier_signals:
            continue
        buy_sigs, sell_sigs = tier_signals[key]
        print(f"\n[report-{tier['label']}] 処理開始")

        # REPORT_PAUSED: 2026-05-21 ユーザー指示で夕方スナップショットDiscord配信を停止。
        # trade_history.json への蓄積は継続（_process_signals 内部で更新）。再開時は send_* のコメント解除。
        if buy_sigs is not None:
            results, all_trades = _process_signals(
                buy_sigs, ohlc, today_str, tier["buy_history_file"])
            # send_report(results, today_str, all_trades, tier=tier)

        if sell_sigs is not None:
            results_sell, all_sell_trades = _process_signals(
                sell_sigs, ohlc, today_str, tier["sell_history_file"])
            # send_sell_report(results_sell, today_str, all_sell_trades, tier=tier)
        print(f"[report-{tier['label']}] 夕方Discord配信は一時停止中（REPORT_PAUSED）・trade_history更新は継続")


if __name__ == "__main__":
    main()
