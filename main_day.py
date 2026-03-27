"""
main_day.py — デイトレシグナル配信エントリーポイント
======================================================
毎営業日 8:05 JST に GitHub Actions から呼ばれる。

実行フロー:
  1. 今日が営業日か判定
  2. 前日のデイトレシグナル結果を確認 → Discord に送信
  3. screener_day.run_screener_day() で新規シグナル選定
  4. Discord にシグナル送信
  5. シグナルを day_signals.json に保存
"""

import sys
import json
import os
from datetime import datetime, date, timedelta
import zoneinfo

import jpholiday
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")
DAY_SIGNALS_FILE = "day_signals.json"


def is_trading_day(d) -> bool:
    if d.weekday() >= 5:
        return False
    if jpholiday.is_holiday(d):
        return False
    return True


def load_day_signals() -> list[dict]:
    if os.path.exists(DAY_SIGNALS_FILE):
        with open(DAY_SIGNALS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_day_signals(signals_data: list[dict]) -> None:
    with open(DAY_SIGNALS_FILE, "w", encoding="utf-8") as f:
        json.dump(signals_data, f, ensure_ascii=False, indent=2)


def check_yesterday_results(yesterday_signals: list[dict], today: date) -> list[dict]:
    """前日のシグナルの実際の損益を計算して返す。"""
    if not yesterday_signals:
        return []

    from screener import batch_download
    tickers = [s["ticker"] for s in yesterday_signals]
    yesterday_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    all_data = batch_download(tickers, period="5d")

    results = []
    for sig in yesterday_signals:
        ticker     = sig["ticker"]
        entry_date = sig.get("entry_date")
        direction  = sig["direction"]
        df = all_data.get(ticker)
        if df is None or df.empty or not entry_date:
            continue

        entry_rows = df[df.index.strftime("%Y-%m-%d") == entry_date]
        if entry_rows.empty:
            continue

        entry_open  = float(entry_rows["Open"].iloc[0])
        entry_close = float(entry_rows["Close"].iloc[0])
        entry_high  = float(entry_rows["High"].iloc[0])
        entry_low   = float(entry_rows["Low"].iloc[0])

        STOP = 2.0
        TP   = 5.0

        if direction == "BUY":
            stop_p = entry_open * (1 - STOP / 100)
            tp_p   = entry_open * (1 + TP   / 100)
            if entry_low <= stop_p:
                pnl, etype = -STOP, "STOP"
            elif entry_high >= tp_p:
                pnl, etype = +TP, "TP"
            else:
                pnl   = (entry_close - entry_open) / entry_open * 100
                etype = "CLOSE"
        else:
            stop_p = entry_open * (1 + STOP / 100)
            tp_p   = entry_open * (1 - TP   / 100)
            if entry_high >= stop_p:
                pnl, etype = -STOP, "STOP"
            elif entry_low <= tp_p:
                pnl, etype = +TP, "TP"
            else:
                pnl   = (entry_open - entry_close) / entry_open * 100
                etype = "CLOSE"

        results.append({
            **sig,
            "pnl_pct":   round(pnl, 3),
            "exit_type": etype,
            "win":       pnl > 0,
        })

    return results


def send_day_results(results: list[dict], today: date) -> None:
    """デイトレ結果を Discord に送信する。"""
    if not results:
        return

    import os, requests
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        return

    date_str = today.strftime("%Y年%m月%d日")
    lines    = ["**── 前日デイトレ結果 ──**"]

    for r in results:
        pnl   = r["pnl_pct"]
        etype = r["exit_type"]
        emoji = "✅" if pnl > 0 else "❌"
        dir_str = "買い" if r["direction"] == "BUY" else "売り"
        lines.append(
            f"{emoji} **{r['name']}**（{r['ticker']}）{dir_str} "
            f"前日{r['prev_return']:+.1f}% → **{pnl:+.2f}%** [{etype}]"
        )

    wins    = sum(1 for r in results if r["win"])
    avg_pnl = sum(r["pnl_pct"] for r in results) / len(results)
    lines.append(f"\n合計: {len(results)}件 / 勝ち{wins}件 / 平均{avg_pnl:+.2f}%")

    payload = {
        "embeds": [{
            "title":       f"📋 {date_str} — デイトレ前日結果",
            "description": "\n".join(lines),
            "color":       0x43A047 if avg_pnl > 0 else 0xFDD835,
        }]
    }
    requests.post(url, json=payload, timeout=10)
    print(f"[main_day] デイトレ結果を Discord に送信しました（{len(results)}件）")


def send_day_signals(signals: list[dict], today: date, macro: dict) -> None:
    """デイトレシグナルを Discord に送信する。"""
    import os, requests
    from datetime import datetime as _dt
    import zoneinfo as _zi

    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        return

    date_str = today.strftime("%Y年%m月%d日")
    time_str = _dt.now(_zi.ZoneInfo("Asia/Tokyo")).strftime("%H:%M JST")

    if not signals:
        payload = {
            "embeds": [{
                "title":       f"📊 {date_str} — デイトレシグナル",
                "description": "本日は条件を満たす銘柄がありません。",
                "color":       0x757575,
            }]
        }
        requests.post(url, json=payload, timeout=10)
        print("[main_day] シグナルなし通知を送信しました")
        return

    buys  = sum(1 for s in signals if s["direction"] == "BUY")
    sells = len(signals) - buys

    embeds = []
    for i, sig in enumerate(signals, 1):
        direction  = sig["direction"]
        prev_close = sig.get("prev_close", 0)

        if direction == "BUY":
            action_str = "🔴 **寄り成り 買い**（9:00エントリー → 15:30大引け決済）"
            color      = 0xE53935
            stop_price = prev_close * 0.97
            tp_price   = prev_close * 1.05
            stop_str   = f"**{stop_price:,.0f}円**（始値-3%）"
            tp_str     = f"**{tp_price:,.0f}円**（始値+5%）"
        else:
            action_str = "🔵 **寄り成り 売り（空売り）**（9:00エントリー → 15:30大引け決済）"
            color      = 0x1E88E5
            stop_price = prev_close * 1.03
            tp_price   = prev_close * 0.95
            stop_str   = f"**{stop_price:,.0f}円**（始値+3%）"
            tp_str     = f"**{tp_price:,.0f}円**（始値-5%）"

        if prev_close > 0:
            shares     = max(100, int(1_000_000 / prev_close / 100) * 100)
            invest_amt = shares * prev_close
            invest_str = f"**{shares:,}株**（約{invest_amt/1e4:.0f}万円）※前日終値{prev_close:,.0f}円基準"
        else:
            invest_str = "**100万円目安**"

        reason_text = "\n".join(f"・{r}" for r in sig["reason"])

        embeds.append({
            "title": f"[デイトレ] #{i}  {sig['name']}（{sig['ticker']}）",
            "color": color,
            "fields": [
                {"name": "📌 アクション",        "value": action_str,   "inline": False},
                {"name": "💴 推奨株数・金額",    "value": invest_str,   "inline": False},
                {"name": "🛑 損切り（目安）",    "value": stop_str,     "inline": True},
                {"name": "✅ 利確（目安）",      "value": tp_str,       "inline": True},
                {"name": "⚠️ 必ず当日決済",     "value": "**15:30大引けで必ず決済**（翌日持ち越し禁止）", "inline": False},
                {"name": "📊 シグナル根拠",      "value": reason_text,  "inline": False},
            ],
            "footer": {"text": f"配信時刻: {time_str}"},
        })

    payload = {
        "content": (
            f"## 📈 デイトレシグナル｜{date_str}\n"
            f"> 本日: **{len(signals)}銘柄**（買い {buys} / 売り {sells}）"
        ),
        "embeds": embeds[:10],
    }
    requests.post(url, json=payload, timeout=10)
    print(f"[main_day] {len(signals)} 件のデイトレシグナルを Discord に送信しました")


def main() -> None:
    today = datetime.now(JST).date()
    print(f"[main_day] 実行日: {today}")

    if not is_trading_day(today):
        print("[main_day] 休場日 → スキップ")
        sys.exit(0)

    from screener_day import run_screener_day

    try:
        # ── ① 前日シグナルの結果チェック ──────────────────
        all_saved   = load_day_signals()
        yesterday   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_signals = [s for s in all_saved if s.get("signal_date") == yesterday]
        print(f"[main_day] 前日シグナル: {len(prev_signals)}件")

        if prev_signals:
            results = check_yesterday_results(prev_signals, today)
            send_day_results(results, today)

        # ── ② 新規スクリーニング ─────────────────────────
        signals, macro = run_screener_day()

        # ── ③ シグナルを保存 ──────────────────────────────
        from datetime import timedelta as _td
        entry_date = today + _td(days=1)
        # 翌営業日を計算
        while not is_trading_day(entry_date):
            entry_date += _td(days=1)

        new_records = [
            {
                "signal_date": today.strftime("%Y-%m-%d"),
                "entry_date":  entry_date.strftime("%Y-%m-%d"),
                **{k: v for k, v in s.items() if k != "reason"},
            }
            for s in signals
        ]
        # 直近30日分だけ保持
        cutoff = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        kept   = [s for s in all_saved if s.get("signal_date", "") >= cutoff]
        save_day_signals(kept + new_records)

        # ── ④ Discord にシグナル送信 ─────────────────────
        send_day_signals(signals, today, macro)
        print("[main_day] 正常終了")

    except Exception as e:
        import traceback, os, requests as req
        err_msg = traceback.format_exc()
        print(f"[main_day] エラー:\n{err_msg}", file=sys.stderr)
        url = os.getenv("DISCORD_WEBHOOK_URL", "")
        if url:
            req.post(url, json={"content": f"[デイトレ] エラー発生:\n```{err_msg[:1500]}```"}, timeout=10)
        sys.exit(1)


if __name__ == "__main__":
    main()
