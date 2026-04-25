"""
main.py — エントリーポイント
==============================
毎営業日 8:00 JST に GitHub Actions から呼ばれる。

実行フロー:
  1. 今日が営業日か判定（土日・祝日はスキップ）
  2. オープンポジションの前日結果を確認 → Discord に送信
  3. screener.run_screener() で新規銘柄選定
  4. notifier.send_signals() で Discord 通知
  5. 新シグナルを positions.json に追加して保存
  6. 例外発生時は notifier.send_error() でエラー通知
"""

import sys
import json
from datetime import datetime, timedelta
import zoneinfo

import jpholiday
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")


def is_trading_day(d) -> bool:
    if d.weekday() >= 5:
        return False
    if jpholiday.is_holiday(d):
        return False
    return True


def next_trading_day(d) -> object:
    """翌営業日を返す。"""
    nxt = d + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt


def main() -> None:
    now   = datetime.now(JST)
    today = now.date()
    print(f"[main] 実行日時: {now.strftime('%Y-%m-%d %H:%M JST')}")

    # 朝7:00〜9:30 JST 以外は誤トリガーとしてスキップ
    if not (7 <= now.hour < 9 or (now.hour == 9 and now.minute <= 30)):
        print(f"[main] 配信時間外（{now.strftime('%H:%M')} JST）→ スキップします")
        sys.exit(0)

    if not is_trading_day(today):
        reason = (
            "土日のため休場" if today.weekday() >= 5
            else f"{jpholiday.is_holiday_name(today)} のため休場"
        )
        print(f"[main] {reason} → スキップします")
        sys.exit(0)

    # 重複送信防止: 当日すでに送信済みならスキップ
    import os, json as _json
    today_str = today.strftime("%Y-%m-%d")
    if os.path.exists("today_signals.json"):
        with open("today_signals.json", encoding="utf-8") as _f:
            _saved = _json.load(_f)
        if _saved.get("date") == today_str:
            print(f"[main] 本日分({today_str})は送信済みです → スキップ")
            sys.exit(0)

    from screener import run_screener
    from notifier import (send_signals, send_results, send_error, send_monthly_report,
                          send_sell_signals, send_sell_results, send_sell_monthly_report)
    from tracker import (load_positions, save_positions, update_positions, add_signals_to_positions,
                         load_sell_positions, save_sell_positions)

    try:
        # ── ① BUY前日ポジションの結果チェック ──────────────────
        positions = load_positions()
        active = [p for p in positions if p["status"] in ("pending", "open")]
        print(f"[main] BUYオープンポジション: {len(active)}件")

        if active:
            positions, closed_today, still_open = update_positions(positions, today)
            print(f"[main] BUY決済: {len(closed_today)}件 / 保有中: {len(still_open)}件")
            send_results(closed_today, still_open, today)
            send_monthly_report(positions, today)
        else:
            closed_today = []
            still_open   = []

        # ── ① SELL前日ポジションの結果チェック ─────────────────
        sell_positions = load_sell_positions()
        sell_active = [p for p in sell_positions if p["status"] in ("pending", "open")]
        print(f"[main] SELLオープンポジション: {len(sell_active)}件")

        if sell_active:
            sell_positions, sell_closed_today, sell_still_open = update_positions(sell_positions, today)
            print(f"[main] SELL決済: {len(sell_closed_today)}件 / 保有中: {len(sell_still_open)}件")
            send_sell_results(sell_closed_today, sell_still_open, today)
            send_sell_monthly_report(sell_positions, today)
        else:
            sell_closed_today = []
            sell_still_open   = []

        # ── ② 新規スクリーニング ─────────────────────────────
        signals, sell_signals, macro = run_screener()

        # ── ③ 新シグナルをポジションに追加 ───────────────────
        entry_date     = today  # 当日寄り付きエントリー
        positions      = add_signals_to_positions(positions, signals, today, entry_date)
        sell_positions = add_signals_to_positions(sell_positions, sell_signals, today, entry_date)
        save_positions(positions)
        save_sell_positions(sell_positions)

        # ── ④ Discord にシグナル送信 ─────────────────────────
        send_signals(signals, today, macro, entry_date)

        # ── ⑤ SELL シグナルを別チャンネルに送信 ─────────────
        send_sell_signals(sell_signals, today, entry_date)

        # ── ⑥ 夕方レポート用にシグナルを保存 ─────────────────
        import json as _json
        today_str = today.strftime("%Y-%m-%d")
        with open("today_signals.json", "w", encoding="utf-8") as f:
            _json.dump({
                "date":    today_str,
                "signals": [{"ticker": s["ticker"], "name": s["name"], "direction": "BUY"} for s in signals],
            }, f, ensure_ascii=False, indent=2)
        with open("today_sell_signals.json", "w", encoding="utf-8") as f:
            _json.dump({
                "date":    today_str,
                "signals": [{"ticker": s["ticker"], "name": s["name"], "direction": "SELL"} for s in sell_signals],
            }, f, ensure_ascii=False, indent=2)
        print(f"[main] today_signals.json ({len(signals)}件) / today_sell_signals.json ({len(sell_signals)}件) 保存")

        # ── ⑤ Twitter に投稿 ────────────────────────────────
        from twitter_notifier import post_swing_signals, post_swing_results
        post_swing_signals(signals, today, macro)
        if closed_today:
            post_swing_results(closed_today, today)

        print("[main] 正常終了")

    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        print(f"[main] エラー発生:\n{err_msg}", file=sys.stderr)
        try:
            send_error(err_msg, today)
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
