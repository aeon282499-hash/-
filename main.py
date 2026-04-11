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

    from screener import run_screener
    from notifier import send_signals, send_results, send_error
    from tracker import load_positions, save_positions, update_positions, add_signals_to_positions

    try:
        # ── ① 前日ポジションの結果チェック ──────────────────
        positions = load_positions()
        active = [p for p in positions if p["status"] in ("pending", "open")]
        print(f"[main] オープンポジション: {len(active)}件")

        if active:
            positions, closed_today, still_open = update_positions(positions, today)
            print(f"[main] 決済: {len(closed_today)}件 / 保有中: {len(still_open)}件")
            send_results(closed_today, still_open, today)
        else:
            closed_today = []
            still_open   = []

        # ── ② 新規スクリーニング ─────────────────────────────
        signals, sell_signals, macro = run_screener()

        # ── ③ 新シグナルをポジションに追加 ───────────────────
        entry_date = next_trading_day(today)
        positions  = add_signals_to_positions(positions, signals, today, entry_date)
        save_positions(positions)

        # ── ④ Discord にシグナル送信 ─────────────────────────
        send_signals(signals, today, macro, entry_date)

        # ── ⑤ SELL シグナルを別チャンネルに送信 ─────────────
        from notifier import send_sell_signals
        send_sell_signals(sell_signals, today, entry_date)

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
