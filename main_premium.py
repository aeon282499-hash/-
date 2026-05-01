"""
main_premium.py — 至高版エントリーポイント
==========================================
既存 main.py と完全独立。GitHub Actions などから呼び出す。

実行フロー:
  1. 営業日判定（土日・祝日はスキップ）
  2. 至高オープンポジションの結果チェック → Discord(Premium) に送信
  3. screener_premium.run_screener_premium() で新規銘柄選定（最大2件）
  4. notifier_premium.send_signals_premium() で Discord(Premium) 通知
  5. 新シグナルを positions_premium.json に追加
  6. 例外発生時は notifier_premium.send_error_premium() で通知
"""

import sys
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


def main() -> None:
    now   = datetime.now(JST)
    today = now.date()
    print(f"[main_prm] 実行日時: {now.strftime('%Y-%m-%d %H:%M JST')}")

    # 朝7:00〜9:30 JST 以外は誤トリガーとしてスキップ
    if not (7 <= now.hour < 9 or (now.hour == 9 and now.minute <= 30)):
        print(f"[main_prm] 配信時間外（{now.strftime('%H:%M')} JST）→ スキップ")
        sys.exit(0)

    if not is_trading_day(today):
        reason = (
            "土日のため休場" if today.weekday() >= 5
            else f"{jpholiday.is_holiday_name(today)} のため休場"
        )
        print(f"[main_prm] {reason} → スキップ")
        sys.exit(0)

    # 重複送信防止: 当日すでに送信済みならスキップ
    import os, json as _json
    today_str = today.strftime("%Y-%m-%d")
    if os.path.exists("today_signals_premium.json"):
        with open("today_signals_premium.json", encoding="utf-8") as _f:
            _saved = _json.load(_f)
        if _saved.get("date") == today_str:
            print(f"[main_prm] 本日分({today_str})は送信済み → スキップ")
            sys.exit(0)

    from screener_premium import run_screener_premium
    from notifier_premium import (send_signals_premium, send_results_premium,
                                  send_error_premium)
    from tracker_premium import (load_positions_premium, save_positions_premium,
                                 update_positions_premium, add_signals_premium)

    try:
        # ── ① 至高ポジションの結果チェック ──────────────
        positions = load_positions_premium()
        active = [p for p in positions if p["status"] in ("pending", "open")]
        print(f"[main_prm] 至高オープンポジション: {len(active)}件")

        closed_today: list = []
        still_open:   list = []
        if active:
            positions, closed_today, still_open = update_positions_premium(positions, today)
            print(f"[main_prm] 至高決済: {len(closed_today)}件 / 保有中: {len(still_open)}件")
            send_results_premium(closed_today, still_open, today)

        # ── ② 新規スクリーニング ─────────────────────────
        signals, macro = run_screener_premium()

        # ── ③ 新シグナルをポジションに追加 ───────────────
        entry_date = today  # 当日寄り付きエントリー
        positions  = add_signals_premium(positions, signals, today, entry_date)
        save_positions_premium(positions)

        # ── ④ Discord(Premium) にシグナル送信 ────────────
        send_signals_premium(signals, today, macro, entry_date)

        # ── ⑤ 当日送信済みフラグを保存 ───────────────────
        with open("today_signals_premium.json", "w", encoding="utf-8") as f:
            _json.dump({
                "date":    today_str,
                "signals": [{"ticker": s["ticker"], "name": s["name"]} for s in signals],
            }, f, ensure_ascii=False, indent=2)
        print(f"[main_prm] today_signals_premium.json ({len(signals)}件) 保存")

        print("[main_prm] 正常終了")

    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        print(f"[main_prm] エラー発生:\n{err_msg}", file=sys.stderr)
        try:
            send_error_premium(e, today)
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
