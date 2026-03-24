"""
main.py — エントリーポイント
==============================
毎営業日 8:30 JST に GitHub Actions から呼ばれる。

実行フロー:
  1. 今日が営業日か判定（土日・祝日はスキップ）
  2. screener.run_screener() で銘柄選定
  3. notifier.send_signals() で Discord 通知
  4. 例外発生時は notifier.send_error() でエラー通知
"""

import sys
import json
from datetime import datetime
import zoneinfo

import jpholiday
from dotenv import load_dotenv

# .env ファイルから環境変数を読み込む（GitHub Actions では Secrets が自動的に環境変数になる）
load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")


def is_trading_day(d) -> bool:
    """
    日本の株式市場の営業日かどうかを判定する。

    Parameters
    ----------
    d : datetime.date

    Returns
    -------
    bool : True = 営業日、False = 休場日（土日 or 祝日）
    """
    # 土曜(5) or 日曜(6)
    if d.weekday() >= 5:
        return False
    # 日本の祝日
    if jpholiday.is_holiday(d):
        return False
    return True


def main() -> None:
    today = datetime.now(JST).date()
    print(f"[main] 実行日: {today}")

    # ── 休場日チェック ───────────────────────────────
    if not is_trading_day(today):
        reason = (
            "土日のため休場" if today.weekday() >= 5
            else f"{jpholiday.is_holiday_name(today)} のため休場"
        )
        print(f"[main] {reason} → スキップします")
        # 休場日でも Discord に通知したい場合は下記のコメントを外す
        # from notifier import send_skip
        # send_skip(reason, today)
        sys.exit(0)

    # ── スクリーニング実行 ────────────────────────────
    from screener import run_screener
    from notifier import send_signals, send_error

    try:
        signals, macro = run_screener()

        # 夕方の結果レポート用にシグナルを保存
        payload = {
            "date": today.strftime("%Y-%m-%d"),
            "signals": [
                {k: v for k, v in s.items() if k != "reason"}
                for s in signals
            ],
        }
        with open("today_signals.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[main] シグナルを today_signals.json に保存しました（{len(signals)}件）")

        send_signals(signals, today, macro)
        print("[main] 正常終了")

    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        print(f"[main] エラー発生:\n{err_msg}", file=sys.stderr)
        try:
            send_error(err_msg, today)
        except Exception:
            pass   # 通知自体が失敗しても握りつぶす
        sys.exit(1)


if __name__ == "__main__":
    main()
