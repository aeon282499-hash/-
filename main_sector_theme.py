"""
main_sector_theme.py — セクター×テーマ シグナル 日次手動エントリ

実行: python main_sector_theme.py [--dry-run]
  --dry-run: Discord 通知せず標準出力のみ

GitHub Actions 自動化は次セッションで schedule_sector_theme.yml を作る。
"""
import json
import sys
from datetime import date, datetime
from pathlib import Path

import jpholiday
from dotenv import load_dotenv

load_dotenv()

from screener_sector_theme import run_sector_theme_screener
from notifier_sector_theme import send_signals, send_error

TODAY_SIGNALS = Path("today_signals_sector_theme.json")


def is_today_trading_day() -> bool:
    today = date.today()
    return today.weekday() < 5 and not jpholiday.is_holiday(today)


def main():
    dry_run = "--dry-run" in sys.argv

    if not is_today_trading_day():
        print("[main_st] 本日は休場日 → スキップ")
        return

    today_str = date.today().strftime("%Y-%m-%d")

    # 重複防止: 既に当日実行済みならスキップ
    if TODAY_SIGNALS.exists():
        try:
            with open(TODAY_SIGNALS, "r", encoding="utf-8") as f:
                prev = json.load(f)
            if prev.get("date") == today_str and not dry_run:
                print(f"[main_st] 本日 ({today_str}) は既に配信済み → スキップ")
                return
        except Exception:
            pass

    try:
        signals, all_pass, macro, diag = run_sector_theme_screener()
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[main_st] スクリーナー例外: {e}\n{tb}")
        if not dry_run:
            send_error(f"スクリーナー失敗: {e}\n\n{tb[-1000:]}")
        return

    print(f"\n[main_st] 配信対象 {len(signals)} 件")
    for s in signals:
        flags = []
        if s.get("in_sector_top"): flags.append(f"sec[{s.get('sector','?')}]")
        if s.get("in_theme"): flags.append("theme")
        print(f"  - [{s['ticker']}] {s['name']} RSI={s['rsi']} dev={s['deviation']:+.1f}% "
              f"代金={s['turnover']/1e8:.0f}億 [{'+'.join(flags)}]")

    if dry_run:
        print("\n[main_st] --dry-run のため Discord 送信せず終了")
        return

    send_signals(signals, macro, diag)

    # 配信履歴を保存 (重複防止用)
    payload = {
        "date": today_str,
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "signals": [
            {
                "ticker": s["ticker"], "name": s["name"],
                "rsi": s["rsi"], "deviation": s["deviation"],
                "turnover_oku": s["turnover"] / 1e8,
                "in_sector_top": s.get("in_sector_top", False),
                "in_theme": s.get("in_theme", False),
                "sector": s.get("sector", ""),
            } for s in signals
        ],
        "diag": diag,
    }
    with open(TODAY_SIGNALS, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[main_st] {TODAY_SIGNALS} 保存完了")


if __name__ == "__main__":
    main()
