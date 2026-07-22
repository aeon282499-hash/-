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
DIAG_LOG = Path("logs/sector_theme_diag.jsonl")


def _today_jst() -> date:
    # GitHubランナーはUTC。朝8時台のJSTはUTC前日なので date.today() は1日古い
    # 日付を返す（2026-06-10 スイングで修正したのと同じ罠）。再開時の事故防止。
    from datetime import datetime, timedelta, timezone
    return datetime.now(timezone(timedelta(hours=9))).date()


def is_today_trading_day() -> bool:
    today = _today_jst()
    return today.weekday() < 5 and not jpholiday.is_holiday(today)


def main():
    dry_run = "--dry-run" in sys.argv

    if not is_today_trading_day():
        print("[main_st] 本日は休場日 → スキップ")
        return

    today_str = _today_jst().strftime("%Y-%m-%d")

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

    # ── 2026-06-11 ユーザー決定: Discordはスイング寄指+15時処分に絞り、セクターローテは
    #    アプリ集約(DISCORD_NOTIFY=False)としていた。
    # ── 2026-07-04 方針変更: セクターローテ(BUY PF1.31/+459% / SELL PF1.37・検証済み)を
    #    専用の別Discordへ配信ONに戻す。webhookは Secret DISCORD_WEBHOOK_URL_SECTOR_THEME
    #    (新チャンネル用に更新すること)。JSONコミット/アプリ表示・SELL1日1件上限は従来どおり維持。
    SELL_ONLY = False
    MAX_SELL_PER_DAY = 1
    DISCORD_NOTIFY = False  # 2026-07-22 ユーザー指示で配信停止（毎日0件通知が不要）。判定/JSON保存は継続=Trueで再開可

    try:
        signals, sell_signals, all_pass, macro, diag = run_sector_theme_screener()
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[main_st] スクリーナー例外: {e}\n{tb}")
        if not dry_run:
            send_error(f"スクリーナー失敗: {e}\n\n{tb[-1000:]}")
        return

    if SELL_ONLY:
        if signals:
            print(f"[main_st] SELL専用モード: BUY {len(signals)}件は配信しない")
        signals = []
    sell_signals = sell_signals[:MAX_SELL_PER_DAY]

    print(f"\n[main_st] BUY配信対象 {len(signals)} 件")
    for s in signals:
        flags = []
        if s.get("in_sector_top"): flags.append(f"sec[{s.get('sector','?')}]")
        if s.get("in_theme"): flags.append("theme")
        print(f"  - [{s['ticker']}] {s['name']} RSI={s['rsi']} dev={s['deviation']:+.1f}% "
              f"代金={s['turnover']/1e8:.0f}億 [{'+'.join(flags)}]")
    print(f"[main_st] SELL配信対象 {len(sell_signals)} 件")
    for s in sell_signals:
        print(f"  - [{s['ticker']}] {s['name']} RSI={s['rsi']} dev={s['deviation']:+.1f}% "
              f"前日比={s.get('day_change',0):+.1f}% 代金={s['turnover']/1e8:.0f}億 "
              f"[最弱sec={s.get('sector','?')}]")

    if dry_run:
        print("\n[main_st] --dry-run のため Discord 送信せず終了")
        return

    if DISCORD_NOTIFY:
        send_signals(signals, sell_signals, macro, diag)
    else:
        print("[main_st] DISCORD_NOTIFY=False → Discord送信せずJSON保存のみ（アプリ集約モード）")

    # 配信履歴を保存 (重複防止用)
    payload = {
        "date": today_str,
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "signals": [
            {
                "ticker": s["ticker"], "name": s["name"],
                "rsi": s["rsi"], "deviation": s["deviation"],
                "turnover_oku": s["turnover"] / 1e8,
                "prev_close": s.get("prev_close", 0),   # アプリ表示用(2026-06-11追加)
                "in_sector_top": s.get("in_sector_top", False),
                "in_theme": s.get("in_theme", False),
                "sector": s.get("sector", ""),
            } for s in signals
        ],
        "sell_signals": [
            {
                "ticker": s["ticker"], "name": s["name"],
                "rsi": s["rsi"], "deviation": s["deviation"],
                "day_change": s.get("day_change", 0),
                "turnover_oku": s["turnover"] / 1e8,
                "prev_close": s.get("prev_close", 0),   # アプリ表示用(2026-06-11追加)
                "sector": s.get("sector", ""),
            } for s in sell_signals
        ],
        "diag": diag,
    }
    with open(TODAY_SIGNALS, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[main_st] {TODAY_SIGNALS} 保存完了")

    # 日次 diag ログを JSONL 形式で追記（過去履歴の保全）
    DIAG_LOG.parent.mkdir(parents=True, exist_ok=True)
    already_logged = False
    if DIAG_LOG.exists():
        with open(DIAG_LOG, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    if json.loads(line).get("date") == today_str:
                        already_logged = True
                        break
                except Exception:
                    continue
    if not already_logged:
        with open(DIAG_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        print(f"[main_st] {DIAG_LOG} 追記完了")
    else:
        print(f"[main_st] {DIAG_LOG} 既に {today_str} 記録済み → 追記スキップ")


if __name__ == "__main__":
    main()
