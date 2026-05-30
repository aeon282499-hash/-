"""
main_theme.py — テーマトラッカー 日次実行

実行: python main_theme.py [--dry-run]
  --dry-run: Discord 通知せず標準出力のみ

「🔥 今ホットなテーマ TOP3 ＋ 出遅れ初動候補」を算出して Discord 配信する。
単体スクリーナーに "テーマ文脈" を足す層 (theme_members.json + theme_tracker.py)。
"""
import json
import sys
from datetime import date, datetime
from pathlib import Path

import jpholiday
from dotenv import load_dotenv

load_dotenv()

from theme_tracker import run_theme_tracker
from notifier_theme import send_theme_signals, send_error

TODAY_SIGNALS = Path("today_signals_theme.json")
DIAG_LOG = Path("logs/theme_diag.jsonl")


def is_today_trading_day() -> bool:
    today = date.today()
    return today.weekday() < 5 and not jpholiday.is_holiday(today)


def _strip_members(row: dict) -> dict:
    """JSON保存用にテーマ行を軽量化(members本体は早期候補だけ残す)。"""
    out = {k: v for k, v in row.items() if k not in ("members",)}
    out["early"] = [
        {
            "ticker": m["ticker"], "name": m["name"], "role": m.get("role", ""),
            "vr": m["vr"], "dev": m["dev"], "rsi": m["rsi"],
            "r1": m["r1"], "r5": m["r5"], "r20": m["r20"], "close": m["close"],
        }
        for m in row.get("early", [])
    ]
    return out


def main():
    dry_run = "--dry-run" in sys.argv

    if not is_today_trading_day():
        print("[main_theme] 本日は休場日 → スキップ")
        return

    today_str = date.today().strftime("%Y-%m-%d")

    # 重複防止: 既に当日配信済みならスキップ
    if TODAY_SIGNALS.exists():
        try:
            with open(TODAY_SIGNALS, "r", encoding="utf-8") as f:
                prev = json.load(f)
            if prev.get("date") == today_str and not dry_run:
                print(f"[main_theme] 本日 ({today_str}) は既に配信済み → スキップ")
                return
        except Exception:
            pass

    try:
        ranked, hot = run_theme_tracker()
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[main_theme] トラッカー例外: {e}\n{tb}")
        if not dry_run:
            send_error(f"トラッカー失敗: {e}\n\n{tb[-1000:]}")
        return

    if not ranked:
        print("[main_theme] ランキング0件(データ取得失敗の可能性) → 終了")
        if not dry_run:
            send_error("テーマランキング0件。J-Quantsデータ取得失敗の可能性。")
        return

    print(f"\n[main_theme] テーマ熱ランキング (全{len(ranked)}テーマ)")
    for i, r in enumerate(ranked, 1):
        drv = "/".join(r["us_drivers"][:3]) if r["us_drivers"] else "国内発"
        print(f"  {i:2d}. {r['theme']:<18} heat={r['heat']:6.1f} "
              f"5d={r['avg_r5']:+6.1f}% 25MA上={r['pct_above_ma25']*100:3.0f}% [{drv}]")

    print(f"\n[main_theme] 点火中テーマの出遅れ初動候補 (候補あり{len(hot)}テーマ):")
    for r in hot:
        early = r.get("early", [])
        print(f"  ■ {r['theme']} (heat={r['heat']}) — 初動候補 {len(early)}件")
        for m in early:
            print(f"     - [{m['ticker']}] {m['name']} 出来高{m['vr']:.1f}倍 "
                  f"乖離{m['dev']:+.1f}% 5d={m['r5']*100:+.1f}%")

    if dry_run:
        print("\n[main_theme] --dry-run のため Discord 送信せず終了")
        return

    send_theme_signals(ranked, hot)

    # 配信履歴を保存 (重複防止用)
    payload = {
        "date": today_str,
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ranked": [_strip_members(r) for r in ranked],
    }
    with open(TODAY_SIGNALS, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[main_theme] {TODAY_SIGNALS} 保存完了")

    # 日次 diag ログを JSONL 追記 (過去履歴の保全)
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
        print(f"[main_theme] {DIAG_LOG} 追記完了")
    else:
        print(f"[main_theme] {DIAG_LOG} 既に {today_str} 記録済み → 追記スキップ")


if __name__ == "__main__":
    main()
