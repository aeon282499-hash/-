"""
main.py — エントリーポイント（Phase 2: 3階層独立運用）
==============================
毎営業日 8:00 JST に GitHub Actions から呼ばれる。

3階層構成（大資金/中資金/小資金）。各階層で独立に銘柄選定・positions管理:
- 大資金: 1件100万 / positions.json / positions_sell.json
- 中資金: 1件 50万 / positions_mid.json / positions_sell_mid.json
- 小資金: 1件 30万 / positions_small.json / positions_sell_small.json

実行フロー（各階層を順に処理）:
  1. 今日が営業日か判定（土日・祝日はスキップ）
  2. screener.run_screener() で「スコア降順全候補」を取得
  3. 各階層ごとに:
     a. 前日ポジションの結果チェック → Discord に送信
     b. サイズで買える銘柄から自分のpositions保有中を除外→top5
     c. notifier.send_signals() で Discord 通知
     d. positions に追加して保存
  4. 例外発生時は notifier.send_error() でエラー通知
"""

import os
import sys
import json
from datetime import datetime, timedelta
import zoneinfo

import jpholiday
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")


# ── 階層定義 ─────────────────────────────────────
# tier=None（または最初の要素）が大資金（既存の DISCORD_WEBHOOK_URL / DISCORD_WEBHOOK_SELL_URL を使う）
TIERS = [
    {
        "key":             "main",
        "label":           "大資金",
        "emoji":           "",
        "size":            1_000_000,
        "buy_pos_file":    "positions.json",
        "sell_pos_file":   "positions_sell.json",
        "buy_webhook":     os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
        "sell_webhook":    os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip(),
        "public_mirror":   True,   # note公開向け公開ミラー
    },
    {
        "key":             "mid",
        "label":           "中資金",
        "emoji":           "🔵",
        "size":            500_000,
        "buy_pos_file":    "positions_mid.json",
        "sell_pos_file":   "positions_sell_mid.json",
        "buy_webhook":     os.getenv("DISCORD_WEBHOOK_BUY_MID_URL", "").strip(),
        "sell_webhook":    os.getenv("DISCORD_WEBHOOK_SELL_MID_URL", "").strip(),
        "public_mirror":   False,
    },
    {
        "key":             "small",
        "label":           "小資金",
        "emoji":           "🟢",
        "size":            300_000,
        "buy_pos_file":    "positions_small.json",
        "sell_pos_file":   "positions_sell_small.json",
        "buy_webhook":     os.getenv("DISCORD_WEBHOOK_BUY_SMALL_URL", "").strip(),
        "sell_webhook":    os.getenv("DISCORD_WEBHOOK_SELL_SMALL_URL", "").strip(),
        "public_mirror":   False,
    },
]


def is_trading_day(d) -> bool:
    if d.weekday() >= 5:
        return False
    if jpholiday.is_holiday(d):
        return False
    return True


def next_trading_day(d) -> object:
    nxt = d + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt


def _select_tier_signals(all_buy_candidates: list[dict],
                         all_sell_candidates: list[dict],
                         tier: dict,
                         buy_positions: list[dict],
                         sell_positions: list[dict],
                         max_signals: int) -> tuple[list[dict], list[dict]]:
    """階層の口座サイズで買える＆自分の保有中銘柄を除外したtop5を返す。"""
    size = tier["size"]
    buy_open = {p["ticker"] for p in buy_positions if p.get("status") in ("pending", "open")}
    sell_open = {p["ticker"] for p in sell_positions if p.get("status") in ("pending", "open")}

    buy_pool = [
        c for c in all_buy_candidates
        if c.get("prev_close", 0) * 100 <= size
        and c["ticker"] not in buy_open
    ]
    sell_pool = [
        c for c in all_sell_candidates
        if c.get("prev_close", 0) * 100 <= size
        and c["ticker"] not in sell_open
    ]
    return buy_pool[:max_signals], sell_pool[:max_signals]


def main() -> None:
    now   = datetime.now(JST)
    today = now.date()
    print(f"[main] 実行日時: {now.strftime('%Y-%m-%d %H:%M JST')}")

    # 配信許可窓 7:00〜10:45 JST。Cloudflare本トリガ(8:05)が落ちた日に、
    # GitHubスケジュール保険(cron 8:20)が最大2h遅延(実測10:37着)しても拾えるよう
    # 9:30→10:45 に拡張。本トリガ成功時は today_signals.json の送信済みガードで二重送信を防ぐ。
    if not (7 <= now.hour < 10 or (now.hour == 10 and now.minute <= 45)):
        print(f"[main] 配信時間外（{now.strftime('%H:%M')} JST）→ スキップします")
        sys.exit(0)

    if not is_trading_day(today):
        reason = (
            "土日のため休場" if today.weekday() >= 5
            else f"{jpholiday.is_holiday_name(today)} のため休場"
        )
        print(f"[main] {reason} → スキップします")
        sys.exit(0)

    today_str = today.strftime("%Y-%m-%d")
    if os.path.exists("today_signals.json"):
        with open("today_signals.json", encoding="utf-8") as _f:
            _saved = json.load(_f)
        if _saved.get("date") == today_str:
            print(f"[main] 本日分({today_str})は送信済みです → スキップ")
            sys.exit(0)

    from screener import run_screener, MAX_SIGNALS
    from notifier import (send_signals, send_results, send_error, send_monthly_report,
                          send_sell_signals, send_sell_results, send_sell_monthly_report)
    from tracker import (load_positions, save_positions, update_positions, add_signals_to_positions,
                         load_sell_positions, save_sell_positions)

    try:
        # ── ① スクリーニング（共通: スコア降順全候補を取得）────────────────
        signals_main, sell_signals_main, macro, all_buy, all_sell = run_screener()
        print(f"[main] 全BUY候補 {len(all_buy)}件 / 全SELL候補 {len(all_sell)}件")

        # 大資金用 top5 はメインチャンネル用に保存
        entry_date = today  # 当日寄り付きエントリー

        # ── ② 各階層を順に処理 ──────────────────────────
        first_tier_signals = []
        first_tier_sell_signals = []
        first_tier_closed = []

        for tier in TIERS:
            label = tier["label"]
            key   = tier["key"]
            if not tier["buy_webhook"] and key != "main":
                # サブ口座のwebhook未設定はスキップ（envなし環境）
                print(f"[main-{label}] buy_webhook未設定 → スキップ")
                continue

            print(f"\n========== [{label} ({tier['size']//10000}万)] ==========")

            # 前日結果チェック
            positions      = load_positions(tier["buy_pos_file"])
            sell_positions = load_sell_positions(tier["sell_pos_file"])

            active = [p for p in positions if p["status"] in ("pending", "open")]
            print(f"[main-{label}] BUYオープン {len(active)}件 / SELLオープン "
                  f"{len([p for p in sell_positions if p['status'] in ('pending','open')])}件")

            closed_today = []
            still_open   = []
            if active:
                positions, closed_today, still_open = update_positions(positions, today)
                send_results(closed_today, still_open, today, tier=tier)
                send_monthly_report(positions, today, tier=tier)

            sell_closed_today = []
            sell_still_open   = []
            sell_active = [p for p in sell_positions if p["status"] in ("pending", "open")]
            if sell_active:
                sell_positions, sell_closed_today, sell_still_open = update_positions(sell_positions, today)
                send_sell_results(sell_closed_today, sell_still_open, today, tier=tier)
                send_sell_monthly_report(sell_positions, today, tier=tier)

            # 階層別シグナル選定
            tier_signals, tier_sell_signals = _select_tier_signals(
                all_buy, all_sell, tier, positions, sell_positions, MAX_SIGNALS,
            )
            print(f"[main-{label}] サイズ{tier['size']//10000}万で買える: "
                  f"BUY {len(tier_signals)}件 / SELL {len(tier_sell_signals)}件")

            # 新規シグナル追加・保存
            positions      = add_signals_to_positions(positions, tier_signals, today, entry_date)
            sell_positions = add_signals_to_positions(sell_positions, tier_sell_signals, today, entry_date)
            save_positions(positions, tier["buy_pos_file"])
            save_sell_positions(sell_positions, tier["sell_pos_file"])

            # 配信
            send_signals(tier_signals, today, macro, entry_date, tier=tier)
            send_sell_signals(tier_sell_signals, today, entry_date, tier=tier)

            # 階層別の当日シグナル記録（夕方report.py用）
            if key == "main":
                today_sig_file      = "today_signals.json"
                today_sell_sig_file = "today_sell_signals.json"
            else:
                today_sig_file      = f"today_signals_{key}.json"
                today_sell_sig_file = f"today_sell_signals_{key}.json"
            with open(today_sig_file, "w", encoding="utf-8") as f:
                json.dump({
                    "date":    today_str,
                    "signals": [{"ticker": s["ticker"], "name": s["name"], "direction": "BUY"}
                                for s in tier_signals],
                }, f, ensure_ascii=False, indent=2)
            with open(today_sell_sig_file, "w", encoding="utf-8") as f:
                json.dump({
                    "date":    today_str,
                    "signals": [{"ticker": s["ticker"], "name": s["name"], "direction": "SELL"}
                                for s in tier_sell_signals],
                }, f, ensure_ascii=False, indent=2)

            # 大資金分は後段（Twitter等）で使うので保持
            if key == "main":
                first_tier_signals = tier_signals
                first_tier_sell_signals = tier_sell_signals
                first_tier_closed = closed_today

        print(f"[main] today_signals.json ({len(first_tier_signals)}件) / "
              f"today_sell_signals.json ({len(first_tier_sell_signals)}件) 保存")

        # ── ④ Twitter（大資金分のみ・既存挙動）──────────────────
        # TWITTER_PAUSED: 2026-05-21 ユーザー指示で一時停止（PEADフィルタB案BT中）。再開時はコメント解除。
        # from twitter_notifier import post_swing_signals, post_swing_results, post_monthly_summary
        # post_swing_signals(first_tier_signals, today, macro, sell_signals=first_tier_sell_signals)
        # if first_tier_closed:
        #     post_swing_results(first_tier_closed, today)
        # if today.day == 1:
        #     post_monthly_summary(today)
        print("[main] Twitter配信は一時停止中（TWITTER_PAUSED）")

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
