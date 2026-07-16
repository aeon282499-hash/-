"""
close_check.py — 大引け前のRSI判定とDiscord通知（Phase 2: 3階層対応）
==================================================
毎営業日 15:00 JST に GitHub Actions から呼ばれる。

実行フロー:
  1. 営業日チェック・時間外スキップ
  2. 各階層（大資金/中資金/小資金）の positions_*.json をロード
  3. status=pending/open のポジションについて:
     - yfinance で当日 current price 取得（~14:45データ・15分遅延）
     - 過去終値（J-Quants）+ current price で RSI(14) 計算
     - 判定:
       * 当日 hold_day == MAX_HOLD: 強制MAXHOLD大引け処分
       * BUY  かつ RSI ≥ 50: RSI回復で大引け処分推奨
       * SELL かつ RSI ≤ 50: RSI回復で大引け買戻し推奨
  4. 該当銘柄があれば階層別Discordチャンネルに通知

ユーザーは通知を受けて15:25-15:30のクロージングオークションでSBI証券アプリから成行発注。
"""

import json
import os
from datetime import datetime, date, timedelta
import zoneinfo

import jpholiday
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")

LAST_RUN_FILE = "last_close_check.json"
MAX_HOLD = 3
RSI_EXIT_THRESHOLD = 50

# 階層定義（main.py の TIERS と整合）
TIERS = [
    {
        "key":           "main",
        "label":         "大資金",
        "emoji":         "",
        "size":          1_000_000,
        "buy_pos_file":  "positions.json",
        "sell_pos_file": "positions_sell.json",
        "buy_webhook":   os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
        "sell_webhook":  os.getenv("DISCORD_WEBHOOK_SELL_URL", "").strip(),
        # 2026-05-21の二重投稿対策（main.py/notifier.pyと同じ理由・note専用チャンネル用意後にTrue）
        "public_mirror": False,
    },
    {
        "key":           "mid",
        "label":         "中資金",
        "emoji":         "🔵",
        "size":          500_000,
        "buy_pos_file":  "positions_mid.json",
        "sell_pos_file": "positions_sell_mid.json",
        "buy_webhook":   os.getenv("DISCORD_WEBHOOK_BUY_MID_URL", "").strip(),
        "sell_webhook": os.getenv("DISCORD_WEBHOOK_SELL_MID_URL", "").strip(),
        "public_mirror": False,
    },
    {
        "key":           "small",
        "label":         "小資金",
        "emoji":         "🟢",
        "size":          300_000,
        "buy_pos_file":  "positions_small.json",
        "sell_pos_file": "positions_sell_small.json",
        "buy_webhook":   os.getenv("DISCORD_WEBHOOK_BUY_SMALL_URL", "").strip(),
        "sell_webhook": os.getenv("DISCORD_WEBHOOK_SELL_SMALL_URL", "").strip(),
        "public_mirror": False,
    },
]


def is_trading_day(d) -> bool:
    if d.weekday() >= 5:
        return False
    if jpholiday.is_holiday(d):
        return False
    return True


def calc_today_hold_day(pos: dict, today: date) -> int:
    entry_dt = datetime.strptime(pos["entry_date"], "%Y-%m-%d").date()
    if entry_dt > today:
        return 0
    if entry_dt == today:
        return 1
    cur, count = entry_dt, 0
    while cur <= today:
        if is_trading_day(cur):
            count += 1
        cur += timedelta(days=1)
    return count


def _entry_day_open(pos: dict, today: date, historical_data: dict) -> float | None:
    """寄指の約定判定用にエントリー日の寄り値を返す（取れなければNone）。
    エントリー日が当日なら yfinance 5分足の最初のバー、過去日なら J-Quants 日足を使う。"""
    ticker = pos["ticker"]
    if pos["entry_date"] == today.strftime("%Y-%m-%d"):
        try:
            import yfinance as yf
            intraday = yf.Ticker(ticker).history(period="1d", interval="5m")
            if not intraday.empty:
                return float(intraday["Open"].iloc[0])
        except Exception as e:
            print(f"  [yfinance] {ticker} 寄り値取得失敗: {e}")
        return None
    df = historical_data.get(ticker)
    if df is not None:
        rows = df[df.index.strftime("%Y-%m-%d") == pos["entry_date"]]
        if not rows.empty:
            return float(rows["Open"].iloc[0])
    return None


def _oco_fill(direction: str, entry_open: float | None,
              day_high: float | None, day_low: float | None) -> dict | None:
    """ザラ場OCO(TP +5% / STOP -3%)が当日中に約定したかを当日高安から判定。
    約定していれば {kind, pnl_pct, level, hit} を返す（STOP優先＝backtest_rangeと同順）。
    OCOは証券会社の実注文なので、約定済み＝もう保有していない＝処分対象でも保有継続でもない。"""
    if not entry_open or day_high is None or day_low is None:
        return None
    if direction == "BUY":
        stop, tp = entry_open * 0.97, entry_open * 1.05
        if day_low <= stop:
            return {"kind": "STOP", "pnl_pct": -3.0, "level": stop, "hit": day_low}
        if day_high >= tp:
            return {"kind": "TP", "pnl_pct": +5.0, "level": tp, "hit": day_high}
    else:  # SELL（空売り：利確は下＝entry×0.95 / 損切は上＝entry×1.03）
        stop, tp = entry_open * 1.03, entry_open * 0.95
        if day_high >= stop:
            return {"kind": "STOP", "pnl_pct": -3.0, "level": stop, "hit": day_high}
        if day_low <= tp:
            return {"kind": "TP", "pnl_pct": +5.0, "level": tp, "hit": day_low}
    return None


def collect_targets(open_positions: list[dict], direction: str, today: date,
                    historical_data: dict) -> tuple[list[dict], list[dict]]:
    """指定directionのオープンポジションから大引け処分対象を抽出する。
    historical_data は事前にbatch取得した J-Quants 日足データ。

    Returns
    -------
    (targets, checked)
      targets: 処分対象（RSI回復/MAXHOLD）
      checked: 処分対象にならなかった判定結果。「対象なし」確認通知用。
               noteがNoneなら正常な保有継続、文字列ならデータ取得失敗等の異常
               （無音だと故障と区別できないため通知に明示する・2026-06-13）。
    """
    if not open_positions:
        return [], []

    import yfinance as yf
    from screener import calc_rsi

    targets = []
    checked = []

    def _checked(note=None, rsi=None, price=None, hold=None):
        checked.append({
            "ticker": ticker, "name": name, "today_hold": hold,
            "rsi_now": rsi, "current_price": price, "note": note,
        })

    for pos in open_positions:
        ticker = pos["ticker"]
        name = pos["name"]
        today_hold = calc_today_hold_day(pos, today)
        entry_open = pos.get("entry_open") or pos.get("prev_close")

        print(f"[close_check] [{direction}] {ticker} {name} - day {today_hold}")

        # ── 寄指の約定確認（BUY・pending・limit_price持ち＝2026-06-11以降の新規）──
        # 寄りが指値を超えた銘柄は約定していない＝ユーザーは株を持っていないので
        # 処分通知の対象外。寄り値が確認できない場合も誤通知よりスキップを優先する。
        lp = pos.get("limit_price")
        if direction == "BUY" and pos.get("status") == "pending" and lp:
            day_open = _entry_day_open(pos, today, historical_data)
            if day_open is None:
                print(f"  [寄指] {ticker} 寄り値が取れず約定不明 → スキップ")
                _checked(note="寄指の約定確認不可（要手動確認）", hold=today_hold)
                continue
            # エントリー当日の寄り値はyfinance 5分足由来で、J-Quants公式寄りと
            # 1円前後ズレることがある（2026-07-15 東海カーボン: 公式1,653円>指値
            # 1,652円のNOFILLを、yf寄り≤1,652円が「約定」と誤判定し保有継続と
            # 誤表示）。指値との差が誤差幅以内なら約定を断定せず手動確認に回す。
            # 過去日はJ-Quants公式寄り＝正確なので緩衝なしで確定判定する。
            if pos["entry_date"] == today.strftime("%Y-%m-%d"):
                eps = max(2.0, lp * 0.001)
                if abs(day_open - lp) <= eps:
                    print(f"  [寄指] {ticker} 境界 (寄り{day_open:,.0f}円 ≒ 指値{lp:,}円) → 約定不明扱いでスキップ")
                    _checked(note=(f"寄指の約定が境界（寄り{day_open:,.0f}円 ≒ 指値{lp:,}円・"
                                   f"データ誤差で断定不可）→ 口座で要確認"),
                             hold=today_hold)
                    continue
            if day_open > lp:
                print(f"  [寄指] {ticker} 不成立 (寄り{day_open:,.0f}円 > 指値{lp:,}円) → 対象外")
                _checked(note=f"寄指不成立（寄り{day_open:,.0f}円 > 指値{lp:,}円・ノーポジ）")
                continue
            entry_open = day_open  # 実際の約定値（含み損益表示の基準を寄り値に補正）

        # ── 当日ザラ場データ取得（OCO判定＋RSI判定に共用）──
        day_high = day_low = current_price = None
        try:
            intraday = yf.Ticker(ticker).history(period="1d", interval="5m")
            if not intraday.empty:
                current_price = float(intraday["Close"].iloc[-1])
                day_high      = float(intraday["High"].max())
                day_low       = float(intraday["Low"].min())
        except Exception as e:
            print(f"  [yfinance] {ticker} 失敗: {e}")

        # ── 最優先: ザラ場OCO(+5%/-3%)約定チェック ──
        # 当日高安がTP/STOPに触れていれば証券会社のOCOで既に決済済み＝
        # 「処分対象」でも「保有継続」でもない。その旨だけ通知して以降の判定はしない。
        fill = _oco_fill(direction, entry_open, day_high, day_low)
        if fill:
            verb = "利確" if fill["kind"] == "TP" else "損切"
            act  = "買戻し" if direction == "SELL" else "決済"
            print(f"  [OCO] {ticker} 本日{fill['kind']} {fill['pnl_pct']:+.0f}% 約定済み")
            _checked(
                note=(f"本日OCO **{fill['pnl_pct']:+.0f}%{verb}**で{act}済み"
                      f"（{fill['hit']:,.0f}円が{fill['kind']} {fill['level']:,.0f}円に到達・保有なし）"),
                price=current_price, hold=today_hold,
            )
            checked[-1]["settled"] = True
            checked[-1]["pnl_pct"] = fill["pnl_pct"]
            continue

        # ── MAXHOLD（OCO未約定で保有最終日 → 大引け強制処分）──
        if today_hold >= MAX_HOLD:
            targets.append({
                "ticker":        ticker,
                "name":          name,
                "direction":     direction,
                "reason_type":   "MAXHOLD",
                "reason":        f"保有{today_hold}日目・強制大引け処分",
                "today_hold":    today_hold,
                "rsi_now":       None,
                "current_price": None,
                "entry_open":    entry_open,
            })
            continue

        # ── RSI判定（現在値が必要）──
        if current_price is None:
            print(f"  [yfinance] {ticker} 現在値取れず → 手動確認")
            _checked(note="現在値の取得失敗（要手動確認）", hold=today_hold)
            continue
        if ticker not in historical_data:
            print(f"  [J-Quants] {ticker} データなし")
            _checked(note="履歴データなし・RSI判定不可（要手動確認）", hold=today_hold)
            continue
        df = historical_data[ticker]
        closes = df["Close"].dropna().tolist()
        closes.append(current_price)
        rsi_now = calc_rsi(pd.Series(closes))

        if rsi_now is None:
            print(f"  [RSI] {ticker} 計算失敗")
            _checked(note="RSI計算失敗（要手動確認）", hold=today_hold)
            continue

        print(f"  RSI={rsi_now:.1f} / current_price={current_price:.0f}")

        rsi_exit = (
            (direction == "BUY"  and rsi_now >= RSI_EXIT_THRESHOLD) or
            (direction == "SELL" and rsi_now <= RSI_EXIT_THRESHOLD)
        )
        if rsi_exit:
            cmp = "≥" if direction == "BUY" else "≤"
            targets.append({
                "ticker":        ticker,
                "name":          name,
                "direction":     direction,
                "reason_type":   "RSI",
                "reason":        f"RSI回復（RSI={rsi_now:.1f} {cmp} 50）",
                "today_hold":    today_hold,
                "rsi_now":       rsi_now,
                "current_price": current_price,
                "entry_open":    entry_open,
            })
        else:
            _checked(rsi=rsi_now, price=current_price, hold=today_hold)

    return targets, checked


def _load_active(path: str, direction: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [p for p in json.load(f)
                if p.get("status") in ("pending", "open")
                and p.get("direction") == direction]


def main():
    now = datetime.now(JST)
    today = now.date()
    print(f"[close_check] 実行: {now.strftime('%Y-%m-%d %H:%M JST')}")

    if not is_trading_day(today):
        print("[close_check] 休場日のためスキップ")
        return

    if not (14 <= now.hour <= 17):
        print(f"[close_check] 時間外スキップ（実行時刻={now.strftime('%H:%M')}）")
        return

    today_str = today.strftime("%Y-%m-%d")
    if os.path.exists(LAST_RUN_FILE):
        try:
            with open(LAST_RUN_FILE, encoding="utf-8") as _f:
                _last = json.load(_f)
            if _last.get("date") == today_str:
                print(f"[close_check] 本日分({today_str})は送信済みです → スキップ")
                return
        except Exception as _e:
            print(f"[close_check] {LAST_RUN_FILE} 読込失敗: {_e} → 続行")

    # ── 全階層の保有銘柄を一覧化→J-Quants一括取得（1回だけ） ──────────
    from screener import batch_download_jquants, _jquants_id_token

    all_tickers = set()
    tier_positions = {}
    for tier in TIERS:
        if not tier["buy_webhook"] and tier["key"] != "main":
            continue
        buy_open  = _load_active(tier["buy_pos_file"],  "BUY")
        sell_open = _load_active(tier["sell_pos_file"], "SELL")
        tier_positions[tier["key"]] = {"buy": buy_open, "sell": sell_open}
        all_tickers.update(p["ticker"] for p in buy_open + sell_open)

    print(f"[close_check] 全階層合計の保有銘柄: {len(all_tickers)} 銘柄")
    historical_data: dict = {}
    if all_tickers:
        token = _jquants_id_token()
        end_str   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        start_str = (today - timedelta(days=45)).strftime("%Y-%m-%d")
        all_data = batch_download_jquants(token, start=start_str, end=end_str)
        for ticker in all_tickers:
            df = all_data.get(ticker)
            if df is not None and not df.empty:
                historical_data[ticker] = df

    # ── 階層ごとに大引け処分判定＆通知 ────────────────────
    marker_payload = {"date": today_str, "ran_at": now.strftime("%Y-%m-%d %H:%M JST"), "tiers": {}}

    for tier in TIERS:
        key = tier["key"]
        if key not in tier_positions:
            continue
        buy_open  = tier_positions[key]["buy"]
        sell_open = tier_positions[key]["sell"]
        print(f"\n[close_check-{tier['label']}] BUY {len(buy_open)} / SELL {len(sell_open)} 件")

        buy_targets,  buy_checked  = collect_targets(buy_open,  "BUY",  today, historical_data)
        sell_targets, sell_checked = collect_targets(sell_open, "SELL", today, historical_data)

        # 対象ありは処分指示、対象なしでも保有銘柄があれば「保有継続」確認を送る
        # （無音だと故障と区別できない・2026-06-12の問い合わせを受けて2026-06-13追加）
        if buy_targets:
            from notifier import send_close_signals
            send_close_signals(buy_targets, today, tier=tier)
        elif buy_checked:
            from notifier import send_close_no_targets
            send_close_no_targets(buy_checked, today, tier=tier, sell=False)
        if sell_targets:
            from notifier import send_close_signals_sell
            send_close_signals_sell(sell_targets, today, tier=tier)
        elif sell_checked:
            from notifier import send_close_no_targets
            send_close_no_targets(sell_checked, today, tier=tier, sell=True)
        if not buy_targets and not sell_targets:
            print(f"[close_check-{tier['label']}] 大引け処分対象なし")

        marker_payload["tiers"][key] = {
            "buy":  [t["ticker"] for t in buy_targets],
            "sell": [t["ticker"] for t in sell_targets],
        }

    # 当日処理済みマーカーを書き出し
    with open(LAST_RUN_FILE, "w", encoding="utf-8") as f:
        json.dump(marker_payload, f, ensure_ascii=False, indent=2)
    print(f"[close_check] {LAST_RUN_FILE} 更新（{today_str}）")


if __name__ == "__main__":
    main()
