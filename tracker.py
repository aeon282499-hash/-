"""
tracker.py — オープンポジション管理・損益追跡
================================================
positions.json にポジションを保存し、毎朝前日の結果を集計する。

positions.json の構造:
[
  {
    "signal_date": "2026-03-25",   # シグナル発生日（=エントリー日と同日・当日寄り買い）
    "entry_date":  "2026-03-25",   # エントリー日（=signal_date 当日）
    "ticker":      "1234.T",
    "name":        "○○株式会社",
    "direction":   "BUY",
    "prev_close":  1000.0,         # シグナル日の前日終値
    "entry_open":  null,           # エントリー日の始値（main.py の翌日にtrackerが取得）
    "status":      "pending",      # pending → open → closed
    "hold_days":   0,              # 「前日までの完了日数」（当日表示は +1 すること）
    "pnl_pct":     null,           # 確定損益%
    "unrealized_pnl": null,        # 含み損益%（保有中）
    "exit_type":   null,           # STOP / TP / RSI / MAXHOLD
    "exit_date":   null
  }
]
"""

import json
import os
from datetime import date, timedelta

from screener import batch_download_jquants, _jquants_id_token, calc_rsi, RSI_WARMUP_CAL_DAYS

POSITIONS_FILE      = "positions.json"
SELL_POSITIONS_FILE = "positions_sell.json"
STOP_LOSS      = 3.0   # %
TAKE_PROFIT    = 5.0   # %
MAX_HOLD       = 3     # 営業日


def load_positions(path: str = POSITIONS_FILE) -> list[dict]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_positions(positions: list[dict], path: str = POSITIONS_FILE) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)
    print(f"[tracker] {path} を保存しました（{len(positions)}件）")


def load_sell_positions(path: str = SELL_POSITIONS_FILE) -> list[dict]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_sell_positions(positions: list[dict], path: str = SELL_POSITIONS_FILE) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)
    print(f"[tracker] {path} を保存しました（{len(positions)}件）")


def add_signals_to_positions(
    positions: list[dict],
    signals: list[dict],
    signal_date: date,
    entry_date: date,
) -> list[dict]:
    """新しいシグナルをポジションリストに追加する。"""
    from screener import yose_limit_price
    existing = {(p["ticker"], p["signal_date"]) for p in positions}
    for sig in signals:
        key = (sig["ticker"], signal_date.strftime("%Y-%m-%d"))
        if key in existing:
            continue
        # BUYは寄指(寄付限定指値)運用(2026-06-11〜)。limit_priceを持つポジションだけが
        # 失効判定の対象になる(過去の成行ポジションには影響しない)。
        limit_price = yose_limit_price(sig.get("prev_close", 0)) if sig["direction"] == "BUY" else None
        positions.append({
            "signal_date":    signal_date.strftime("%Y-%m-%d"),
            "entry_date":     entry_date.strftime("%Y-%m-%d"),
            "ticker":         sig["ticker"],
            "name":           sig["name"],
            "direction":      sig["direction"],
            "prev_close":     sig.get("prev_close", 0),
            "limit_price":    limit_price,
            "entry_open":     None,
            "status":         "pending",
            "hold_days":      0,
            "pnl_pct":        None,
            "unrealized_pnl": None,
            "exit_type":      None,
            "exit_date":      None,
        })
    return positions


def update_positions(positions: list[dict], today: date,
                     all_data: dict | None = None) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """
    オープンポジションを前日データで更新する。

    all_data に取得済みの {ticker: DataFrame} を渡すとJ-Quants取得をスキップする
    （週次レポートのドライラン等、複数階層で同じデータを使い回す用）。

    Returns
    -------
    (updated_positions, closed_today, expired_today, still_open)
    """
    active = [p for p in positions if p["status"] in ("pending", "open")]
    if not active:
        return positions, [], [], []

    if all_data is None:
        tickers = list({p["ticker"] for p in active})
        # 30日窓だとRSI(ewm)のウォームアップ不足で±2〜4ptブレ、朝runと窓が1日ズレた
        # だけで50境界の出口判定が割れて帳簿が遡って誤決済しうる（2026-07-17ビックカメラ）
        start   = (today - timedelta(days=RSI_WARMUP_CAL_DAYS)).strftime("%Y-%m-%d")
        end     = today.strftime("%Y-%m-%d")

        print(f"[tracker] {len(tickers)} 銘柄のデータ取得中（結果チェック用）...")
        token    = _jquants_id_token()
        all_data = batch_download_jquants(token, start=start, end=end, tickers=tickers)

    closed_today  = []
    expired_today = []
    still_open    = []

    updated = []
    for pos in positions:
        # closed=決済済み / expired=寄指不成立(NOFILL・約定なし) はどちらも終端状態。
        # 再処理すると失効ポジに含み損益・保有日数が付き still_open に混入し、
        # 朝レポートで「保有中・本日処分」と誤通知される（2026-06-25 日油の事故）。
        if pos["status"] in ("closed", "expired"):
            updated.append(pos)
            continue

        ticker = pos["ticker"]
        df = all_data.get(ticker)
        if df is None or df.empty:
            updated.append(pos)
            continue

        today_str      = today.strftime("%Y-%m-%d")
        entry_date_str = pos["entry_date"]

        # ── エントリー日の始値を取得（pending → open）────────────
        if pos["status"] == "pending":
            entry_rows = df[df.index.strftime("%Y-%m-%d") == entry_date_str]
            if entry_rows.empty:
                # まだエントリー日が来ていない
                updated.append(pos)
                continue
            entry_open_val = float(entry_rows["Open"].iloc[0])
            # ── 寄指不成立の失効（BUY・limit_price持ちのみ＝2026-06-11以降の新規）──
            # 寄りが指値を超えた=高寄り → 寄付限定指値は約定せず失効。トレード無しとして
            # 記録し、以後の損益・処分通知の対象から外す（実際の発注と帳簿を一致させる）。
            lp = pos.get("limit_price")
            if pos["direction"] == "BUY" and lp and entry_open_val > lp:
                pos["entry_open"] = entry_open_val
                pos["status"]     = "expired"
                pos["exit_type"]  = "NOFILL"
                pos["exit_date"]  = entry_date_str
                print(f"[tracker] {ticker} 寄指不成立 (寄り{entry_open_val:,.0f}円 > 指値{lp:,}円) → 失効")
                expired_today.append(pos)
                updated.append(pos)
                continue
            pos["entry_open"] = entry_open_val
            pos["status"]     = "open"
            pos["hold_days"]  = 0

        entry_open = pos["entry_open"]
        if not entry_open or entry_open <= 0:
            updated.append(pos)
            continue

        direction   = pos["direction"]
        stop_price  = (entry_open * (1 - STOP_LOSS   / 100) if direction == "BUY"
                       else entry_open * (1 + STOP_LOSS   / 100))
        tp_price    = (entry_open * (1 + TAKE_PROFIT / 100) if direction == "BUY"
                       else entry_open * (1 - TAKE_PROFIT / 100))

        # entry_date より後、today より前のデータで確認
        post_df = df[
            (df.index.strftime("%Y-%m-%d") >= entry_date_str) &
            (df.index.strftime("%Y-%m-%d") < today_str)
        ]

        pos["hold_days"] = 0  # 毎回エントリー日から再計算（累積バグ防止）
        closed = False
        for dt_idx, row in post_df.iterrows():
            pos["hold_days"] += 1
            day_high  = float(row["High"])
            day_low   = float(row["Low"])
            day_close = float(row["Close"])
            check_date_str = dt_idx.strftime("%Y-%m-%d")

            # 損切り・利確チェック
            if direction == "BUY":
                if day_low <= stop_price:
                    pos.update(pnl_pct=-STOP_LOSS, exit_type="STOP",
                               exit_date=check_date_str, status="closed")
                    closed = True; break
                if day_high >= tp_price:
                    pos.update(pnl_pct=+TAKE_PROFIT, exit_type="TP",
                               exit_date=check_date_str, status="closed")
                    closed = True; break
            else:
                if day_high >= stop_price:
                    pos.update(pnl_pct=-STOP_LOSS, exit_type="STOP",
                               exit_date=check_date_str, status="closed")
                    closed = True; break
                if day_low <= tp_price:
                    pos.update(pnl_pct=+TAKE_PROFIT, exit_type="TP",
                               exit_date=check_date_str, status="closed")
                    closed = True; break

            # RSI回復チェック
            hist_df = df[df.index <= dt_idx]
            rsi_now = calc_rsi(hist_df["Close"].dropna())
            rsi_exit = rsi_now is not None and (
                (direction == "BUY"  and rsi_now >= 50) or
                (direction == "SELL" and rsi_now <= 50)
            )
            pnl = ((day_close - entry_open) / entry_open * 100 if direction == "BUY"
                   else (entry_open - day_close) / entry_open * 100)

            if rsi_exit or pos["hold_days"] >= MAX_HOLD:
                exit_type = "RSI" if rsi_exit else "MAXHOLD"
                pos.update(pnl_pct=round(pnl, 3), exit_type=exit_type,
                           exit_date=check_date_str, status="closed")
                closed = True; break

        if closed:
            closed_today.append(pos)
        else:
            # 含み損益（最新終値ベース）
            last_rows = df[df.index.strftime("%Y-%m-%d") < today_str]
            if not last_rows.empty:
                last_close = float(last_rows["Close"].iloc[-1])
                pnl = ((last_close - entry_open) / entry_open * 100 if direction == "BUY"
                       else (entry_open - last_close) / entry_open * 100)
                pos["unrealized_pnl"] = round(pnl, 3)
            still_open.append(pos)

        updated.append(pos)

    return updated, closed_today, expired_today, still_open
