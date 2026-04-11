"""
tracker.py — オープンポジション管理・損益追跡
================================================
positions.json にポジションを保存し、毎朝前日の結果を集計する。

positions.json の構造:
[
  {
    "signal_date": "2026-03-25",   # シグナル発生日
    "entry_date":  "2026-03-26",   # エントリー日（翌営業日）
    "ticker":      "1234.T",
    "name":        "○○株式会社",
    "direction":   "BUY",
    "prev_close":  1000.0,         # シグナル日の終値
    "entry_open":  null,           # エントリー日の始値（翌日取得）
    "status":      "pending",      # pending → open → closed
    "hold_days":   0,
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

from screener import batch_download_jquants, _jquants_id_token, calc_rsi

POSITIONS_FILE      = "positions.json"
SELL_POSITIONS_FILE = "positions_sell.json"
STOP_LOSS      = 3.0   # %
TAKE_PROFIT    = 5.0   # %
MAX_HOLD       = 3     # 営業日


def load_positions() -> list[dict]:
    if os.path.exists(POSITIONS_FILE):
        with open(POSITIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_positions(positions: list[dict]) -> None:
    with open(POSITIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)
    print(f"[tracker] positions.json を保存しました（{len(positions)}件）")


def load_sell_positions() -> list[dict]:
    if os.path.exists(SELL_POSITIONS_FILE):
        with open(SELL_POSITIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_sell_positions(positions: list[dict]) -> None:
    with open(SELL_POSITIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)
    print(f"[tracker] positions_sell.json を保存しました（{len(positions)}件）")


def add_signals_to_positions(
    positions: list[dict],
    signals: list[dict],
    signal_date: date,
    entry_date: date,
) -> list[dict]:
    """新しいシグナルをポジションリストに追加する。"""
    existing = {(p["ticker"], p["signal_date"]) for p in positions}
    for sig in signals:
        key = (sig["ticker"], signal_date.strftime("%Y-%m-%d"))
        if key in existing:
            continue
        positions.append({
            "signal_date":    signal_date.strftime("%Y-%m-%d"),
            "entry_date":     entry_date.strftime("%Y-%m-%d"),
            "ticker":         sig["ticker"],
            "name":           sig["name"],
            "direction":      sig["direction"],
            "prev_close":     sig.get("prev_close", 0),
            "entry_open":     None,
            "status":         "pending",
            "hold_days":      0,
            "pnl_pct":        None,
            "unrealized_pnl": None,
            "exit_type":      None,
            "exit_date":      None,
        })
    return positions


def update_positions(positions: list[dict], today: date) -> tuple[list[dict], list[dict], list[dict]]:
    """
    オープンポジションを前日データで更新する。

    Returns
    -------
    (updated_positions, closed_today, still_open)
    """
    active = [p for p in positions if p["status"] in ("pending", "open")]
    if not active:
        return positions, [], []

    tickers = list({p["ticker"] for p in active})
    start   = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    end     = today.strftime("%Y-%m-%d")

    print(f"[tracker] {len(tickers)} 銘柄のデータ取得中（結果チェック用）...")
    token    = _jquants_id_token()
    all_data = batch_download_jquants(token, start=start, end=end, tickers=tickers)

    closed_today = []
    still_open   = []

    updated = []
    for pos in positions:
        if pos["status"] == "closed":
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
            pos["entry_open"] = float(entry_rows["Open"].iloc[0])
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
            (df.index.strftime("%Y-%m-%d") > entry_date_str) &
            (df.index.strftime("%Y-%m-%d") < today_str)
        ]

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

    return updated, closed_today, still_open
