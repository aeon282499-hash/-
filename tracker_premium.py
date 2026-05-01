"""
tracker_premium.py — 至高版ポジション管理
================================================
positions_premium.json で別管理（既存 positions.json と完全独立）。

【ルール】
  STOP_LOSS  = -3.0%（OCO・ザラ場ヒット）
  TAKE_PROFIT= +5.0%（OCO・ザラ場ヒット）
  MAX_HOLD   = 5 営業日（最大保有日数を超えたら大引け処分）
  RSI回復     終値ベースで RSI≧50 → 翌営業日寄りで決済
  方向       BUY のみ（至高版は買いシグナル特化）
"""

import json
import os
from datetime import date, timedelta

from screener import batch_download_jquants, _jquants_id_token, calc_rsi

POSITIONS_FILE_PRM = "positions_premium.json"
STOP_LOSS_PRM      = 3.0   # %
TAKE_PROFIT_PRM    = 3.0   # % (v2: 5.0→3.0・利確を早める)
MAX_HOLD_PRM       = 5     # 営業日


def load_positions_premium() -> list[dict]:
    if os.path.exists(POSITIONS_FILE_PRM):
        with open(POSITIONS_FILE_PRM, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_positions_premium(positions: list[dict]) -> None:
    with open(POSITIONS_FILE_PRM, "w", encoding="utf-8") as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)
    print(f"[tracker_prm] {POSITIONS_FILE_PRM} を保存しました（{len(positions)}件）")


def add_signals_premium(
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
            "direction":      "BUY",
            "prev_close":     sig.get("prev_close", 0),
            "entry_open":     None,
            "status":         "pending",
            "hold_days":      0,
            "pnl_pct":        None,
            "unrealized_pnl": None,
            "exit_type":      None,
            "exit_date":      None,
            "score":          sig.get("score"),
        })
    return positions


def update_positions_premium(positions: list[dict], today: date) -> tuple[list[dict], list[dict], list[dict]]:
    """
    オープンポジションを前日データで更新する。BUY 専用。

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

    print(f"[tracker_prm] {len(tickers)} 銘柄のデータ取得中（結果チェック用）...")
    token    = _jquants_id_token()
    all_data = batch_download_jquants(token, start=start, end=end, tickers=tickers)

    closed_today: list[dict] = []
    still_open:   list[dict] = []
    updated:      list[dict] = []

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

        # ── pending → open（エントリー日始値を確定）──
        if pos["status"] == "pending":
            entry_rows = df[df.index.strftime("%Y-%m-%d") == entry_date_str]
            if entry_rows.empty:
                updated.append(pos)
                continue
            pos["entry_open"] = float(entry_rows["Open"].iloc[0])
            pos["status"]     = "open"
            pos["hold_days"]  = 0

        entry_open = pos["entry_open"]
        if not entry_open or entry_open <= 0:
            updated.append(pos)
            continue

        stop_price = entry_open * (1 - STOP_LOSS_PRM   / 100)
        tp_price   = entry_open * (1 + TAKE_PROFIT_PRM / 100)

        post_df = df[
            (df.index.strftime("%Y-%m-%d") >= entry_date_str) &
            (df.index.strftime("%Y-%m-%d") < today_str)
        ]

        pos["hold_days"] = 0
        closed = False
        for dt_idx, row in post_df.iterrows():
            pos["hold_days"] += 1
            day_high  = float(row["High"])
            day_low   = float(row["Low"])
            day_close = float(row["Close"])
            check_date_str = dt_idx.strftime("%Y-%m-%d")

            # OCO（BUYのみ）
            if day_low <= stop_price:
                pos.update(pnl_pct=-STOP_LOSS_PRM, exit_type="STOP",
                           exit_date=check_date_str, status="closed")
                closed = True; break
            if day_high >= tp_price:
                pos.update(pnl_pct=+TAKE_PROFIT_PRM, exit_type="TP",
                           exit_date=check_date_str, status="closed")
                closed = True; break

            # RSI回復（≧50で早期決済）
            hist_df  = df[df.index <= dt_idx]
            rsi_now  = calc_rsi(hist_df["Close"].dropna())
            rsi_exit = rsi_now is not None and rsi_now >= 50
            pnl = (day_close - entry_open) / entry_open * 100

            if rsi_exit or pos["hold_days"] >= MAX_HOLD_PRM:
                exit_type = "RSI" if rsi_exit else "MAXHOLD"
                pos.update(pnl_pct=round(pnl, 3), exit_type=exit_type,
                           exit_date=check_date_str, status="closed")
                closed = True; break

        if closed:
            closed_today.append(pos)
        else:
            last_rows = df[df.index.strftime("%Y-%m-%d") < today_str]
            if not last_rows.empty:
                last_close = float(last_rows["Close"].iloc[-1])
                pnl = (last_close - entry_open) / entry_open * 100
                pos["unrealized_pnl"] = round(pnl, 3)
            still_open.append(pos)

        updated.append(pos)

    return updated, closed_today, still_open
