"""極上デイ（紙の並走台帳・2026-11-26 新設）

極上と同じシグナル（today_signals_gokujo.json の先頭1銘柄）を、寄りで買って当日中に手仕舞う版。
今の極上（shadow_exit_gokujo.json・損切り-3%/最大3日保有）は一切変えずに並べて記帳し、
数週間比べてから一本化するかを決める。

ルール（1日で完結・持ち越しなし）:
  入口  当日の寄り。寄りが指値(limit_price)より上なら不成立（NOFILL＝極上と同じ扱い）
  損切り 日中に建値-3%に触れたら -3%（STOP・利確より優先＝BTと同じ保守的な順）
  利確  日中に建値+5%に触れたら +5%（TP・極上と同じ水準）
  それ以外 当日の引けで手仕舞い（CLOSE）。極上のRSI利確も引けの値なので、当日引けに含まれる
  枠    1日1銘柄。当日中に決済するので翌日の枠は必ず空く＝極上より件数が増える

台帳: shadow_exit_gokujo_day.json（配信はまだしない＝台帳と集計だけ）
集計: python gokujo_day.py
"""
from __future__ import annotations

import json
import os
from datetime import date

LEDGER        = "shadow_exit_gokujo_day.json"
SIG_FILE      = "today_signals_gokujo.json"
STOP_PCT      = 3.0
TAKE_PROFIT   = 5.0
SIZE          = 1_000_000      # 極上と同じ玉サイズ（shadow_exit.GOKUJO_SIZE に追従）
PX_CAP        = 10_000         # 極上と同じ値がさカット
MAX_PER_DAY   = 1

try:   # 極上の設定が変わったら自動で追従する（import できない環境では上の既定値）
    from shadow_exit import GOKUJO_SIZE as SIZE, GOKUJO_PX_CAP as PX_CAP   # noqa: F811
except Exception:
    pass


def load() -> list[dict]:
    if not os.path.exists(LEDGER):
        return []
    try:
        with open(LEDGER, encoding="utf-8") as f:
            rows = json.load(f)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def save(rows: list[dict]) -> None:
    with open(LEDGER, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def record(rows: list[dict], today: date) -> int:
    """今朝の極上シグナルを pending で記帳する。同じ (ticker, signal_date) は二重に入れない。"""
    if not os.path.exists(SIG_FILE):
        return 0
    with open(SIG_FILE, encoding="utf-8") as f:
        payload = json.load(f)
    today_str = today.strftime("%Y-%m-%d")
    if payload.get("date") != today_str:
        return 0
    seen = {(r["ticker"], r["signal_date"]) for r in rows}
    if any(r["signal_date"] == today_str for r in rows):
        return 0                                   # 1日1銘柄（再実行でも増やさない）
    added = 0
    for s in payload.get("signals", []):
        if added >= MAX_PER_DAY:
            break
        if s.get("direction") != "BUY":
            continue
        if (s.get("prev_close") or 0) > PX_CAP:
            continue
        if (s["ticker"], today_str) in seen:
            continue
        rows.append({
            "signal_date": today_str, "entry_date": today_str,
            "ticker": s["ticker"], "name": s.get("name", s["ticker"]),
            "size": SIZE, "prev_close": s.get("prev_close"),
            "limit_price": s.get("limit_price"),
            "status": "pending", "entry_open": None,
            "pnl_pct": None, "exit_type": None, "exit_date": None,
        })
        added += 1
    return added


def settle_row(pos: dict, o: float, h: float, lo: float, c: float) -> None:
    """エントリー日の日足1本で1件を決着させる（その場で更新）。"""
    lp = pos.get("limit_price")
    if lp and o > lp:
        pos.update(entry_open=o, status="expired", exit_type="NOFILL", exit_date=pos["entry_date"])
        return
    eo = o
    if lo <= eo * (1 - STOP_PCT / 100):
        pnl, ex = -STOP_PCT, "STOP"
    elif h >= eo * (1 + TAKE_PROFIT / 100):
        pnl, ex = TAKE_PROFIT, "TP"
    else:
        pnl, ex = round((c - eo) / eo * 100, 3), "CLOSE"
    pos.update(entry_open=eo, status="closed", pnl_pct=pnl, exit_type=ex,
               exit_date=pos["entry_date"])


def advance(rows: list[dict], today: date, all_data: dict) -> int:
    """前日までの日足で pending を決着させる（当日分は翌朝の実行で決まる）。戻り値=決着件数。"""
    today_str = today.strftime("%Y-%m-%d")
    n = 0
    for pos in rows:
        if pos.get("status") != "pending" or pos["entry_date"] >= today_str:
            continue
        df = all_data.get(pos["ticker"])
        if df is None or df.empty:
            continue
        er = df[df.index.strftime("%Y-%m-%d") == pos["entry_date"]]
        if er.empty:
            continue
        r = er.iloc[0]
        settle_row(pos, float(r["Open"]), float(r["High"]), float(r["Low"]), float(r["Close"]))
        n += 1
    return n


def update(today: date, all_data: dict) -> tuple[int, int]:
    """run_shadow から呼ぶ入口。決着→記帳の順（極上の台帳と同じ）。"""
    rows = load()
    settled = advance(rows, today, all_data)
    added = record(rows, today)
    if settled or added:
        save(rows)
    return settled, added


def summary(rows: list[dict] | None = None) -> str:
    rows = load() if rows is None else rows
    done = [r for r in rows if r.get("status") == "closed"]
    if not done:
        return "極上デイ: 決済済みなし"
    pnls = [r["pnl_pct"] for r in done]
    yen = sum(r["size"] * r["pnl_pct"] / 100 for r in done)
    wins = sum(p > 0 for p in pnls)
    lines = [f"極上デイ: {len(done)}件 {wins}勝{len(done) - wins}敗 "
             f"平均{sum(pnls) / len(pnls):+.2f}% 合計{yen:+,.0f}円"]
    for r in done:
        lines.append(f"  {r['entry_date']} {r['name']} {r['exit_type']} {r['pnl_pct']:+.2f}%")
    nofill = [r for r in rows if r.get("exit_type") == "NOFILL"]
    if nofill:
        lines.append(f"  不成立 {len(nofill)}件: " + "、".join(r["name"] for r in nofill))
    return "\n".join(lines)


if __name__ == "__main__":
    print(summary())
