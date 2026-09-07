# -*- coding: utf-8 -*-
"""tachibana_board_recorder.py — 土俵リストの銘柄の「板10本＋現在値＋出来高」を寄り後1時間、10秒おきに記録する。

目的（2026-09-07 本人依頼）: 勝っているデイトレーダーの情報源は板と歩み値であって前夜の指標ではない。
  俺たちのデータ（日足・15分足）にはその情報が無く、スキャル/板読みの買いは「審判不能」の領域だった。
  ここで初めて「勝者と同じ情報」を貯める。10秒スナップショットの現在値と累積出来高の差分が歩み値の近似になる。
対象: arena_watchlist.json の board_codes（前夜配信でCIがコミット→起動時に git pull で取り込む）。
      無ければ空振り終了。時間窓 am=08:55〜10:05（寄り前気配を含む）/ pm=12:25〜13:05。
負荷: 1要求/10秒（120銘柄まで1要求）＝am約420要求。MIN_INTERVAL は既存どおり。共用配慮で高頻度化しない。
保存: board_ticks/YYYY-MM-DD_{am|pm}.pkl  {"codes", "rows": DataFrame(ts, code, ...)}  60秒ごとに上書き保存。
実行: python -X utf8 tachibana_board_recorder.py --slot am [--interval 10] [--codes 7203,6758] [--seconds 30]
      Windowsタスク TachibanaBoard_am 08:55 / TachibanaBoard_pm 12:25（平日・ログオン中のみ）
読込: from tachibana_board_recorder import load_days; load_days()
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import pickle
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
from tachibana import TachibanaClient, TachibanaError  # noqa: E402
from tachibana.client import BOARD_COLUMNS  # noqa: E402

OUT_DIR = ROOT / "board_ticks"
LOG = ROOT / "logs" / "board_recorder.log"
ARENA = ROOT / "arena_watchlist.json"
WINDOWS = {"am": ("08:55", "10:05"), "pm": ("12:25", "13:05")}
PRICE_COLS = ("pDPP", "tDPP:T", "pDOP", "pDHP", "pDLP", "pDV", "pDJ", "pVWAP", "pPRP", "pQAP", "pQBP", "pAV", "pBV", "pAAV", "pABV")
NAMES = {"pDPP": "last", "tDPP:T": "last_time", "pDOP": "open", "pDHP": "high", "pDLP": "low", "pDV": "volume", "pDJ": "turnover",
         "pVWAP": "vwap", "pPRP": "prev_close", "pQAP": "ask", "pQBP": "bid", "pAV": "ask_qty", "pBV": "bid_qty",
         "pAAV": "mkt_sell_qty", "pABV": "mkt_buy_qty", "pQOV": "over", "pQUV": "under"}
for _i in range(1, 11):
    NAMES[f"pGAP{_i}"] = f"a{_i}p"; NAMES[f"pGAV{_i}"] = f"a{_i}q"; NAMES[f"pGBP{_i}"] = f"b{_i}p"; NAMES[f"pGBV{_i}"] = f"b{_i}q"


def git_pull(log) -> None:
    """前夜配信でCIがコミットした arena_watchlist.json を取り込む。失敗しても続行（ローカルの前回分で動く）。"""
    try:
        r = subprocess.run(["git", "pull", "--ff-only", "--quiet"], cwd=ROOT, capture_output=True, text=True, timeout=90)
        log(f"git pull rc={r.returncode} {r.stderr.strip()[:120]}")
    except Exception as e:
        log(f"git pull 失敗（続行）: {e}")


def load_codes(log, today: str) -> list[str]:
    if not ARENA.exists():
        log("arena_watchlist.json なし"); return []
    w = json.load(open(ARENA, encoding="utf-8"))
    codes = [str(c) for c in w.get("board_codes") or [r["code"] for r in w.get("rows", [])]]
    if w.get("target_date") != today:
        log(f"⚠️ 土俵リストは {w.get('target_date')} 分（今日={today}）。古いが記録は続ける")
    return codes[:120]


def record(slot: str, codes: list[str], interval: float, seconds: int, log) -> Path | None:
    tc = TachibanaClient(); tc.MIN_INTERVAL = 0.5; tc.ensure_session()
    day = tc.date_info().get("sTheDay") or datetime.now().strftime("%Y%m%d")
    day_s = f"{day[:4]}-{day[4:6]}-{day[6:]}"
    OUT_DIR.mkdir(exist_ok=True)
    out = OUT_DIR / f"{day_s}_{slot}.pkl"
    cols = PRICE_COLS + BOARD_COLUMNS
    now = datetime.now()
    if seconds:
        end_at = now + timedelta(seconds=seconds)
    else:
        hh, mm = map(int, WINDOWS[slot][1].split(":"))
        end_at = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    rows: list[dict] = []; n_req = 0; last_save = time.monotonic(); t0 = time.monotonic()
    log(f"記録開始 {slot} {len(codes)}銘柄 {interval}s間隔 → {end_at:%H:%M:%S} / {out.name}")
    while datetime.now() < end_at:
        tick = time.monotonic()
        try:
            res = tc.market_price(codes, cols); n_req += 1
            ts = datetime.now().strftime("%H:%M:%S")
            for r in res:
                d = {"ts": ts, "code": r.get("sIssueCode")}
                for k, v in r.items():
                    if k in NAMES:
                        d[NAMES[k]] = v
                rows.append(d)
        except TachibanaError as e:
            log(f"取得失敗: {e}")
            try:
                tc.ensure_session()
            except Exception:
                pass
        except Exception as e:
            log(f"例外: {e}")
        if time.monotonic() - last_save > 60 and rows:
            _save(out, codes, rows, slot, day_s); last_save = time.monotonic()
        time.sleep(max(0.0, interval - (time.monotonic() - tick)))
    if rows:
        _save(out, codes, rows, slot, day_s)
    log(f"記録終了 要求{n_req}回 行{len(rows):,} {time.monotonic()-t0:.0f}s → {out}")
    return out if rows else None


def _save(out: Path, codes: list[str], rows: list[dict], slot: str, day_s: str) -> None:
    df = pd.DataFrame(rows)
    for c in df.columns:
        if c not in ("ts", "code", "last_time"):
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", ""), errors="coerce")
    pickle.dump({"date": day_s, "slot": slot, "codes": codes, "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "rows": df},
                open(out, "wb"), protocol=pickle.HIGHEST_PROTOCOL)


def load_days() -> pd.DataFrame:
    frames = []
    for p in sorted(OUT_DIR.glob("*.pkl")):
        d = pickle.load(open(p, "rb")); df = d["rows"].copy()
        df.insert(0, "slot", d["slot"]); df.insert(0, "date", d["date"]); frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slot", default="am", choices=list(WINDOWS))
    ap.add_argument("--interval", type=float, default=10.0)
    ap.add_argument("--codes", default="")
    ap.add_argument("--seconds", type=int, default=0, help="テスト用: 指定秒だけ記録して終了")
    ap.add_argument("--no-pull", action="store_true")
    a = ap.parse_args()
    LOG.parent.mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(LOG, encoding="utf-8"), logging.StreamHandler(sys.stdout)])
    log = logging.getLogger("board").info
    log(f"=== board recorder slot={a.slot}")
    today = datetime.now().strftime("%Y-%m-%d")
    if datetime.now().weekday() >= 5 and not a.seconds:
        log("週末 → 終了"); return 0
    if not a.no_pull:
        git_pull(log)
    codes = [c.strip() for c in a.codes.split(",") if c.strip()] or load_codes(log, today)
    if not codes:
        log("対象なし → 終了"); return 0
    try:
        record(a.slot, codes, a.interval, a.seconds, log)
    except TachibanaError as e:
        logging.getLogger("board").error(f"失敗: {e}"); return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
