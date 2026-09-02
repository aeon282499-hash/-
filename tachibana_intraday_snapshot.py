# -*- coding: utf-8 -*-
"""tachibana_intraday_snapshot.py — 立花APIの時価スナップショットを1日2回（後場寄り直後・大引け後）保存する。

目的: ギャップフェード（寄り+3〜8%→12:30空売り→引成）の検証データを **正確な約定価格** で前向きに貯める。
      今は Yahoo 15分足（60日ローリング・足ラベルの罠あり）が唯一の分足で、10月150日再測定の材料もそれ。
      立花なら 寄り値(pDOP)・後場寄り直後の現在値(pDPP@12:31)・VWAP・高安・出来高・売買代金・大引け(pDPP@15:35) を
      全銘柄分そのまま取れる（120銘柄/要求・約32要求・共用配慮で0.5秒間隔＝1回30秒）。
保存: tachibana_intraday/YYYY-MM-DD_{pm_open|close}.pkl  {"taken_at", "slot", "prices": DataFrame}
実行: python -X utf8 tachibana_intraday_snapshot.py --slot pm_open|close [--limit N]
      Windowsタスク TachibanaIntraday_pm_open 12:31 / TachibanaIntraday_close 15:35（大引けは15:30）
読込: from tachibana_intraday_snapshot import load_days; load_days()  # 日付×銘柄のロング形式（両スロット結合）
"""
from __future__ import annotations

import argparse
import logging
import os
import pickle
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
from tachibana import TachibanaClient, TachibanaError  # noqa: E402

OUT_DIR = ROOT / "tachibana_intraday"
LOG = ROOT / "logs" / "tachibana_intraday.log"
COLS = ("pDOP", "tDOP:T", "pDPP", "tDPP:T", "pDHP", "pDLP", "pDV", "pDJ", "pVWAP", "pPRP", "pQAP", "pQBP", "pAV", "pBV", "pDYRP")
NAMES = {"pDOP": "open", "tDOP:T": "open_time", "pDPP": "last", "tDPP:T": "last_time", "pDHP": "high", "pDLP": "low", "pDV": "volume",
         "pDJ": "turnover", "pVWAP": "vwap", "pPRP": "prev_close", "pQAP": "ask", "pQBP": "bid", "pAV": "ask_qty", "pBV": "bid_qty", "pDYRP": "chg_pct"}


def universe_codes() -> list[str]:
    d = pickle.load(open(ROOT / "jquants_cache.pkl", "rb"))
    return [t.removesuffix(".T") for t, _ in d["universe"]]


def take(slot: str, limit: int = 0, log=print) -> Path:
    codes = universe_codes()
    if limit:
        codes = codes[:limit]
    tc = TachibanaClient(); tc.MIN_INTERVAL = 0.5; tc.ensure_session()
    t0 = time.time()
    rows = tc.market_price(codes, COLS)
    df = pd.DataFrame(rows).rename(columns={"sIssueCode": "code", **NAMES})
    for c in df.columns:
        if c not in ("code", "open_time", "last_time"):
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", ""), errors="coerce")
    df = df[df["prev_close"].notna() | df["last"].notna()].reset_index(drop=True)   # 寄り前は現在値が空
    day = tc.date_info().get("sTheDay") or datetime.now().strftime("%Y%m%d")
    OUT_DIR.mkdir(exist_ok=True)
    out = OUT_DIR / f"{day[:4]}-{day[4:6]}-{day[6:]}_{slot}.pkl"
    pickle.dump({"taken_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "slot": slot, "prices": df}, open(out, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
    gu = df[(df.prev_close > 0)]
    gu = gu[(gu.open / gu.prev_close - 1) * 100 >= 3]
    log(f"保存 {out} 銘柄 {len(df)} / 寄り+3%以上 {len(gu)} / {time.time()-t0:.0f}s")
    return out


def load_days() -> pd.DataFrame:
    frames = []
    for p in sorted(OUT_DIR.glob("*.pkl")):
        d, s = p.stem.split("_", 1)
        snap = pickle.load(open(p, "rb")); df = snap["prices"].copy()
        df.insert(0, "slot", s); df.insert(0, "date", d); frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> int:
    ap = argparse.ArgumentParser(); ap.add_argument("--slot", required=True, choices=["pm_open", "close"]); ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    LOG.parent.mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(LOG, encoding="utf-8"), logging.StreamHandler(sys.stdout)])
    log = logging.getLogger("intraday").info
    log(f"=== intraday snapshot slot={a.slot}")
    try:
        take(a.slot, a.limit, log)
    except TachibanaError as e:
        logging.getLogger("intraday").error(f"失敗: {e}"); return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
