# -*- coding: utf-8 -*-
"""_fetch_jsf_history.py — 日本証券金融（taisyaku.jp）の銘柄別 貸借取引 日次履歴CSVを取得して pkl 化（2026-09-03）。

サイトの銘柄詳細ページは 2023-04-03 以降の日次（融資/貸株の新規・返済・残高、差引残高、貸借値段、
品貸料率＝逆日歩、最高/最低料率、応札ランク、制限措置）を1銘柄1CSVで返す。
立花APIの証金残は当日値のみなので、過去分はここから。→ _jsf_daily.pkl = {code: DataFrame(index=日付)}

使い方:
    python -X utf8 _fetch_jsf_history.py --fade          # フェード候補(2023-05以降)の銘柄
    python -X utf8 _fetch_jsf_history.py --codes 7203,6501
再実行は続きから。共用サイトなので1.5秒間隔。
"""
from __future__ import annotations

import argparse
import io
import pickle
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

OUT = Path("_jsf_daily.pkl")
BASE = "https://www.taisyaku.jp/app/stock/"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/128 Safari/537.36",
      "Accept-Language": "ja"}
COLMAP = {"申込日": "date", "融資新規（株）": "loan_new", "融資返済（株）": "loan_repay", "融資残高（株）": "loan_bal",
          "貸株新規（株）": "lend_new", "貸株返済（株）": "lend_repay", "貸株残高（株）": "lend_bal",
          "差引残高（株）": "net_bal", "貸借値段（円）": "price", "品貸料率（品貸日数分/円）": "premium",
          "品貸日数": "premium_days", "品貸料率（年率換算/％）": "premium_pct", "最高料率（品貸日数分/円）": "max_rate",
          "最低料率（品貸日数分/円）": "min_rate", "応札ランク": "bid_rank", "制限措置": "restriction",
          "臨時措置": "temp_measure", "特別措置": "special_measure", "貸借区分": "taishaku_kubun"}


def fade_codes() -> list[str]:
    P = pd.read_pickle("_fade_pool_v5_100.pkl")
    G = P[(P.gain >= 7.0) & (P.vr < 6.0) & (P.atr >= 5.0) & (P.dev >= 12.0)
          & (P.tov >= 3e8) & (P.rng > 5.0) & (P.vol_avg >= 100_000) & (P.ent >= "2023-04-01")]
    return sorted({t.replace(".T", "") for t in G.ticker.unique()})


def fetch_one(s: requests.Session, code: str) -> pd.DataFrame | None:
    d = s.get(f"{BASE}detail/{code}-01", timeout=30, headers={"Referer": BASE + "search"})
    if d.status_code != 200:
        return None
    m = re.search(r'name="csrf_test_name" value="([0-9a-f]+)"', d.text)
    nm = re.search(r'name="orgMgrMei" value="([^"]*)"', d.text)
    if not m:
        return None
    f = {"csrf_test_name": m.group(1), "orgMgrCd": code, "orgMgrMei": nm.group(1) if nm else "", "sort": "", "page": "",
         "fsort": "", "fpage": "", "mkYmdFrom": "2015 / 01 / 01", "mkYmdTo": "2030 / 12 / 31", "kjnYmdDays": "", "trjoKbn": "01"}
    s.post(f"{BASE}detail/{code}/search", data=f, headers={"Referer": f"{BASE}detail/{code}-01"}, timeout=60)
    c = s.get(f"{BASE}detail/{code}/csv", timeout=120, headers={"Referer": f"{BASE}detail/{code}/search"})
    if c.status_code != 200 or "html" in (c.headers.get("Content-Type") or ""):
        return None
    df = pd.read_csv(io.BytesIO(c.content), encoding="cp932")
    if df.empty or "申込日" not in df.columns:
        return pd.DataFrame()
    df = df.rename(columns=COLMAP)
    df["date"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"]).set_index("date").sort_index()
    for col in ("loan_new", "loan_repay", "loan_bal", "lend_new", "lend_repay", "lend_bal", "net_bal", "price",
                "premium", "premium_days", "premium_pct", "max_rate", "min_rate"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "").replace({"-": None, "*****": None}), errors="coerce")
    keep = [c for c in COLMAP.values() if c in df.columns and c != "date"]
    return df[keep]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fade", action="store_true")
    ap.add_argument("--codes", default="")
    ap.add_argument("--sleep", type=float, default=1.5)
    a = ap.parse_args()
    codes = [c.strip() for c in a.codes.split(",") if c.strip()] if a.codes else fade_codes()
    data: dict = pickle.load(open(OUT, "rb")) if OUT.exists() else {}
    todo = [c for c in codes if c not in data]
    print(f"対象 {len(codes)} / 未取得 {len(todo)}", flush=True)
    s = requests.Session(); s.headers.update(UA)
    s.get(BASE, timeout=30)
    t0 = time.time(); ok = err = 0
    for i, code in enumerate(todo, 1):
        try:
            df = fetch_one(s, code)
            if df is None:
                err += 1; print(f"  NG {code}", flush=True)
            else:
                data[code] = df; ok += 1
        except Exception as e:  # noqa: BLE001
            err += 1; print(f"  ERR {code}: {e}", flush=True)
        if i % 20 == 0 or i == len(todo):
            pickle.dump(data, open(OUT, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
            print(f"  {i}/{len(todo)} ok={ok} err={err} {time.time()-t0:.0f}s", flush=True)
        time.sleep(a.sleep)
    pickle.dump(data, open(OUT, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
    spans = [(c, df.index.min().date(), len(df)) for c, df in data.items() if len(df)]
    print(f"保存 {OUT} 銘柄 {len(data)} / 最古 {min(s[1] for s in spans) if spans else '-'} / 行数中央値 {pd.Series([s[2] for s in spans]).median() if spans else 0}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
