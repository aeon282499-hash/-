# -*- coding: utf-8 -*-
"""fetch_short_positions.py — 空売り残高報告(/markets/short-sale-report)の10年スナップ（2026-08-03）。

決算持ち越しのイベント銘柄(907銘柄=_shortpos_tickers.txt)について、大口空売り残高報告
（0.5%以上・報告者別・DiscDate/CalcDate/ShrtPosToSO）を全履歴取得して保存する。
10年ローリング窓の対象＝取れるうちにpklスナップ（DATA_CATALOG.md §1に追加すること）。

出力: _short_positions_10y.pkl  {code4: DataFrame[DiscDate, CalcDate, SSName, ShrtPosToSO, ShrtPosShares]}
実行: python -X utf8 fetch_short_positions.py
"""
from __future__ import annotations

import pickle
import time

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(".env")
from screener import _JQUANTS_BASE, _JQUANTS_VERIFY_SSL, _jquants_id_token  # noqa: E402

import sys

TICKER_FILE = sys.argv[1] if len(sys.argv) > 1 else "_shortpos_tickers.txt"
OUT_PKL = sys.argv[2] if len(sys.argv) > 2 else "_short_positions_10y.pkl"

tok = _jquants_id_token()
tickers = [t.strip() for t in open(TICKER_FILE, encoding="utf-8") if t.strip()]
out: dict = {}
t0 = time.time()
for i, tk in enumerate(tickers):
    code5 = tk.split(".")[0][:4] + "0"
    rows, key = [], None
    while True:
        params = {"code": code5}
        if key:
            params["pagination_key"] = key
        for attempt in range(3):
            try:
                r = requests.get(f"{_JQUANTS_BASE}/markets/short-sale-report",
                                 headers={"x-api-key": tok}, params=params,
                                 timeout=(10, 50), verify=_JQUANTS_VERIFY_SSL)
                if r.status_code == 429:
                    time.sleep(60)
                    continue
                r.raise_for_status()
                break
            except Exception as e:
                if attempt == 2:
                    print(f"  [warn] {tk} 取得失敗: {e}", flush=True)
                    r = None
                time.sleep(5)
        if r is None:
            break
        d = r.json()
        rows += d.get("data", [])
        key = d.get("pagination_key")
        time.sleep(1.2)
        if not key:
            break
    if rows:
        df = pd.DataFrame(rows)[["DiscDate", "CalcDate", "SSName", "ShrtPosToSO", "ShrtPosShares"]]
        df["ShrtPosToSO"] = pd.to_numeric(df["ShrtPosToSO"], errors="coerce")
        out[tk.split(".")[0][:4]] = df.sort_values("DiscDate").reset_index(drop=True)
    if (i + 1) % 50 == 0:
        el = time.time() - t0
        print(f"  {i+1}/{len(tickers)} ({el/60:.0f}分) 報告あり{len(out)}銘柄", flush=True)

pickle.dump(out, open(OUT_PKL, "wb"))
n_rows = sum(len(v) for v in out.values())
print(f"[save] {OUT_PKL} 報告あり{len(out)}/{len(tickers)}銘柄 / {n_rows:,}報告", flush=True)
