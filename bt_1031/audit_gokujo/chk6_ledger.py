# -*- coding: utf-8 -*-
"""chk6: 紙台帳との食い違いの原因調べ: 2282 日本ハム(9/25〜29 分割)の生の足 / 10-01 9143, 10-09 9831, 10-22 8304 の特徴量"""
import os, sys, pickle, json
import numpy as np, pandas as pd
os.chdir(r"C:\Users\星野\Documents\my-first-project")
OUT = r"C:\Users\星野\AppData\Local\Temp\claude\C--Users---\d8b95c87-1d6b-4a78-b3bc-5f808d8f4331\scratchpad\audit_gokujo"
B = pickle.load(open("_bars_daily_5y_1024.pkl", "rb"))
x = B[(B.Code == "22820") & (B.Date >= "2026-09-24") & (B.Date <= "2026-09-30")][["Date", "O", "H", "L", "C", "AdjFactor"]]
print("2282 日本ハム 生の足:"); print(x.to_string(index=False))
E0 = 6022.0; print(f"  建値6022 損切 {E0*0.97:.1f}  → 9/25の安値{x.iloc[1].L} 9/28の安値{x.iloc[2].L} 9/29(分割後)安値{x.iloc[3].L}×3={x.iloc[3].L*3:.1f}")
CD = pd.read_pickle(os.path.join(OUT, "jq_cands.pkl"))
MG = pickle.load(open("_margin_10y_full.pkl", "rb"))
for code, d in (("91430", "2026-10-01"), ("94010", "2026-10-01"), ("98310", "2026-10-09"), ("83040", "2026-10-22"), ("94340", "2026-10-27")):
    r = CD[(CD.code == code) & (CD.entry == d)]
    m = MG.get(code[:4]); last_m = pd.to_datetime(m.index).max().date() if m is not None and len(m) else None
    print(code, d, r[["score", "rawC", "earn", "dc", "rsi", "dev"]].round(3).to_dict("records"), " 信用残の最終日", last_m)
# 10/1 と 10/22 の候補一覧
for d in ("2026-10-01", "2026-10-09", "2026-10-22"):
    print(d, CD[CD.entry == d].sort_values("score", ascending=False)[["code", "score", "rawC", "earn", "dc"]].round(3).to_string(index=False))
# earnings_calendar.json に 9143/8304/9831 の10月開示はあるか
ec = json.load(open("earnings_calendar.json", encoding="utf-8"))
for tk in ("9143.T", "8304.T", "9831.T", "9401.T"):
    print(tk, [d for d in ec.get(tk, []) if d >= "2026-09-01"])
