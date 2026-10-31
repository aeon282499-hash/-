# -*- coding: utf-8 -*-
"""chk3: 営業日カレンダー: BTの決済日(DAYS[d+EXO]) と 銘柄自身の足の日付 が一致するか / 祝日またぎ"""
import os, sys, pickle, time
import numpy as np, pandas as pd
os.chdir(r"C:\Users\星野\Documents\my-first-project")
OUT = r"C:\Users\星野\AppData\Local\Temp\claude\C--Users---\d8b95c87-1d6b-4a78-b3bc-5f808d8f4331\scratchpad\audit_gokujo"
X = pd.read_pickle(os.path.join(OUT, "R0_10y.pkl"))
Z = pickle.load(open("_bt_gokujo_cache_0926.pkl", "rb")); DAYS = Z["DAYS"]
ALL = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
mism = []; hol = 0; nrow_gap = 0
for r in X.itertuples():
    df = ALL[r.ticker].dropna(subset=["Close"])
    ix = df.index
    k = ix.get_loc(pd.Timestamp(r.entry))
    exo = (DAYS.get_loc(pd.Timestamp(r.exit)) - DAYS.get_loc(pd.Timestamp(r.entry)))
    real_exit = ix[k + exo]
    if real_exit != pd.Timestamp(r.exit): mism.append((r.entry.date(), r.ticker, r.exit.date(), real_exit.date()))
    # 営業日3つの間に暦日が5超(=祝日またぎ)
    if (ix[min(k + 2, len(ix) - 1)] - ix[k]).days > 4: hol += 1
print("玉", len(X), " BT決済日≠銘柄の足の日付", len(mism), mism[:10])
print("保有3営業日の間に祝日/連休を挟む玉", hol, f"({hol/len(X)*100:.0f}%)")
# 建て日の前営業日(=シグナル日)が銘柄の足とDAYSで一致するか
bad_sig = 0
for r in X.itertuples():
    df = ALL[r.ticker].dropna(subset=["Close"]); ix = df.index
    k = ix.get_loc(pd.Timestamp(r.entry)); d = DAYS.get_loc(pd.Timestamp(r.entry))
    if ix[k - 1] != DAYS[d - 1]: bad_sig += 1
print("シグナル日(前営業日)がDAYSと銘柄の足で食い違う玉", bad_sig)
