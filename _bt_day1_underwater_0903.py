# -*- coding: utf-8 -*-
"""_bt_kiwami_slots_dc12.py — 枠数撤去/拡大の実測（dc1.2×100万・2026-08-05）（2026-08-05）。

エンジンは_bt_kiwami_axes2.py（参照エンジン・select→sim分離・dc1.2採用の根拠数字を出したもの）と同一。
構成=dc≤1.2 × 株価≤1万円 × 業種cap3 × 1日5件 × 3枠 × 1玉100万 × 損切り-3%/TP+5%/RSI50/3日。
月別の帰属は決済日（_bt_kiwami_monthly.py 2026-07-29の慣習に合わせる＝口座に現れる日）。
本番無変更・出力を月別にしただけ。

実行: python -X utf8 _bt_kiwami_monthly_dc12.py
"""
from __future__ import annotations

import json
import pickle

import numpy as np
import pandas as pd

MAXH, MAX_SIG, SECTOR_CAP = 3, 5, 3
TP, LIVE_STOP = 5.0, 3.0
SIZE, SLOTS = 1_000_000, 3

C0 = pd.read_csv("_probe_shortdc_atr.csv", parse_dates=["entry"])
C0 = C0.sort_values(["entry", "score"], ascending=[True, False]).reset_index(drop=True)

old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
SER = {}
for tk in C0["ticker"].unique():
    dfs = [d for src in (old["all_data"], new["all_data"])
           if (d := src.get(tk)) is not None and len(d)]
    if not dfs:
        continue
    df = pd.concat(dfs).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    cl = df["Close"].astype(float)
    dlt = cl.diff()
    ag = dlt.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
    al = (-dlt).clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
    rsi = (100 - 100 / (1 + ag / al.replace(0, np.nan))).round(2)
    SER[tk] = (df["Open"].astype(float).to_numpy(), df["High"].astype(float).to_numpy(),
               df["Low"].astype(float).to_numpy(), cl.to_numpy(), rsi.to_numpy(),
               {d: i for i, d in enumerate(df.index.strftime("%Y-%m-%d"))}, len(cl))
del old, new
SECMAP = json.load(open("sector33_map.json", encoding="utf-8"))
ALLDAYS = sorted(C0["entry"].unique())
GDI = {d: i for i, d in enumerate(ALLDAYS)}
DAYSTR = [pd.Timestamp(d).strftime("%Y-%m-%d") for d in ALLDAYS]

n = len(C0)
ds = C0["entry"].dt.strftime("%Y-%m-%d").to_numpy()
E = np.full(n, np.nan)
OP = np.full((n, MAXH), np.nan); HI = np.full((n, MAXH), np.nan)
LO = np.full((n, MAXH), np.nan); CL = np.full((n, MAXH), np.nan)
RS = np.full((n, MAXH), np.nan)
for i in range(n):
    a = SER.get(C0["ticker"].iat[i])
    if a is None:
        continue
    o, h, l, c, r, pos, ln = a
    p = pos.get(ds[i])
    if p is None or p + MAXH - 1 >= ln:
        continue
    e = o[p]
    if not (e > 0) or np.isnan(e):
        continue
    E[i] = e
    for k in range(MAXH):
        OP[i, k], HI[i, k], LO[i, k], CL[i, k], RS[i, k] = o[p+k], h[p+k], l[p+k], c[p+k], r[p+k]
by_day: dict = {}
for i in range(n):
    by_day.setdefault(GDI[C0["entry"].iat[i]], []).append(i)
TICK = C0["ticker"].to_numpy(); YEAR = C0["year"].to_numpy()
SEC = np.array([SECMAP.get(t) or f"__u{t}" for t in TICK], dtype=object)
PRICE = C0["price"].to_numpy(); DC = C0["days_cover"].to_numpy()

pnl = np.full(n, np.nan); exo = np.zeros(n, dtype=np.int8); done = ~np.isfinite(E)
sl = E * (1 - LIVE_STOP / 100); tl = E * (1 + TP / 100)
for k in range(MAXH):
    live = ~done
    if not live.any():
        break
    if k > 0:
        op = OP[:, k]
        g = live & (op > 0) & np.isfinite(op) & ((op <= sl) | (op >= tl))
        pnl[g] = (op[g] - E[g]) / E[g] * 100; exo[g] = k; done |= g; live = ~done
    s = live & (LO[:, k] <= sl); pnl[s] = -LIVE_STOP; exo[s] = k; done |= s; live = ~done
    t = live & (HI[:, k] >= tl); pnl[t] = TP; exo[t] = k; done |= t; live = ~done
    r = live & (((RS[:, k] >= 50) & np.isfinite(RS[:, k])) | (k == MAXH - 1))
    pnl[r] = (CL[r, k] - E[r]) / E[r] * 100; exo[r] = k; done |= r

mask = (PRICE <= 10000) & ~(DC > 1.2)          # 採用構成 dc1.2×1万円
ok = np.isfinite(pnl) & mask
d0 = (CL[:, 0] - E) / E * 100
df = pd.DataFrame({"d0": d0[ok], "pnl": pnl[ok], "exo": exo[ok]})
print(f"候補{len(df)}件  初日引けで含み損の割合 {(df.d0<0).mean()*100:.1f}%  初日平均{df.d0.mean():+.2f}%")
bins=[-99,-3,-2,-1,0,1,2,99]; labs=["≤-3%","-3〜-2","-2〜-1","-1〜0","0〜1","1〜2",">2%"]
df["b"]=pd.cut(df.d0,bins,labels=labs)
g=df.groupby("b",observed=True).agg(n=("pnl","size"),final=("pnl","mean"),win=("pnl",lambda x:(x>0).mean()*100))
g["改善"]=g.final-df.groupby("b",observed=True).d0.mean()
print(g.round(2).to_string())
u=df[df.d0<0]; print(f"\n初日含み損の玉 n={len(u)} 初日平均{u.d0.mean():+.2f}% → 最終平均{u.pnl.mean():+.2f}% 最終勝率{(u.pnl>0).mean()*100:.1f}%  初日から改善した割合{(u.pnl>u.d0).mean()*100:.1f}%")
