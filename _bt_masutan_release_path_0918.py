# -*- coding: utf-8 -*-
"""解除日(最終規制公表日=0日)前後の1日ごとの値動き（TOPIX差・%）。本人「解除3日前から上がらない?」(2026-09-18)"""
import pickle, bisect, numpy as np, pandas as pd
T = pd.read_csv("_bt_masutan_release_0918.csv", dtype={"code": str})
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb")); new = pickle.load(open("jquants_cache.pkl", "rb"))
SER = {}
for c in T.code.unique():
    dfs = [d for src in (old["all_data"], new["all_data"]) if (d := src.get(c + ".T")) is not None and len(d)]
    if not dfs: continue
    df = pd.concat(dfs).sort_index(); df = df[~df.index.duplicated(keep="last")]
    cl = df["Close"].astype(float); cl = cl[cl > 0]
    SER[c] = (cl.to_numpy(), [d.strftime("%Y-%m-%d") for d in cl.index])
ix = pd.read_pickle("_indices_10y.pkl"); ix = ix[ix.Code == "0000"].sort_values("Date")
TPX = ix.C.astype(float).to_numpy(); TD = ix.Date.astype(str).tolist()
OFF = list(range(-6, 6))            # 日次リターン r[k] = close[k]/close[k-1]-1, k=-5..+5
rows = []
for r in T.itertuples():
    s = SER.get(r.code)
    if s is None: continue
    cl, ds = s; i = bisect.bisect_left(ds, r.end); j = bisect.bisect_left(TD, r.end)
    if ds[i] != r.end if i < len(ds) else True: continue
    if i - 6 < 0 or i + 5 >= len(cl) or j + 5 >= len(TPX): continue
    d = {"yr": r.end[:4], "n": r.n}
    for k in range(-5, 6):
        d[k] = (cl[i + k] / cl[i + k - 1] - 1 - (TPX[j + k] / TPX[j + k - 1] - 1)) * 100
    d["c_m3_0"] = (cl[i] / cl[i - 3] - 1 - (TPX[j] / TPX[j - 3] - 1)) * 100         # 3日前終値→0日終値
    d["c_m3_p1"] = (cl[i + 1] / cl[i - 3] - 1 - (TPX[j + 1] / TPX[j - 3] - 1)) * 100  # →解除実施日終値
    d["c_m3_p3"] = (cl[i + 3] / cl[i - 3] - 1 - (TPX[j + 3] / TPX[j - 3] - 1)) * 100
    rows.append(d)
P = pd.DataFrame(rows)
def show(P, t):
    print(f"\n== {t} (n={len(P)}) ==  日別 TOPIX差の平均 / 中央値 / 上昇率   (0日=規制フラグ最終日・+1=解除実施日)")
    for k in range(-5, 6):
        v = P[k]; print(f"  {k:+d}日  {v.mean():+6.2f}% / {v.median():+6.2f}% / {(v > 0).mean() * 100:3.0f}%")
    for k, lab in (("c_m3_0", "3日前終値→0日終値"), ("c_m3_p1", "3日前終値→解除実施日終値"), ("c_m3_p3", "3日前終値→+3日終値")):
        v = P[k]; print(f"  {lab:<14} {v.mean():+6.2f}% / {v.median():+6.2f}% / 上昇率{(v > 0).mean() * 100:3.0f}%")
show(P, "全期間 2016-2026")
show(P[P.yr == "2025"], "2025年"); show(P[P.yr == "2026"], "2026年(〜7月)"); show(P[P.yr.isin(["2025", "2026"])], "2025-26合算")
print("\n年別: 3日前→0日 / 3日前→+1日 / 3日前→+3日 (平均TOPIX差%・上昇率%)")
g = P.groupby("yr").agg(n=("n", "size"), a=("c_m3_0", "mean"), aw=("c_m3_0", lambda x: (x > 0).mean() * 100),
                        b=("c_m3_p1", "mean"), bw=("c_m3_p1", lambda x: (x > 0).mean() * 100),
                        c=("c_m3_p3", "mean"), cw=("c_m3_p3", lambda x: (x > 0).mean() * 100)).round(1)
print(g.to_string())

# ── 事前に見える版: 規制中(実施5日目以降)に TOPIX差-4%以下の日が出たら翌日寄り前に買い→3日後終値（解除日は知らない前提）
E = pd.read_csv("_bt_masutan_release_0918.csv", dtype={"code": str})
DIDX = {d: i for i, d in enumerate(TD)}
ex = []
for r in E.itertuples():
    s = SER.get(r.code)
    if s is None: continue
    cl, ds = s; i0 = bisect.bisect_left(ds, r.start); i1 = bisect.bisect_left(ds, r.end)
    if i1 >= len(ds) or ds[i1] != r.end: continue
    for i in range(i0 + 5, i1 + 1):
        j = DIDX.get(ds[i]);
        if j is None or i + 3 >= len(cl) or j + 3 >= len(TPX): continue
        dr = (cl[i] / cl[i - 1] - 1 - (TPX[j] / TPX[j - 1] - 1)) * 100
        if dr <= -4:
            c3 = (cl[i + 3] / cl[i] - 1 - (TPX[j + 3] / TPX[j] - 1)) * 100
            c1 = (cl[i + 1] / cl[i] - 1 - (TPX[j + 1] / TPX[j] - 1)) * 100
            ex.append({"yr": ds[i][:4], "c1": c1, "c3": c3, "to_release": i1 - i})
X = pd.DataFrame(ex)
print(f"\n== 事前版: 規制中(実施5日目〜)の-4%以下の日の翌日以降 (n={len(X)}) ==")
for t, g in (("全期間", X), ("2025-26", X[X.yr.isin(['2025', '2026'])]), ("2026", X[X.yr == '2026'])):
    print(f"  {t:<8} 翌1日 {g.c1.mean():+.2f}%/{(g.c1 > 0).mean() * 100:.0f}%   3日 {g.c3.mean():+.2f}%/中央{g.c3.median():+.2f}%/上昇率{(g.c3 > 0).mean() * 100:.0f}%   解除がちょうど4日後だった割合 {(g.to_release == 4).mean() * 100:.0f}%")
g = X[X.to_release == 4]; print(f"  うち本当に解除4日前だった玉 n={len(g)} 3日 {g.c3.mean():+.2f}%/上昇率{(g.c3 > 0).mean() * 100:.0f}%")
g = X[X.to_release != 4]; print(f"  それ以外(解除はまだ)     n={len(g)} 3日 {g.c3.mean():+.2f}%/上昇率{(g.c3 > 0).mean() * 100:.0f}%")
