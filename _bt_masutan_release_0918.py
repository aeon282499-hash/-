# -*- coding: utf-8 -*-
"""_bt_masutan_release_0918.py — 本人「増担解除前の銘柄って価格あがらない?」の実測（2026-09-18）。
_margin_alert_bal_10y.pkl の Restricted(取引所規制=増担保・TSEMrgnRegCls 003/004/005) を
銘柄×連続営業日でエピソード化し、規制前の走り/規制中/解除前5日/解除後の終値リターンを
TOPIX(0000)差で集計する。価格は jquants_cache(_2016_2021).pkl。
"""
import pickle, bisect
import numpy as np, pandas as pd

A = pd.read_pickle("_margin_alert_bal_10y.pkl")
A["code4"] = A["Code"].astype(str).str[:4]
dates = sorted(A.PubDate.unique()); DI = {d: i for i, d in enumerate(dates)}
R = A[A.Restricted == 1][["PubDate", "code4", "TSEMrgnRegCls"]].copy()
R["di"] = R.PubDate.map(DI)

# エピソード化（同銘柄で公表日が連続＝1本。3営業日以内の穴は同一扱い）
eps = []
for c, g in R.sort_values("di").groupby("code4"):
    di = g.di.to_numpy(); cls = g.TSEMrgnRegCls.to_numpy()
    s = 0
    for k in range(1, len(di) + 1):
        if k == len(di) or di[k] - di[k - 1] > 3:
            eps.append((c, dates[di[s]], dates[di[k - 1]], di[k - 1] - di[s] + 1, "".join(sorted(set(cls[s:k])))))
            s = k
E = pd.DataFrame(eps, columns=["code", "start", "end", "ndays", "cls"])
E = E[E.end < dates[-1]]                   # 進行中(未解除)は除く
print("エピソード", len(E), "銘柄", E.code.nunique(), "期間", E.start.min(), "〜", E.end.max())
print("規制日数の分布(営業日):", E.ndays.describe()[["25%", "50%", "75%"]].round(0).to_dict())

old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb")); new = pickle.load(open("jquants_cache.pkl", "rb"))
SER = {}
for c in E.code.unique():
    tk = c + ".T"
    dfs = [d for src in (old["all_data"], new["all_data"]) if (d := src.get(tk)) is not None and len(d)]
    if not dfs: continue
    df = pd.concat(dfs).sort_index(); df = df[~df.index.duplicated(keep="last")]
    cl = df["Close"].astype(float); cl = cl[cl > 0]
    SER[c] = (cl.to_numpy(), [d.strftime("%Y-%m-%d") for d in cl.index])
ix = pd.read_pickle("_indices_10y.pkl"); ix = ix[ix.Code == "0000"].sort_values("Date")
TPX = ix.C.astype(float).to_numpy(); TD = ix.Date.astype(str).tolist()

def ret(cl, ds, d_from, d_to_off_from, off_from=0, off_to=0):
    """d_from(文字列)の位置+off_from → +off_to の終値リターン%。範囲外はnan"""
    i = bisect.bisect_left(ds, d_from) + off_from
    j = i + off_to if d_to_off_from is None else bisect.bisect_left(ds, d_to_off_from) + off_to
    if i < 0 or j >= len(cl) or i >= len(cl) or j < 0 or i == j: return np.nan
    return (cl[j] / cl[i] - 1) * 100

rows = []
for r in E.itertuples():
    s = SER.get(r.code)
    if s is None: continue
    cl, ds = s
    def X(a, b, oa=0, ob=0):
        x = ret(cl, ds, a, b, oa, ob); m = ret(TPX, TD, a, b, oa, ob)
        return x, x - m
    pre20, pre20x = X(r.start, r.start, -20, -1)          # 規制前20日(公表前日まで)の走り
    d1, d1x = X(r.start, r.start, -1, 1)                  # 公表前日終値→実施初日終値
    dur, durx = X(r.start, r.end, 0, 0)                   # 公表日終値→最終規制日終値
    last5, last5x = X(r.end, r.end, -5, 0)                # 解除前5日
    p1, p1x = X(r.end, r.end, 0, 1)                       # 解除後1日
    p5, p5x = X(r.end, r.end, 0, 5)
    p10, p10x = X(r.end, r.end, 0, 10)
    p20, p20x = X(r.end, r.end, 0, 20)
    rows.append(dict(code=r.code, start=r.start, end=r.end, n=r.ndays, cls=r.cls, pre20=pre20, pre20x=pre20x,
                     d1=d1, d1x=d1x, dur=dur, durx=durx, dur_pd=dur / r.ndays, last5=last5, last5x=last5x,
                     p1=p1, p1x=p1x, p5=p5, p5x=p5x, p10=p10, p10x=p10x, p20=p20, p20x=p20x))
T = pd.DataFrame(rows); T.to_csv("_bt_masutan_release_0918.csv", index=False)
print("価格つき", len(T))

def show(T, title):
    print(f"\n== {title} (n={len(T)}) ==  平均 / 中央値 / 勝率(>0) ・ TOPIX差=x")
    for k, lab in (("pre20", "規制前20日の走り"), ("d1", "公表→実施初日"), ("dur", "規制中(公表日終値→最終日)"),
                   ("dur_pd", "規制中 1日あたり"), ("last5", "解除前5日"), ("p1", "解除後1日"), ("p5", "解除後5日"),
                   ("p10", "解除後10日"), ("p20", "解除後20日")):
        v = T[k].dropna(); vx = T[k + "x"].dropna() if k + "x" in T else None
        s = f"  {lab:<16} {v.mean():+6.2f}% / {v.median():+6.2f}% / 勝率{(v > 0).mean() * 100:4.0f}%"
        if vx is not None: s += f"   ｜TOPIX差 {vx.mean():+6.2f}% / {vx.median():+6.2f}% / {(vx > 0).mean() * 100:4.0f}%"
        print(s)

show(T, "全エピソード")
show(T[T.n <= 10], "短い規制(≤10営業日)")
show(T[(T.n > 10) & (T.n <= 30)], "中(11〜30営業日)")
show(T[T.n > 30], "長い規制(>30営業日)")
T["yr"] = T.start.str[:4]
show(T[T.yr <= "2020"], "2016-2020"); show(T[T.yr >= "2021"], "2021-2026")
show(T[T.cls.str.contains("004|005")], "段階引き上げあり(004/005)")
# 解除前5日が下げていた玉ほど解除後に上がる？
q = T.dropna(subset=["last5x", "p5x"])
for lo, hi in ((-99, -5), (-5, 0), (0, 5), (5, 99)):
    g = q[(q.last5x >= lo) & (q.last5x < hi)]
    if len(g): print(f"解除前5日TOPIX差 {lo:>3}〜{hi:<3}%: n={len(g):4d} 解除後5日 {g.p5x.mean():+.2f}% 勝率{(g.p5x > 0).mean() * 100:.0f}%  解除後20日 {g.p20x.mean():+.2f}%")
# 規制中の年別
print("\n年別 規制中TOPIX差(平均%) / 解除後20日TOPIX差")
print(T.groupby("yr").agg(n=("dur", "size"), dur=("durx", "mean"), p20=("p20x", "mean")).round(2).to_string())
