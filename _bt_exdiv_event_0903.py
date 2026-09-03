# -*- coding: utf-8 -*-
"""_bt_exdiv_event_0903.py — 新しい土俵①: 配当権利落ちイベント（2026-09-03・本番無変更・探索）。
権利確定日=決算期末(CurFYEn)と中間期末(2Q CurPerEn)。権利落ち日=確定日の直前営業日(2019-07-16以降・T+2)／
それ以前は2営業日前。予想配当=FDivFY(直近開示・年額)、中間はFDivAnn。利回り=配当/前日終値。
測るもの: 権利付き最終日・権利落ち日(寄り/引け)・落ち後1〜5日の平均リターン、利回り帯別。全銘柄10年(J-Quantsキャッシュ)。"""
import sys, io, pickle, numpy as np, pandas as pd, jpholiday
from datetime import date, timedelta
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
def tse_open(d): return d.weekday() < 5 and not jpholiday.is_holiday(d) and not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3))
def prev_bd(d, k):
    while k > 0:
        d -= timedelta(days=1)
        if tse_open(d): k -= 1
    return d
def last_bd_on_or_before(d):
    while not tse_open(d): d -= timedelta(days=1)
    return d
F = pd.read_pickle("_fins_history.pkl")
F = F[F.DocType.astype(str).str.contains("FinancialStatements", na=False)].copy()
F["DiscDate"] = pd.to_datetime(F.DiscDate); F["code4"] = F.Code.astype(str).str[:4]
F["FDivFY"] = pd.to_numeric(F.FDivFY, errors="coerce"); F["FDivAnn"] = pd.to_numeric(F.FDivAnn, errors="coerce"); F["DivFY"] = pd.to_numeric(F.DivFY, errors="coerce")
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb")); new = pickle.load(open("jquants_cache.pkl", "rb"))
data = {}
for src in (old["all_data"], new["all_data"]):
    for tk, df in src.items(): data.setdefault(tk, []).append(df)
data = {tk: pd.concat(v).sort_index() for tk, v in data.items()}; data = {tk: df[~df.index.duplicated(keep="last")] for tk, df in data.items()}
print(f"銘柄 {len(data)} / 開示 {len(F):,}")
# 権利確定日の候補: FY末(年1回)＋中間末(2Q)
ev = []
for r in F[F.CurPerType.isin(["FY", "2Q"])].itertuples():
    try:
        rec = pd.Timestamp(r.CurPerEn if r.CurPerType == "2Q" else r.CurFYEn).date()
    except Exception: continue
    ev.append((r.code4, rec, r.CurPerType))
E = pd.DataFrame(ev, columns=["code4", "rec", "ptype"]).drop_duplicates()
E = E[(E.rec >= date(2016, 9, 1)) & (E.rec <= date(2026, 8, 31))]
E["rec_bd"] = E.rec.map(last_bd_on_or_before)
E["ex"] = [prev_bd(rb, 1) if rb >= date(2019, 7, 16) else prev_bd(rb, 2) for rb in E.rec_bd]
E["last"] = E.ex.map(lambda d: prev_bd(d, 1))
print(f"イベント {len(E):,}件（FY {int((E.ptype=='FY').sum()):,} / 2Q {int((E.ptype=='2Q').sum()):,}）")
rows = []
Fs = F.sort_values("DiscDate")
by_code = {c: g for c, g in Fs.groupby("code4")}
for r in E.itertuples():
    tk = f"{r.code4}.T"; df = data.get(tk)
    if df is None or len(df) < 60: continue
    idx = df.index
    exd = pd.Timestamp(r.ex)
    p = idx.searchsorted(exd)
    if p >= len(idx) or idx[p] != exd or p < 25 or p + 5 >= len(idx): continue
    g = by_code.get(r.code4)
    if g is None: continue
    g2 = g[g.DiscDate < exd - pd.Timedelta(days=1)]
    if g2.empty: continue
    last = g2.iloc[-1]
    div = last.FDivFY if r.ptype == "FY" else last.FDivAnn
    if not (div == div) or div <= 0: continue
    c = df["Close"].astype(float).to_numpy(); o = df["Open"].astype(float).to_numpy(); v = df["Volume"].astype(float).to_numpy()
    c_last = c[p - 1]                       # 権利付き最終日終値
    if not (c_last > 0): continue
    tov = float(np.nanmedian(c[p-21:p-1] * v[p-21:p-1]))
    y = div / c_last * 100                  # 利回り%（FYは年額・2Qは中間額）
    rows.append({"tk": tk, "ex": exd, "y": exd.year, "ptype": r.ptype, "yield": y, "tov": tov,
                 "r_last": (c[p-1] / c[p-2] - 1) * 100,             # 権利付き最終日 引け/前日引け
                 "r_pre5": (c[p-1] / c[p-6] - 1) * 100,             # 最終日までの5日
                 "gap_ex": (o[p] / c_last - 1) * 100 + y,           # 落ち日の寄り(配当分を戻して)
                 "ex_oc": (c[p] / o[p] - 1) * 100,                  # 落ち日 寄→引
                 "ex_cc": (c[p] / c_last - 1) * 100 + y,            # 落ち日 引/前引(配当込み)
                 "post1": (c[p+1] / c[p] - 1) * 100, "post3": (c[p+3] / c[p] - 1) * 100, "post5": (c[p+5] / c[p] - 1) * 100,
                 "o_post3": (c[p+3] / o[p] - 1) * 100, "o_post5": (c[p+5] / o[p] - 1) * 100})
R = pd.DataFrame(rows); R = R[(R.tov >= 2e8) & (R["yield"] < 15)]
print(f"計測 {len(R):,}件（代金2億以上）")
def tbl(x, label):
    cols = ["r_pre5", "r_last", "gap_ex", "ex_oc", "ex_cc", "post1", "post3", "post5", "o_post3", "o_post5"]
    print(f"\n■ {label} n={len(x)}"); print("  " + "  ".join(f"{c}:{x[c].mean():+.2f}" for c in cols))
    print("  勝率  " + "  ".join(f"{c}:{(x[c]>0).mean()*100:.0f}%" for c in cols))
tbl(R, "全体")
for pt in ("FY", "2Q"): tbl(R[R.ptype == pt], f"{pt}")
R["yq"] = pd.qcut(R["yield"], 5, labels=["Y1低", "Y2", "Y3", "Y4", "Y5高"])
for q, x in R.groupby("yq", observed=True): tbl(x, f"利回り{q} [{x['yield'].min():.2f}〜{x['yield'].max():.2f}%]")
print("\n■ 年別（落ち日寄り→3日後 o_post3 平均% / 勝率）")
print(R.groupby("y").agg(n=("o_post3", "size"), o_post3=("o_post3", "mean"), win=("o_post3", lambda s: (s > 0).mean() * 100), ex_oc=("ex_oc", "mean"), r_last=("r_last", "mean")).round(2).to_string())
print("\n■ 高利回り(Y5)×年別 o_post3"); print(R[R.yq == "Y5高"].groupby("y").agg(n=("o_post3", "size"), o_post3=("o_post3", "mean"), win=("o_post3", lambda s: (s > 0).mean() * 100)).round(2).to_string())
R.to_pickle("_exdiv_events.pkl"); print("\n[done]")
