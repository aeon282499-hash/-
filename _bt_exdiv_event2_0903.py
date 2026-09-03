# -*- coding: utf-8 -*-
"""_bt_exdiv_event2_0903.py — 権利落ちイベントの第2段: 同日コントロール(非イベント銘柄の同日平均)で超過を測り、3枠×100万でシム。2026-09-03。
※2Qの配当額は年間予想しか無いので利回りはFYのみ。価格リターンは両方。"""
import sys, io, pickle, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
R = pd.read_pickle("_exdiv_events.pkl")
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb")); new = pickle.load(open("jquants_cache.pkl", "rb"))
data = {}
for src in (old["all_data"], new["all_data"]):
    for tk, df in src.items(): data.setdefault(tk, []).append(df)
data = {tk: pd.concat(v).sort_index() for tk, v in data.items()}; data = {tk: df[~df.index.duplicated(keep="last")] for tk, df in data.items()}
# 全銘柄の日次リターン行列（終値ベース）と寄→引、寄→3日後引
C = pd.DataFrame({tk: df["Close"].astype(float) for tk, df in data.items() if len(df) > 200}).sort_index()
O = pd.DataFrame({tk: df["Open"].astype(float) for tk, df in data.items() if len(df) > 200}).sort_index().reindex(C.index)
V = pd.DataFrame({tk: df["Volume"].astype(float) for tk, df in data.items() if len(df) > 200}).sort_index().reindex(C.index)
liq = (C * V).rolling(20).median().shift(1) >= 2e8
r1 = C.pct_change() * 100; r5 = C.pct_change(5) * 100; oc = (C / O - 1) * 100; o3 = (C.shift(-3) / O - 1) * 100; c3 = (C.shift(-3) / C - 1) * 100
def ctrl(M, dates, ev_tk_by_date):
    out = {}
    for d in dates:
        row = M.loc[d]; mask = liq.loc[d]
        ex = ev_tk_by_date.get(d, set())
        cols = [c for c in row.index if mask.get(c, False) and c not in ex]
        out[d] = float(np.nanmean(row[cols].to_numpy())) if cols else np.nan
    return out
R["ex"] = pd.to_datetime(R.ex)
R["last_d"] = [C.index[C.index.searchsorted(d) - 1] for d in R.ex]
R["pre5_d"] = [C.index[max(C.index.searchsorted(d) - 6, 0)] for d in R.ex]
ev_by_last = R.groupby("last_d").tk.apply(set).to_dict(); ev_by_ex = R.groupby("ex").tk.apply(set).to_dict()
cl = ctrl(r1, R.last_d.unique(), ev_by_last); c5 = ctrl(r5, R.last_d.unique(), ev_by_last)
co = ctrl(oc, R.ex.unique(), ev_by_ex); c3o = ctrl(o3, R.ex.unique(), ev_by_ex); c3c = ctrl(c3, R.ex.unique(), ev_by_ex)
R["x_last"] = R.r_last - R.last_d.map(cl); R["x_pre5"] = R.r_pre5 - R.last_d.map(c5)
R["x_ex_oc"] = R.ex_oc - R.ex.map(co); R["x_o_post3"] = R.o_post3 - R.ex.map(c3o); R["x_post3"] = R.post3 - R.ex.map(c3c)
def tbl(x, label):
    cols = ["x_pre5", "x_last", "x_ex_oc", "x_o_post3", "x_post3"]
    print(f"■ {label} n={len(x)}  " + "  ".join(f"{c}:{x[c].mean():+.2f}({(x[c]>0).mean()*100:.0f}%)" for c in cols))
print("同日の非イベント銘柄(代金2億以上)平均を引いた超過リターン%（勝率）")
tbl(R, "全体"); tbl(R[R.ptype == "FY"], "FY"); tbl(R[R.ptype == "2Q"], "2Q")
Rf = R[R.ptype == "FY"].copy(); Rf["yq"] = pd.qcut(Rf["yield"], 5, labels=["Y1低", "Y2", "Y3", "Y4", "Y5高"])
for q, x in Rf.groupby("yq", observed=True): tbl(x, f"FY 利回り{q} [{x['yield'].min():.2f}〜{x['yield'].max():.2f}%]")
print("\n■ 年別 超過（x_pre5 / x_last / x_o_post3）とイベント日数")
print(R.groupby("y").agg(n=("tk", "size"), days=("ex", "nunique"), x_pre5=("x_pre5", "mean"), x_last=("x_last", "mean"), x_o_post3=("x_o_post3", "mean"),
                         w_pre5=("x_pre5", lambda s: (s > 0).mean() * 100), w_post3=("x_o_post3", lambda s: (s > 0).mean() * 100)).round(2).to_string())
print("\n■ 月別（権利確定月）超過 x_pre5 / x_last")
R["m"] = R.ex.dt.month
print(R.groupby("m").agg(n=("tk", "size"), x_pre5=("x_pre5", "mean"), x_last=("x_last", "mean"), x_o_post3=("x_o_post3", "mean")).round(2).to_string())
# ── シム: 3枠×100万・各イベント日に利回り上位(FYはyield/2Qは代金)から3本・最終日の5営業日前の引けで買い→最終日引け売り ──
print("\n■ シム① 権利取り: 最終5日前引け買い→最終日引け売り（3枠×100万・利回り上位3本・代金5億以上）")
S = R[(R.tov >= 5e8)].copy(); S["rank_key"] = np.where(S.ptype == "FY", S["yield"], S.tov / 1e12)
pick = S.sort_values(["last_d", "rank_key"], ascending=[True, False]).groupby("last_d").head(3)
pick["yen"] = pick.r_pre5 / 100 * 1_000_000
yy = pick.groupby("y").yen.sum(); print("  玉/年:", pick.groupby("y").size().to_dict()); print("  損益(万):", (yy / 1e4).round(1).to_dict(), " 計", round(yy.sum() / 1e4, 1), "勝年", int((yy > 0).sum()), "/", len(yy))
print("\n■ シム② 落ち後ショート: 落ち日寄り売り→3日後引け買戻し（3枠×100万・利回り上位3本）")
pick2 = S.sort_values(["ex", "rank_key"], ascending=[True, False]).groupby("ex").head(3); pick2["yen"] = -pick2.o_post3 / 100 * 1_000_000
yy2 = pick2.groupby("y").yen.sum(); print("  損益(万):", (yy2 / 1e4).round(1).to_dict(), " 計", round(yy2.sum() / 1e4, 1), "勝年", int((yy2 > 0).sum()), "/", len(yy2))
print("\n[done]")
