# -*- coding: utf-8 -*-
"""_bt_kiwami_vol_tilt_26y_0904.py — 玉サイズのボラ傾斜を26年で検証（2026-09-04・本番無変更）。
日経VI代替(オプション)は2016-09以降しか無いので、立花20年日足から「市場ボラ代替」(全銘柄の日次リターンの横断中央値|r|の20日平均・年率換算)
を作り、重なる期間で日経VI代替と較正（線形回帰）→ VI相当値を26年分作る。傾斜: VI相当≥20→1.3 / ≤15→0.7（ラグ+1日）。
対象=_bt_kiwami_20y_picks_tachibana.csv（3枠×100万・現行ルール・2001-2026）と jquants(2017-2026)。"""
import sys, io, pickle, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
d = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
C = pd.DataFrame({tk: df["Close"].astype(float) for tk, df in d.items() if len(df) > 500}).sort_index()
V = pd.DataFrame({tk: df["Volume"].astype(float) for tk, df in d.items() if len(df) > 500}).sort_index().reindex(C.index)
liq = (C * V).rolling(20).median() >= 1e8          # 代金1億以上の銘柄だけ
r = C.pct_change().where(liq)
xs = r.abs().median(axis=1)                          # 横断中央値 |r|
mv = xs.rolling(20).mean() * np.sqrt(250) * 100 * 1.2533   # E|r|→σ換算(√(π/2)) 年率%
mv = mv.dropna(); mv.name = "mv"
NV = pd.read_pickle("_nk225_iv_daily.pkl"); NV["Date"] = pd.to_datetime(NV.Date); nv = pd.to_numeric(NV.set_index("Date").bv1, errors="coerce").dropna()
J = pd.concat([mv, nv.rename("nvi")], axis=1).dropna()
a, b = np.polyfit(J.mv, J.nvi, 1); corr = J.mv.corr(J.nvi)
print(f"較正: 重なり{len(J)}日 相関{corr:.3f}  VI相当 = {a:.3f}×市場ボラ + {b:.2f}")
vi_eq = (a * mv + b).rename("vi_eq")
print("VI相当の年別中央値:", {y: round(v, 1) for y, v in vi_eq.groupby(vi_eq.index.year).median().items()})
vi_lag = vi_eq.shift(2)   # 本番パリティ: エントリー日E に対し E-2 営業日のVI（前夜18:50に使える最新値）
def tilt_table(picks, label, hi=20, lo=15, up=1.3, dn=0.7, rev=False):
    P = picks.copy(); P["entry"] = pd.to_datetime(P.entry)
    P = pd.merge_asof(P.sort_values("entry"), pd.DataFrame({"Date": vi_lag.index, "v": vi_lag.values}).dropna(), left_on="entry", right_on="Date", allow_exact_matches=True)
    v = P.v.to_numpy(); m = np.where(v >= hi, dn if rev else up, np.where(v <= lo, up if rev else dn, 1.0)); m = np.where(np.isfinite(v), m, 1.0)
    m = m / m.mean(); P["yen_t"] = P.yen * m
    yy0 = P.groupby("y").yen.sum(); yy1 = P.groupby("y").yen_t.sum()
    def per(yy): return {p: yy[(yy.index >= s) & (yy.index <= e)].sum() / 1e4 for p, (s, e) in {"01-08": (2001, 2008), "09-16": (2009, 2016), "17-21": (2017, 2021), "22-26": (2022, 2026)}.items()}
    def pf(x): return x[x > 0].sum() / -x[x <= 0].sum()
    print(f"\n■ {label}")
    print(f"  現行   計{yy0.sum()/1e4:+.0f}万 PF{pf(P.yen):.2f} 勝年{int((yy0>0).sum())}/{len(yy0)} 最悪年{yy0.min()/1e4:+.0f} 期間:{ {k: round(v) for k, v in per(yy0).items()} }")
    print(f"  傾斜   計{yy1.sum()/1e4:+.0f}万 PF{pf(P.yen_t):.2f} 勝年{int((yy1>0).sum())}/{len(yy1)} 最悪年{yy1.min()/1e4:+.0f} 期間:{ {k: round(v) for k, v in per(yy1).items()} }")
    print("  年別差分(万): " + " ".join(f"{y}:{(yy1[y]-yy0[y])/1e4:+.0f}" for y in yy0.index))
    b_ = pd.cut(P.v, [0, 15, 20, 25, 99], labels=["<=15", "15-20", "20-25", ">25"])
    g = P.groupby(b_, observed=True).agg(n=("pnl", "size"), avg=("pnl", "mean"), win=("pnl", lambda x: (x > 0).mean() * 100))
    print("  VI相当帯:", " ".join(f"{k}:n{int(r.n)}/{r.avg:+.2f}%/{r.win:.0f}%" for k, r in g.iterrows()))
    P["half"] = np.where(P.y <= 2016, "01-16", "17-26")
    g2 = P.groupby(["half", b_], observed=True).agg(n=("pnl", "size"), avg=("pnl", "mean"))
    print("  期間×帯:", " ".join(f"{h}/{k}:n{int(r.n)}/{r.avg:+.2f}%" for (h, k), r in g2.iterrows()))
T = pd.read_csv("_bt_kiwami_20y_picks_tachibana.csv"); Q = pd.read_csv("_bt_kiwami_20y_picks_jquants.csv")
tilt_table(T, "立花26年(2001-2026)・VI相当≥20→1.3/≤15→0.7")
tilt_table(T, "立花26年・逆傾斜", rev=True)
tilt_table(T, "立花26年・閾値22/16", hi=22, lo=16)
tilt_table(T, "立花26年・閾値18/14", hi=18, lo=14)
tilt_table(Q, "J-Quants10年(2017-2026)・VI相当≥20→1.3/≤15→0.7")
print("\n[done]")
