# -*- coding: utf-8 -*-
"""_bt_fade_jq_axes.py — J-Quants既取得データの未検証軸（2026-08-29未明・本人「J-Qからデータ取れてまだ未検証あるでしょ」）。
A 財務(_fins_history.pkl・開示日ラグ付きで最新の本決算/四半期): 赤字/黒字・自己資本比率・時価総額(概算=px×NP/EPS)・PBR概算・売上成長
B 指数(_indices_10y.pkl): グロース/マザーズ・TOPIXの25MA上下と前日騰落
100/50土台・10年グロス。"""
import numpy as np, pandas as pd
exec(open("_bt_fade_untested_sweep.py", encoding="utf-8").read().split('print("=" * 118); print("■ 基準')[0])
F = pd.read_pickle("_fins_history.pkl")
F = F[F.DocType.str.contains("FinancialStatements", na=False)].copy()
F["code4"] = F.Code.astype(str).str[:4]
num = lambda c: pd.to_numeric(F[c], errors="coerce")
F["np_"] = num("NP"); F["eps"] = num("EPS"); F["eqv"] = num("Eq"); F["ta"] = num("TA"); F["sales"] = num("Sales"); F["fsales"] = num("FSales"); F["eqar"] = num("EqAR")
F["shares"] = np.where((F.eps.abs() > 0) & F.np_.notna(), F.np_ / F.eps, np.nan)
F = F.sort_values(["code4", "DiscDate"])
P2 = P.copy(); P2["code4"] = P2.ticker.str[:4]
# 直近の開示（DiscDate < sig）を結合
P2["sigd"] = pd.to_datetime(P2.sig); P2 = P2.sort_values("sigd")
Fj = F[["code4", "DiscDate", "np_", "eps", "eqv", "ta", "eqar", "sales", "fsales", "shares", "CurPerType"]].rename(columns={"DiscDate": "disc"})
Fj["discd"] = pd.to_datetime(Fj.disc, errors="coerce"); Fj = Fj.dropna(subset=["discd"]).sort_values("discd")
M = pd.merge_asof(P2, Fj, left_on="sigd", right_on="discd", by="code4", allow_exact_matches=False)
M["mcap"] = M.px * M.shares.abs()
M["pbr"] = np.where(M.eqv > 0, M.px * M.shares.abs() / M.eqv, np.nan)
M["loss"] = M.np_ < 0
M["sales_g"] = np.where(M.sales > 0, M.fsales / M.sales - 1, np.nan)
print(f"財務結合率: {M.disc.notna().mean()*100:.1f}%")
d0 = select(M); base = evaluate(d0)
r1 = d0[d0.rk == 1]
for col, lab in (("mcap", "時価総額(概算・円)"), ("pbr", "PBR(概算)"), ("eqar", "自己資本比率"), ("sales_g", "予想売上成長")):
    x = r1[np.isfinite(r1[col].astype(float))].copy()
    if len(x) < 100: print(lab, "n不足", len(x)); continue
    x["q"] = pd.qcut(x[col].rank(method="first"), 5, labels=False)
    g = x.groupby("q").agg(n=("pnl", "size"), avg=("pnl", "mean"), win=("pnl", lambda v: (v > 0).mean()), lo=(col, "min"), hi=(col, "max"))
    print(f"\n① {lab} 五分位（件あたり%）"); print(g.round(3).to_string())
for lab, m in (("赤字企業", r1.loss == True), ("黒字企業", r1.loss == False)):
    sub = r1[m]; print(f"   {lab}: n={len(sub)} 平均{sub.pnl.mean():+.2f}% 勝率{(sub.pnl>0).mean()*100:.0f}%")
print("\n" + hdr); show("現行 100/50", base)
print("■ 財務フィルタ（再ランク）")
for lab, mask in {"赤字除外": M.loss != True, "赤字だけ": M.loss == True, "時価総額>=100億": M.mcap >= 1e10, "時価総額>=300億": M.mcap >= 3e10,
                  "時価総額<=1000億": M.mcap <= 1e11, "時価総額<=300億": M.mcap <= 3e10, "PBR>=3": M.pbr >= 3, "PBR<=1.5": M.pbr <= 1.5,
                  "自己資本比率>=30%": M.eqar >= 0.3, "自己資本比率<=30%": M.eqar <= 0.3}.items():
    show(lab, evaluate(select(M[mask.fillna(False)])))
def norm(raw_fn):
    def f(d):
        raw = raw_fn(d); m = raw[d.rk == 1].mean(); return np.where(d.rk == 1, raw * S1 / m, S2)
    return f
print("■ 財務サイズ傾斜（①平均100万正規化）")
show("時価総額<=100億→1.3 / >=500億→0.7", evaluate(d0, norm(lambda d: np.where(d.mcap.fillna(2e10) <= 1e10, 1.3*S1, np.where(d.mcap.fillna(2e10) >= 5e10, 0.7*S1, S1)))))
show("時価総額>=500億→1.3 / <=100億→0.7(逆)", evaluate(d0, norm(lambda d: np.where(d.mcap.fillna(2e10) >= 5e10, 1.3*S1, np.where(d.mcap.fillna(2e10) <= 1e10, 0.7*S1, S1)))))
show("赤字→1.3 / 黒字→0.7", evaluate(d0, norm(lambda d: np.where(d.loss == True, 1.3*S1, 0.7*S1))))
show("黒字→1.3 / 赤字→0.7", evaluate(d0, norm(lambda d: np.where(d.loss == True, 0.7*S1, 1.3*S1))))

print("\n■ B 指数（_indices_10y.pkl）")
I = pd.read_pickle("_indices_10y.pkl"); I["C"] = pd.to_numeric(I.C, errors="coerce")
print("  収録コード:", sorted(I.Code.unique())[:40])
def idx_feats(code, name):
    s = I[I.Code == code].sort_values("Date").set_index("Date").C.dropna()
    ma = s.rolling(25).mean(); chg = s.pct_change() * 100
    f = pd.DataFrame({f"{name}_above": (s > ma), f"{name}_chg": chg, f"{name}_5d": s.pct_change(5) * 100})
    return f
feats = None
for code, name in (("0000", "TOPIX"), ("0070", "GROWTH"), ("0075", "MOTHERS"), ("0080", "NIKKEI225")):
    if (I.Code == code).any():
        f = idx_feats(code, name); feats = f if feats is None else feats.join(f)
if feats is not None:
    M2 = M.join(feats, on="sig")
    d1 = select(M2); r1 = d1[d1.rk == 1]
    for c in [c for c in feats.columns if c.endswith("_above")]:
        for v in (True, False):
            sub = r1[r1[c] == v]; print(f"   {c}={v}: n={len(sub)} 平均{sub.pnl.mean():+.2f}% 勝率{(sub.pnl>0).mean()*100:.0f}%")
    for c in [c for c in feats.columns if c.endswith("_5d")]:
        x = r1[r1[c].notna()].copy(); x["q"] = pd.qcut(x[c].rank(method="first"), 5, labels=False)
        g = x.groupby("q").agg(n=("pnl", "size"), avg=("pnl", "mean"), lo=(c, "min"), hi=(c, "max")); print(f"\n① {c}（5日騰落）五分位"); print(g.round(2).to_string())
    print(hdr)
    for c in [c for c in feats.columns if c.endswith("_above")]:
        show(f"{c}=True だけ", evaluate(select(M2[M2[c] == True])))
        show(f"{c}=False だけ", evaluate(select(M2[M2[c] == False])))
        show(f"{c} 傾斜 True→0.7/False→1.3", evaluate(d1, norm(lambda d, c=c: np.where(d[c] == True, 0.7*S1, 1.3*S1))))
        show(f"{c} 傾斜 True→1.3/False→0.7", evaluate(d1, norm(lambda d, c=c: np.where(d[c] == True, 1.3*S1, 0.7*S1))))
