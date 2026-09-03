# -*- coding: utf-8 -*-
"""_bt_fade_flow_vix_0903.py — フェード(売り) × VIX(FRED) / 投資部門別(J-Quants)（2026-09-03・本番無変更）。
土台=_bt_fade_untested_sweep.py（現行選定×上位2×①100万/②50万・10年グロス）。
VIX=エントリー日より前の最新米国終値(朝の発注時点で既知・夜の配信時点では未知)。投資部門別=PubDate<エントリー日。
除外(プールに掛けて再ランク)とサイズ傾斜(①玉を1.3/0.7倍)の両方を見る。
実行: python -X utf8 _bt_fade_flow_vix_0903.py"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
exec(open("_bt_fade_untested_sweep.py", encoding="utf-8").read().split('print("=" * 118); print("■ 基準')[0])
P["_ent"] = pd.to_datetime(P.ent); P = P.sort_values("_ent")
V = pd.read_csv("_vixcls.csv"); V.columns = ["Date", "vix"]; V["Date"] = pd.to_datetime(V.Date); V["vix"] = pd.to_numeric(V.vix, errors="coerce")
V = V.dropna().sort_values("Date"); V["vix_chg1"] = V.vix.pct_change() * 100; V["vix_ma20r"] = V.vix / V.vix.rolling(20).mean()
P = pd.merge_asof(P, V, left_on="_ent", right_on="Date", allow_exact_matches=False).drop(columns=["Date"])
IT = pd.read_pickle("_investor_types.pkl"); W = IT[IT.Section == "TokyoNagoya"].copy()
for c in ("FrgnBal", "IndBal", "TotTot"): W[c] = pd.to_numeric(W[c], errors="coerce")
W["PubDate"] = pd.to_datetime(W.PubDate); W = W.sort_values("PubDate")
W["frgn"] = W.FrgnBal / W.TotTot * 100; W["ind"] = W.IndBal / W.TotTot * 100; W["frgn4"] = W.frgn.rolling(4).sum()
P = pd.merge_asof(P, W[["PubDate", "frgn", "ind", "frgn4"]], left_on="_ent", right_on="PubDate", allow_exact_matches=False).drop(columns=["PubDate"])
P = P.sort_index()
print(f"結合率 VIX {P.vix.notna().mean()*100:.1f}% / 投資部門別 {P.frgn.notna().mean()*100:.1f}%")
d0 = select(P); r1 = d0.rk == 1
def quint(col, label, nq=5):
    x = d0[d0[col].notna()].copy(); x["q"] = pd.qcut(x[col].rank(method="first"), nq, labels=False)
    cap = np.where(x.rk == 1, S1, S2); sh = (cap / x.px // 100 * 100).astype(int); sh = np.where((x.rk == 2) & (x.px * 100 > S2), 0, sh)
    x["yen"] = x.pnl / 100 * sh * x.o1
    g = x.groupby("q").agg(n=("pnl", "size"), avg=("pnl", "mean"), win=("pnl", lambda v: (v > 0).mean() * 100), yen=("yen", "sum"), lo=(col, "min"), hi=(col, "max"))
    print(f"\n  ▶ {label} 五分位（建てた玉ベース）")
    for q, r in g.iterrows():
        print(f"     Q{q+1} n={int(r.n):>4} 平均{r.avg:+.3f}% 勝率{r.win:.1f}% 円{r.yen:>+12,.0f}  [{r.lo:.2f}〜{r.hi:.2f}]")
for col, lab in (("vix", "VIX水準"), ("vix_chg1", "VIX前日変化%"), ("vix_ma20r", "VIX/20日平均"), ("frgn", "海外勢買越し%(週)"), ("frgn4", "海外勢買越し%(4週)"), ("ind", "個人買越し%(週)")):
    quint(col, lab)
print(); print(hdr); show("現行 100/50", base)
print("■ 除外（プールに掛けて再ランク）")
for th in (13, 15, 20, 25, 30):
    show(f"VIX<{th}除外", evaluate(select(P[~(P.vix < th)]))); show(f"VIX>{th}除外", evaluate(select(P[~(P.vix > th)])))
for th in (-10, 10):
    show(f"VIX前日変化<{th}%除外", evaluate(select(P[~(P.vix_chg1 < th)]))); show(f"VIX前日変化>{th}%除外", evaluate(select(P[~(P.vix_chg1 > th)])))
for th in (-1.0, 0.0, 1.0):
    show(f"海外週<{th}除外", evaluate(select(P[~(P.frgn < th)]))); show(f"海外週>{th}除外", evaluate(select(P[~(P.frgn > th)])))
for th in (-2.0, 0.0, 2.0):
    show(f"海外4週<{th}除外", evaluate(select(P[~(P.frgn4 < th)]))); show(f"海外4週>{th}除外", evaluate(select(P[~(P.frgn4 > th)])))
print("■ サイズ傾斜（①玉のみ・平均100万に正規化）")
def mk(mult):
    def fn(d):
        m = np.asarray(mult, float); m = m / m[r1.to_numpy()].mean(); return np.where(d.rk == 1, S1 * m, S2)
    return fn
def tilt(col, hi_t, lo_t, hi=1.3, lo=0.7):
    v = d0[col].to_numpy(); return np.where(v >= hi_t, hi, np.where(v <= lo_t, lo, 1.0))
for hi_t, lo_t in ((20, 15), (25, 15), (18, 13)):
    show(f"T VIX>={hi_t}→1.3 / <={lo_t}→0.7", evaluate(d0, mk(tilt("vix", hi_t, lo_t))))
    show(f"T VIX>={hi_t}→0.7 / <={lo_t}→1.3(逆)", evaluate(d0, mk(tilt("vix", hi_t, lo_t, 0.7, 1.3))))
show("T VIX/20MA>=1.1→1.3 / <=0.9→0.7", evaluate(d0, mk(tilt("vix_ma20r", 1.1, 0.9))))
show("T 海外4週<=-2→1.3 / >=2→0.7", evaluate(d0, mk(tilt("frgn4", 2.0, -2.0, 0.7, 1.3))))
show("T 海外4週>=2→1.3 / <=-2→0.7(逆)", evaluate(d0, mk(tilt("frgn4", 2.0, -2.0))))
print("\n[done]")
