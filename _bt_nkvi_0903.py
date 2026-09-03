# -*- coding: utf-8 -*-
"""_bt_nkvi_0903.py — 日経VI代替（J-Quants 日経225オプションの基準ボラ BaseVol・_nk225_iv_daily.pkl）× 極み買い / フェード（2026-09-03・本番無変更）。
極み買い: エントリー日より前の最新値（前夜配信時点で既知）。フェード: 同じ。除外と(フェードは)サイズ傾斜。
実行: python -X utf8 _bt_nkvi_0903.py"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
NV = pd.read_pickle("_nk225_iv_daily.pkl"); NV["Date"] = pd.to_datetime(NV.Date); NV = NV.sort_values("Date")
NV["nvi"] = pd.to_numeric(NV.get("bv1"), errors="coerce"); NV["iv_atm"] = pd.to_numeric(NV.get("iv1"), errors="coerce")
NV = NV.dropna(subset=["nvi"]); NV["nvi_chg1"] = NV.nvi.pct_change() * 100; NV["nvi_chg5"] = NV.nvi.pct_change(5) * 100
NV["nvi_ma20r"] = NV.nvi / NV.nvi.rolling(20).mean(); NV["nvi_pct"] = NV.nvi.rolling(250).rank(pct=True) * 100
print(f"日経VI代替 {NV.Date.min().date()}〜{NV.Date.max().date()} {len(NV)}日  水準分位: {NV.nvi.quantile([.1,.5,.9]).round(1).to_dict()}")
FE = ["nvi", "nvi_chg1", "nvi_chg5", "nvi_ma20r", "nvi_pct"]

print("\n" + "=" * 122); print("A 極み買い × 日経VI代替"); print("=" * 122)
_src = open("_bt_kiwami_resid_vol_0903.py", encoding="utf-8").read().split('print("\n" + "=" * 122); print("① 業種指数')[0]
exec(_src)
Ct = pd.DataFrame({"_i": np.arange(n), "entry": C0.entry}).sort_values("entry")
J = pd.merge_asof(Ct, NV[["Date"] + FE], left_on="entry", right_on="Date", allow_exact_matches=False).sort_values("_i")
F = {f: J[f].to_numpy() for f in FE}
COV = np.isfinite(F["nvi"]) & BASE
print(f"結合率 {np.isfinite(F['nvi'][BASE]).mean()*100:.1f}%（未取得期間は除外＝比較は同じ母集団で）")
for f in FE: R0[f] = F[f][R0.i.to_numpy()]
for f, lab in (("nvi", "日経VI代替 水準"), ("nvi_chg1", "前日変化%"), ("nvi_chg5", "5日変化%"), ("nvi_ma20r", "VI/20日平均"), ("nvi_pct", "1年パーセンタイル")):
    quint(R0, f, lab)
print(); print(HDR); print(line("現行(全期間)", st0)); st_c = sim("現行(VI取得期間のみ=比較基準)", COV)
for th in (15, 18, 20, 25, 30):
    sim(f"VI<{th}除外", COV & ~(F["nvi"] < th)); sim(f"VI>{th}除外", COV & ~(F["nvi"] > th))
for th in (-10, 10, 20):
    sim(f"VI前日変化<{th}%除外", COV & ~(F["nvi_chg1"] < th)); sim(f"VI前日変化>{th}%除外", COV & ~(F["nvi_chg1"] > th))
for th in (0.9, 1.1, 1.3):
    sim(f"VI/20MA<{th}除外", COV & ~(F["nvi_ma20r"] < th)); sim(f"VI/20MA>{th}除外", COV & ~(F["nvi_ma20r"] > th))

print("\n" + "=" * 122); print("B フェード × 日経VI代替"); print("=" * 122)
_g = dict(globals()); 
exec(open("_bt_fade_untested_sweep.py", encoding="utf-8").read().split('print("=" * 118); print("■ 基準')[0])
P["_ent"] = pd.to_datetime(P.ent); P = P.sort_values("_ent")
P = pd.merge_asof(P, NV[["Date"] + FE], left_on="_ent", right_on="Date", allow_exact_matches=False).drop(columns=["Date"]).sort_index()
Pc = P[P.nvi.notna()]
print(f"結合率 {P.nvi.notna().mean()*100:.1f}%")
d0 = select(Pc); r1 = d0.rk == 1; base_c = evaluate(d0)
def quintF(col, label, nq=5):
    x = d0[d0[col].notna()].copy(); x["q"] = pd.qcut(x[col].rank(method="first"), nq, labels=False)
    cap = np.where(x.rk == 1, S1, S2); sh = (cap / x.px // 100 * 100).astype(int); sh = np.where((x.rk == 2) & (x.px * 100 > S2), 0, sh)
    x["yen"] = x.pnl / 100 * sh * x.o1
    g = x.groupby("q").agg(n=("pnl", "size"), avg=("pnl", "mean"), win=("pnl", lambda v: (v > 0).mean() * 100), yen=("yen", "sum"), lo=(col, "min"), hi=(col, "max"))
    print(f"\n  ▶ {label} 五分位")
    for q, r in g.iterrows(): print(f"     Q{q+1} n={int(r.n):>4} 平均{r.avg:+.3f}% 勝率{r.win:.1f}% 円{r.yen:>+12,.0f}  [{r.lo:.2f}〜{r.hi:.2f}]")
for f, lab in (("nvi", "日経VI代替 水準"), ("nvi_chg1", "前日変化%"), ("nvi_ma20r", "VI/20日平均")): quintF(f, lab)
def showc(label, m):
    d = m["tot"] - base_c["tot"]
    print(f"  {label:<34}{m['n']:>5}{m['tot']/1e4:>+9.0f}万{d/1e4:>+7.0f}{m['pf']:>6.2f}{m['h1']/1e4:>+8.0f}{m['h2']/1e4:>+8.0f}{m['wy']:>4}/{m['ny']}{m['wm']/1e4:>+7.0f}{m['m20']:>5}{m['dd']/1e4:>+7.0f}")
print(); print(hdr); showc("現行(VI取得期間)", base_c)
for th in (15, 18, 20, 25, 30):
    showc(f"VI<{th}除外", evaluate(select(Pc[~(Pc.nvi < th)]))); showc(f"VI>{th}除外", evaluate(select(Pc[~(Pc.nvi > th)])))
for th in (-10, 10):
    showc(f"VI前日変化<{th}%除外", evaluate(select(Pc[~(Pc.nvi_chg1 < th)]))); showc(f"VI前日変化>{th}%除外", evaluate(select(Pc[~(Pc.nvi_chg1 > th)])))
def mk(mult):
    def fn(d):
        m = np.asarray(mult, float); m = m / m[r1.to_numpy()].mean(); return np.where(d.rk == 1, S1 * m, S2)
    return fn
def tilt(col, hi_t, lo_t, hi=1.3, lo=0.7):
    v = d0[col].to_numpy(); return np.where(v >= hi_t, hi, np.where(v <= lo_t, lo, 1.0))
for hi_t, lo_t in ((25, 18), (30, 20), (20, 15)):
    showc(f"T VI>={hi_t}→1.3 / <={lo_t}→0.7", evaluate(d0, mk(tilt("nvi", hi_t, lo_t))))
    showc(f"T VI>={hi_t}→0.7 / <={lo_t}→1.3(逆)", evaluate(d0, mk(tilt("nvi", hi_t, lo_t, 0.7, 1.3))))
print("\n[done]")
