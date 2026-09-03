# -*- coding: utf-8 -*-
"""_bt_kiwami_nkvi_tilt_0903.py — 極み買いの1玉サイズを日経VI代替で傾斜（選定不変・配分だけ・平均100万に正規化）。2026-09-03・本番無変更。"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
NV = pd.read_pickle("_nk225_iv_daily.pkl"); NV["Date"] = pd.to_datetime(NV.Date); NV = NV.sort_values("Date")
NV["nvi"] = pd.to_numeric(NV.bv1, errors="coerce"); NV = NV.dropna(subset=["nvi"]); NV["nvi_chg1"] = NV.nvi.pct_change() * 100
_src = open("_bt_kiwami_resid_vol_0903.py", encoding="utf-8").read().split('print("\n" + "=" * 122); print("① 業種指数')[0]
exec(_src)
Ct = pd.DataFrame({"_i": np.arange(n), "entry": C0.entry}).sort_values("entry")
J = pd.merge_asof(Ct, NV[["Date", "nvi", "nvi_chg1"]], left_on="entry", right_on="Date", allow_exact_matches=False).sort_values("_i")
R0["nvi"] = J.nvi.to_numpy()[R0.i.to_numpy()]; R0["nvi_chg1"] = J.nvi_chg1.to_numpy()[R0.i.to_numpy()]
def tilt_stats(m, label):
    m = np.asarray(m, float); m = np.where(np.isfinite(m), m, 1.0); m = m / m.mean()
    R = R0.copy(); R["yen"] = R.yen * m
    yy = R.groupby("y").yen.sum(); gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    cum = R.sort_values("d").yen.cumsum(); dd = float((cum - cum.cummax()).min())
    top20 = R.yen.sum() - R.nlargest(20, "yen").yen.sum()
    print(f"  {label:<34}{len(R):>5}{gp/gl:>6.2f}{R.yen.sum():>+13,.0f}{yy[yy.index<=2021].sum():>+12,.0f}{yy[yy.index>=2022].sum():>+12,.0f}"
          f"{int((yy>0).sum()):>4}/10{yy.min():>+11,.0f}{dd:>+11,.0f}{top20:>+13,.0f}  最大玉{np.nanmax(m)*100:.0f}万")
print(f"  {'構成':<34}{'玉':>5}{'PF':>6}{'10年計':>13}{'前半':>12}{'後半':>12}{'勝年':>5}{'最悪年':>11}{'最大DD':>11}{'上位20除去':>13}")
tilt_stats(np.ones(len(R0)), "現行(一律100万)")
v = R0.nvi.to_numpy(); c = R0.nvi_chg1.to_numpy()
for hi_t, lo_t in ((20, 15), (18, 14), (25, 18), (22, 16)):
    for hi, lo in ((1.3, 0.7), (1.5, 0.5), (1.2, 0.8)):
        tilt_stats(np.where(v >= hi_t, hi, np.where(v <= lo_t, lo, 1.0)), f"VI>={hi_t}→{hi} / <={lo_t}→{lo}")
    tilt_stats(np.where(v >= hi_t, 0.7, np.where(v <= lo_t, 1.3, 1.0)), f"VI>={hi_t}→0.7 / <={lo_t}→1.3(逆)")
for hi, lo in ((1.3, 0.7), (1.5, 0.5)):
    tilt_stats(np.where(c >= 5, hi, np.where(c <= -5, lo, 1.0)), f"VI前日+5%以上→{hi} / -5%以下→{lo}")
tilt_stats(np.clip(v / 18.0, 0.6, 1.6), "連続: VI/18 (0.6〜1.6)")
tilt_stats(np.clip(v / 18.0, 0.6, 1.6) ** 0.5, "連続: sqrt(VI/18)")
print("\n[done]")
