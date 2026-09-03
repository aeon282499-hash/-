# -*- coding: utf-8 -*-
"""_bt_kiwami_nkvi_tilt2_0903.py — VI傾斜の頑健性: 年別差分 / ラグ+1日 / 非正規化(実資金) / 4分割期間。2026-09-03・本番無変更。"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
NV = pd.read_pickle("_nk225_iv_daily.pkl"); NV["Date"] = pd.to_datetime(NV.Date); NV = NV.sort_values("Date")
NV["nvi"] = pd.to_numeric(NV.bv1, errors="coerce"); NV = NV.dropna(subset=["nvi"]); NV["nvi_lag"] = NV.nvi.shift(1)
_src = open("_bt_kiwami_resid_vol_0903.py", encoding="utf-8").read().split('print("\n" + "=" * 122); print("① 業種指数')[0]
exec(_src)
Ct = pd.DataFrame({"_i": np.arange(n), "entry": C0.entry}).sort_values("entry")
J = pd.merge_asof(Ct, NV[["Date", "nvi", "nvi_lag"]], left_on="entry", right_on="Date", allow_exact_matches=False).sort_values("_i")
R0["nvi"] = J.nvi.to_numpy()[R0.i.to_numpy()]; R0["nvi_lag"] = J.nvi_lag.to_numpy()[R0.i.to_numpy()]
def run(m, label, normalize=True):
    m = np.asarray(m, float); m = np.where(np.isfinite(m), m, 1.0)
    if normalize: m = m / m.mean()
    R = R0.copy(); R["yen"] = R.yen * m; R["cap"] = 1_000_000 * m
    yy = R.groupby("y").yen.sum(); gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    cum = R.sort_values("d").yen.cumsum(); dd = float((cum - cum.cummax()).min())
    q = {p: yy[(yy.index >= a) & (yy.index <= b)].sum() for p, (a, b) in {"17-19": (2017, 2019), "20-21": (2020, 2021), "22-23": (2022, 2023), "24-26": (2024, 2026)}.items()}
    print(f"  {label:<36}{gp/gl:>5.2f}{R.yen.sum()/1e4:>+8.0f}万 {' '.join(f'{k}:{v/1e4:+.0f}' for k, v in q.items())}  勝年{int((yy>0).sum())}/10 最悪年{yy.min()/1e4:+.0f} DD{dd/1e4:+.0f} 最大玉{R.cap.max()/1e4:.0f}万 平均玉{R.cap.mean()/1e4:.0f}万")
    return yy
v = R0.nvi.to_numpy(); vl = R0.nvi_lag.to_numpy()
print("■ 正規化(平均100万)・VI=シグナル日の値 vs ラグ+1日(前々日の値)")
y0 = run(np.ones(len(R0)), "現行")
y1 = run(np.where(v >= 20, 1.3, np.where(v <= 15, 0.7, 1.0)), "VI>=20→1.3/<=15→0.7")
run(np.where(vl >= 20, 1.3, np.where(vl <= 15, 0.7, 1.0)), "同・ラグ+1日")
run(np.where(v >= 22, 1.3, np.where(v <= 16, 0.7, 1.0)), "VI>=22→1.3/<=16→0.7")
run(np.where(vl >= 22, 1.3, np.where(vl <= 16, 0.7, 1.0)), "同・ラグ+1日")
run(np.clip(v / 18.0, 0.6, 1.6), "連続 VI/18")
run(np.clip(vl / 18.0, 0.6, 1.6), "同・ラグ+1日")
print("■ 非正規化（実際の資金: 基準100万のまま片側だけ動かす）")
run(np.ones(len(R0)), "現行 100万一律", normalize=False)
run(np.where(v >= 20, 1.3, 1.0), "VI>=20の日だけ130万", normalize=False)
run(np.where(v <= 15, 0.7, 1.0), "VI<=15の日だけ70万", normalize=False)
run(np.where(v >= 20, 1.3, np.where(v <= 15, 0.7, 1.0)), "両方 130/100/70万", normalize=False)
run(np.where(v >= 20, 1.5, np.where(v <= 15, 0.5, 1.0)), "両方 150/100/50万", normalize=False)
run(np.where(v <= 15, 0.5, 1.0), "VI<=15の日だけ50万", normalize=False)
print("■ 年別差分（VI>=20→1.3/<=15→0.7 正規化 − 現行・万円）")
print("  " + " ".join(f"{y}:{(y1[y]-y0[y])/1e4:+.0f}" for y in y0.index))
print("■ VI帯ごとの玉数と成績（建てた玉）")
b = pd.cut(R0.nvi, [0, 15, 20, 25, 99], labels=["<=15", "15-20", "20-25", ">25"])
g = R0.groupby(b, observed=True).agg(n=("pnl", "size"), avg=("pnl", "mean"), win=("pnl", lambda x: (x > 0).mean() * 100), yen=("yen", "sum"))
print(g.round(3).to_string())
yrs = R0.assign(b=b).groupby(["y", "b"], observed=True).size().unstack(fill_value=0); print(yrs.to_string())
print("\n[done]")
