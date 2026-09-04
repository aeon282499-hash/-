# -*- coding: utf-8 -*-
"""_bt_kiwami_gate10y_0904.py — 反発指標ゲートを「発注ルール」として公式10年エンジンに載せる（2026-09-04・本番無変更）。
指標=kiwami_rebound_gauge.gauge()（BTキャッシュ・全候補の2日目リターン126日平均・3営業日ラグ）。
ゲート: 指標 > TH の日だけ新規（TH=-0.25/-0.15/0）。＋日経VI傾斜(ラグ+1日)との併用も測る。"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
_src = open("_bt_kiwami_resid_vol_0903.py", encoding="utf-8").read().split('print("\n" + "=" * 122); print("① 業種指数')[0]
exec(_src)
from kiwami_rebound_gauge import load as _gload, gauge as _gauge
_all, _nm = _gload(); D2, IND = _gauge(_all, _nm)
INDL = IND.shift(3)                         # 3営業日ラグ（データが判明してから使う）
Ct = pd.DataFrame({"_i": np.arange(n), "entry": C0.entry}).sort_values("entry")
G = pd.merge_asof(Ct, pd.DataFrame({"Date": INDL.index, "ind": INDL.values}).dropna(), left_on="entry", right_on="Date", allow_exact_matches=True).sort_values("_i")
INDV = G.ind.to_numpy()
print(f"指標 結合率 {np.isfinite(INDV[BASE]).mean()*100:.1f}%  年平均: " + " ".join(f"{y}:{v:+.2f}" for y, v in INDL.groupby(INDL.index.year).mean().items() if y >= 2017))
R0["ind"] = INDV[R0.i.to_numpy()]
quint(R0, "ind", "反発指標(3日ラグ)")
def yearly(st_df, label):
    yy = st_df.groupby("y").yen.sum().reindex(range(2017, 2027), fill_value=0.0)
    print(f"  {label:<28}" + " ".join(f"{y}:{v/1e4:+.0f}" for y, v in yy.items()) + f"  計{yy.sum()/1e4:+.0f}万")
print(); print(HDR); print(line("現行", st0)); yearly(R0, "現行 年別(万)")
for th in (-0.25, -0.15, 0.0):
    m = BASE & ~(INDV <= th)   # 指標が閾値以下の日は撃たない（NaNは撃つ）
    R = run2(m, pnl0, exo0); st = stats(R); print(line(f"ゲート 指標>{th}", st)); yearly(R, f"  年別(万) >{th}")
# ゲート日数（撃たない日の割合）
for th in (-0.25, -0.15, 0.0):
    off = (INDL <= th); off = off[off.index.year >= 2017]
    print(f"  指標≤{th} で止まる営業日: {off.mean()*100:.1f}%（年別: " + " ".join(f"{y}:{v*100:.0f}%" for y, v in off.groupby(off.index.year).mean().items()) + "）")
# 併用: ゲート(-0.25) × VI傾斜（ラグ+1日・1.3/0.7・平均100万正規化）
NV = pd.read_pickle("_nk225_iv_daily.pkl"); NV["Date"] = pd.to_datetime(NV.Date); NV = NV.sort_values("Date"); NV["nvi_lag"] = pd.to_numeric(NV.bv1, errors="coerce").shift(1)
J = pd.merge_asof(Ct, NV[["Date", "nvi_lag"]], left_on="entry", right_on="Date", allow_exact_matches=False).sort_values("_i"); VL = J.nvi_lag.to_numpy()
def tilt_stats(R, mult, label):
    m = np.asarray(mult, float); m = np.where(np.isfinite(m), m, 1.0); m = m / m.mean()
    Rt = R.copy(); Rt["yen"] = Rt.yen * m; st = stats(Rt); print(line(label, st)); yearly(Rt, "  年別(万)")
Rg = run2(BASE & ~(INDV <= -0.25), pnl0, exo0)
v = VL[Rg.i.to_numpy()]
tilt_stats(Rg, np.where(v >= 20, 1.3, np.where(v <= 15, 0.7, 1.0)), "ゲート-0.25 × VI傾斜1.3/0.7")
v0 = VL[R0.i.to_numpy()]
tilt_stats(R0, np.where(v0 >= 20, 1.3, np.where(v0 <= 15, 0.7, 1.0)), "VI傾斜のみ(参照)")
print("\n[done]")
