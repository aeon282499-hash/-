# -*- coding: utf-8 -*-
"""_bt_kiwami_flow_vix_0903.py — 極み買い×(投資部門別売買状況 / VIX)（2026-09-03・本番無変更）。
投資部門別=J-Quants /equities/investor-types（週次・PubDate=公表日・公表日<エントリー日で結合）。
VIX=FRED VIXCLS（米国前日終値・エントリー日より前の最新値）。
エンジン=_bt_kiwami_resid_vol_0903.py の前半（公式+3,108,516円と一致をassert）。
実行: python -X utf8 _bt_kiwami_flow_vix_0903.py"""
import numpy as np, pandas as pd
_src = open("_bt_kiwami_resid_vol_0903.py", encoding="utf-8").read().split('print("\n" + "=" * 122); print("① 業種指数')[0]
exec(_src)

print("\n" + "=" * 122); print("③ 投資部門別売買状況（週次・公表日<エントリー日）"); print("=" * 122)
IT = pd.read_pickle("_investor_types.pkl")
print("Section:", IT.Section.value_counts().to_dict())
sec = "TokyoNagoya" if "TokyoNagoya" in set(IT.Section) else IT.Section.iloc[0]
W = IT[IT.Section == sec].copy()
for c in W.columns:
    if c not in ("PubDate", "StDate", "EnDate", "Section"):
        W[c] = pd.to_numeric(W[c], errors="coerce")
W["PubDate"] = pd.to_datetime(W.PubDate); W = W.sort_values("PubDate")
tot = W.TotTot.replace(0, np.nan)
W["frgn"] = W.FrgnBal / tot * 100          # 海外勢の買越し（総売買代金比%）
W["ind"] = W.IndBal / tot * 100            # 個人
W["trst"] = W.TrstBnkBal / tot * 100       # 信託銀行（年金）
W["prop"] = W.PropBal / tot * 100          # 自己
W["frgn4"] = W.frgn.rolling(4).sum(); W["ind4"] = W.ind.rolling(4).sum()
W["frgn_sh"] = W.FrgnTot / tot * 100       # 海外勢シェア
W["ind_sh"] = W.IndTot / tot * 100
W["tot_chg"] = W.TotTot.pct_change() * 100 # 売買代金の前週比
W["frgn_z"] = (W.frgn - W.frgn.rolling(52).mean()) / W.frgn.rolling(52).std()
FEATS = ["frgn", "ind", "trst", "prop", "frgn4", "ind4", "frgn_sh", "ind_sh", "tot_chg", "frgn_z"]
Ct = pd.DataFrame({"_i": np.arange(n), "entry": C0.entry}).sort_values("entry")
J3 = pd.merge_asof(Ct, W[["PubDate"] + FEATS], left_on="entry", right_on="PubDate", allow_exact_matches=False).sort_values("_i")
print(f"結合率 {np.isfinite(J3.frgn.to_numpy()[BASE]).mean()*100:.1f}%  公表→エントリーの遅れ中央値 {(J3.entry - J3.PubDate).dt.days.median():.0f}日")
FV = {f: J3[f].to_numpy() for f in FEATS}
for f in FEATS:
    R0[f] = FV[f][R0.i.to_numpy()]
LBL = {"frgn": "海外勢 買越し%(週)", "ind": "個人 買越し%(週)", "trst": "信託銀 買越し%(週)", "prop": "自己 買越し%(週)",
       "frgn4": "海外勢 買越し%(4週計)", "ind4": "個人 買越し%(4週計)", "frgn_sh": "海外勢シェア%", "ind_sh": "個人シェア%",
       "tot_chg": "売買代金 前週比%", "frgn_z": "海外勢買越し z(52週)"}
for f in FEATS:
    quint(R0, f, LBL[f])
print(); print(HDR); print(line("現行", st0))
for f, ths in (("frgn", (-1.0, -0.5, 0.0, 0.5, 1.0)), ("frgn4", (-2.0, 0.0, 2.0)), ("ind", (-1.0, 0.0, 1.0)),
               ("frgn_z", (-1.0, 0.0, 1.0)), ("tot_chg", (-10.0, 10.0))):
    for th in ths:
        sim(f"{LBL[f]}<{th}除外", BASE & ~(FV[f] < th)); sim(f"{LBL[f]}>{th}除外", BASE & ~(FV[f] > th))

print("\n" + "=" * 122); print("④ VIX（FRED VIXCLS・エントリー日より前の最新終値）"); print("=" * 122)
V = pd.read_csv("_vixcls.csv"); V.columns = ["Date", "vix"]; V["Date"] = pd.to_datetime(V.Date); V["vix"] = pd.to_numeric(V.vix, errors="coerce")
V = V.dropna().sort_values("Date")
V["vix_chg1"] = V.vix.pct_change() * 100; V["vix_chg5"] = V.vix.pct_change(5) * 100
V["vix_pct"] = V.vix.rolling(250).rank(pct=True) * 100
V["vix_ma20r"] = V.vix / V.vix.rolling(20).mean()
J4 = pd.merge_asof(Ct, V, left_on="entry", right_on="Date", allow_exact_matches=False).sort_values("_i")
VF = {f: J4[f].to_numpy() for f in ("vix", "vix_chg1", "vix_chg5", "vix_pct", "vix_ma20r")}
for f in VF:
    R0[f] = VF[f][R0.i.to_numpy()]
print(f"結合率 {np.isfinite(VF['vix'][BASE]).mean()*100:.1f}%")
quint(R0, "vix", "VIX水準"); quint(R0, "vix_chg1", "VIX前日変化%"); quint(R0, "vix_chg5", "VIX5日変化%"); quint(R0, "vix_pct", "VIX 1年パーセンタイル"); quint(R0, "vix_ma20r", "VIX/20日平均")
print(); print(HDR); print(line("現行", st0))
for th in (13, 15, 18, 20, 25, 30):
    sim(f"VIX<{th}除外", BASE & ~(VF["vix"] < th)); sim(f"VIX>{th}除外", BASE & ~(VF["vix"] > th))
for th in (-10, 10, 20):
    sim(f"VIX前日変化<{th}%除外", BASE & ~(VF["vix_chg1"] < th)); sim(f"VIX前日変化>{th}%除外", BASE & ~(VF["vix_chg1"] > th))
for th in (0.9, 1.1, 1.3):
    sim(f"VIX/20MA<{th}除外", BASE & ~(VF["vix_ma20r"] < th)); sim(f"VIX/20MA>{th}除外", BASE & ~(VF["vix_ma20r"] > th))
print("\n[done]")
