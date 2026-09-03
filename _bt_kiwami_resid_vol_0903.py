# -*- coding: utf-8 -*-
"""_bt_kiwami_resid_vol_0903.py — 極み買いの未検証2軸（2026-09-03・本番無変更）。

① 業種相対（残差）リバーサル: Hameed & Mian (2015) / Da, Liu & Schaumburg (2014)。
   9/2の G は S33MAP がJPX業種コード(2050等)で _indices_10y.pkl(J-Quants 0040-0060)と結合率0.3%＝未検証だった。
   正しい対応(0040=水産…0060=サービス・相関で確認)で作り直し、さらに
   「銘柄の下げ − 業種の下げ」(残差)で層別。
② ボラ・レジーム: Nagel (2012) "Evaporating Liquidity" ＝ 短期リバーサルの収益は VIX が高い時に集中。
   TOPIX(0000) の20日実現ボラ / 5日騰落 で層別。
エンジン=_bt_kiwami_untested_0902.py の⓪まで（公式+3,108,516円と一致をassert）。
実行: python -X utf8 _bt_kiwami_resid_vol_0903.py
"""
import numpy as np, pandas as pd
_src = open("_bt_kiwami_untested_0902.py", encoding="utf-8").read().split("# ══ A 財務")[0]
exec(_src)

S33 = {"水産・農林業":"0040","鉱業":"0041","建設業":"0042","食料品":"0043","繊維製品":"0044","パルプ・紙":"0045",
 "化学":"0046","医薬品":"0047","石油･石炭製品":"0048","ゴム製品":"0049","ガラス･土石製品":"004A","鉄鋼":"004B",
 "非鉄金属":"004C","金属製品":"004D","機械":"004E","電気機器":"004F","輸送用機器":"0050","精密機器":"0051",
 "その他製品":"0052","電気･ガス業":"0053","陸運業":"0054","海運業":"0055","空運業":"0056","倉庫･運輸関連業":"0057",
 "情報･通信業":"0058","卸売業":"0059","小売業":"005A","銀行業":"005B","証券･商品先物取引業":"005C","保険業":"005D",
 "その他金融業":"005E","不動産業":"005F","サービス業":"0060"}
I = pd.read_pickle("_indices_10y.pkl"); I["C"] = pd.to_numeric(I.C, errors="coerce"); I["Date"] = pd.to_datetime(I.Date)
feat = []
for code, g in I.groupby("Code"):
    s = g.sort_values("Date").set_index("Date").C.dropna()
    feat.append(pd.DataFrame({"Date": s.index, "s33": code,
        "s_above": (s > s.rolling(25).mean()).astype(float).values,
        "s_dev25": (s / s.rolling(25).mean() - 1).values * 100,
        "s_chg5": (s.pct_change(5) * 100).values, "s_chg1": (s.pct_change() * 100).values}))
FT = pd.concat(feat).sort_values("Date")
secname = C0.ticker.map(SECMAP)
Cs = pd.DataFrame({"_i": np.arange(n), "s33": secname.map(S33), "entry": C0.entry}).sort_values("entry")
J = pd.merge_asof(Cs, FT, left_on="entry", right_on="Date", by="s33", allow_exact_matches=False).sort_values("_i")
print(f"\n業種指数 結合率 {np.isfinite(J.s_chg5.to_numpy()[BASE]).mean()*100:.1f}%  未対応業種名: {sorted(set(secname[secname.map(S33).isna()].dropna()))[:10]}")
# 銘柄側の前日までの 5日騰落 / 25MA乖離（SERの終値・p-1基準）
R5 = np.full(n, np.nan); D25 = np.full(n, np.nan); R1 = np.full(n, np.nan)
for i in range(n):
    a = SER.get(TICK[i]);
    if a is None: continue
    c, pos = a[3], a[5]; p = pos.get(ds[i])
    if p is None or p < 26: continue
    R5[i] = (c[p-1] / c[p-6] - 1) * 100; D25[i] = (c[p-1] / c[p-26:p-1].mean() - 1) * 100; R1[i] = (c[p-1]/c[p-2]-1)*100
S_ABOVE = J.s_above.to_numpy(); S_DEV = J.s_dev25.to_numpy(); S_CHG5 = J.s_chg5.to_numpy(); S_CHG1 = J.s_chg1.to_numpy()
RES5 = R5 - S_CHG5; RESD = D25 - S_DEV; RES1 = R1 - S_CHG1
for col, arr in (("s_above",S_ABOVE),("s_dev",S_DEV),("s_chg5",S_CHG5),("s_chg1",S_CHG1),("r5",R5),("d25",D25),("r1",R1),("res5",RES5),("resd",RESD),("res1",RES1)):
    R0[col] = arr[R0.i.to_numpy()]

print("\n" + "=" * 122); print("① 業種指数（正しい対応表）と残差リバーサル"); print("=" * 122)
grp(R0.dropna(subset=["s_above"]), "s_above", "業種指数25MA上(1.0)/下(0.0)")
quint(R0, "s_dev", "業種指数25MA乖離%"); quint(R0, "s_chg5", "業種指数5日騰落%"); quint(R0, "s_chg1", "業種指数前日騰落%")
quint(R0, "r5", "銘柄5日騰落%"); quint(R0, "res5", "残差5日(銘柄−業種)%"); quint(R0, "resd", "残差25MA乖離(銘柄−業種)%"); quint(R0, "res1", "残差前日(銘柄−業種)%")
print(); print(HDR); print(line("現行", st0))
sim("業種25MA下だけ", BASE & ~(S_ABOVE == 1)); sim("業種25MA上だけ", BASE & ~(S_ABOVE == 0))
for th in (-3.0, -2.0, -1.0):
    sim(f"業種5日<{th}%除外", BASE & ~(S_CHG5 < th))
for th in (1.0, 2.0):
    sim(f"業種5日>{th}%除外", BASE & ~(S_CHG5 > th))
sim("業種前日<-1%除外", BASE & ~(S_CHG1 < -1)); sim("業種前日<-0.5%除外", BASE & ~(S_CHG1 < -0.5)); sim("業種前日>+1%除外", BASE & ~(S_CHG1 > 1))
for th in (-1.0, -2.0, -3.0, -4.0):
    sim(f"残差5日>{th}%除外(業種と一緒に下げた玉を捨てる)", BASE & ~(RES5 > th))
for th in (-1.0, -2.0, -3.0):
    sim(f"残差5日<{th}%除外(銘柄固有の下げを捨てる)", BASE & ~(RES5 < th))
for th in (-1.0, -2.0, -3.0):
    sim(f"残差25MA>{th}%除外", BASE & ~(RESD > th)); sim(f"残差25MA<{th}%除外", BASE & ~(RESD < th))
sim("残差前日<-2%除外", BASE & ~(RES1 < -2)); sim("残差前日>-0.5%除外", BASE & ~(RES1 > -0.5))

print("\n" + "=" * 122); print("② ボラ・レジーム（TOPIX 0000・Nagel 2012）"); print("=" * 122)
tp = I[I.Code == "0000"].sort_values("Date").set_index("Date").C.dropna()
r = tp.pct_change()
VF = pd.DataFrame({"Date": tp.index, "rv20": (r.rolling(20).std() * np.sqrt(250) * 100).values,
                   "rv5": (r.rolling(5).std() * np.sqrt(250) * 100).values,
                   "t5": (tp.pct_change(5) * 100).values, "t1": (r * 100).values,
                   "t_above": (tp > tp.rolling(25).mean()).astype(float).values,
                   "rv_rel": (r.rolling(20).std() / r.rolling(120).std()).values}).sort_values("Date")
Ct = pd.DataFrame({"_i": np.arange(n), "entry": C0.entry}).sort_values("entry")
JT = pd.merge_asof(Ct, VF, left_on="entry", right_on="Date", allow_exact_matches=False).sort_values("_i")
RV20 = JT.rv20.to_numpy(); RV5 = JT.rv5.to_numpy(); T5 = JT.t5.to_numpy(); T1 = JT.t1.to_numpy(); TAB = JT.t_above.to_numpy(); RVREL = JT.rv_rel.to_numpy()
for col, arr in (("rv20",RV20),("rv5",RV5),("t5",T5),("t1",T1),("t_above",TAB),("rv_rel",RVREL)):
    R0[col] = arr[R0.i.to_numpy()]
print(f"TOPIX 結合率 {np.isfinite(RV20[BASE]).mean()*100:.1f}%")
quint(R0, "rv20", "TOPIX 20日実現ボラ(年率%)"); quint(R0, "rv5", "TOPIX 5日実現ボラ"); quint(R0, "rv_rel", "ボラ比 20日/120日")
quint(R0, "t5", "TOPIX 5日騰落%"); quint(R0, "t1", "TOPIX 前日騰落%"); grp(R0.dropna(subset=["t_above"]), "t_above", "TOPIX 25MA上/下")
print(); print(HDR); print(line("現行", st0))
for th in (12, 15, 18, 22):
    sim(f"RV20<{th}%の日は撃たない", BASE & ~(RV20 < th)); sim(f"RV20>{th}%の日は撃たない", BASE & ~(RV20 > th))
for th in (0.8, 1.0, 1.3):
    sim(f"ボラ比<{th}除外", BASE & ~(RVREL < th)); sim(f"ボラ比>{th}除外", BASE & ~(RVREL > th))
sim("TOPIX前日<-1%除外", BASE & ~(T1 < -1)); sim("TOPIX前日>+1%除外", BASE & ~(T1 > 1))
sim("TOPIX5日<-3%除外", BASE & ~(T5 < -3)); sim("TOPIX5日>+3%除外", BASE & ~(T5 > 3))
# 交差: 高ボラ×残差
sim("RV20>18 かつ 残差5日<-2 だけ", BASE & (RV20 > 18) & (RES5 < -2))
print("\n[done]")
