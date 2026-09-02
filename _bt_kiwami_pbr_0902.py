# -*- coding: utf-8 -*-
"""_bt_kiwami_pbr_0902.py — _bt_kiwami_untested_0902.py の追試（2026-09-02・本番無変更）。
  ① PBR≤2除外が採用候補級（+311→+370万・両期間改善・上位20除去でも残る）→ 閾値の山 / 年別 / 4分割OOS /
     前後半の五分位 / 除外玉の業種構成 / 赤字除外との合成 / 株数推定の頑健性（EPS由来 vs 直前本決算固定）
  ② 業種指数（G）は33業種コードの対応ミス(0.3%)だったので J-Quants指数コード 0040〜0060 で再計測
"""
import pickle
import numpy as np
import pandas as pd

_src = open("_bt_kiwami_untested_0902.py", encoding="utf-8").read().split("# ══ B カレンダー")[0]
_src = _src.replace('print("\\n  ▶ フィルタ・シム（欠損はフェイルオープン', 'if False: print("')
exec(_src)

print("\n" + "=" * 122); print("① PBR 閾値スイープ（欠損はフェイルオープン）"); print("=" * 122); print(HDR)
print(line("現行", st0))
res = {}
for th in (1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 3.0, 4.0, 5.0):
    res[th] = sim(f"PBR≤{th}", fo(np.nan_to_num((PBR <= th).astype(float), nan=0) > 0))
sim("PBR≤2 かつ 赤字除外", fo(np.nan_to_num(((PBR <= 2) & ~LOSS).astype(float), nan=0) > 0))
sim("PBR欠損も除外(≤2)", BASE & (PBR <= 2))

print("\n" + "=" * 122); print("② 年別（現行 vs PBR≤2 / ≤2.5 / ≤3）"); print("=" * 122)
def yearly(mask):
    R = run2(mask, pnl0, exo0); return R.groupby("y").yen.sum().reindex(YEARS, fill_value=0.0), R
Y0, _ = yearly(BASE); Y2, R2 = yearly(fo(np.nan_to_num((PBR <= 2).astype(float), nan=0) > 0))
Y25, _ = yearly(fo(np.nan_to_num((PBR <= 2.5).astype(float), nan=0) > 0)); Y3, _ = yearly(fo(np.nan_to_num((PBR <= 3).astype(float), nan=0) > 0))
print(f"  {'年':>6}{'現行':>12}{'PBR≤2':>12}{'差':>11}{'PBR≤2.5':>12}{'PBR≤3':>12}{'除外玉数':>8}{'除外玉の損益':>12}")
excl = R0[R0.pbr > 2]
for y in YEARS:
    ex = excl[excl.y == y]
    print(f"  {y:>6}{Y0[y]:>+12,.0f}{Y2[y]:>+12,.0f}{Y2[y]-Y0[y]:>+11,.0f}{Y25[y]:>+12,.0f}{Y3[y]:>+12,.0f}{len(ex):>8}{ex.yen.sum():>+12,.0f}")
print(f"  改善年: {int((Y2 > Y0).sum())}/10（≤2） {int((Y25 > Y0).sum())}/10（≤2.5） {int((Y3 > Y0).sum())}/10（≤3）")
print("\n  4分割OOS（17-19 / 20-21 / 22-23 / 24-26）: 現行 → PBR≤2")
for a, b in ((2017, 2019), (2020, 2021), (2022, 2023), (2024, 2026)):
    s0 = Y0[(Y0.index >= a) & (Y0.index <= b)].sum(); s2 = Y2[(Y2.index >= a) & (Y2.index <= b)].sum()
    print(f"    {a}-{b}: {s0:>+12,.0f} → {s2:>+12,.0f}  ({s2-s0:+,.0f})")

print("\n" + "=" * 122); print("③ 前後半それぞれの PBR 五分位（建てた玉ベース・現行構成）"); print("=" * 122)
for lab, m in (("前半17-21", R0.y <= 2021), ("後半22-26", R0.y >= 2022)):
    quint(R0[m], "pbr", f"PBR {lab}")
print("\n  PBR>2 の玉（現行で建てた分）の中身:")
ex = R0[R0.pbr > 2]
print(f"    n={len(ex)} 平均{ex.pnl.mean():+.3f}% 勝率{(ex.pnl>0).mean()*100:.1f}% 円{ex.yen.sum():+,.0f} / PBR>3: n={(R0.pbr>3).sum()} 平均{R0[R0.pbr>3].pnl.mean():+.3f}%")
sec_ex = pd.Series(SEC[ex.i.to_numpy()]).value_counts().head(10)
sec_all = pd.Series(SEC[R0.i.to_numpy()]).value_counts()
print("    業種構成(上位10・除外玉数/現行全体):", {k: f"{v}/{sec_all.get(k, 0)}" for k, v in sec_ex.items()})
print("    除外玉のPBR中央値:", f"{ex.pbr.median():.2f}", " 価格帯中央値:", f"{np.median(E[ex.i.to_numpy()]):,.0f}円")
# 業種を落とすのと同じか？（高PBR業種そのものが毒なのか、業種内の高PBRが毒なのか）
print("\n  業種内で見た PBR>2 vs ≤2（現行で建てた玉・n≥20の業種）")
R0["sec"] = SEC[R0.i.to_numpy()]
for s, g in R0.groupby("sec"):
    hi = g[g.pbr > 2]; lo = g[g.pbr <= 2]
    if len(hi) >= 20 and len(lo) >= 20:
        print(f"    {s:<12} >2: n={len(hi):>3} {hi.pnl.mean():+.3f}%   ≤2: n={len(lo):>3} {lo.pnl.mean():+.3f}%")

print("\n" + "=" * 122); print("④ 株数推定の頑健性: 直前「本決算(FY)」の EPS/NP だけで株数を固定した PBR"); print("=" * 122)
Ffy = F[F.CurPerType.astype(str).str.contains("FY", na=False)].dropna(subset=["discd"])
Ffy = Ffy[["code4", "discd", "eqv", "shares"]].sort_values("discd")
M2 = pd.merge_asof(Cc, Ffy, left_on="entry", right_on="discd", by="code4", allow_exact_matches=False).sort_values("_i")
PBR_FY = np.where(M2.eqv.to_numpy() > 0, E * M2.shares.abs().to_numpy() / M2.eqv.to_numpy(), np.nan)
print(f"  結合率 {np.isfinite(PBR_FY[BASE]).mean()*100:.1f}% / 四半期版との相関 {pd.Series(PBR).corr(pd.Series(PBR_FY)):.3f}")
print(HDR); print(line("現行", st0))
for th in (1.5, 2.0, 2.5, 3.0):
    sim(f"PBR_FY≤{th}", fo(np.nan_to_num((PBR_FY <= th).astype(float), nan=0) > 0))

print("\n" + "=" * 122); print("⑤ 業種指数（再計測・J-Quants 33業種コード 0040〜0060）"); print("=" * 122)
S33IDX = dict(zip(["水産・農林業", "鉱業", "建設業", "食料品", "繊維製品", "パルプ・紙", "化学", "医薬品", "石油･石炭製品",
                   "ゴム製品", "ガラス･土石製品", "鉄鋼", "非鉄金属", "金属製品", "機械", "電気機器", "輸送用機器", "精密機器",
                   "その他製品", "電気･ガス業", "陸運業", "海運業", "空運業", "倉庫･運輸関連業", "情報･通信業", "卸売業",
                   "小売業", "銀行業", "証券･商品先物取引業", "保険業", "その他金融業", "不動産業", "サービス業"],
                  ["0040", "0041", "0042", "0043", "0044", "0045", "0046", "0047", "0048", "0049", "004A", "004B", "004C",
                   "004D", "004E", "004F", "0050", "0051", "0052", "0053", "0054", "0055", "0056", "0057", "0058", "0059",
                   "005A", "005B", "005C", "005D", "005E", "005F", "0060"]))
I = pd.read_pickle("_indices_10y.pkl"); I["C"] = pd.to_numeric(I.C, errors="coerce"); I["Date"] = pd.to_datetime(I.Date)
feat = []
for code, g in I.groupby("Code"):
    s = g.sort_values("Date").set_index("Date").C.dropna()
    feat.append(pd.DataFrame({"Date": s.index, "s33": code, "above": (s > s.rolling(25).mean()).astype(float).values,
                              "chg5": (s.pct_change(5) * 100).values, "chg1": (s.pct_change() * 100).values,
                              "dev25": ((s / s.rolling(25).mean() - 1) * 100).values}))
FT = pd.concat(feat).sort_values("Date")
Cs = pd.DataFrame({"_i": np.arange(n), "s33": C0.ticker.map(SECMAP).map(S33IDX), "entry": C0.entry}).sort_values("entry")
J = pd.merge_asof(Cs, FT, left_on="entry", right_on="Date", by="s33", allow_exact_matches=False).sort_values("_i")
ABOVE = J.above.to_numpy(); CHG5 = J.chg5.to_numpy(); CHG1 = J.chg1.to_numpy(); DEV = J.dev25.to_numpy()
print(f"  結合率 {np.isfinite(CHG5[BASE]).mean()*100:.1f}%")
for c, a in (("s_above", ABOVE), ("s_chg5", CHG5), ("s_chg1", CHG1), ("s_dev", DEV)):
    R0[c] = a[R0.i.to_numpy()]
grp(R0.dropna(subset=["s_above"]), "s_above", "業種指数25MA上(1.0)/下(0.0)")
quint(R0, "s_chg5", "業種指数5日騰落%"); quint(R0, "s_chg1", "業種指数前日騰落%"); quint(R0, "s_dev", "業種指数25MA乖離%")
print(HDR); print(line("現行", st0))
sim("業種25MA下だけ", BASE & ~(ABOVE == 1)); sim("業種25MA上だけ", BASE & ~(ABOVE == 0))
for th in (-3.0, -2.0, -1.0):
    sim(f"業種5日<{th}%除外", BASE & ~(CHG5 < th))
for th in (1.0, 2.0, 3.0):
    sim(f"業種5日>{th}%除外", BASE & ~(CHG5 > th))
sim("業種前日<-1%除外", BASE & ~(CHG1 < -1)); sim("業種前日>+1%除外", BASE & ~(CHG1 > 1))
for th in (-3.0, -2.0):
    sim(f"業種乖離<{th}%除外", BASE & ~(DEV < th))
for th in (2.0, 3.0):
    sim(f"業種乖離>{th}%除外", BASE & ~(DEV > th))
print("\n[done]")
