# -*- coding: utf-8 -*-
"""_bt_gokujo_round12_addon_split_0919.py — 本玉と勝ち乗せ玉の資金配分（最大400万を固定して、本玉を小さく乗せを大きく＝「小さく試して勝ったら厚く」）
乗せ玉は初日が+1%で終わった玉にだけ入る＝条件付き期待値が本玉より高いなら、資金は乗せ側に寄せるべき。R2は同額/半額しか見ていない。
実行: python -X utf8 _bt_gokujo_round12_addon_split_0919.py > _log_gokujo_round12_addon_split_0919.txt"""
import numpy as np, pandas as pd, time, re
src = open("_bt_gokujo_round6_26y.py", encoding="utf-8").read(); exec(src.split("# ── A 決算日近接 ──")[0])
r9 = open("_bt_gokujo_round9_capital_freq_0919.py", encoding="utf-8").read()
m = re.search(r"TICKv = C\.ticker\.to_numpy\(\).*?(?=\nprint\(\"\\n■ A 資金構成)", r9, re.S); exec(m.group(0))
def jl(r, b): return " ★" if (r["a"] > b["a"] and r["b"] > b["b"] and r["e3"] > b["e3"] and r["e4"] > b["e4"] and r["dd"] >= b["dd"] - 1 and r["worst"] >= b["worst"] - 1) else ""
# 乗せ玉の素の期待値
R = run_gen(G, SCORE, slots=1, size=2_000_000, addon_frac=0.0); ii = R.i.to_numpy()
o2 = OP[:, 1]; exit_px = E0 * (1 + PNL1 / 100); add_ok = (CL[:, 0] > E0 * 1.01) & (EXO1 >= 1) & np.isfinite(o2) & (o2 > 0)
ap = (exit_px / o2 - 1) * 100
print(f"■ 本玉 {len(ii)}件 件あたり{PNL1[ii].mean():+.2f}% 勝率{(PNL1[ii]>0).mean()*100:.1f}% / 乗せ条件成立 {add_ok[ii].sum()}件({add_ok[ii].mean()*100:.0f}%) 乗せ玉の件あたり{ap[ii][add_ok[ii]].mean():+.2f}% 勝率{(ap[ii][add_ok[ii]]>0).mean()*100:.1f}%")
for a, b, era in ((2001, 2013, "01-13"), (2014, 2026, "14-26"), (2017, 2021, "17-21"), (2022, 2026, "22-26")):
    ye = (YEARv[ii] >= a) & (YEARv[ii] <= b); k = add_ok[ii] & ye
    print(f"    {era}: 本玉{PNL1[ii][ye].mean():+.2f}% / 乗せ玉{ap[ii][k].mean():+.2f}% (n={k.sum()})")
print("\n■ 最大400万固定で本玉:乗せ を振る（26年）", flush=True)
B0 = show("本玉200＋乗せ200(現行)", run_gen(G, SCORE, slots=1, size=2_000_000, addon_frac=1.0), 400)
for base, add in ((100, 300), (130, 270), (150, 250), (250, 150), (300, 100), (400, 0)):
    r = show(f"本玉{base}＋乗せ{add}", run_gen(G, SCORE, slots=1, size=base * 10_000, addon_frac=add / base), 400); print(f"      利益/DD {r['tot']/-r['dd']:.2f}(現行{B0['tot']/-B0['dd']:.2f}) 判定:{jl(r, B0) or ' —'}")
print("\n■ 乗せ条件の段階（初日終値の上げ幅で乗せ額を変える・本玉200）", flush=True)
def run_tier(lab, tiers):
    Rb = run_gen(G, SCORE, slots=1, size=2_000_000, addon_frac=0.0).copy(); jj = Rb.i.to_numpy(); c1r = (CL[:, 0] / E0 - 1) * 100
    frac = np.zeros(n)
    for lo_, hi_, f in tiers: frac[(c1r > lo_) & (c1r <= hi_)] = f
    okk = (EXO1 >= 1) & np.isfinite(o2) & (o2 > 0) & (frac > 0)
    ash = np.where(okk[jj], (2_000_000 * frac[jj] / o2[jj] // 100 * 100), 0).astype(int)
    Rb["yen"] = Rb.yen + (exit_px[jj] - o2[jj]) * ash; Rb["expo"] = Rb.expo + ash * o2[jj]
    r = show(lab, Rb, 400); print(f"      最大建玉{r['maxexpo']:.0f}万 年利(最大建玉基準){r['tot']/r['maxexpo']/26*100:+.1f}% 利益/DD {r['tot']/-r['dd']:.2f} 判定:{jl(r, B0) or ' —'}")
run_tier("+1〜2%→同額 / +2%超→同額(現行)", [(1, 2, 1.0), (2, 99, 1.0)])
run_tier("+1〜2%→半額 / +2%超→同額", [(1, 2, 0.5), (2, 99, 1.0)])
run_tier("+1〜2%→同額 / +2%超→なし", [(1, 2, 1.0), (2, 99, 0.0)])
run_tier("+0.5〜1%→半額 / +1%超→同額", [(0.5, 1, 0.5), (1, 99, 1.0)])
run_tier("+1〜3%→同額 / +3%超→なし", [(1, 3, 1.0), (3, 99, 0.0)])
c1r = (CL[:, 0] / E0 - 1) * 100
for lo_, hi_ in ((0, 0.5), (0.5, 1), (1, 2), (2, 3), (3, 5), (5, 99)):
    k = ii[(c1r[ii] > lo_) & (c1r[ii] <= hi_) & (EXO1[ii] >= 1)]
    print(f"    初日終値 +{lo_}〜{hi_}%: n={len(k):>4} 乗せ玉の期待値{ap[k].mean():+.2f}% 勝率{(ap[k]>0).mean()*100:.0f}%")
print(f"\n[done] {time.time()-t0:.0f}s")
