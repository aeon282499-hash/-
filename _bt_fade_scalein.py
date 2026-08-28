# -*- coding: utf-8 -*-
"""_bt_fade_scalein.py — 前夜に置ける「寄成＋前日終値+k%の追加指値」の資金正規化と年別（2026-08-29未明）。
寄り≧指値なら寄り値で約定・寄り後に高値が指値に届けばその水準で約定（悲観寄り: 高値タッチ=約定）。②は50万寄成固定。"""
import numpy as np, pandas as pd
exec(open("_bt_fade_untested_sweep.py", encoding="utf-8").read().split('print("=" * 118); print("■ 基準')[0])
d0 = select(P)
def sim(d, base_sz, add_sz, k):
    r1 = d.rk == 1; lvl = d.px * (1 + k / 100)
    fo = (d.o1 >= lvl) & r1; fh = (~fo) & (d.h1 >= lvl) & r1; hit = fo | fh; fpx = np.where(fo, d.o1, lvl)
    sh_b = np.where(r1, base_sz / d.px // 100 * 100, S2 / d.px // 100 * 100).astype(int); sh_b = np.where((d.rk == 2) & (d.px * 100 > S2), 0, sh_b)
    sh_a = np.where(hit, add_sz / d.px // 100 * 100, 0).astype(int)
    yen = (d.o1 - d.c1) * sh_b + np.where(hit, (fpx - d.c1) * sh_a, 0); dep = sh_b * d.o1 + sh_a * fpx
    t = pd.DataFrame({"y": d.y, "ym": d.ent.str[:7], "ent": d.ent, "rk": d.rk, "yen": yen, "dep": dep}); return t[(sh_b > 0) | (sh_a > 0)]
def rep(t, label):
    yy = t.groupby("y").yen.sum(); mo = t.groupby("ym").yen.sum(); day = t.groupby("ent").yen.sum(); eq = day.sort_index().cumsum(); dd = (eq - eq.cummax()).min()
    cap1 = t[t.rk == 1].dep.mean()
    print(f"  {label:<36}①平均投入{cap1/1e4:>4.0f}万 10年{t.yen.sum()/1e4:>+7.0f}万 年{yy.mean()/1e4:>+5.0f}万 PF{pf(t.yen):.2f} 前半{yy[yy.index<=2021].sum()/1e4:>+5.0f} 後半{yy[yy.index>=2022].sum()/1e4:>+5.0f} 最悪日{day.min()/1e4:>+4.0f} 最悪月{mo.min()/1e4:>+4.0f} -20万超{(mo<-2e5).sum():>2} -40万超{(mo<-4e5).sum():>2} DD{dd/1e4:>+5.0f} 2026{yy.get(2026,0)/1e4:>+5.0f}")
    return yy, mo
print("■ 資金を現行(①平均投入93万)に揃えた追加指値版")
rep(sim(d0, 1_000_000, 0, 3), "現行 ①100万寄成")
for b, a, k in ((50, 100, 3), (40, 80, 3), (35, 70, 3), (30, 60, 3), (30, 100, 3), (20, 100, 3), (40, 80, 2), (40, 80, 5), (0, 100, 3), (0, 100, 2)):
    rep(sim(d0, b * 10000, a * 10000, k), f"①{b}万寄成 + 前日終値+{k}%指値{a}万")
print("\n■ 年別（万円）: 現行 vs ①40万寄成+前日終値+3%指値80万 vs ①50万+100万")
y0, _ = rep(sim(d0, 1_000_000, 0, 3), "現行"); y1, m1 = rep(sim(d0, 400_000, 800_000, 3), "40+80@+3%"); y2, m2 = rep(sim(d0, 500_000, 1_000_000, 3), "50+100@+3%")
print("  年   現行   40+80  50+100")
for y in y0.index: print(f"  {y} {y0[y]/1e4:>+6.0f} {y1.get(y,0)/1e4:>+6.0f} {y2.get(y,0)/1e4:>+6.0f}")
print("  40+80 のワースト月:", m1.nsmallest(5).round(-3).to_dict())
