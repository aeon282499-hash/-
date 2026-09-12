# -*- coding: utf-8 -*-
"""_bt_fade_limit_allday_0912.py — 本人「①寄り指値が刺さらず、当日中指値なら刺さって爆益だった(9/11モルフォ)」(2026-09-12)。
ハイカラ(HYPER)は指値のみ＝実務の比較は「寄成」ではなく「寄付指値(寄りだけ) vs 当日中指値」。26年プールで①②とも同じ注文種別を適用。
mkt=寄成 / open=寄付指値(o1≥指値なら寄値約定・それ以外は見送り) / day=当日中指値(o1≥指値なら寄値・でなければ高値≥指値×(1+ovs)で指値約定・でなければ見送り)。
指値=前日終値×(1+k%)。ovs=約定に要求する上抜け%。"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from _bt_fade_26y_lib import load, base_mask, rank, pf, ERAS, S1, S2
D = load("near"); P = rank(D[base_mask(D)]); P = P[P.rk <= 2].copy()
P["sh"] = np.where(P.rk == 1, S1 / P.px // 100 * 100, S2 / P.px // 100 * 100).astype(int); P = P[P.sh > 0].copy()

def sim(mode, k=0.0, ovs=0.0, only1=False):
    d = P.copy(); lvl = d.px * (1 + k / 100)
    at_open = d.o1 >= lvl
    if mode == "mkt":
        ent = d.o1; fill = np.ones(len(d), bool); kind = np.where(True, "open", "open")
    elif mode == "open":
        ent = d.o1; fill = at_open.values; kind = np.where(at_open, "open", "skip")
    else:
        intra = (~at_open) & (d.h1 >= lvl * (1 + ovs / 100))
        ent = np.where(at_open, d.o1, lvl); fill = (at_open | intra).values
        kind = np.where(at_open, "open", np.where(intra, "intra", "skip"))
    if only1:   # ②は寄成のまま
        r2 = (d.rk == 2).values; ent = np.where(r2, d.o1, ent); fill = fill | r2; kind = np.where(r2, "open", kind)
    d["ent_px"] = ent; d["kind"] = kind; d["fill"] = fill
    d["yen"] = np.where(fill, (d.ent_px - d.c1) * d.sh, 0.0)
    d["mkt_yen"] = (d.o1 - d.c1) * d.sh   # 同じ玉を寄成で建てていたら
    return d

def rep(d, label, show=True):
    t = d[d.fill]
    yy = t.groupby("y").yen.sum(); ym = t.groupby("ym").yen.sum(); day = t.groupby("ent").yen.sum()
    eq = day.sort_index().cumsum(); dd = (eq - eq.cummax()).min()
    era = {nm: t[(t.y >= lo) & (t.y <= hi)].yen.sum() / 1e4 for lo, hi, nm in ERAS}
    if show:
        print(f"{label:<40} n{len(t):>5} 勝率{(t.yen>0).mean()*100:5.1f} PF{pf(t.yen):5.2f} 件{(t.yen/(t.ent_px*t.sh)).mean()*100:+5.2f}% "
              f"26年{t.yen.sum()/1e4:>+7,.0f} 10年{t[t.y>=2017].yen.sum()/1e4:>+7,.0f} "
              f"e1{era['e1']:>+6,.0f} e2{era['e2']:>+6,.0f} e3{era['e3']:>+6,.0f} e4{era['e4']:>+6,.0f} "
              f"勝年{(yy>0).sum():>2}/{len(yy)} 最悪月{ym.min()/1e4:>+5,.0f} 最悪日{day.min()/1e4:>+5,.0f} DD{dd/1e4:>+6,.0f} -20万月{(ym<-2e5).sum():>3} "
              f"25年{yy.get(2025,0)/1e4:>+5,.0f} 26年YTD{yy.get(2026,0)/1e4:>+5,.0f} 上位3日除去{(t.yen.sum()-day.nlargest(3).sum())/1e4:>+7,.0f}")
    return t

print("■ 26年プール・①100万/②50万・①②とも同じ注文種別（HYPERは指値のみ＝実務）")
rep(sim("mkt"), "寄成（現行BT）")
for k in (0, 1, 2, 3):
    rep(sim("open", k), f"寄付指値 @前終+{k}%（寄りだけ・昨日の①の形）")
for k in (0, 1, 2, 3):
    for ovs in (0.0, 0.3, 0.5, 1.0):
        rep(sim("day", k, ovs), f"当日中指値 @前終+{k}% 上抜け{ovs}%要求")
print("\n■ ①だけ注文種別を変え②は寄成のまま")
for k in (0, 1):
    rep(sim("open", k, only1=True), f"①寄付指値 @前終+{k}%・②寄成")
    for ovs in (0.0, 0.3):
        rep(sim("day", k, ovs, only1=True), f"①当日中指値 @前終+{k}% 上抜け{ovs}%・②寄成")

print("\n■ 機構: 当日中指値@前終(上抜け0.3%) の玉種別（同じ玉を寄成で建てた場合との比較）")
d = sim("day", 0, 0.3)
for kd in ("open", "intra", "skip"):
    s = d[d.kind == kd]
    if kd == "skip":
        print(f"  {kd:<6} n{len(s):>5}  寄成なら 件{(s.mkt_yen/(s.o1*s.sh)).mean()*100:+5.2f}% 勝率{(s.mkt_yen>0).mean()*100:5.1f} 26年{s.mkt_yen.sum()/1e4:>+7,.0f}万（見送りで失う分）")
    else:
        print(f"  {kd:<6} n{len(s):>5}  指値 件{(s.yen/(s.ent_px*s.sh)).mean()*100:+5.2f}% 勝率{(s.yen>0).mean()*100:5.1f} 26年{s.yen.sum()/1e4:>+7,.0f}万 | 寄成なら 件{(s.mkt_yen/(s.o1*s.sh)).mean()*100:+5.2f}% 26年{s.mkt_yen.sum()/1e4:>+7,.0f}万")
print("  時代別 intra玉(場中約定) 件%/勝率:", "  ".join(f"{nm}:{(d[(d.kind=='intra')&(d.y>=lo)&(d.y<=hi)].yen/(d[(d.kind=='intra')&(d.y>=lo)&(d.y<=hi)].ent_px*d[(d.kind=='intra')&(d.y>=lo)&(d.y<=hi)].sh)).mean()*100:+.2f}%/{(d[(d.kind=='intra')&(d.y>=lo)&(d.y<=hi)].yen>0).mean()*100:.0f}%" for lo,hi,nm in ERAS))

print("\n■ 年別(万): 寄成 / 寄付指値@前終 / 当日中指値@前終(上抜け0.3%)")
a = rep(sim("mkt"), "", False).groupby("y").yen.sum(); b = rep(sim("open", 0), "", False).groupby("y").yen.sum(); c = rep(sim("day", 0, 0.3), "", False).groupby("y").yen.sum()
print("  " + " ".join(f"{y}:{a.get(y,0)/1e4:+.0f}/{b.get(y,0)/1e4:+.0f}/{c.get(y,0)/1e4:+.0f}" for y in a.index))
print("\n■ 2026年の①玉 月別(万): 寄成 / 寄付指値 / 当日中指値0.3%")
a1 = sim("mkt"); a1 = a1[(a1.y==2026)&(a1.fill)].groupby("ym").yen.sum(); b1 = sim("open",0); b1=b1[(b1.y==2026)&(b1.fill)].groupby("ym").yen.sum(); c1 = sim("day",0,0.3); c1=c1[(c1.y==2026)&(c1.fill)].groupby("ym").yen.sum()
print("  " + " ".join(f"{m}:{a1.get(m,0)/1e4:+.0f}/{b1.get(m,0)/1e4:+.0f}/{c1.get(m,0)/1e4:+.0f}" for m in a1.index))

print("\n■ 追加: 指値を前終より下に置く(HYPERで「ほぼ成行」を作る形・寄値≥指値なら寄値約定・上抜け0.3%)")
for k in (-1, -2, -3, -5, -10):
    rep(sim("day", k, 0.3), f"当日中指値 @前終{k}% 上抜け0.3%要求")
print("\n■ 年別勝敗: 当日中指値@前終(0.3%) が 寄付指値@前終 を上回った年数")
a = rep(sim("open", 0), "", False).groupby("y").yen.sum(); c = rep(sim("day", 0, 0.3), "", False).groupby("y").yen.sum()
print(f"  {(c>=a).sum()}/{len(a)} 年で当日中≥寄付  合計差 {(c.sum()-a.sum())/1e4:+,.0f}万/26年 = {(c.sum()-a.sum())/1e4/26:+.0f}万/年")
print("\n■ 9/11 モルフォ(3653)の当てはめ: 前終1490 寄1460 高1580 終1289 600株")
for nm, e in (("寄付指値1490", None), ("当日中指値1490", 1490), ("寄成(≒指値1416=-5%)", 1460)):
    print(f"  {nm:<22} {'見送り(0円)' if e is None else f'{(e-1289)*600:+,}円 ({(e-1289)/e*100:+.1f}%)'}")
