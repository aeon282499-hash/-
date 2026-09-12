# -*- coding: utf-8 -*-
"""ハイカラ料(円/株)を引いた手取りで 成行/寄付限定指値/当日中指値 を比較（2026-09-12・本人「7円とか15円・どれがバランスいい」）"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import importlib.util
spec = importlib.util.spec_from_file_location("m", "_bt_fade_limit_allday_0912.py")
src = open("_bt_fade_limit_allday_0912.py", encoding="utf-8").read().split('print("■ 26年プール')[0].replace("sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding=\"utf-8\")", "")
ns = {}; exec(compile(src, "lib", "exec"), ns); sim = ns["sim"]; pf = ns["pf"]
P1 = None
def net(d, p):  # p=円/株。約定玉だけ料を払う
    t = d[d.fill].copy(); t["net"] = t.yen - p * t.sh; return t
modes = [("成行", sim("mkt")), ("寄付限定指値@前終", sim("open", 0)), ("当日中指値@前終", sim("day", 0, 0.3)), ("当日中指値@前終-3%", sim("day", -3, 0.3))]
print("■ ①玉(100万)だけ・料金 円/株 別の手取り(万) 26年 / 10年 / PF(26年)  ※料は約定した玉だけ払う")
print(f"{'料/株':>6} " + " ".join(f"{nm:>22}" for nm, _ in modes))
for p in (0, 3, 5, 7, 10, 15, 20, 25, 30, 40):
    row = []
    for nm, d in modes:
        t = net(d[d.rk == 1], p)
        row.append(f"{t.net.sum()/1e4:>+7,.0f}/{t[t.y>=2017].net.sum()/1e4:>+6,.0f} {pf(t.net):4.2f}")
    print(f"{p:>5}円 " + " ".join(f"{r:>22}" for r in row))
print("\n■ 料を「資金に対する%」で見る（料÷株価）: ①玉の玉種別 gross 件あたり%（26年・上抜け0.3%）")
d = sim("day", 0, 0.3); d1 = d[d.rk == 1]
for kd, lab in (("open", "上に寄った玉(3パターン共通で建つ)"), ("intra", "下に寄って前終まで戻った玉(当日中だけ建つ)"), ("skip", "戻らなかった玉(成行だけ建つ)")):
    s = d1[d1.kind == kd]
    g = (s.mkt_yen / (s.o1 * s.sh)) if kd == "skip" else (s.yen / (s.ent_px * s.sh))
    print(f"  {lab:<28} n{len(s):>5} 件{g.mean()*100:+5.2f}% 勝率{(g>0).mean()*100:5.1f} 中央値{g.median()*100:+5.2f}% 10年件{g[s.y>=2017].mean()*100:+5.2f}%")
print("\n■ 株価別: 7円/15円が資金の何%か（100万÷株価の株数×料）と、その株価帯の答え")
for px in (300, 500, 1000, 1500, 2000, 3000, 5000):
    sh = 1_000_000 // px // 100 * 100
    print(f"  株価{px:>5}円 {sh:>5}株: 7円={7*sh/1e4:4.1f}万({7*sh/1e6*100:.2f}%) 15円={15*sh/1e4:4.1f}万({15*sh/1e6*100:.2f}%)")

print("\n■ ①玉・料を『資金比 r%』で引いた時の、下に寄った玉(1,241玉)1玉あたり手取り%（3パターン）と全玉合計")
d = sim("day", 0, 0.3); d1 = d[d.rk == 1].copy(); m = sim("mkt"); m1 = m[m.rk == 1]
down = d1[d1.kind != "open"]; up = d1[d1.kind == "open"]
mkt_down = (m1.loc[down.index].yen / (m1.loc[down.index].o1 * m1.loc[down.index].sh))
intra = down[down.kind == "intra"]; g_intra = intra.yen / (intra.ent_px * intra.sh); share_intra = len(intra) / len(down)
g_up = up.yen / (up.ent_px * up.sh)
for lab, msk in (("26年", np.ones(len(down), bool)), ("10年", (down.y >= 2017).values)):
    md = mkt_down[msk].mean() * 100; gi = g_intra[(intra.y >= 2017).values if lab == "10年" else np.ones(len(intra), bool)].mean() * 100
    si = ((intra.y >= 2017).sum() / (down.y >= 2017).sum()) if lab == "10年" else share_intra
    gu = g_up[(up.y >= 2017).values if lab == "10年" else np.ones(len(up), bool)].mean() * 100
    print(f"  [{lab}] 上寄り玉 gross{gu:+.2f}%  下寄り玉: 成行で建てると{md:+.2f}%/玉  当日中は{si*100:.0f}%が前終で建ち その玉{gi:+.2f}%")
    print(f"     r%   成行(下寄り玉)  当日中(下寄り玉)  寄付限定  ← 大きい順が答え / 上寄り玉の手取り")
    for r in (0.0, 0.3, 0.45, 0.6, 0.7, 0.8, 1.0, 1.2, 1.3, 1.5, 2.0):
        a = md - r; b = si * (gi - r); c = 0.0; best = max([("成行", a), ("当日中", b), ("寄付限定", c)], key=lambda x: x[1])[0]
        print(f"   {r:4.2f}%   {a:+6.2f}%        {b:+6.2f}%        {c:+.2f}%   → {best:<6}  上寄り玉{gu - r:+.2f}%{'(見送り線)' if gu - r < 0 else ''}")
