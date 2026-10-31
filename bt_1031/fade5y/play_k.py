import re, os, numpy as np, pandas as pd
src = open("fade5y/vamin_bt.py", encoding="utf-8").read()
head, rest = src.split('L = ["# 今日の3本', 1)
build = rest.split("\n", 2)[2].split("out = os.path.join")[0]
build = build.replace('report(U5, "2024-01-01", "5年（J-Quants 2021-11〜2026-10・上場廃止込み）", L)', 'R5()')
build = build.replace('report(U20, "2014-01-01", "20年（立花 2001〜2026・現存銘柄のみ）", L)', 'R20()')
build = re.sub(r'z26 = U5.*?\ndel G, C', 'del G, C', build, flags=re.S)
play = '''
def play_run(T, k, size=250000, strict=True):
    rows = []
    for r in T.itertuples():
        if r.pin: continue
        Lp = lim(r.px, k)
        s = r.o if r.o >= Lp else (Lp if r.h >= Lp + (tk(Lp) if strict else 0) else None)
        if s is None: continue
        sh = int(size / Lp // 100 * 100)
        if sh < 100:
            if Lp * 100 > 2 * size: continue
            sh = 100
        y = (s - r.c * 1.001) * sh; rows.append({"d": r.d, "yen": y, "r": y / (s * sh), "fill_open": s == r.o})
    Z = pd.DataFrame(rows, columns=["d", "yen", "r", "fill_open"]); Z["m"] = Z.d.str[:7]; return Z
def rep(U, split, title):
    T = U[(U.g >= 12) & (U.va >= 1e8) & (U.va < 3e8)].sort_values(["d", "g", "code"], ascending=[True, False, True])
    T = T.assign(rk=T.groupby("d").cumcount() + 1); T = T[T.rk == 1]
    n_place = len(T[~T.pin])
    L.append(""); L.append(f"## {title}（1日1本・25万・引成・1tick抜け判定）置いた日 {n_place}日")
    L.append("| 指値 | 約定 | 約定率 | 勝率 | 平均 | PF | 合計 | 最大DD | 最悪月 | 負け月 | 前半/後半 PF | 最悪1本 | 合計÷DD |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for k in (0.03, 0.05, 0.07, 0.09, 0.10, 0.12, 0.14, 0.15, 0.17, 0.20):
        Z = play_run(T, k)
        if not len(Z): continue
        day = Z.groupby("d").yen.sum().sort_index().cumsum().to_numpy(); dd = (np.maximum.accumulate(np.r_[0, day])[1:] - day).max()
        mo = Z.groupby("m").yen.sum(); a, b = Z[Z.d < split], Z[Z.d >= split]
        L.append(f"| **+{k*100:.0f}%** | {len(Z)} | {len(Z)/n_place*100:.0f}% | {(Z.yen>0).mean()*100:.0f}% | {Z.r.mean()*100:+.2f}% | {pf(Z.yen.to_numpy()):.2f} | {Z.yen.sum()/1e4:+,.0f}万 | {dd/1e4:.0f}万 | {mo.min()/1e4:+.0f}万 | {int((mo<0).sum())}/{Z.m.nunique()} | {pf(a.yen.to_numpy()):.2f} / {pf(b.yen.to_numpy()):.2f} | {Z.r.min()*100:+.0f}% | {Z.yen.sum()/max(dd,1):.1f} |")
R5 = lambda: rep(U5, "2024-01-01", "5年")
R20 = lambda: rep(U20, "2014-01-01", "20年")
'''
L = ["# 遊び: 指値を何%に置くか（代金1〜3億・+12%以上の1位）"]
g = {"L": L, "__file__": os.path.abspath("fade5y/vamin_bt.py")}
exec(head + play + build, g)
out = "\n".join(g["L"]); print(out)
open(r"C:/Users/星野/Downloads/trade_bt/uwick_bt/fade5y/RESULTS_play_k.md", "w", encoding="utf-8").write(out + "\n")
