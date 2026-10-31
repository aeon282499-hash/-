# -*- coding: utf-8 -*-
"""_bt_gokujo_score_grid_1031.py — 極上の選定スコア(RSI中心/幅・乖離中心/幅・重み)を総当たり（2026-10-31 本人「スコア設定が間違ってるかも 総当たりで」・検証のみ）
物差し = _bt_gokujo_entry_sweep_1029.py と同じ(engine2・広い候補・1枠×150万・往復0.13%・2017-01〜2026-08)。入口・出口は本番のまま、1位を選ぶ並べ方だけ変える。
前半(2017-21)で選び後半(2022-26)で確認。噪音床=ランダムに1本(100回)。
実行: python -X utf8 _bt_gokujo_score_grid_1031.py"""
import sys, itertools, time
import numpy as np, pandas as pd
sys.stdout.reconfigure(encoding="utf-8")
src = open("_bt_gokujo_entry_sweep_1029.py", encoding="utf-8").read().split('L = ["# 極上の入口')[0]
exec(src)
t0 = time.time()
M = mask()
R_, D_, T_ = E.RSI_, E.DEV_, E.TOV_
ft = np.log10(np.maximum(T_, 1) / 1e9 + 1) / 3
BY0 = E.by_day
def set_rank(score):
    by = [[] for _ in range(len(E.DAYS))]
    for i in np.where(E.BASE_OK)[0]: by[E.DAYI[i]].append(i)
    E.by_day = [np.array(sorted(g, key=lambda i: -score[i]), dtype=int) for g in by]
def ev(score):
    set_rank(score); R = E.run(slots=1, size=SIZE, mask=M, cost_rt=COST)
    s, h1, h2, y = st(R), st(R, "2017-01-01", "2021-12-31"), st(R, "2022-01-01"), st(R, "2026-01-01")
    return dict(n=s["n"], win=s["win"], pf=s["pf"], tot=s["tot"], dd=s["dd"], wy=s["wy"], h1=h1["tot"], h2=h2["tot"], h1pf=h1["pf"], h2pf=h2["pf"], y26=y["tot"])
W = {"今(0.3/0.3/0.4)": (.3, .3, .4), "均等": (1/3, 1/3, 1/3), "代金重め(.25/.25/.5)": (.25, .25, .5), "代金軽め(.4/.4/.2)": (.4, .4, .2),
     "乖離軽め(.2/.3/.5)": (.2, .3, .5), "RSI軽め(.3/.2/.5)": (.3, .2, .5), "代金なし(.5/.5/0)": (.5, .5, 0), "RSI重め(.5/.25/.25)": (.5, .25, .25),
     "乖離重め(.25/.5/.25)": (.25, .5, .25), "RSIなし(0/.5/.5)": (0, .5, .5), "乖離なし(.5/0/.5)": (.5, 0, .5)}
rows = []
for rc, rw, dc, dw, (wl, (wr, wd, wt)) in itertools.product((30, 34, 38, 42, 45), (4, 8, 12), (-1.5, -3.0, -4.5, -6.0), (1, 2, 4), W.items()):
    fr = 1 / (1 + ((R_ - rc) / rw) ** 2); fd = 1 / (1 + ((D_ - dc) / dw) ** 2)
    r = ev(wr * fr + wd * fd + wt * ft); r.update(rc=rc, rw=rw, dc=dc, dw=dw, w=wl); rows.append(r)
G = pd.DataFrame(rows)
cur = G[(G.rc == 38) & (G.rw == 8) & (G.dc == -3.0) & (G.dw == 2) & (G.w == "今(0.3/0.3/0.4)")].iloc[0]
# 噪音床: ランダムに1本
E.by_day = BY0
rs = np.array([st(E.run(slots=1, size=SIZE, mask=M, cost_rt=COST, rank="random", seed=sd))["tot"] for sd in range(100)])
E.by_day = BY0
HD = "| RSI中心/幅 | 乖離中心/幅 | 重み | 玉 | 勝率 | PF | 10年 | 最大DD | 最悪年 | 17-21 / 22-26 | 後半PF | 2026 |\n|---|---|---|---|---|---|---|---|---|---|---|---|"
def ln(r): return (f"| {r.rc:g}/{r.rw:g} | {r.dc:g}/{r.dw:g} | {r.w} | {int(r.n)} | {r.win:.1f}% | {r.pf:.2f} | {r.tot:+.1f}万 | {r.dd:.1f}万 | {r.wy:+.1f}万 | "
                   f"{r.h1:+.1f} / {r.h2:+.1f} | {r.h2pf:.2f} | {r.y26:+.1f}万 |")
L = [f"# 極上 選定スコアの総当たり {len(G)}通り（入口・出口は本番のまま・1位だけ買う・engine2・1枠×150万・往復0.13%・2017-01〜2026-08）\n",
     f"噪音床（ランダムに1本・100回）: 平均 {rs.mean():+.1f}万・標準偏差 {rs.std():.1f}万・95%点 {np.quantile(rs, .95):+.1f}万・最大 {rs.max():+.1f}万\n",
     "## 今のスコア", HD, ln(cur),
     f"\n今より10年合計が上のセル: {int((G.tot > cur.tot).sum())}/{len(G)}（{(G.tot > cur.tot).mean()*100:.0f}%）・今の順位 {int((G.tot > cur.tot).sum())+1}位・"
     f"噪音1sd({rs.std():.0f}万)以上 上回るセル: {int((G.tot > cur.tot + rs.std()).sum())}・2sd以上: {int((G.tot > cur.tot + 2*rs.std()).sum())}",
     "\n## 10年合計の上位15", HD]
for _, r in G.sort_values("tot", ascending=False).head(15).iterrows(): L.append(ln(r))
L += ["\n## 前半(2017-21)で選んだ上位20 → 後半(2022-26)で確認", HD]
top = G.sort_values("h1", ascending=False).head(20)
for _, r in top.iterrows(): L.append(ln(r))
L.append(f"\n前半上位20の後半合計 平均 {top.h2.mean():+.1f}万（今 {cur.h2:+.1f}万）・後半PF平均 {top.h2pf.mean():.2f}（今 {cur.h2pf:.2f}）・前半上位20のうち後半も今より上: {int((top.h2 > cur.h2).sum())}/20")
L += ["\n## 軸ごとの平均（他の軸は全部まとめて平均）"]
for ax, nm in (("rc", "RSI中心"), ("rw", "RSI幅"), ("dc", "乖離中心"), ("dw", "乖離幅"), ("w", "重み")):
    g = G.groupby(ax).agg(tot=("tot", "mean"), h1=("h1", "mean"), h2=("h2", "mean"), pf=("pf", "mean"), dd=("dd", "mean")).reset_index()
    L.append(f"\n### {nm}\n| 値 | 10年平均 | 17-21平均 | 22-26平均 | PF平均 | DD平均 |\n|---|---|---|---|---|---|")
    for _, r in g.iterrows(): L.append(f"| {r[ax]} | {r.tot:+.1f}万 | {r.h1:+.1f} | {r.h2:+.1f} | {r.pf:.2f} | {r.dd:.1f} |")
L += ["\n## 今のセルの隣（1つだけ動かす）", HD]
for col, vals in (("rc", (30, 34, 42, 45)), ("rw", (4, 12)), ("dc", (-1.5, -4.5, -6.0)), ("dw", (1, 4))):
    for v in vals:
        q = {"rc": 38, "rw": 8, "dc": -3.0, "dw": 2, "w": "今(0.3/0.3/0.4)"}; q[col] = v
        r = G[(G.rc == q["rc"]) & (G.rw == q["rw"]) & (G.dc == q["dc"]) & (G.dw == q["dw"]) & (G.w == q["w"])].iloc[0]; L.append(ln(r))
for wl in W:
    if wl.startswith("今"): continue
    r = G[(G.rc == 38) & (G.rw == 8) & (G.dc == -3.0) & (G.dw == 2) & (G.w == wl)].iloc[0]; L.append(ln(r))
L.append(f"\n[done] {time.time()-t0:.0f}s")
open("_bt_gokujo_score_grid_1031.md", "w", encoding="utf-8").write("\n".join(L) + "\n")
G.to_pickle("_bt_gokujo_score_grid_1031.pkl")
print("\n".join(L))
