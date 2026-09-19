# -*- coding: utf-8 -*-
"""_bt_surge_short_subset_0920.py — 9:30噴き売りの実行可能サブセット（貸借○×噴きの大きさ・日別本数・上位除去）（2026-09-20）"""
from __future__ import annotations
import numpy as np, pandas as pd, sys
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
src = open("_bt_fibo_first15_0919.py", encoding="utf-8").read(); exec(src.split("rows = []\nfor e in events:")[0])
ev2 = [e for e in events if e["rise_o"] >= 5]; COST = 0.1
import daytrade_paper as dp
iss = dp.fetch_iss_map(dp._jq_token())
def pf(x): x = np.asarray(x, float); g = x[x > 0].sum(); l = -x[x <= 0].sum(); return g / l if l else np.inf
rows = []
for e in ev2:
    b = e["bars"]; hm = b.hm.to_numpy(); cl = b.c.to_numpy(float)
    idx = np.where(hm == "09:30")[0]
    if len(idx) == 0: continue
    p = cl[idx[0]]
    if not (p > 0): continue
    rows.append(dict(tk=e["tk"], day=e["day"], ym=e["day"][:7], rise_o=e["rise_o"], gap=e["gap"], iss=iss.get(e["tk"][:4]) == "2", px=p, ret=(p - e["c"]) / p * 100 - COST))
R = pd.DataFrame(rows)
def line(lab, q):
    d = q.groupby("day").ret.mean(); q3 = q[q.ret < q.ret.quantile(.97)] if len(q) > 30 else q
    print(f"  {lab:<34} n{len(q):>4}({q.day.nunique():>2}日・日平均{len(q)/max(q.day.nunique(),1):.1f}本) 平均{q.ret.mean():+.2f}% 中央値{q.ret.median():+.2f}% 勝率{(q.ret > 0).mean()*100:.0f}% PF{pf(q.ret):.2f} | 上位3%除去 平均{q3.ret.mean():+.2f}% PF{pf(q3.ret):.2f} | 日別勝ち{(d > 0).sum()}/{len(d)} 最悪日{d.min():+.1f}% | 月別 " + " ".join(f"{ym[5:]}月{g.ret.mean():+.1f}%" for ym, g in q.groupby("ym")))
print(f"[T2] 9:30建て n{len(R)}・貸借○ {R.iss.sum()}")
print("\n■ 貸借○（制度信用で売れる＝料なし）だけ")
Q = R[R.iss]
line("貸借○ 全部", Q)
for lo, hi in ((5, 8), (8, 12), (12, 999)): line(f"貸借○ 噴き+{lo}〜{hi}%", Q[(Q.rise_o >= lo) & (Q.rise_o < hi)])
line("貸借○ 噴き+8%以上", Q[Q.rise_o >= 8]); line("貸借○ 噴き+10%以上", Q[Q.rise_o >= 10])
line("貸借○ 噴き+8%以上×ギャップ<3%", Q[(Q.rise_o >= 8) & (Q.gap < 3)]); line("貸借○ 噴き+8%以上×株価≥500円", Q[(Q.rise_o >= 8) & (Q.px >= 500)])
print("\n■ 日別に「最も噴いた1本だけ」（1日1本・貸借○）")
top1 = Q.sort_values("rise_o", ascending=False).groupby("day").head(1); line("貸借○ 1日1本(最大噴き)", top1)
top1b = Q[Q.rise_o >= 8].sort_values("rise_o", ascending=False).groupby("day").head(1); line("貸借○ 噴き+8%以上 1日1本", top1b)
print("\n■ 参考: 貸借✕（ハイカラ在庫＋プレミアム料が要る）")
line("貸借✕ 全部", R[~R.iss]); line("貸借✕ 噴き+8%以上", R[(~R.iss) & (R.rise_o >= 8)])
print("\n[done]")
