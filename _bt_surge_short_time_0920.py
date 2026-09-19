# -*- coding: utf-8 -*-
"""_bt_surge_short_time_0920.py — 寄り後15分で+5%噴いた銘柄(テクニスコ型・581銘柄日)を「時刻tで空売り→大引け(公式終値)で買戻し」した時の成績（2026-09-20）
比較用: 同じ時刻で買って引けまで持った場合。往復コスト0.1%。実行: python -X utf8 _bt_surge_short_time_0920.py > _log_surge_short_time_0920.txt"""
from __future__ import annotations
import numpy as np, pandas as pd, sys
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
src = open("_bt_fibo_first15_0919.py", encoding="utf-8").read(); exec(src.split("rows = []\nfor e in events:")[0])
ev2 = [e for e in events if e["rise_o"] >= 5]; COST = 0.1
print(f"[T2] {len(ev2)}銘柄日")
rows = []
for e in ev2:
    b = e["bars"]; hm = b.hm.to_numpy(); cl = b.c.to_numpy(float); c_off = e["c"]
    for t in ("09:20", "09:30", "10:00", "10:30", "11:30", "12:35", "13:00"):
        idx = np.where(hm == t)[0]
        if len(idx) == 0: continue
        p = cl[idx[0]]
        if not (p > 0): continue
        rows.append(dict(tk=e["tk"], day=e["day"], t=t, rise_o=e["rise_o"], short=(p - c_off) / p * 100 - COST, long=(c_off - p) / p * 100 - COST))
R = pd.DataFrame(rows)
def pf(x): x = np.asarray(x); g = x[x > 0].sum(); l = -x[x <= 0].sum(); return g / l if l else np.inf
print("\n■ 時刻tの5分足終値で建て→大引け（公式終値）・往復0.1%込み・件あたり%")
print(f"  {'時刻':<7}{'n':>5}{'空売り平均%':>10}{'中央値':>8}{'勝率':>6}{'PF':>6}  |{'買い平均%':>9}{'勝率':>6}")
for t in ("09:20", "09:30", "10:00", "10:30", "11:30", "12:35", "13:00"):
    s = R[R.t == t]
    print(f"  {t:<7}{len(s):>5}{s.short.mean():>+10.2f}{s.short.median():>+8.2f}{(s.short > 0).mean()*100:>5.0f}%{pf(s.short):>6.2f}  |{s.long.mean():>+9.2f}{(s.long > 0).mean()*100:>5.0f}%")
print("\n■ 10:00建ての空売りを噴きの大きさ別・日別合計（1日1本100万相当の円換算=%×1万）")
s = R[R.t == "10:00"]
for lo, hi in ((5, 8), (8, 12), (12, 999)):
    q = s[(s.rise_o >= lo) & (s.rise_o < hi)]
    print(f"  噴き+{lo}〜{hi}%: n{len(q):>4} 平均{q.short.mean():+.2f}% 勝率{(q.short > 0).mean()*100:.0f}% PF{pf(q.short):.2f}")
d = s.groupby("day").short.mean(); print(f"  日別（各日の平均%）: 勝ち日{(d > 0).sum()}/{len(d)} 平均{d.mean():+.2f}% 最悪日{d.min():+.2f}% 最良日{d.max():+.2f}%")
print("\n[done]")
