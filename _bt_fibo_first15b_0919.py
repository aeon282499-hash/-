# -*- coding: utf-8 -*-
"""_bt_fibo_first15b_0919.py — first15 の追補: 深い水準だけで待つ(50/61.8のみ)・引けまで持つ・初押しの時刻別・噴きの大きさ別（2026-09-20）
実行: python -X utf8 _bt_fibo_first15b_0919.py > _log_fibo_first15b_0919.txt"""
from __future__ import annotations
import numpy as np, pandas as pd, time, sys
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
src = open("_bt_fibo_first15_0919.py", encoding="utf-8").read()
exec(src.split("rows = []\nfor e in events:")[0])          # events / simulate / ALLOC / TPV を再利用
TPV["引け"] = 9.99                                          # 利確なし＝損切りか大引け
ALLOC2 = {"E 23.6のみ": (("23.6", 1),), "G 50のみ": (("50", 1),), "H 61.8のみ": (("61.8", 1),), "I 38.2/61.8=1:1": (("38.2", 1), ("61.8", 1)),
          "J 23.6/61.8=1:1": (("23.6", 1), ("61.8", 1))}
EXITS = ("+2%", "+5%", "引け", "トレール")
ev2 = [e for e in events if e["rise_o"] >= 5]                 # テクニスコ型
print(f"[T2] 寄り後15分で+5%以上噴いた {len(ev2)}銘柄日", flush=True)
rows = []
for e in ev2:
    for an, alloc in ALLOC2.items():
        for tg in EXITS:
            for sm in ("1段下", "安値割れ"):
                r = simulate(e, alloc, tg, sm)
                if r is None: continue
                r.update(tk=e["tk"], day=e["day"], rise_o=e["rise_o"], gap=e["gap"], alloc=an, target=tg, stop=sm); rows.append(r)
R = pd.DataFrame(rows)
def cell(s):
    f = s[s.n_fill > 0]; return f"{s.R.mean():>+6.2f}({(f.pnl_yen > 0).mean()*100 if len(f) else 0:>3.0f}%/{len(f)/max(len(s),1)*100:>3.0f}%)"
print("\n■ 追加セル（平均R/銘柄日・( )内=勝率/約定率）")
print(f"  {'配分':<18}{'損切り':<6}" + "".join(f"{x:>18}" for x in EXITS))
for an in ALLOC2:
    for sm in ("1段下", "安値割れ"):
        print(f"  {an:<18}{sm:<6}" + "".join(f"{cell(R[(R.alloc == an) & (R.target == tg) & (R.stop == sm)]):>18}" for tg in EXITS))
print("\n■ 初押し(23.6)の時刻別（E 23.6のみ・損切り安値割れ・出口=+2%/引け）")
for tg in ("+2%", "引け"):
    s = R[(R.alloc == "E 23.6のみ") & (R.target == tg) & (R.stop == "安値割れ") & (R.n_fill > 0)]
    for lo, hi in (("09:15", "09:35"), ("09:35", "10:00"), ("10:00", "11:30"), ("11:30", "14:30")):
        q = s[(s.first_hm > lo) & (s.first_hm <= hi)]
        print(f"  {tg:<4} 初押し {lo}〜{hi}: n{len(q):>4} 平均{q.R.mean() if len(q) else 0:+.2f}R 勝率{(q.pnl_yen > 0).mean()*100 if len(q) else 0:.0f}% MFE中央値{q.mfe.median() if len(q) else np.nan:+.1f}% MAE中央値{q.mae.median() if len(q) else np.nan:+.1f}%")
print("\n■ 噴きの大きさ別（寄り→9:15高値の上昇率・E 23.6のみ・安値割れ・出口=+2%）")
s = R[(R.alloc == "E 23.6のみ") & (R.target == "+2%") & (R.stop == "安値割れ")]
for lo, hi in ((5, 8), (8, 12), (12, 20), (20, 999)):
    q = s[(s.rise_o >= lo) & (s.rise_o < hi)]; f = q[q.n_fill > 0]
    print(f"  +{lo}〜{hi}%: n{len(q):>4} 約定{len(f)/max(len(q),1)*100:.0f}% 平均{q.R.mean() if len(q) else 0:+.2f}R 勝率{(f.pnl_yen > 0).mean()*100 if len(f) else 0:.0f}% MFE≥5%:{(f.mfe >= 5).mean()*100 if len(f) else 0:.0f}% MAE中央値{f.mae.median() if len(f) else np.nan:+.1f}%")
print("\n■ 参考: 23.6で買って何もせず引けまで持った時の分布（損切りなし・初弾価格比）")
s = R[(R.alloc == "E 23.6のみ") & (R.target == "引け") & (R.stop == "安値割れ") & (R.n_fill > 0)]
print(f"  n{len(s)} MFE: 中央値{s.mfe.median():+.1f}% 75%点{s.mfe.quantile(.75):+.1f}% ／ MAE: 中央値{s.mae.median():+.1f}% 25%点{s.mae.quantile(.25):+.1f}%")
print(f"\n[done] {time.time()-t0:.0f}s")
