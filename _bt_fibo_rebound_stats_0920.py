# -*- coding: utf-8 -*-
"""_bt_fibo_rebound_stats_0920.py — 本人「朝15分で+5%上がった銘柄の押し目は、その後どのくらい戻す？23.6で買って平均3%戻すなら3%で利確する。
50%まで押したら50%の価格から何%反発する？」（2026-09-20）
対象: 寄り後15分（9:00-9:15）の高値が寄り値比+5%以上（テクニスコ型・581銘柄日・Yahoo5分足60日×立花公式値）
フィボ: その時点の当日安値→当日高値で引く（トレーダーが場中に引く線と同じ・水準に初めて触れた時の線で固定）
各水準に初めて触れた価格を基準に: 最大反発%（引けまで）・30分/60分以内の最大反発・さらに押した最大%・引け%・
「利確x%が次の水準割れより先に来る確率」（x=1/2/3/5%）
実行: python -X utf8 _bt_fibo_rebound_stats_0920.py > _log_fibo_rebound_stats_0920.txt"""
from __future__ import annotations
import numpy as np, pandas as pd, time, sys
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
src = open("_bt_fibo_first15_0919.py", encoding="utf-8").read()
exec(src.split("rows = []\nfor e in events:")[0])          # events（対象銘柄日・5分足）を再利用
LV = [("23.6", 0.236), ("38.2", 0.382), ("50", 0.5), ("61.8", 0.618), ("78.6", 0.786), ("100(安値割れ)", 1.0)]
NEXT = {"23.6": 0.382, "38.2": 0.5, "50": 0.618, "61.8": 0.786, "78.6": 1.0, "100(安値割れ)": None}
ev2 = [e for e in events if e["rise_o"] >= 5]
print(f"[T2] 寄り後15分で+5%以上噴いた {len(ev2)}銘柄日（60日・寄り後上昇 中央値{np.median([e['rise_o'] for e in ev2]):+.1f}%）", flush=True)
rows = []
for e in ev2:
    bars = e["bars"]; hm = bars.hm.to_numpy(); H = bars.h.to_numpy(float); L = bars.l.to_numpy(float); n = len(bars); c_off = e["c"]
    run_h, run_l = e["h15"], e["l15"]; touched = {}
    for k in range(n):
        if hm[k] <= "09:15":
            continue
        gh, gl = run_h, run_l
        if gh > gl and (gh - gl) / gh >= 0.02:
            for name, r in LV:
                if name in touched: continue
                px = gh - r * (gh - gl)
                if L[k] <= px:
                    nx = NEXT[name]; nx_px = (gh - nx * (gh - gl)) if nx else None
                    rest_h = H[k + 1:]; rest_l = L[k + 1:]
                    reb = (rest_h.max() / px - 1) * 100 if len(rest_h) else np.nan
                    reb30 = (H[k + 1:k + 7].max() / px - 1) * 100 if k + 1 < n else np.nan
                    reb60 = (H[k + 1:k + 13].max() / px - 1) * 100 if k + 1 < n else np.nan
                    drop = (rest_l.min() / px - 1) * 100 if len(rest_l) else np.nan
                    race = {}
                    for x in (1, 2, 3, 5):
                        tp_j = next((j for j in range(k + 1, n) if H[j] >= px * (1 + x / 100)), None)
                        st_j = next((j for j in range(k + 1, n) if L[j] <= nx_px), None) if nx_px else None
                        race[x] = "TP" if (tp_j is not None and (st_j is None or tp_j < st_j)) else ("STOP" if st_j is not None else "NONE")
                    touched[name] = dict(tk=e["tk"], day=e["day"], lv=name, hm=hm[k], px=px, reb=reb, reb30=reb30, reb60=reb60, dd_after=drop,
                                         close_pct=(c_off / px - 1) * 100, rise_o=e["rise_o"], gap=e["gap"], **{f"race{x}": race[x] for x in (1, 2, 3, 5)})
        run_h = max(run_h, H[k]); run_l = min(run_l, L[k])
    rows += list(touched.values())
R = pd.DataFrame(rows); N = len(ev2)
print("\n■ 水準ごと: 初めて触れた価格からの反発（%）・さらに押した深さ・引け", flush=True)
print(f"  {'水準':<12}{'触れた日':>9}{'触れた時刻(中央値)':>12}{'最大反発 中央値':>10}{'平均':>7}{'75%点':>7}{'≥1%':>6}{'≥2%':>6}{'≥3%':>6}{'≥5%':>6}{'30分内中央値':>9}{'60分内中央値':>9}{'さらに押す中央値':>10}{'引け中央値':>8}{'引け>買値':>8}")
for name, _ in LV:
    s = R[R.lv == name]
    if len(s) == 0: continue
    print(f"  {name:<12}{len(s):>5}({len(s)/N*100:>3.0f}%){sorted(s.hm)[len(s)//2]:>10}{s.reb.median():>+10.1f}{s.reb.mean():>+7.1f}{s.reb.quantile(.75):>+7.1f}"
          + "".join(f"{(s.reb >= x).mean()*100:>6.0f}" for x in (1, 2, 3, 5)) + f"{s.reb30.median():>+9.1f}{s.reb60.median():>+9.1f}{s['dd_after'].median():>+10.1f}{s['close_pct'].median():>+8.1f}{(s['close_pct'] > 0).mean()*100:>7.0f}%")
print("\n■ 利確x%が「次の水準割れ」より先に来る確率（=そこで買って利確x%・損切り次の水準の勝率）", flush=True)
print(f"  {'水準':<12}{'n':>5}" + "".join(f"{'利確+'+str(x)+'%':>12}" for x in (1, 2, 3, 5)) + "   ※残り=次の水準割れが先／どちらも来ず")
for name, _ in LV[:-1]:
    s = R[R.lv == name]
    if len(s) == 0: continue
    print(f"  {name:<12}{len(s):>5}" + "".join(f"{(s[f'race{x}'] == 'TP').mean()*100:>11.0f}%" for x in (1, 2, 3, 5)))
print("\n■ 噴きの大きさ別（23.6と50・最大反発の中央値／≥3%／≥5%・さらに押す中央値）", flush=True)
for name in ("23.6", "50"):
    for lo, hi in ((5, 8), (8, 12), (12, 999)):
        s = R[(R.lv == name) & (R.rise_o >= lo) & (R.rise_o < hi)]
        if len(s) == 0: continue
        print(f"  {name:<6} 噴き+{lo}〜{hi}%: n{len(s):>4} 反発中央値{s.reb.median():+.1f}% ≥3%:{(s.reb >= 3).mean()*100:.0f}% ≥5%:{(s.reb >= 5).mean()*100:.0f}% さらに押す中央値{s['dd_after'].median():+.1f}% 引け>買値{(s['close_pct'] > 0).mean()*100:.0f}%")
print("\n■ 押しが浅く止まった日 vs 深く行った日（23.6に触れた日を、その日の最深水準で分ける）", flush=True)
deep = R.groupby(["tk", "day"]).lv.apply(lambda v: [x for x, _ in LV if x in set(v)][-1])
s23 = R[R.lv == "23.6"].set_index(["tk", "day"]); s23["deepest"] = deep.reindex(s23.index)
for d_ in [x for x, _ in LV]:
    q = s23[s23.deepest == d_]
    if len(q) == 0: continue
    print(f"  最深{d_:<12}: n{len(q):>4}({len(q)/len(s23)*100:>3.0f}%) 23.6買値からの最大反発 中央値{q.reb.median():+.1f}% 引け{q['close_pct'].median():+.1f}% 引け>買値{(q['close_pct'] > 0).mean()*100:.0f}%")
print(f"\n[done] {time.time()-t0:.0f}s")
