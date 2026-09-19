# -*- coding: utf-8 -*-
"""_bt_gokujo_round13_10permonth_0919.py — 本人「なんとか月10本建てたい」: 最大400万固定で枠数×サイズ×乗せを振り、月あたり本数と代償（26年/10年/22-26・DD）を並べる"""
import numpy as np, pandas as pd, time, re
src = open("_bt_gokujo_round6_26y.py", encoding="utf-8").read(); exec(src.split("# ── A 決算日近接 ──")[0])
r9 = open("_bt_gokujo_round9_capital_freq_0919.py", encoding="utf-8").read()
m = re.search(r"TICKv = C\.ticker\.to_numpy\(\).*?(?=\nprint\(\"\\n■ A 資金構成)", r9, re.S); exec(m.group(0))
KEY_PRIO = SCORE + 10.0 * G; ALLM = np.ones(n, dtype=bool)
def row(lab, R, cap):
    r = metrics(R); R10 = R[R.y >= 2017]; R22 = R[R.y >= 2022]
    dd10 = (lambda s: (s.cumsum() - s.cumsum().cummax()).min() / 1e4)(R10.assign(date=DATEv[R10.i.to_numpy()]).groupby("date").yen.sum().sort_index())
    mo = R10.assign(ym=DATEv[R10.i.to_numpy()].strftime("%Y-%m")).groupby("ym").size()
    print(f"  {lab:<30} 月{len(R10)/117:>4.1f}本(10年) 0本の月{(117-len(mo))/117*100:>3.0f}% | 26年{r['tot']:>+6,.0f}万 PF{r['pf']:.2f} DD{r['dd']:>+5,.0f} 最悪年{r['worst']:>+4,.0f} | 10年{r['y10']:>+5,.0f}万 PF{r['pf10']:.2f} 勝率{r['win10']:.1f}% DD{dd10:>+5,.0f} 最悪年{r['worst10']:+.0f} | 22-26{r['e4']:>+5,.0f} | 4分割 {r['a']:>+4,.0f}/{r['b']:>+4,.0f}/{r['e3']:>+4,.0f}/{r['e4']:>+4,.0f} | 最大建玉{r['maxexpo']:.0f}", flush=True)
print("■ 最大400万固定（26年・極上 新ルール）", flush=True)
for lab, mask, key, kw in (
    ("現行 1×200＋同額乗せ", G, SCORE, dict(slots=1, size=2_000_000, addon_frac=1.0)),
    ("1×400 乗せなし", G, SCORE, dict(slots=1, size=4_000_000, addon_frac=0.0)),
    ("2×200 乗せなし", G, SCORE, dict(slots=2, size=2_000_000, addon_frac=0.0)),
    ("2×100＋同額乗せ", G, SCORE, dict(slots=2, size=1_000_000, addon_frac=1.0)),
    ("2×130＋乗せ70", G, SCORE, dict(slots=2, size=1_300_000, addon_frac=70/130)),
    ("3×130 乗せなし", G, SCORE, dict(slots=3, size=1_300_000, addon_frac=0.0)),
    ("3×100＋乗せ1/3", G, SCORE, dict(slots=3, size=1_000_000, addon_frac=1/3)),
    ("4×100 乗せなし", G, SCORE, dict(slots=4, size=1_000_000, addon_frac=0.0)),
    ("2×200 極上優先＋空き日は極み候補", ALLM, KEY_PRIO, dict(slots=2, size=2_000_000, addon_frac=0.0)),
    ("3×130 極上優先＋空き日は極み候補", ALLM, KEY_PRIO, dict(slots=3, size=1_300_000, addon_frac=0.0)),
    ("1×200乗せ＋2枠目=極み候補100", ALLM, KEY_PRIO, dict(slots=2, size=2_000_000, addon_frac=1.0, mult=np.where(G, 1.0, 0.5))),
):
    row(lab, run_gen(mask, key, **kw), 400)
print("\n■ 保有日数を短くして回転を上げる（1×200＋同額乗せ・利確/損切り/RSIは現行）", flush=True)
for hold in (2, 3):
    p, e = replay(hold=hold); p, e, _ = day1cut(p, e)
    row(f"保有{hold}日", run_gen(G, SCORE, slots=1, size=2_000_000, addon_frac=1.0, pnl=p, exo=e), 400)
    row(f"保有{hold}日 2×200乗せなし", run_gen(G, SCORE, slots=2, size=2_000_000, addon_frac=0.0, pnl=p, exo=e), 400)
print("\n■ 参考: 候補の出方（極上候補 2017-26）", flush=True)
ent = pd.to_datetime(C.entry.to_numpy()); g10 = G & (ent >= pd.Timestamp("2017-01-01")).to_numpy()
mo = pd.Series(1, index=ent[g10]).groupby(ent[g10].strftime("%Y-%m")).sum()
print(f"  候補 {g10.sum()}件/117か月 = 月{g10.sum()/117:.1f}件・候補日 {len(set(ent[g10]))}日 = 月{len(set(ent[g10]))/117:.1f}日・候補0の月 {117-len(mo)}か月・月別中央値{mo.median():.0f} 最大{mo.max()}")
print(f"\n[done] {time.time()-t0:.0f}s")
