# -*- coding: utf-8 -*-
"""_bt_fade_2026.py — 新ルール（ATR5%以上×25MA乖離12%以上）で今年はどうか（2026-07-31）。

2026年はまだ7/27までの部分年なので、過去年も **1/1〜7/27** に揃えて比べる
（通年と混ぜると今年だけ短くて不当に小さく見える）。

実行: python -X utf8 _bt_fade_2026.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SIZE, NPICK = 500_000, 2
CUT = "07-27"                      # 各年この日までで切る（2026のデータ終端に合わせる）

D0 = pd.read_pickle("_fade_deep.pkl")
D0 = D0[(D0.gain >= 6.0) & (D0.vr < 6.0) & (D0.dev < 80.0)].copy()
D0["ym"] = pd.to_datetime(D0.sig).dt.strftime("%Y-%m")
D0["md"] = pd.to_datetime(D0.sig).dt.strftime("%m-%d")


def build(d, n=NPICK):
    d = d.copy()
    r = None
    for c in ("dev", "atr"):
        x = d.groupby("sig")[c].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix"])
    d["rank"] = d.groupby("sig").cumcount() + 1
    d = d[d["rank"] <= n].copy()
    d["sh"] = (SIZE / d.o1 // 100 * 100).astype(int)
    return d[(d.sh > 0) & (d.px * 100 <= SIZE)]


NEW = (D0.atr >= 5) & (D0.dev >= 12)
cur, new = build(D0), build(D0[NEW])
YEARS = list(range(2017, 2027))    # 2016はデータが8月開始で1-7月が無い

print("=" * 118)
print(f"① 各年の 1/1〜{CUT} だけを切り出して比較（1玉50万・上位2本・実株数）")
print("=" * 118)
print(f"  {'年':>6}{'現行 損益':>14}{'新 損益':>14}{'差':>12}"
      f"{'現行 勝率':>10}{'新 勝率':>10}{'現行 撃つ日':>12}{'新 撃つ日':>11}")
rows = []
for y in YEARS:
    a = cur[(cur.y == y) & (cur.md <= CUT)]
    b = new[(new.y == y) & (new.md <= CUT)]
    if not len(a):
        continue
    rows.append((y, a.yen.sum(), b.yen.sum(), (a.pnl > 0).mean() * 100,
                 (b.pnl > 0).mean() * 100 if len(b) else np.nan,
                 a.sig.nunique(), b.sig.nunique()))
    print(f"  {y:>6}{rows[-1][1]:>+13,.0f}円{rows[-1][2]:>+13,.0f}円"
          f"{rows[-1][2]-rows[-1][1]:>+11,.0f}円{rows[-1][3]:>9.1f}%{rows[-1][4]:>9.1f}%"
          f"{rows[-1][5]:>11,}日{rows[-1][6]:>10,}日")
R = pd.DataFrame(rows, columns=["y", "cur", "new", "cwr", "nwr", "cd", "nd"])
print(f"  {'平均':>6}{R.cur.mean():>+13,.0f}円{R.new.mean():>+13,.0f}円"
      f"{R.new.mean()-R.cur.mean():>+11,.0f}円{R.cwr.mean():>9.1f}%{R.nwr.mean():>9.1f}%"
      f"{R.cd.mean():>11,.0f}日{R.nd.mean():>10,.0f}日")
n26 = R[R.y == 2026].iloc[0]
print(f"\n  2026の順位: 現行ルール {int((R.cur > n26.cur).sum())+1}位/{len(R)}年 ・ "
      f"新ルール {int((R.new > n26.new).sum())+1}位/{len(R)}年")
print(f"  2026は過去9年平均（新ルール {R[R.y<2026].new.mean():+,.0f}円）の "
      f"{n26.new/R[R.y<2026].new.mean()*100:.0f}%")

print("\n" + "=" * 118)
print("② 2026年の月別")
print("=" * 118)
print(f"  {'月':>9}{'現行':>14}{'新':>14}{'差':>12}{'新 件数':>9}{'新 勝率':>9}{'新 撃つ日':>10}")
for m in sorted(x for x in D0.ym.unique() if x.startswith("2026")):
    a, b = cur[cur.ym == m], new[new.ym == m]
    print(f"  {m:>9}{a.yen.sum():>+13,.0f}円{b.yen.sum():>+13,.0f}円"
          f"{b.yen.sum()-a.yen.sum():>+11,.0f}円{len(b):>9}"
          f"{((b.pnl > 0).mean()*100 if len(b) else 0):>8.1f}%{b.sig.nunique():>9,}日")
a26 = cur[cur.y == 2026]; b26 = new[new.y == 2026]
print(f"  {'計':>9}{a26.yen.sum():>+13,.0f}円{b26.yen.sum():>+13,.0f}円"
      f"{b26.yen.sum()-a26.yen.sum():>+11,.0f}円{len(b26):>9}"
      f"{(b26.pnl > 0).mean()*100:>8.1f}%{b26.sig.nunique():>9,}日")

print("\n" + "=" * 118)
print("③ 2026年に新ルールが負けている理由（差分の中身）")
print("=" * 118)
np_ = set(zip(b26.sig, b26.ticker))
drop = a26[~a26.set_index(["sig", "ticker"]).index.isin(np_)]
add = b26[~b26.set_index(["sig", "ticker"]).index.isin(set(zip(a26.sig, a26.ticker)))]
lost_days = sorted(set(a26.sig) - set(b26.sig))
d2 = a26[a26.sig.isin(lost_days)]
print(f"  撃たなくなる玉        {len(drop):>4}玉 勝率{(drop.pnl>0).mean()*100:>5.1f}% "
      f"{drop.yen.sum():>+10,.0f}円")
print(f"    うち0件になる日     {len(d2):>4}玉 勝率{(d2.pnl>0).mean()*100:>5.1f}% "
      f"{d2.yen.sum():>+10,.0f}円（{len(lost_days)}日）")
print(f"  新しく撃つ玉          {len(add):>4}玉 勝率{(add.pnl>0).mean()*100:>5.1f}% "
      f"{add.yen.sum():>+10,.0f}円")
print(f"\n  2026で撃たなくなった玉のうち大きかったもの（上位5・現行なら取れていた利益）:")
for r in drop.nlargest(5, "yen").itertuples():
    print(f"    {r.sig} {r.ticker:<8} 前日+{r.gain:>5.1f}% ATR{r.atr:>5.1f}% 乖離{r.dev:>6.1f}% "
          f"{r.yen:>+9,.0f}円")
print(f"\n  2026で撃たなくなった玉のうち損だったもの（上位5・避けられた損）:")
for r in drop.nsmallest(5, "yen").itertuples():
    print(f"    {r.sig} {r.ticker:<8} 前日+{r.gain:>5.1f}% ATR{r.atr:>5.1f}% 乖離{r.dev:>6.1f}% "
          f"{r.yen:>+9,.0f}円")

print("\n" + "=" * 118)
print("④ 通年ベース（参考・2026だけ7ヶ月なので過小に出る）")
print("=" * 118)
print(f"  {'年':>6}{'現行':>14}{'新':>14}{'差':>12}")
for y in range(2016, 2027):
    a, b = cur[cur.y == y], new[new.y == y]
    print(f"  {y:>6}{a.yen.sum():>+13,.0f}円{b.yen.sum():>+13,.0f}円{b.yen.sum()-a.yen.sum():>+11,.0f}円")
print(f"\n  2026を年率換算（1-7月の実績×12/7）: 現行 {a26.yen.sum()*12/7:>+,.0f}円 / "
      f"新 {b26.yen.sum()*12/7:>+,.0f}円")
