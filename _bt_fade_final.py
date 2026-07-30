# -*- coding: utf-8 -*-
"""_bt_fade_final.py — フェード「ATR下限×乖離下限」の最終確認（2026-07-31）。

面（_bt_fade_atrdev.py）で高原の中心に来たのは ATR5%以上 × 25MA乖離12%以上。
ここでは採否に必要な残りを潰す:
  ① 近傍4セルの年別（1年でも大きく崩れないか）
  ② 直近2年の月別（今の相場でも効いているか）
  ③ 落とす玉だけで組んだ成績（本当に捨てて良い側か）
  ④ 実運用の変化（撃つ日が何日減るか・現行の1番/2番が何%入れ替わるか）

実行: python -X utf8 _bt_fade_final.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SIZE, NPICK = 500_000, 2
YEARS = list(range(2016, 2027))

D0 = pd.read_pickle("_fade_deep.pkl")
D0 = D0[(D0.gain >= 6.0) & (D0.vr < 6.0) & (D0.dev < 80.0)].copy()
D0["ym"] = pd.to_datetime(D0.sig).dt.strftime("%Y-%m")


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


SETS = [("現行", D0),
        ("ATR5×乖離10", D0[(D0.atr >= 5) & (D0.dev >= 10)]),
        ("ATR5×乖離12", D0[(D0.atr >= 5) & (D0.dev >= 12)]),
        ("ATR5×乖離15", D0[(D0.atr >= 5) & (D0.dev >= 15)]),
        ("ATR4.5×乖離12", D0[(D0.atr >= 4.5) & (D0.dev >= 12)]),
        ("ATR5.5×乖離12", D0[(D0.atr >= 5.5) & (D0.dev >= 12)])]
B = {lab: build(d) for lab, d in SETS}

print("=" * 130)
print("① 年別（円）")
print("=" * 130)
print(f"  {'年':>6}" + "".join(f"{lab:>17}" for lab, _ in SETS))
for y in YEARS:
    print(f"  {y:>6}" + "".join(f"{B[lab][B[lab].y == y].yen.sum():>+16,.0f}円" for lab, _ in SETS))
print(f"  {'計':>6}" + "".join(f"{B[lab].yen.sum():>+16,.0f}円" for lab, _ in SETS))
print(f"  {'年平均':>6}" + "".join(f"{B[lab].yen.sum()/11:>+16,.0f}円" for lab, _ in SETS))
print(f"  {'勝率':>6}" + "".join(f"{(B[lab].pnl>0).mean()*100:>16.1f}%" for lab, _ in SETS))
print(f"  {'勝ち年':>6}" + "".join(
    f"{int((B[lab].groupby('y').yen.sum().reindex(YEARS, fill_value=0)>0).sum()):>15}/11" for lab, _ in SETS))
print(f"  {'撃つ日':>6}" + "".join(f"{B[lab].sig.nunique():>16,}日" for lab, _ in SETS))
print(f"  {'最悪年':>6}" + "".join(
    f"{B[lab].groupby('y').yen.sum().reindex(YEARS, fill_value=0).min():>+16,.0f}円" for lab, _ in SETS))
print(f"  {'最悪月':>6}" + "".join(f"{B[lab].groupby('ym').yen.sum().min():>+16,.0f}円" for lab, _ in SETS))

print("\n" + "=" * 130)
print("② 直近2年の月別（円）")
print("=" * 130)
recent = sorted(m for m in D0.ym.unique() if m >= "2024-08")
print(f"  {'月':>9}" + "".join(f"{lab:>17}" for lab, _ in SETS))
for m in recent:
    print(f"  {m:>9}" + "".join(f"{B[lab][B[lab].ym == m].yen.sum():>+16,.0f}円" for lab, _ in SETS))
print(f"  {'計':>9}" + "".join(
    f"{B[lab][B[lab].ym >= '2024-08'].yen.sum():>+16,.0f}円" for lab, _ in SETS))
print(f"  {'勝月':>9}" + "".join(
    f"{int((B[lab][B[lab].ym>='2024-08'].groupby('ym').yen.sum()>0).sum()):>15}/{len(recent):<2}" for lab, _ in SETS))

print("\n" + "=" * 130)
print("③ 落とす側だけで組んだらどうなるか（＝捨てて良い玉か）")
print("=" * 130)
KEEP = (D0.atr >= 5) & (D0.dev >= 12)
for lab, d in (("残す ATR5×乖離12", D0[KEEP]), ("落とす それ以外", D0[~KEEP]),
               ("　うち ATR5未満", D0[D0.atr < 5]), ("　うち 乖離12未満", D0[D0.dev < 12])):
    b = build(d)
    yr = b.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    print(f"  {lab:<20}候補{len(d):>6}件 撃つ日{b.sig.nunique():>5,} 勝率{(b.pnl>0).mean()*100:>5.1f}% "
          f"年{b.yen.sum()/11:>+10,.0f}円 勝ち{int((yr>0).sum()):>2}/11 "
          f"前半{b[b.y<=2021].yen.sum():>+11,.0f} 後半{b[b.y>=2022].yen.sum():>+11,.0f}")

print("\n" + "=" * 130)
print("④ 実運用の変化")
print("=" * 130)
cur, new = B["現行"], B["ATR5×乖離12"]
cur_days = set(cur.sig); new_days = set(new.sig)
print(f"  撃つ日: {len(cur_days):,}日 → {len(new_days):,}日 "
      f"（年{len(cur_days)/11:.0f}日 → 年{len(new_days)/11:.0f}日・{len(cur_days-new_days):,}日が『撃たない』に）")
n1 = new.groupby("sig").size()
print(f"  撃つ日のうち 2本ある日 {int((n1 == 2).sum()):,}日 / 1本だけの日 {int((n1 == 1).sum()):,}日")
cur_pairs = set(zip(cur.sig, cur.ticker)); new_pairs = set(zip(new.sig, new.ticker))
print(f"  玉の入れ替わり: 現行{len(cur_pairs):,}玉 → 新{len(new_pairs):,}玉 / "
      f"共通{len(cur_pairs & new_pairs):,}玉（新の{len(cur_pairs & new_pairs)/len(new_pairs)*100:.0f}%は現行でも撃っていた）")
drop = cur[~cur.set_index(['sig', 'ticker']).index.isin(new_pairs)]
add = new[~new.set_index(['sig', 'ticker']).index.isin(cur_pairs)]
print(f"  撃たなくなる{len(drop):,}玉の成績: 勝率{(drop.pnl>0).mean()*100:.1f}% 計{drop.yen.sum():+,.0f}円")
print(f"  新しく撃つ{len(add):,}玉の成績  : 勝率{(add.pnl>0).mean()*100:.1f}% 計{add.yen.sum():+,.0f}円")
print(f"\n  条件を満たす候補の中身: 株価中央{D0[KEEP].px.median():,.0f}円 "
      f"（現行 {D0.px.median():,.0f}円）/ 代金中央{D0[KEEP].tov.median()/1e8:.1f}億 "
      f"（現行 {D0.tov.median()/1e8:.1f}億）")
