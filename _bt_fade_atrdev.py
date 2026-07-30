# -*- coding: utf-8 -*-
"""_bt_fade_atrdev.py — フェードの ATR下限 × 25MA乖離下限 の面（2026-07-31・本番無変更）。

_bt_fade_winrate.py で最良に見えたのは「ATR5%以上 × 乖離15%以上」
  勝率59.7%(+4.3pt) / PF1.46 / 年+62.7万 / 勝ち11-11年 / 両期間改善。
ただし2軸の1セルを拾っただけならカーブフィット。ここで
  ① 面にして高原かを見る（年平均・勝率・前半・後半・勝ち年を別々に）
  ② 本数との相互作用
  ③ コスト振り／上位3日・3銘柄除去／年別 で殴る
を通す。落ちたら不採用。

実行: python -X utf8 _bt_fade_atrdev.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SIZE, NPICK = 500_000, 2
YEARS = list(range(2016, 2027))
ATRS = (0, 3.0, 4.0, 4.5, 5.0, 5.5, 6.0, 7.0)
# -100 = 真の無フィルタ。0 は「25MA以上」という別のフィルタなので分ける（最初は混同していた）
DEVS = (-100, 0, 5, 10, 12, 15, 18, 20, 25)

D0 = pd.read_pickle("_fade_deep.pkl")
D0 = D0[(D0.gain >= 6.0) & (D0.vr < 6.0) & (D0.dev < 80.0)].copy()
D0["ym"] = pd.to_datetime(D0.sig).dt.strftime("%Y-%m")


def build(d, n=NPICK, cost=0.0):
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
    d = d[(d.sh > 0) & (d.px * 100 <= SIZE)].copy()
    if cost:
        d["pnl"] = d["pnl"] - cost
        d["yen"] = d["pnl"] / 100 * d.sh * d.o1
    return d


def stat(d):
    yr = d.groupby("y")["yen"].sum().reindex(YEARS, fill_value=0)
    m = d.groupby("ym")["yen"].sum()
    p = d["pnl"]
    loss = -p[p < 0].sum()
    return dict(n=len(d), days=d.sig.nunique(), wr=(p > 0).mean() * 100,
                pf=(p[p > 0].sum() / loss) if loss > 0 else np.inf,
                tot=d.yen.sum(), avg=d.yen.sum() / 11, win=int((yr > 0).sum()),
                worst=yr.min(), wm=m.min(),
                a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()))


def sub(a, dv):
    return D0[(D0.atr >= a) & (D0.dev >= dv)]


BASE = stat(build(D0))
print(f"[base] 現行: 勝率{BASE['wr']:.1f}% PF{BASE['pf']:.2f} 年{BASE['avg']:+,.0f}円 "
      f"勝ち{BASE['win']}/11 前半{BASE['a']:+,.0f} 後半{BASE['b']:+,.0f} 撃つ日{BASE['days']:,}日\n")

CELLS = {(a, dv): stat(build(sub(a, dv))) for a in ATRS for dv in DEVS}


def surface(title, key, fmt, mark=None):
    print("=" * 140)
    print(title)
    print("=" * 140)
    print(f"  {'ATR＼乖離':<12}" + "".join(f"{('なし' if d == -100 else f'{d}%+'):>13}" for d in DEVS))
    for a in ATRS:
        line = f"  {('なし' if a == 0 else f'{a}%+'):<12}"
        for dv in DEVS:
            s = CELLS[(a, dv)]
            v = format(s[key], fmt)
            if mark and mark(s):
                v = "*" + v
            line += f"{v:>13}"
        print(line)
    print()


surface("① 年平均（円）　※ * = 両期間とも現行超え", "avg", "+,.0f",
        mark=lambda s: s["a"] > BASE["a"] and s["b"] > BASE["b"])

# 実コスト（往復0.15%＝寄成/引成のスリッページ想定）を乗せた面。フィルタ側は
# 件数が減るぶんコスト総額も減るので、ここで序列が変わるなら現実側の判断はこちら。
CELLS_RAW = CELLS
CELLS = {k: stat(build(sub(*k), cost=0.15)) for k in CELLS_RAW}
BASE_C = stat(build(D0, cost=0.15))
print(f"[cost] 往復0.15%込みの現行: 勝率{BASE_C['wr']:.1f}% PF{BASE_C['pf']:.2f} "
      f"年{BASE_C['avg']:+,.0f}円 勝ち{BASE_C['win']}/11 "
      f"前半{BASE_C['a']:+,.0f} 後半{BASE_C['b']:+,.0f}\n")
surface("①-2 年平均（円・往復0.15%込み）　※ * = 両期間とも現行(同コスト)超え", "avg", "+,.0f",
        mark=lambda s: s["a"] > BASE_C["a"] and s["b"] > BASE_C["b"])
surface("①-3 勝ち年（/11・往復0.15%込み）", "win", "d")
CELLS = CELLS_RAW
surface("② 勝率（%）", "wr", ".1f")
surface("③ PF", "pf", ".2f")
surface("④ 前半 2016-21（円）", "a", "+,.0f")
surface("⑤ 後半 2022-26（円）", "b", "+,.0f")
surface("⑥ 勝ち年（/11）", "win", "d")
surface("⑦ 撃つ日数", "days", ",d")

print("=" * 140)
print("⑧ 本数との相互作用（ATR5%＋乖離15%）")
print("=" * 140)
print(f"  {'本数':<8}{'件数':>7}{'撃つ日':>7}{'勝率':>8}{'PF':>7}{'年平均':>12}{'勝ち年':>7}{'前半':>12}{'後半':>12}")
for n in (1, 2, 3, 4, 5):
    s = stat(build(sub(5.0, 15), n=n))
    print(f"  上位{n}本{'':<3}{s['n']:>7}{s['days']:>7}{s['wr']:>7.1f}%{s['pf']:>7.2f}"
          f"{s['avg']:>+11,.0f}円{s['win']:>5}/11{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円")

print("\n" + "=" * 140)
print("⑨ コスト振り（往復・%）")
print("=" * 140)
print(f"  {'コスト':<10}{'現行 年平均':>16}{'現行 勝率':>12}{'ATR5×乖離15 年平均':>22}{'ATR5×乖離15 勝率':>18}")
for c in (0.0, 0.05, 0.10, 0.20, 0.30):
    s0, s1 = stat(build(D0, cost=c)), stat(build(sub(5.0, 15), cost=c))
    print(f"  {c:.2f}%{'':<5}{s0['avg']:>+15,.0f}円{s0['wr']:>11.1f}%{s1['avg']:>+21,.0f}円{s1['wr']:>17.1f}%")

print("\n" + "=" * 140)
print("⑩ 上位の日・銘柄を除いても残るか")
print("=" * 140)
for lab, d in (("現行", build(D0)), ("ATR5%×乖離15%", build(sub(5.0, 15)))):
    byday = d.groupby("sig").yen.sum().sort_values(ascending=False)
    bytk = d.groupby("ticker").yen.sum().sort_values(ascending=False)
    tot = d.yen.sum()
    d3 = d[~d.sig.isin(byday.index[:3])]
    t3 = d[~d.ticker.isin(bytk.index[:3])]
    print(f"  {lab:<16} 全体{tot:>+11,.0f}円 / 上位3日除去{d3.yen.sum():>+11,.0f}円"
          f"（{d3.yen.sum()/tot*100:>5.1f}%残）/ 上位3銘柄除去{t3.yen.sum():>+11,.0f}円"
          f"（{t3.yen.sum()/tot*100:>5.1f}%残）")

print("\n" + "=" * 140)
print("⑪ 年別・月別（ATR5%×乖離15%）")
print("=" * 140)
CANDS = [("現行", build(D0)), ("ATR5%のみ", build(sub(5.0, 0))), ("乖離15%のみ", build(sub(0, 15))),
         ("ATR5%×乖離15%", build(sub(5.0, 15))), ("ATR4.5%×乖離12%", build(sub(4.5, 12)))]
print(f"  {'年':>6}" + "".join(f"{lab:>20}" for lab, _ in CANDS))
for y in YEARS:
    line = f"  {y:>6}"
    for _, b in CANDS:
        line += f"{b[b.y == y].yen.sum():>+19,.0f}円"
    print(line)
print(f"  {'計':>6}" + "".join(f"{b.yen.sum():>+19,.0f}円" for _, b in CANDS))
print(f"  {'勝率':>6}" + "".join(f"{(b.pnl > 0).mean() * 100:>19.1f}%" for _, b in CANDS))
print(f"  {'撃つ日':>6}" + "".join(f"{b.sig.nunique():>19,}日" for _, b in CANDS))
print(f"  {'最悪月':>6}" + "".join(f"{b.groupby('ym').yen.sum().min():>+19,.0f}円" for _, b in CANDS))

F = build(sub(5.0, 15))
print("\n  ■ ATR5%×乖離15% の月別ワースト5")
for ym, v in F.groupby("ym").yen.sum().sort_values().head(5).items():
    print(f"    {ym}  {v:>+10,.0f}円")
print(f"\n  ■ 撃たない日が増える影響: 現行{BASE['days']:,}日 → {F.sig.nunique():,}日 "
      f"（10年で{BASE['days']-F.sig.nunique():,}日ぶん機会が減る＝年{(BASE['days']-F.sig.nunique())/11:.0f}日）")
