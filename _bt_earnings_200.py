# -*- coding: utf-8 -*-
"""_bt_earnings_200.py — 決算シグナルを資金200万で運用したらどうなるか（2026-07-31）。

本人「これ決算を200万資金だとどうなる?」。現行は 1玉100万×8枠＝ピーク800万。
200万の割り方は一意でないので総当たりする:
  枠を減らす（1玉100万×2枠）… 玉は大きいが枠が埋まらず**テールを取り逃す**
  玉を小さくする（1玉25万×8枠）… 枠は保てるが**株価上限が2,500円に落ちて母集団が痩せる**
                                （建てられるのは price ≤ size/100 の玉だけ＝本番と同じ制約）

決算は「上位20玉で利益の84%」のテール依存なので、どちらの副作用が重いかは測らないと分からない。
現行ルール: RSI≤55 × 直近5日騰落<-3% × 20日代金7.5億以上 → RSI昇順で枠数まで
            → 決算当日大引け買い → 翌寄り売り（翌寄り+8%超なら5営業日目の大引けまで＝PEAD延長）

実行: python -X utf8 _bt_earnings_200.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

YEARS = list(range(2017, 2027))
E = pd.read_csv("_earnings_events_rich2.csv")


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


def run(size: int, slots: int):
    A = E[(E.rsi <= 55.0) & (E.runup5 < -3.0) & (E.tov20 >= 7.5e8)
          & (E.price <= size / 100)].sort_values(["d0", "rsi"])
    days = sorted(A["d0"].unique()); di = {d: i for i, d in enumerate(days)}
    busy, held, out = [], {}, []
    for d, g in A.groupby("d0", sort=True):
        i = di[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in g.itertuples():
            if len(busy) >= slots:
                break
            if not np.isfinite(r.gap) or r.ticker in held:
                continue
            sh = int(size / r.price / 100) * 100
            if sh <= 0:
                continue
            pnl, span = (r.r5, 5) if (r.gap > 8.0 and np.isfinite(r.r5)) else (r.gap, 1)
            busy.append(i + span); held[r.ticker] = i + span
            out.append({"date": r.d0, "y": r.year, "ticker": r.ticker, "pnl_pct": pnl,
                        "yen": pnl / 100 * sh * r.price, "used": sh * r.price})
    return pd.DataFrame(out)


def stat(T, cap):
    yr = T.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    daily = T.groupby("date").yen.sum().sort_index()
    cum = daily.cumsum()
    dd = (cum - cum.cummax()).min()                  # 最大DD（円・決済日ベース）
    return dict(n=len(T), wr=(T.pnl_pct > 0).mean() * 100, pf=pf(T.pnl_pct),
                tot=T.yen.sum(), avg=T.yen.sum() / 10, win=int((yr > 0).sum()),
                worst=yr.min(), dd=dd, ddp=dd / cap * 100,
                y26=T[T.date >= "2026-01-01"].yen.sum(), roi=T.yen.sum() / 10 / cap * 100)


CAP = 2_000_000
PLANS = [(1_000_000, 2), (670_000, 3), (500_000, 4), (400_000, 5),
         (330_000, 6), (250_000, 8), (200_000, 10), (170_000, 12)]

print("=" * 132)
print("① 資金200万の割り方 総当たり（1玉サイズ × 枠数 ＝ 200万）")
print("=" * 132)
print(f"  {'構成':<20}{'株価上限':>9}{'件数':>7}{'勝率':>8}{'PF':>7}{'年平均':>13}{'資金年利':>9}"
      f"{'勝ち年':>7}{'最悪年':>13}{'最大DD':>13}{'DD/資金':>9}{'2026(7ヶ月)':>14}")
res = {}
for size, slots in PLANS:
    T = run(size, slots)
    s = stat(T, CAP)
    res[(size, slots)] = (T, s)
    print(f"  1玉{size//10000}万×{slots}枠{'':<9}{size//100:>8,}円{s['n']:>7}{s['wr']:>7.1f}%{s['pf']:>7.2f}"
          f"{s['avg']:>+12,.0f}円{s['roi']:>+8.1f}%{s['win']:>5}/10{s['worst']:>+12,.0f}円"
          f"{s['dd']:>+12,.0f}円{s['ddp']:>8.1f}%{s['y26']:>+13,.0f}円")

T8, s8 = run(1_000_000, 8), None
s8 = stat(T8, 8_000_000)
print(f"\n  【参考】現行 1玉100万×8枠（ピーク800万）")
print(f"  {'':<20}{10000:>8,}円{s8['n']:>7}{s8['wr']:>7.1f}%{s8['pf']:>7.2f}"
      f"{s8['avg']:>+12,.0f}円{s8['roi']:>+8.1f}%{s8['win']:>5}/10{s8['worst']:>+12,.0f}円"
      f"{s8['dd']:>+12,.0f}円{s8['ddp']:>8.1f}%{s8['y26']:>+13,.0f}円")

best = max(res.items(), key=lambda kv: kv[1][1]["tot"])
bk, (BT, bs) = best
print(f"\n  → 200万の中での最良は 1玉{bk[0]//10000}万×{bk[1]}枠"
      f"（年{bs['avg']:+,.0f}円・現行800万の{bs['tot']/s8['tot']*100:.0f}%を"
      f"資金{CAP/8_000_000*100:.0f}%で取る）")

print("\n" + "=" * 132)
print("② 年別（円）")
print("=" * 132)
SHOW = [(1_000_000, 2), (500_000, 4), (250_000, 8), (200_000, 10)]
print(f"  {'年':>6}" + "".join(f"{f'1玉{s//10000}万×{n}枠':>17}" for s, n in SHOW)
      + f"{'現行(800万)':>18}")
for y in YEARS:
    line = f"  {y:>6}"
    for k in SHOW:
        line += f"{res[k][0].query('y==@y').yen.sum():>16,.0f}円"
    line += f"{T8.query('y==@y').yen.sum():>17,.0f}円"
    print(line)
print(f"  {'計':>6}" + "".join(f"{res[k][0].yen.sum():>16,.0f}円" for k in SHOW)
      + f"{T8.yen.sum():>17,.0f}円")

print("\n" + "=" * 132)
print("③ なぜ差が出るか（枠を減らすと何を取り逃すか）")
print("=" * 132)
for size, slots in [(1_000_000, 2), (500_000, 4), (250_000, 8)]:
    T = res[(size, slots)][0]
    top = T.nlargest(20, "yen").yen.sum()
    print(f"  1玉{size//10000}万×{slots}枠: 候補プール{len(E[(E.rsi<=55)&(E.runup5<-3)&(E.tov20>=7.5e8)&(E.price<=size/100)]):>6,}件"
          f"（株価{size//100:,}円以下）→ 執行{len(T):>5}件 ／ 上位20玉が利益の"
          f"{top/T.yen.sum()*100:>5.1f}% ／ 1玉平均{T.yen.mean():>+7,.0f}円")
print(f"  現行100万×8枠   : 候補プール{len(E[(E.rsi<=55)&(E.runup5<-3)&(E.tov20>=7.5e8)&(E.price<=10000)]):>6,}件"
      f"（株価10,000円以下）→ 執行{len(T8):>5}件 ／ 上位20玉が利益の"
      f"{T8.nlargest(20,'yen').yen.sum()/T8.yen.sum()*100:>5.1f}% ／ 1玉平均{T8.yen.mean():>+7,.0f}円")

print("\n" + "=" * 132)
print("④ 2026年の月別（円・上位3構成）")
print("=" * 132)
mons = sorted({d[:7] for d in T8[T8.date >= "2026-01-01"].date})
print(f"  {'月':>9}" + "".join(f"{f'1玉{s//10000}万×{n}枠':>17}" for s, n in SHOW) + f"{'現行(800万)':>18}")
for m in mons:
    line = f"  {m:>9}"
    for k in SHOW:
        line += f"{res[k][0][res[k][0].date.str.startswith(m)].yen.sum():>16,.0f}円"
    line += f"{T8[T8.date.str.startswith(m)].yen.sum():>17,.0f}円"
    print(line)
print(f"  {'計':>9}" + "".join(f"{res[k][1]['y26']:>16,.0f}円" for k in SHOW)
      + f"{s8['y26']:>17,.0f}円")
