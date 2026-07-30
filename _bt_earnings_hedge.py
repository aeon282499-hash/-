# -*- coding: utf-8 -*-
"""_bt_earnings_hedge.py — 決算持ち越しの「市場成分」を剥がすと何が起きるか（2026-07-31・本番無変更）。

問題意識:
  期待値を上げる方向はほぼ出尽くした（絞り込み系は12軸すべて棄却＝テール依存だから
  候補を削ると負ける）。いま実際に効いている制約は期待値ではなく **DD**:
  10年最大DD -448万 = 資本800万の-56% で、本人の生存線 -30% を単独で突破する。

  ところが翌寄りギャップは「市場全体のギャップ ＋ その銘柄の決算反応」の合計であって、
  前者は決算とは何の関係もないただのノイズ。8枚同時保有＝800万のロングを毎晩
  裸で市場に晒している。市場成分の期待値はほぼゼロなのに、DDだけは負っている。

  → 市場成分を剥がしたら、期待値をほとんど落とさずにDDだけ下がるのでは。
    （実運用なら日経先物や1321ETFの同額ショートで近似できる形）

測り方: 各玉の損益から、同じ区間の市場リターン（1321.T）を beta 倍だけ差し引く。
  gap玉  … 市場の 前日終値→翌寄り
  PEAD玉 … 市場の 前日終値→5営業日目の終値
beta=0 が現行（裸）、beta=1 が全額ヘッジ。

実行: python -X utf8 _bt_earnings_hedge.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

PEAD_THR, PEAD_DAYS, SLOTS = 8.0, 5, 8
RSI_MAX, RUNUP_MAX, TOV_MIN, PRICE_CAP = 55.0, -3.0, 7.5e8, 10_000
SIZE = 1_000_000
ERAS = [("2016-21", 2016, 2021), ("2022-26", 2022, 2026)]

# ── 市場（1321.T）の同区間リターンを作る ──────────────────────
M = pd.concat([pd.read_pickle("_mkt_jquant016_2021.pkl"),
               pd.read_pickle("_mkt_jquantts_cache.pkl")]).sort_index()
M = M[~M.index.duplicated(keep="last")]
M.index = [str(i)[:10] for i in M.index]
o, c = M["Open"].astype(float), M["Close"].astype(float)
days = list(M.index)
di = {d: i for i, d in enumerate(days)}

mkt_gap, mkt_r5 = {}, {}
for i, d in enumerate(days[:-1]):
    prev = c.iloc[i]
    if not (prev > 0):
        continue
    mkt_gap[d] = (o.iloc[i + 1] - prev) / prev * 100
    j = i + PEAD_DAYS
    if j < len(days):
        mkt_r5[d] = (c.iloc[j] - prev) / prev * 100

E = pd.read_csv("_earnings_events_rich2.csv")
E = E[(E["rsi"] <= RSI_MAX) & (E["runup5"] < RUNUP_MAX)
      & (E["tov20"] >= TOV_MIN) & (E["price"] <= PRICE_CAP)].sort_values(["d0", "rsi"])
E["mgap"] = [mkt_gap.get(d, np.nan) for d in E["d0"]]
E["mr5"] = [mkt_r5.get(d, np.nan) for d in E["d0"]]
print(f"[市場] 1321.T {days[0]}〜{days[-1]} / 候補{len(E):,}件に市場ギャップ付与 "
      f"{E['mgap'].notna().mean()*100:.0f}%")


def sim(A, beta: float, slots: int = SLOTS):
    ds = sorted(A["d0"].unique())
    ix = {d: i for i, d in enumerate(ds)}
    busy, held, out = [], {}, []
    for d, g in A.groupby("d0", sort=True):
        i = ix[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in g.itertuples():
            if len(busy) >= slots:
                break
            if not np.isfinite(r.gap) or r.ticker in held:
                continue
            if r.gap > PEAD_THR and np.isfinite(r.r5):
                pnl, span, m = r.r5, PEAD_DAYS, r.mr5
            else:
                pnl, span, m = r.gap, 1, r.mgap
            if beta and np.isfinite(m):
                pnl = pnl - beta * m
            busy.append(i + span)
            held[r.ticker] = i + span
            out.append(dict(d0=r.d0, year=r.year, pnl=pnl))
    return pd.DataFrame(out)


def score(P, slots=SLOTS, size=SIZE):
    yen = P["pnl"] * size / 100
    cum = yen.cumsum()
    dd = float((cum - cum.cummax()).min())
    night = P.assign(y=yen).groupby("d0")["y"].sum()
    o = dict(n=len(P), tot=float(yen.sum()), dd=dd, cap=slots * size,
             worst=float(night.min()),
             win=int(sum(1 for y in range(2016, 2027) if (P[P.year == y]["pnl"]).sum() > 0)))
    for lab, y0, y1 in ERAS:
        o[lab] = float((P[(P.year >= y0) & (P.year <= y1)]["pnl"] * size / 100).sum())
    return o


print("\n" + "=" * 112)
print("① 市場成分をβだけ剥がす（β=0が現行の裸・β=1が全額ヘッジ・8枠/1玉100万/資本800万）")
print("=" * 112)
print(f"  {'β':<8}{'10年計':>12}{'年平均':>11}{'最大DD':>12}{'資本比':>9}{'最悪の晩':>12}"
      f"{'勝ち年':>8}{'前半16-21':>12}{'後半22-26':>12}")
base = None
for beta in (0.0, 0.25, 0.5, 0.75, 1.0):
    r = score(sim(E, beta))
    if base is None:
        base = r
    tag = "  ← 現行" if beta == 0 else ""
    print(f"  {beta:<8.2f}{r['tot']/1e4:>11,.0f}万{r['tot']/1e4/10:>10,.0f}万"
          f"{r['dd']/1e4:>11,.0f}万{r['dd']/r['cap']*100:>8.1f}%{r['worst']/1e4:>11,.0f}万"
          f"{r['win']:>6}/11{r['2016-21']/1e4:>11,.0f}万{r['2022-26']/1e4:>11,.0f}万{tag}")

print("\n[意味] リターン/DD比が上がるなら、同じ痛みでより大きく張れる＝実質の増強。")
r0, r1 = score(sim(E, 0.0)), score(sim(E, 1.0))
print(f"  β=0: 10年{r0['tot']/1e4:,.0f}万 / DD{r0['dd']/1e4:,.0f}万 → 比 {r0['tot']/abs(r0['dd']):.2f}")
print(f"  β=1: 10年{r1['tot']/1e4:,.0f}万 / DD{r1['dd']/1e4:,.0f}万 → 比 {r1['tot']/abs(r1['dd']):.2f}")

print("\n" + "=" * 112)
print("② 生存線 -30%（=800万で-240万）に対して、ヘッジ後は1玉いくらまで張れるか")
print("=" * 112)
print(f"  {'β':<8}{'DD(100万/玉)':>16}{'-30%に収まる玉サイズ':>24}{'その時の年利(口座800万比)':>26}")
for beta in (0.0, 0.5, 1.0):
    P = sim(E, beta)
    yen = P["pnl"] * 1_000_000 / 100
    cum = yen.cumsum()
    dd = abs(float((cum - cum.cummax()).min()))
    size = 2_400_000 / dd * 1_000_000
    tot = float(yen.sum()) * (size / 1_000_000)
    print(f"  {beta:<8.2f}{-dd/1e4:>15,.0f}万{size/1e4:>22,.0f}万{tot/8e6*10:>24.1f}%")

print("\n" + "=" * 112)
print("③ 市場成分そのものの寄与（剥がして良いものか＝期待値を捨てていないか）")
print("=" * 112)
P0, P1 = sim(E, 0.0), sim(E, 1.0)
d = P0["pnl"].sum() - P1["pnl"].sum()
print(f"  市場成分の累積寄与 = {d:+.1f}%（1玉100万換算 {d*1e4:+,.0f}円）")
for lab, y0, y1 in ERAS:
    a = P0[(P0.year >= y0) & (P0.year <= y1)]["pnl"].sum()
    b = P1[(P1.year >= y0) & (P1.year <= y1)]["pnl"].sum()
    print(f"  {lab}: 市場成分 {a-b:+.1f}%（裸{a:+.1f}% → ヘッジ後{b:+.1f}%）")
