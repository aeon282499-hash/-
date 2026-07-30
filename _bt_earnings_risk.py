# -*- coding: utf-8 -*-
"""_bt_earnings_risk.py — 決算持ち越しの「耐えられるか」を測る（2026-07-30・本番無変更）。

なぜ今これか:
  ・2026-07-26に「10年最大DD=-360.7万(800万の45%)＝生存線-30%に単独で触れる」と記録したが、
    その数字は 2026-07-29 に修正した価格キャッシュ穴バグ（10年計が37%水増し）より前の値。
  ・さらに 2026-07-30 に入口RSI上限を45→55へ緩和した（件数が増える＝リスク量も変わる）。
  → 現行本番構成での正しいリスク量を出し直す。期待値の話は済んでいるので、ここでは
    「一晩でいくら飛ぶか」「口座がどこまで凹むか」だけを見る。

構成は _bt_earnings_final10y.py と同一（PEAD延長+8%→5営業日 / 流動性7.5億 /
株価≤1万 / RSI昇順8枠）。決算持ち越しは大資金1階層のみ稼働なので大に絞る。

実行: python -X utf8 _bt_earnings_risk.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

PEAD_THR, PEAD_DAYS = 8.0, 5
RUNUP_MAX, TOV_MIN, PRICE_CAP = -3.0, 7.5e8, 10_000
SIZE = 1_000_000                      # 1玉100万

E = pd.read_csv("_earnings_events_rich2.csv")


def sim(A, slots: int):
    """本番同一の選定。1件ずつ d0（＝仕掛けた晩）を持たせて返す。"""
    days = sorted(A["d0"].unique())
    di = {d: i for i, d in enumerate(days)}
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
            if r.gap > PEAD_THR and np.isfinite(r.r5):
                pnl, span, kind = r.r5, PEAD_DAYS, "PEAD"
            else:
                pnl, span, kind = r.gap, 1, "gap"
            busy.append(i + span)
            held[r.ticker] = i + span
            out.append(dict(d0=r.d0, year=r.year, ticker=r.ticker, pnl=pnl, kind=kind))
    return pd.DataFrame(out).sort_values("d0").reset_index(drop=True)


def candidates(rsi_max: float):
    return E[(E["rsi"] <= rsi_max) & (E["runup5"] < RUNUP_MAX)
             & (E["tov20"] >= TOV_MIN) & (E["price"] <= PRICE_CAP)].sort_values(["d0", "rsi"])


def risk(P, slots: int, size: int = SIZE):
    cap = slots * size
    yen = P["pnl"] * size / 100
    cum = yen.cumsum()
    dd = float((cum - cum.cummax()).min())
    night = P.assign(yen=yen).groupby("d0")["yen"].agg(["sum", "count"])
    return dict(
        n=len(P), tot=float(yen.sum()), cap=cap, dd=dd, dd_pct=dd / cap * 100,
        worst_night=float(night["sum"].min()),
        worst_night_pct=float(night["sum"].min()) / cap * 100,
        worst_night_d=str(night["sum"].idxmin()),
        worst_trade=float(yen.min()), nights=len(night),
        max_slots_used=int(night["count"].max()),
        win_years=int(sum(1 for y in range(2016, 2027)
                          if (P[P.year == y]["pnl"] * size / 100).sum() > 0)),
        night=night["sum"],
    )


print("=" * 104)
print("① 入口RSI上限を45→55に緩めた影響（2026-07-30に本番反映済み・8枠/1玉100万/資本800万）")
print("=" * 104)
print(f"  {'RSI上限':<10}{'件数':>7}{'10年計':>13}{'年平均':>12}{'最大DD':>13}{'資本比':>9}"
      f"{'最悪の一晩':>13}{'資本比':>9}{'陽性年':>8}")
keep = {}
for rmax in (45.0, 50.0, 55.0, 60.0):
    P = sim(candidates(rmax), 8)
    r = risk(P, 8)
    keep[rmax] = (P, r)
    mark = "  ← 現行本番" if rmax == 55.0 else ("  ← 旧" if rmax == 45.0 else "")
    print(f"  ≤{rmax:<9.0f}{r['n']:>7}{r['tot']/1e4:>12,.0f}万{r['tot']/1e4/10:>11,.0f}万"
          f"{r['dd']/1e4:>12,.0f}万{r['dd_pct']:>8.1f}%"
          f"{r['worst_night']/1e4:>12,.0f}万{r['worst_night_pct']:>8.1f}%"
          f"{r['win_years']:>6}/11{mark}")

P55, R55 = keep[55.0]
print(f"\n[現行本番の実像] 仕掛けた晩 {R55['nights']}回 / 同時保有の最大 {R55['max_slots_used']}枠 / "
      f"1件の最悪 {R55['worst_trade']:,.0f}円")
print(f"  最悪の一晩 = {R55['worst_night_d']} に {R55['worst_night']:,.0f}円"
      f"（資本800万の{abs(R55['worst_night_pct']):.1f}%）")

print("\n[痛い晩ワースト8]")
for d, v in R55["night"].nsmallest(8).items():
    g = P55[P55.d0 == d]
    print(f"   {d}  {v:>12,.0f}円 ({v/8e6*100:>5.1f}%)  {len(g)}枚: "
          + " ".join(f"{t}{p:+.1f}%" for t, p in zip(g.ticker, g.pnl)))

print("\n" + "=" * 104)
print("② 生存線に触れるか（本人ルール: 口座-30%で全停止＝800万なら-240万）")
print("=" * 104)
yen = P55["pnl"] * SIZE / 100
cum = yen.cumsum()
under = (cum - cum.cummax())
print(f"  最大DD {under.min()/1e4:,.1f}万 = 資本の {under.min()/8e6*100:.1f}%")
for line in (-0.10, -0.20, -0.30):
    hit = (under <= 8e6 * line)
    if hit.any():
        first = P55.loc[hit.idxmax(), "d0"]
        print(f"  {line*100:>4.0f}%線: 到達する（初回 {first} ・のべ{int(hit.sum())}件が線の下）")
    else:
        print(f"  {line*100:>4.0f}%線: 到達しない")

print("\n" + "=" * 104)
print("③ 枠と玉サイズをどうすれば -30% 線に触れないか（期待値と安全度のトレードオフ）")
print("=" * 104)
print(f"  {'構成':<22}{'資本':>10}{'10年計':>12}{'資本比':>9}{'最大DD':>12}{'資本比':>9}{'最悪の晩':>12}{'資本比':>9}")
for slots, size in ((8, 1_000_000), (8, 700_000), (8, 500_000),
                    (5, 1_000_000), (4, 1_000_000), (3, 1_000_000)):
    P = sim(candidates(55.0), slots)
    r = risk(P, slots, size)
    print(f"  {slots}枠 × {size/1e4:.0f}万{'':<10}{r['cap']/1e4:>9,.0f}万{r['tot']/1e4:>11,.0f}万"
          f"{r['tot']/r['cap']*100:>8.0f}%{r['dd']/1e4:>11,.0f}万{r['dd_pct']:>8.1f}%"
          f"{r['worst_night']/1e4:>11,.0f}万{r['worst_night_pct']:>8.1f}%")

print("\n注: DDはトレード列の累積（同じ晩の玉は連続して積まれるので一晩の集中は反映される）。")
print("    枠を減らすと1晩の被弾は減るが、決算はテール依存（上位20玉で利益の84%）なので")
print("    枠を埋め続けられないと期待値が急速に落ちる点に注意。")
