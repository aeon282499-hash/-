# -*- coding: utf-8 -*-
"""_bt_earnings_vol_sizing.py — ブリーフ問B「サイズで差をつける」の実測（2026-08-01・本番無変更）。

前提（_BRIEF_for_fable5.md）:
  ・順位に情報がない（方向は当てられない）
  ・エッジは非対称。決算ボラゲート≥2.0%だけが効いた＝「動く銘柄」を選ぶのは筋が良い
  ・候補を削る系は基本マイナス（枠を埋めるゲーム）

仮説: 削らずに「よく動く体質の銘柄ほど1玉を大きく」すれば、
      候補数を維持したまま非対称への露出だけ増やせるのでは。
対照: 逆傾斜（動かないほど大きく）が悪化しないなら、傾斜はただのサイズ増と区別できない。

判定バー（家訓）: 両期間で改善 / 同DD換算で現行超え / 勝ち年数維持 /
                変種間で高原（針なら棄却）/ 上位20玉除去でも改善が残る。

実行: python -X utf8 _bt_earnings_vol_sizing.py
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

RSI_MAX, RUNUP_MAX, TOV_MIN, PRICE_CAP = 55.0, -3.0, 7.5e8, 10_000
PEAD_THR, PEAD_DAYS, SLOTS = 8.0, 5, 8
SIZE = 1_000_000
DD_BUDGET = 2_400_000
VOL_GATE = 2.0

RAW = pd.read_csv("_earnings_events_rich2.csv")
RAW = RAW.sort_values(["ticker", "d0"])
g = RAW.groupby("ticker")["gap"]
RAW["egap_vol"] = g.transform(lambda s: s.abs().shift(1).expanding(min_periods=3).median())

E = RAW[(RAW["rsi"] <= RSI_MAX) & (RAW["runup5"] < RUNUP_MAX)
        & (RAW["tov20"] >= TOV_MIN) & (RAW["price"] <= PRICE_CAP)].copy()
# 本番ゲート: 欠測フェイルオープン × ボラ>=2.0
E = E[E["egap_vol"].isna() | (E["egap_vol"] >= VOL_GATE)].copy()
E = E.sort_values(["d0", "rsi"]).reset_index(drop=True)
print(f"[候補] ゲート通過 {len(E):,}件 / ボラ欠測 {E.egap_vol.isna().mean()*100:.1f}%")


def sim_w(weight_fn):
    """本番同一の8枠シム＋玉サイズだけweight_fn(egap_vol)倍。夜間露出も記録。"""
    days = sorted(E["d0"].unique())
    di = {d: i for i, d in enumerate(days)}
    busy, held, out = [], {}, []   # busy: (解放index, weight)
    for d, gg in E.groupby("d0", sort=True):
        i = di[d]
        busy = [(x, w) for x, w in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in gg.itertuples():
            if len(busy) >= SLOTS:
                break
            if not np.isfinite(r.gap) or r.ticker in held:
                continue
            w = weight_fn(r.egap_vol)
            if r.gap > PEAD_THR and np.isfinite(r.r5):
                pnl, span = r.r5, PEAD_DAYS
            else:
                pnl, span = r.gap, 1
            busy.append((i + span, w))
            held[r.ticker] = i + span
            out.append(dict(year=r.year, pnl=pnl, w=w,
                            exposure=sum(x[1] for x in busy)))
    return pd.DataFrame(out)


def score(P):
    yen = P["pnl"] * SIZE * P["w"] / 100
    cum = yen.cumsum()
    dd = float((cum - cum.cummax()).min())
    yr = yen.groupby(P["year"]).sum().reindex(range(2016, 2027), fill_value=0)
    top20 = yen.nlargest(20).sum()
    return dict(n=len(P), avgw=P.w.mean(), tot=float(yen.sum()), dd=dd,
                ratio=float(yen.sum()) / abs(dd),
                norm=float(yen.sum()) * DD_BUDGET / abs(dd),
                win=int((yr > 0).sum()),
                e1=float(yr[yr.index <= 2021].sum()), e2=float(yr[yr.index >= 2022].sum()),
                maxexp=float(P.exposure.max()) * SIZE,
                extop20=float(yen.sum() - top20))


VARIANTS = [
    ("現行=フラット1.0", lambda v: 1.0),
    ("連続傾斜 v/3 [0.5-2.0]", lambda v: 1.0 if not np.isfinite(v) else float(np.clip(v / 3.0, 0.5, 2.0))),
    ("段階 2-3=1.0/3-4=1.25/4+=1.5", lambda v: 1.0 if not np.isfinite(v) else (1.0 if v < 3 else 1.25 if v < 4 else 1.5)),
    ("段階 2-3=0.75/3-4=1.0/4+=1.5", lambda v: 1.0 if not np.isfinite(v) else (0.75 if v < 3 else 1.0 if v < 4 else 1.5)),
    ("段階 2-3=0.5/3+=1.5", lambda v: 1.0 if not np.isfinite(v) else (0.5 if v < 3 else 1.5)),
    ("【対照】逆傾斜 3/v [0.5-2.0]", lambda v: 1.0 if not np.isfinite(v) else float(np.clip(3.0 / v, 0.5, 2.0))),
]

print(f"\n{'設定':<30}{'件数':>6}{'平均w':>6}{'10年計':>9}{'最大DD':>8}{'比':>6}"
      f"{'同DD換算':>9}{'勝年':>5}{'前半':>8}{'後半':>8}{'夜間最大':>9}{'上20除去':>9}")
for tag, fn in VARIANTS:
    r = score(sim_w(fn))
    print(f"{tag:<30}{r['n']:>6}{r['avgw']:>6.2f}{r['tot']/1e4:>8,.0f}万{r['dd']/1e4:>7,.0f}万"
          f"{r['ratio']:>6.2f}{r['norm']/1e4:>8,.0f}万{r['win']:>4}/11{r['e1']/1e4:>7,.0f}万"
          f"{r['e2']/1e4:>7,.0f}万{r['maxexp']/1e4:>8,.0f}万{r['extop20']/1e4:>8,.0f}万")

print("\n[読み方] 傾斜が「同DD換算」で現行を超え、前半後半とも崩れず、逆傾斜が明確に劣るなら採用検討。"
      "\n         逆傾斜も同じだけ良い場合は『ただサイズを増やしただけ』＝棄却。")
