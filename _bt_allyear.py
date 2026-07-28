# -*- coding: utf-8 -*-
"""_bt_allyear.py — 3システムで「毎年プラス」にできるかの総当たり（2026-07-28）。

現状: フェード11/11年・決算9/11年・極み9/10年で、合算は9/11年（2018 -88万 / 2019 -76万）。
2018-19は決算とスイングが同時に沈む年で、個別システムのゲートは9軸+1軸すべて棄却済み。
そこで「配分」の側で解けるかを探す。**2018-19を狙い撃つ後付けは採らない**＝
全期間で同じルールを適用し、両期間（前半/後半）とも劣化しないものだけを候補にする。

軸:
  A. 静的な配分比率（フェードを厚く/決算を薄く…の総当たり）
  B. 単純なリスク管理（前年がマイナスだったシステムを翌年だけ半分にする等）
  C. 相関を見る（どのシステムが逆相関か）

実行: python -X utf8 _bt_allyear.py
"""
from __future__ import annotations

import json
import itertools

import numpy as np
import pandas as pd


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


# ── 各システムの「1単位あたり年別損益」を作る（サイズは後で掛ける）──
def fade_yearly(size=500_000):
    F = pd.read_pickle("_fade_deep.pkl").copy()
    F = F[(F.gain >= 6) & (F.vr < 6) & (F.dev < 80)]
    for k in ("dev", "atr"):
        F["r_" + k] = F.groupby("sig")[k].rank(ascending=False, pct=True)
    F["mix"] = (F["r_dev"] + F["r_atr"]) / 2
    S = F.sort_values(["sig", "mix"], ascending=[True, True]).groupby("sig").head(1).copy()
    S["sh"] = (size / S["o1"] // 100 * 100).astype(int)
    S = S[S["sh"] > 0]
    S["yen"] = (S["o1"] - S["c1"]) * S["sh"]      # 1株あたりの値幅×株数＝円
    return S.groupby("y")["yen"].sum()


def earn_yearly(size=1_000_000, slots=8):
    E = pd.read_csv("_earnings_events_rich2.csv")
    A = E[(E.rsi <= 45) & (E.runup5 < -3) & (E.tov20 >= 7.5e8)
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
            out.append({"y": r.year, "yen": pnl / 100 * sh * r.price})
    return pd.DataFrame(out).groupby("y")["yen"].sum()


def swing_yearly(size=1_000_000, slots=3):
    C = pd.read_csv("_bt10y_candidates_margin.csv", parse_dates=["entry"])
    C = C[~(C["days_cover"] > 0.8)]
    SEC = json.load(open("sector33_map.json", encoding="utf-8"))
    C["sector"] = C["ticker"].map(SEC).fillna("")
    C["day"] = C["entry"].dt.strftime("%Y-%m-%d")
    C = C.sort_values(["day", "score"], ascending=[True, False])
    days = sorted(C["day"].unique()); di = {d: i for i, d in enumerate(days)}
    busy, held, out = [], {}, []
    for d, g in C.groupby("day", sort=True):
        i = di[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        used, n = {}, 0
        for r in g.itertuples():
            if n >= slots or len(busy) >= slots:
                break
            if r.ticker in held or not np.isfinite(r.pnl) or r.price * 100 > size:
                continue
            if r.sector and used.get(r.sector, 0) >= 3:
                continue
            if r.sector:
                used[r.sector] = used.get(r.sector, 0) + 1
            sh = int(size / r.price / 100) * 100
            span = int(r.exoff) + 1
            busy.append(i + span); held[r.ticker] = i + span; n += 1
            out.append({"y": r.year, "yen": r.pnl / 100 * sh * r.price})
    return pd.DataFrame(out).groupby("y")["yen"].sum()


if __name__ == "__main__":
    YEARS = list(range(2017, 2027))          # 極みが2017からなので揃える
    fd = fade_yearly(500_000).reindex(YEARS, fill_value=0)
    ea = earn_yearly(1_000_000, 8).reindex(YEARS, fill_value=0)
    sw = swing_yearly(1_000_000, 3).reindex(YEARS, fill_value=0)
    base = pd.DataFrame({"フェード": fd, "決算": ea, "極み": sw})
    print("■ 基準（フェード50万 / 決算100万×8枠 / 極み100万×3枠）")
    print((base / 10000).round(0).to_string())
    print(f"\n  合計 {base.sum(axis=1).sum()/10000:+.0f}万 / 勝ち年 "
          f"{(base.sum(axis=1) > 0).sum()}/{len(YEARS)}")

    print("\n■ 相関（年次リターン）")
    print(base.corr().round(2).to_string())

    print("\n■ A. 配分比率の総当たり（各システムの倍率を0.5〜3.0で振る）")
    print("   ※資金制約は無視して『比率だけ』で毎年プラスにできるかを見る")
    best = []
    grid = [0.5, 1.0, 1.5, 2.0, 3.0]
    for a, b, c in itertools.product(grid, repeat=3):
        tot = base["フェード"] * a + base["決算"] * b + base["極み"] * c
        wins = (tot > 0).sum()
        worst = tot.min()
        best.append((wins, worst, tot.sum(), a, b, c))
    best.sort(key=lambda x: (-x[0], -x[1]))
    print(f"  {'勝ち年':>6}{'最悪年':>12}{'10年計':>13}   フェード×  決算×  極み×")
    for wins, worst, tot, a, b, c in best[:8]:
        print(f"  {wins:>4}/10{worst/10000:>+11.0f}万{tot/10000:>+12.0f}万"
              f"{a:>10.1f}{b:>7.1f}{c:>7.1f}")
