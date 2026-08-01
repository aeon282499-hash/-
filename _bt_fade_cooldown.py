# -*- coding: utf-8 -*-
"""_bt_fade_cooldown.py — 同一銘柄の連投を制限すると3月・4月型の傷は浅くなるか（2026-07-28）。

発端: 2026年3月は ジャパンディスプレイ(6740) を月内に何度も撃って -72万、
      4月は ユニチカ(3103) で -38万。負けが同一銘柄の連続踏み上げに集中していた。
      過去に「クールダウン却下（連投PF2.23が最強・除外は-360万）」と結論しているが、
      それは旧ロジック（上昇率降順）での検証。新ロジック（25MA乖離+ATR順）では未検証。

設計: クールダウン中の銘柄は飛ばして**次点を繰り上げる**（＝毎日1件という頻度は落とさない）。
      これが「撃たない」との違い。頻度を保ったまま銘柄を分散できるかを見る。

実行: python -X utf8 _bt_fade_cooldown.py
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

SIZE = 1_000_000
GAIN_MIN = 6.0

D = pd.read_pickle("_fade_deep.pkl").copy()
for k in ("dev", "atr"):
    D["r_" + k] = D.groupby("sig")[k].rank(ascending=False, pct=True)
D["mix"] = (D["r_dev"] + D["r_atr"]) / 2
D = D[D["gain"] >= GAIN_MIN].sort_values(["sig", "mix"], ascending=[True, True])

old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
NM = dict(old["name_map"]); NM.update(new["name_map"])

DAYS = sorted(D["sig"].unique())
DI = {d: i for i, d in enumerate(DAYS)}


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


def sim(cool_days: int, per_month: int | None = None):
    """cool_days: 同一銘柄を撃ってから何営業日あけるか（0=制限なし）。
    per_month: 同一銘柄の月内上限回数（None=制限なし）。どちらも次点繰り上げ。"""
    last: dict[str, int] = {}
    cnt: dict[tuple[str, str], int] = {}
    out = []
    for d, g in D.groupby("sig", sort=True):
        i = DI[d]
        for r in g.itertuples():
            if cool_days and r.ticker in last and i - last[r.ticker] < cool_days:
                continue
            key = (r.ticker, r.ent)
            if per_month is not None and cnt.get(key, 0) >= per_month:
                continue
            sh = int(SIZE / r.o1 / 100) * 100
            if sh <= 0:
                continue
            last[r.ticker] = i
            cnt[key] = cnt.get(key, 0) + 1
            out.append({"sig": r.sig, "ent": r.ent, "y": r.y, "ticker": r.ticker,
                        "pnl": r.pnl, "yen": r.pnl / 100 * sh * r.o1})
            break                                  # その日は1件だけ
    return pd.DataFrame(out)


if __name__ == "__main__":
    print("■ 同一銘柄クールダウン（次点繰り上げ＝毎日1件は維持）")
    print(f"  {'設定':<20}{'件数':>6}{'PF':>7}{'年平均':>11}{'最悪年':>12}{'勝ち年':>7}"
          f"{'2026-03':>10}{'2026-04':>10}")
    base = None
    for cd, lab in [(0, "制限なし(現行)"), (1, "1日あける"), (3, "3日あける"),
                    (5, "5日あける"), (10, "10日あける"), (20, "20日あける")]:
        P = sim(cd)
        yr = P.groupby("y")["yen"].sum()
        m3 = P[P["ent"] == "2026-03"]["yen"].sum()
        m4 = P[P["ent"] == "2026-04"]["yen"].sum()
        w = sum(1 for v in yr if v > 0)
        if base is None:
            base = P["yen"].sum()
        print(f"  {lab:<20}{len(P):>6}{pf(P['pnl']):>7.2f}{P['yen'].sum() / 10:>+10,.0f}円"
              f"{yr.min():>+11,.0f}円{w:>4}/{len(yr)}{m3 / 10000:>+9.0f}万{m4 / 10000:>+9.0f}万")

    print("\n■ 月内の同一銘柄の回数上限（次点繰り上げ）")
    print(f"  {'設定':<20}{'件数':>6}{'PF':>7}{'年平均':>11}{'最悪年':>12}{'勝ち年':>7}"
          f"{'2026-03':>10}{'2026-04':>10}")
    for pm, lab in [(None, "制限なし(現行)"), (4, "月4回まで"), (3, "月3回まで"),
                    (2, "月2回まで"), (1, "月1回まで")]:
        P = sim(0, pm)
        yr = P.groupby("y")["yen"].sum()
        m3 = P[P["ent"] == "2026-03"]["yen"].sum()
        m4 = P[P["ent"] == "2026-04"]["yen"].sum()
        w = sum(1 for v in yr if v > 0)
        print(f"  {lab:<20}{len(P):>6}{pf(P['pnl']):>7.2f}{P['yen'].sum() / 10:>+10,.0f}円"
              f"{yr.min():>+11,.0f}円{w:>4}/{len(yr)}{m3 / 10000:>+9.0f}万{m4 / 10000:>+9.0f}万")

    print("\n■ 現行(制限なし)で同一銘柄を月に何回撃っているか")
    P = sim(0)
    rep = P.groupby(["ent", "ticker"]).size()
    print(f"  月×銘柄の組 {len(rep)} / 平均{rep.mean():.2f}回 / 最多{rep.max()}回")
    top = rep.sort_values(ascending=False).head(8)
    for (m, tk), c in top.items():
        y = P[(P["ent"] == m) & (P["ticker"] == tk)]["yen"].sum()
        print(f"    {m} {NM.get(tk, tk)[:14]:<14}({tk[:4]}) {c}回 {y:+,.0f}円")
