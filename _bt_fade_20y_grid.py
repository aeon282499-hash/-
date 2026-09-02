# -*- coding: utf-8 -*-
"""_bt_fade_20y_grid.py — 売りフェードの入口×並び順を26年でフルグリッド（2026-09-03）。

今までの入口(前日+7%/ATR≥5/乖離≥12/vr<6/rng>5/代金3億)は10年で最適化した値。26年（_bt_fade_20y_pool.csv）で
  ①フルグリッド → ②「2001-16で最良」を確定 → ③その2017-26（設計外＝OOS）を見る → ④4期間すべてPF>1の面の広さ
の作法で、現行より頑健で期待値の高いセルがあるかを測る。評価=1番×100万＋2番×50万の合算（実運用）。
グリッド: gain{7,8,10} × atr{4,5,6} × dev{8,12,16,20} × vr{4,6,10} × rng{3,5,8} × tov{3,5,10億} = 972
         × 並び順{乖離×ATR(現行), 乖離のみ, ATRのみ, 前日比のみ, 乖離×ATR×前日比} = 4,860評価
注意: 2016以前は貸借判定なし・現存銘柄のみ（売り側は保守的）・張り付き除外なし（現行と同じ）。
実行: python -X utf8 _bt_fade_20y_grid.py → _bt_fade_20y_grid.csv / _log_fade_20y_grid.txt
"""
from __future__ import annotations

import itertools
import time

import numpy as np
import pandas as pd

SIZE1, SIZE2 = 1_000_000, 500_000
D = pd.read_csv("_bt_fade_20y_pool.csv")
D["ym"] = D.ent.str[:7]
print(f"[pool] {len(D):,}件 {D.sig.min()}〜{D.sig.max()}", flush=True)
# 並び順の百分位（日ごと・降順）を事前計算しておくと各セルで再計算不要 → セル内の候補集合が変わるので都度計算が正しい。
# 速度のため、セルごとに絞った後に rank する（groupby は sig）。

RANKS = {
    "乖離×ATR(現行)": lambda d: (d.groupby("sig").dev.rank(ascending=False, pct=True) + d.groupby("sig").atr.rank(ascending=False, pct=True)) / 2,
    "乖離のみ": lambda d: d.groupby("sig").dev.rank(ascending=False, pct=True),
    "ATRのみ": lambda d: d.groupby("sig").atr.rank(ascending=False, pct=True),
    "前日比のみ": lambda d: d.groupby("sig").gain.rank(ascending=False, pct=True),
    "乖離×ATR×前日比": lambda d: (d.groupby("sig").dev.rank(ascending=False, pct=True) + d.groupby("sig").atr.rank(ascending=False, pct=True) + d.groupby("sig").gain.rank(ascending=False, pct=True)) / 3,
}


def pf(x):
    n = -x[x <= 0].sum(); return x[x > 0].sum() / n if n else np.inf


def evaluate(G, key_fn):
    d = G.copy(); d["key"] = key_fn(d)
    d = d.sort_values(["sig", "key", "ticker"], kind="stable"); d["rk"] = d.groupby("sig").cumcount() + 1
    d = d[d.rk <= 2].copy()
    d["size"] = np.where(d.rk == 1, SIZE1, SIZE2)
    d["sh"] = (d["size"] / d.px // 100 * 100).astype(int); d = d[d.sh > 0]
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    out = {"n1": int((d.rk == 1).sum()), "n2": int((d.rk == 2).sum())}
    yy = d.groupby("y").yen.sum(); ym = d.groupby("ym").yen.sum()
    for lo, hi, nm in ((2001, 2008, "e1"), (2009, 2016, "e2"), (2017, 2021, "e3"), (2022, 2026, "e4")):
        s = d[(d.y >= lo) & (d.y <= hi)]
        out[nm] = s.yen.sum(); out[nm + "_pf"] = pf(s.yen) if len(s) else np.nan
    b1 = d[d.rk == 1]
    out["total"] = d.yen.sum(); out["design"] = out["e1"] + out["e2"]; out["oos"] = out["e3"] + out["e4"]
    out["win_years"] = int((yy > 0).sum()); out["worst_year"] = yy.min(); out["worst_month"] = ym.min()
    out["ex3"] = d.yen.sum() - d.nlargest(3, "yen").yen.sum()
    out["pf1"] = pf(b1.yen); out["avg1"] = b1.pnl.mean()
    return out


ENTRY = list(itertools.product((7.0, 8.0, 10.0), (4.0, 5.0, 6.0), (8.0, 12.0, 16.0, 20.0), (4.0, 6.0, 10.0), (3.0, 5.0, 8.0), (3e8, 5e8, 1e9)))
rows = []; t0 = time.time(); k = 0
for (gain, atr, dev, vr, rng, tov) in ENTRY:
    G = D[(D.gain >= gain) & (D.atr >= atr) & (D.dev >= dev) & (D.vr < vr) & (D.rng > rng) & (D.tov >= tov)]
    if len(G) < 200:
        continue
    for rname, fn in RANKS.items():
        st = evaluate(G, fn); st.update(gain=gain, atr=atr, dev=dev, vr=vr, rng=rng, tov=tov, rank=rname, n_cand=len(G)); rows.append(st); k += 1
    if k % 250 < 5:
        print(f"  {k}/{len(ENTRY)*len(RANKS)} {time.time()-t0:.0f}s", flush=True)
        pd.DataFrame(rows).to_csv("_bt_fade_20y_grid.csv", index=False)
Gd = pd.DataFrame(rows); Gd.to_csv("_bt_fade_20y_grid.csv", index=False)

lines = []
def p(s=""):
    print(s); lines.append(s)
cols = ["gain", "atr", "dev", "vr", "rng", "tov", "rank", "n1", "n2", "e1", "e2", "e3", "e4", "e1_pf", "e2_pf", "e3_pf", "e4_pf", "design", "oos", "total", "win_years", "worst_year", "worst_month", "ex3", "pf1", "avg1"]
fmt = {c: "{:,.0f}".format for c in ("e1", "e2", "e3", "e4", "design", "oos", "total", "worst_year", "worst_month", "ex3", "tov")}
fmt.update({c: "{:.2f}".format for c in ("e1_pf", "e2_pf", "e3_pf", "e4_pf", "pf1", "avg1")})
p("=" * 200); p(f"フェード26年グリッド {len(Gd)}評価（1番×100万＋2番×50万）  e1=2001-08 e2=2009-16 e3=2017-21 e4=2022-26 design=e1+e2 oos=e3+e4"); p("=" * 200)
cur = Gd[(Gd.gain == 7) & (Gd.atr == 5) & (Gd.dev == 12) & (Gd.vr == 6) & (Gd.rng == 5) & (Gd.tov == 3e8) & (Gd["rank"] == "乖離×ATR(現行)")]
p("\n[現行ルールのセル]"); p(cur[cols].to_string(index=False, formatters=fmt))
p("\n[① 設計期間(2001-16)で最良の15 → その2017-26(OOS)]"); p(Gd.sort_values("design", ascending=False).head(15)[cols].to_string(index=False, formatters=fmt))
rob = Gd[(Gd.e1_pf > 1) & (Gd.e2_pf > 1) & (Gd.e3_pf > 1) & (Gd.e4_pf > 1)]
p(f"\n[② 4期間すべてPF>1: {len(rob)}/{len(Gd)}]  → その中で OOS(2017-26) 上位15")
p(rob.sort_values("oos", ascending=False).head(15)[cols].to_string(index=False, formatters=fmt))
p("\n[③ 26年合計 上位15]"); p(Gd.sort_values("total", ascending=False).head(15)[cols].to_string(index=False, formatters=fmt))
p("\n[④ 現行の各軸を1つずつ動かした近傍（並び順=現行）]")
base = dict(gain=7.0, atr=5.0, dev=12.0, vr=6.0, rng=5.0, tov=3e8)
for ax, vals in (("gain", (7.0, 8.0, 10.0)), ("atr", (4.0, 5.0, 6.0)), ("dev", (8.0, 12.0, 16.0, 20.0)), ("vr", (4.0, 6.0, 10.0)), ("rng", (3.0, 5.0, 8.0)), ("tov", (3e8, 5e8, 1e9))):
    sub = Gd[(Gd["rank"] == "乖離×ATR(現行)")]
    for k2, v2 in base.items():
        if k2 != ax:
            sub = sub[sub[k2] == v2]
    p(f"  -- {ax}"); p(sub.sort_values(ax)[cols].to_string(index=False, formatters=fmt))
p("\n[⑤ 並び順だけ変えた場合（入口=現行）]")
sub = Gd[(Gd.gain == 7) & (Gd.atr == 5) & (Gd.dev == 12) & (Gd.vr == 6) & (Gd.rng == 5) & (Gd.tov == 3e8)]
p(sub[cols].to_string(index=False, formatters=fmt))
p("\n[⑥ 時代別に PF>1 の評価数]")
for e in ("e1", "e2", "e3", "e4"):
    p(f"  {e}: {int((Gd[e+'_pf']>1).sum())}/{len(Gd)}  中央値 {Gd[e].median():,.0f}")
open("_log_fade_20y_grid.txt", "w", encoding="utf-8").write("\n".join(lines))
